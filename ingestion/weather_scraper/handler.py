"""Weather scraper Lambda.

Fetches weather observations from Meteostat for MLB games. Always reads
game_logs from S3 to discover games and their venues.

Daily (empty event): reads current season's game_logs, filters to yesterday.
Backfill (event with 'season'): reads the full season's game_logs.
Domed venues always get zeroed-out weather.
Venue coordinates are fetched from the MLB Stats API (cached per invocation).
"""

import io
import json
import math
import os
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen

import boto3
import polars as pl
from meteostat import Daily, Point
from meteostat.interface.base import Base

# Lambda filesystem is read-only except /tmp — redirect Meteostat cache
# Must set on Base so all subclasses (Daily, Stations, etc.) inherit it
Base.cache_dir = "/tmp/.meteostat"

S3_BUCKET = os.environ["S3_BUCKET_DATA"]
S3_PREFIX = "raw"

s3 = boto3.client("s3")

# Venues with dome or retractable roof — weather is irrelevant, zero out.
# Keyed by venue_id (str). This list changes rarely (new stadiums ~once a decade).
DOMED_VENUE_IDS = {
    "15",    # Chase Field (retractable)
    "2392",  # Daikin Park / formerly Minute Maid Park (retractable)
    "4169",  # loanDepot park (retractable)
    "32",    # American Family Field (retractable)
    "14",    # Rogers Centre (retractable)
    "12",    # Tropicana Field (dome)
    "5325",  # Globe Life Field (retractable)
    "680",   # T-Mobile Park (retractable)
}

# In-memory cache for venue coordinates fetched from MLB Stats API
_venue_cache: dict[str, dict] = {}


def fetch_venue_coords(venue_id: str) -> dict | None:
    """Fetch venue lat/lon from the MLB Stats API. Cached per invocation."""
    if venue_id in _venue_cache:
        return _venue_cache[venue_id]

    try:
        url = f"https://statsapi.mlb.com/api/v1/venues/{venue_id}?hydrate=location"
        with urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        venue = data["venues"][0]
        coords = venue["location"]["defaultCoordinates"]
        result = {"lat": coords["latitude"], "lon": coords["longitude"]}
        _venue_cache[venue_id] = result
        return result
    except Exception as e:
        print(f"WARNING: Could not fetch coordinates for venue {venue_id}: {e}")
        _venue_cache[venue_id] = None
        return None


def read_existing(key: str) -> pl.DataFrame | None:
    """Read existing Parquet from S3, or return None."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pl.read_parquet(io.BytesIO(resp["Body"].read()))
    except Exception:
        return None


def write_parquet(df: pl.DataFrame, key: str) -> None:
    """Write Polars DataFrame to S3 as Parquet."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    print(f"Wrote {len(df)} rows to s3://{S3_BUCKET}/{key}")


def deg_to_compass(deg: float) -> str:
    """Convert wind degrees to compass direction."""
    directions = [
        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
    ]
    idx = round(deg / 22.5) % 16
    return directions[idx]


def fetch_weather(lat: float, lon: float, game_date: str) -> dict:
    """Fetch weather observation from Meteostat for a specific date."""
    point = Point(lat, lon)
    dt = datetime.strptime(game_date, "%Y-%m-%d")
    data = Daily(point, dt, dt).fetch()

    if data.empty:
        return {"temp_f": 0.0, "wind_mph": 0.0, "wind_dir": "None"}

    row = data.iloc[0]
    # Meteostat returns pandas — NA values raise on bool coercion (e.g. `or 0`).
    # Convert to Python dict so NaN becomes float('nan'), then use math.isnan.
    vals = row.to_dict()

    def safe(val, default=0):
        try:
            return default if val is None or math.isnan(val) else val
        except TypeError:
            return default

    temp_c = safe(vals.get("tavg"), safe(vals.get("tmax")))
    temp_f = round(temp_c * 9 / 5 + 32, 1)
    wind_kmh = safe(vals.get("wspd"))
    wind_mph = round(wind_kmh * 0.621371, 1)
    wind_deg = safe(vals.get("wdir"))

    return {
        "temp_f": temp_f,
        "wind_mph": wind_mph,
        "wind_dir": deg_to_compass(wind_deg),
    }


def handler(event, context):
    """Lambda handler.

    Daily (empty event): reads current season's game_logs from S3,
    filters to yesterday's games, fetches weather via Meteostat.
    Backfill (event with 'season'): reads the full season's game_logs
    from S3, fetches weather for all games via Meteostat.
    """
    season_param = event.get("season")

    if season_param is not None:
        season = int(season_param)
        game_date_filter = None
        print(f"Backfill: fetching weather for full season {season}")
    else:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        season = yesterday.year
        game_date_filter = yesterday.strftime("%Y-%m-%d")
        print(f"Daily: fetching weather for {game_date_filter}")

    # Read game_logs from S3
    game_logs_key = f"{S3_PREFIX}/game_logs/{season}/game_logs.parquet"
    game_logs = read_existing(game_logs_key)
    if game_logs is None or len(game_logs) == 0:
        print(f"No game_logs found for season {season}")
        return {"status": "no_game_logs", "season": season}

    # Filter to specific date for daily mode
    if game_date_filter is not None:
        game_logs = game_logs.filter(pl.col("game_date") == game_date_filter)
        if len(game_logs) == 0:
            print(f"No games found for {game_date_filter}")
            return {"status": "no_games", "date": game_date_filter}

    print(f"Processing weather for {len(game_logs)} games")

    rows = []
    for game in game_logs.iter_rows(named=True):
        game_id = game["game_id"]
        venue_name = game.get("venue_name", "")
        venue_id = str(game.get("venue_id", "") or "")
        game_date = game.get("game_date", "")

        # Domed/retractable venues — zero out weather
        if venue_id in DOMED_VENUE_IDS:
            rows.append({
                "game_id": game_id,
                "temp_f": 0.0,
                "wind_mph": 0.0,
                "wind_dir": "None",
            })
            continue

        # Fetch venue coordinates from MLB Stats API
        coords = fetch_venue_coords(venue_id) if venue_id else None
        if coords is None:
            print(f"WARNING: No coordinates for venue '{venue_name}' (id={venue_id}), game {game_id}, skipping")
            continue

        try:
            weather = fetch_weather(coords["lat"], coords["lon"], game_date)
            rows.append({
                "game_id": game_id,
                "temp_f": weather["temp_f"],
                "wind_mph": weather["wind_mph"],
                "wind_dir": weather["wind_dir"],
            })
        except Exception as e:
            print(f"WARNING: Weather fetch failed for {venue_name} game {game_id} ({game_date}): {e}")
            continue

    if not rows:
        print("No weather data collected")
        return {"status": "no_data", "season": season}

    weather_df = pl.DataFrame(rows)
    weather_key = f"{S3_PREFIX}/weather/{season}/weather.parquet"
    existing = read_existing(weather_key)
    if existing is not None and len(existing) > 0:
        combined = pl.concat([existing, weather_df], how="diagonal_relaxed")
        weather_df = combined.unique(subset=["game_id"], keep="last")
    write_parquet(weather_df, weather_key)

    return {
        "status": "success",
        "season": season,
        "games_with_weather": len(rows),
    }
