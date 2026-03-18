"""Weather scraper Lambda.

Fetches weather forecasts for today's MLB games using the OpenWeather API.
Domed venues get zeroed-out weather. Appends to season Parquet in S3.
"""

import io
import json
import os
from datetime import datetime, timezone

import boto3
import polars as pl
import requests
import statsapi

from venues import VENUES

S3_BUCKET = os.environ["S3_BUCKET_DATA"]
S3_PREFIX = "raw"
SSM_PARAM_NAME = "/bases-loaded/openweather-api-key"

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


def get_api_key() -> str:
    """Fetch OpenWeather API key from SSM Parameter Store."""
    resp = ssm.get_parameter(Name=SSM_PARAM_NAME, WithDecryption=True)
    return resp["Parameter"]["Value"]


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


def fetch_weather(lat: float, lon: float, api_key: str) -> dict:
    """Fetch current weather from OpenWeather One Call API 3.0."""
    url = "https://api.openweathermap.org/data/3.0/onecall"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "imperial",
        "exclude": "minutely,hourly,daily,alerts",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    current = data.get("current", {})
    wind_deg = current.get("wind_deg", 0)
    return {
        "temp_f": round(current.get("temp", 0), 1),
        "wind_mph": round(current.get("wind_speed", 0), 1),
        "wind_dir": deg_to_compass(wind_deg),
    }


def deg_to_compass(deg: float) -> str:
    """Convert wind degrees to compass direction."""
    directions = [
        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
    ]
    idx = round(deg / 22.5) % 16
    return directions[idx]


def handler(event, context):
    """Lambda handler. Expects event with 'date' and/or 'season'."""
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    date_str = event.get("date", today)
    season = int(event.get("season", datetime.now(timezone.utc).year))

    print(f"Fetching weather for date={date_str}, season={season}")

    # Fetch today's schedule
    games = statsapi.schedule(start_date=date_str, end_date=date_str)
    print(f"Found {len(games)} games for {date_str}")

    if not games:
        print("No games today, exiting")
        return {"status": "no_games", "date": date_str}

    api_key = get_api_key()

    rows = []
    for g in games:
        game_id = str(g["game_id"])
        venue_name = g.get("venue_name", "")
        venue = VENUES.get(venue_name)

        if venue is None:
            print(f"WARNING: Unknown venue '{venue_name}' for game {game_id}, skipping")
            continue

        if venue["roof"] == "dome":
            # Domed stadiums: no weather impact
            rows.append({
                "game_id": game_id,
                "temp_f": 0.0,
                "wind_mph": 0.0,
                "wind_dir": "None",
            })
        else:
            try:
                weather = fetch_weather(venue["lat"], venue["lon"], api_key)
                rows.append({
                    "game_id": game_id,
                    "temp_f": weather["temp_f"],
                    "wind_mph": weather["wind_mph"],
                    "wind_dir": weather["wind_dir"],
                })
            except Exception as e:
                print(f"WARNING: Weather fetch failed for {venue_name} (game {game_id}): {e}")
                continue

    if not rows:
        print("No weather data collected")
        return {"status": "no_data", "date": date_str}

    weather_df = pl.DataFrame(rows)
    weather_key = f"{S3_PREFIX}/weather/{season}/weather.parquet"
    existing = read_existing(weather_key)
    if existing is not None and len(existing) > 0:
        combined = pl.concat([existing, weather_df], how="diagonal_relaxed")
        weather_df = combined.unique(subset=["game_id"], keep="last")
    write_parquet(weather_df, weather_key)

    return {
        "status": "success",
        "date": date_str,
        "season": season,
        "games_with_weather": len(rows),
    }
