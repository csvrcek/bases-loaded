"""MLB Stats API scraper Lambda.

Fetches game logs, pitcher game logs, team batting, and schedules
using the MLB-StatsAPI library. Writes Parquet files to S3.
"""

import io
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import polars as pl
import statsapi

S3_BUCKET = os.environ["S3_BUCKET_DATA"]
S3_PREFIX = "raw"

s3 = boto3.client("s3")

# MLB team abbreviation mapping (statsapi uses full names)
TEAM_ABBREV = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CHW",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}


def get_team_abbrev(full_name: str) -> str:
    """Map full team name to abbreviation."""
    return TEAM_ABBREV.get(full_name, full_name)


def read_existing(key: str) -> pl.DataFrame | None:
    """Read existing Parquet from S3, or return None."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pl.read_parquet(io.BytesIO(resp["Body"].read()))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception:
        return None


def write_parquet(df: pl.DataFrame, key: str) -> None:
    """Write Polars DataFrame to S3 as Parquet."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    print(f"Wrote {len(df)} rows to s3://{S3_BUCKET}/{key}")


def merge_and_deduplicate(
    existing: pl.DataFrame | None,
    new: pl.DataFrame,
    dedup_col: str,
) -> pl.DataFrame:
    """Append new rows to existing, deduplicate by column (keep latest)."""
    if existing is not None and len(existing) > 0:
        combined = pl.concat([existing, new], how="diagonal_relaxed")
    else:
        combined = new
    return combined.unique(subset=[dedup_col], keep="last")


REGULAR_GAME_TYPES = {"R", "F", "D", "L", "W"}  # Regular season + postseason


def _safe_int(val) -> int:
    """Convert a value to int, handling string numbers from boxscore data."""
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


UPCOMING_STATUSES = {"Scheduled", "Pre-Game", "Preview", "Warmup"}


def fetch_probable_pitcher_ids(start_date: str, end_date: str) -> dict[str, dict]:
    """Fetch probable pitcher numeric IDs from the hydrated schedule API.

    Returns dict mapping game_id -> {"home_sp_id": str, "away_sp_id": str}.
    """
    pitcher_ids = {}
    try:
        data = statsapi.get(
            "schedule",
            {
                "startDate": start_date,
                "endDate": end_date,
                "sportId": 1,
                "hydrate": "probablePitcher",
            },
        )
        for date_entry in data.get("dates", []):
            for game in date_entry.get("games", []):
                gid = str(game.get("gamePk", ""))
                teams = game.get("teams", {})
                home_pp = teams.get("home", {}).get("probablePitcher", {})
                away_pp = teams.get("away", {}).get("probablePitcher", {})
                pitcher_ids[gid] = {
                    "home_sp_id": str(home_pp.get("id", "")) if home_pp else "",
                    "away_sp_id": str(away_pp.get("id", "")) if away_pp else "",
                }
    except Exception as e:
        print(f"WARNING: Could not fetch probable pitcher IDs: {e}")
    return pitcher_ids


def build_game_logs(
    games: list[dict], season: int, pitcher_ids: dict[str, dict] | None = None
) -> pl.DataFrame:
    """Build game_logs DataFrame from statsapi schedule data.

    Includes completed (Final) games with scores, and upcoming (Scheduled/
    Pre-Game) games with null scores — needed by processing to compute
    inference features before game time.
    """
    rows = []
    for g in games:
        if g.get("game_type", "") not in REGULAR_GAME_TYPES:
            continue
        status = g.get("status", "")
        is_final = status == "Final"
        is_upcoming = status in UPCOMING_STATUSES
        if not is_final and not is_upcoming:
            continue
        rows.append(
            {
                "game_id": str(g["game_id"]),
                "game_date": g["game_date"],
                "season": season,
                "home_team": get_team_abbrev(g.get("home_name", "")),
                "away_team": get_team_abbrev(g.get("away_name", "")),
                "home_score": g.get("home_score", 0) if is_final else None,
                "away_score": g.get("away_score", 0) if is_final else None,
                "venue_name": g.get("venue_name", ""),
                "venue_id": str(g.get("venue_id", "")),
                "home_sp_id": (pitcher_ids or {}).get(str(g["game_id"]), {}).get("home_sp_id", ""),
                "away_sp_id": (pitcher_ids or {}).get(str(g["game_id"]), {}).get("away_sp_id", ""),
                "status": status,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "game_id": pl.Utf8,
                "game_date": pl.Utf8,
                "season": pl.Int64,
                "home_team": pl.Utf8,
                "away_team": pl.Utf8,
                "home_score": pl.Int64,
                "away_score": pl.Int64,
                "venue_name": pl.Utf8,
                "venue_id": pl.Utf8,
                "home_sp_id": pl.Utf8,
                "away_sp_id": pl.Utf8,
                "status": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)


def build_schedules(games: list[dict], season: int) -> pl.DataFrame:
    """Build schedules DataFrame — one row per team per game date."""
    rows = []
    for g in (g for g in games if g.get("game_type", "") in REGULAR_GAME_TYPES):
        venue = g.get("venue_name", "")
        game_date = g["game_date"]
        home_team = get_team_abbrev(g.get("home_name", ""))
        away_team = get_team_abbrev(g.get("away_name", ""))
        # Both home and away teams get a row
        for team in [home_team, away_team]:
            rows.append(
                {
                    "team": team,
                    "game_date": game_date,
                    "venue_name": venue,
                    "venue_tz": "",  # filled by processing layer if needed
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "team": pl.Utf8,
                "game_date": pl.Utf8,
                "venue_name": pl.Utf8,
                "venue_tz": pl.Utf8,
            }
        )
    return pl.DataFrame(rows).unique(subset=["team", "game_date"], keep="last")


def fetch_pitcher_game_logs(game_id: str, game_date: str, season: int) -> list[dict]:
    """Fetch pitcher boxscore data for a single game.

    statsapi.boxscore_data() returns flat dicts per pitcher. The first entry
    in {side}Pitchers is a header row (personId=0) — skip it. The second
    entry is the starting pitcher.
    """
    rows = []
    try:
        box = statsapi.boxscore_data(game_id)
    except Exception as e:
        print(f"WARNING: Could not fetch boxscore for game {game_id}: {e}")
        return rows

    team_info = box.get("teamInfo", {})

    for side in ["home", "away"]:
        team_abbrev = team_info.get(side, {}).get("abbreviation", "")
        pitchers = box.get(f"{side}Pitchers", [])
        # Skip header row (personId == 0) at index 0
        actual_pitchers = [p for p in pitchers if p.get("personId", 0) != 0]
        for i, p in enumerate(actual_pitchers):
            pid = p.get("personId", 0)
            # Stats are flat keys: ip, h, r, er, bb, k, hr, p (pitches), s (strikes)
            ip_str = p.get("ip", "0")
            try:
                ip = float(ip_str)
            except (ValueError, TypeError):
                ip = 0.0
            hits = _safe_int(p.get("h", 0))
            walks = _safe_int(p.get("bb", 0))
            strikeouts = _safe_int(p.get("k", 0))
            rows.append(
                {
                    "pitcher_id": str(pid),
                    "game_id": str(game_id),
                    "game_date": game_date,
                    "season": season,
                    "team": team_abbrev,
                    "role": "SP" if i == 0 else "RP",
                    "handedness": "",  # filled from roster data if available
                    "is_closer": False,  # heuristic: set later
                    "innings_pitched": ip,
                    "strikeouts": strikeouts,
                    "walks": walks,
                    "earned_runs": _safe_int(p.get("er", 0)),
                    "hits_allowed": hits,
                    "home_runs_allowed": _safe_int(p.get("hr", 0)),
                    "pitches": _safe_int(p.get("p", 0)),
                    "batters_faced": hits + walks + strikeouts,
                }
            )
    return rows


def fetch_team_batting(game_id: str, game_date: str, season: int) -> list[dict]:
    """Fetch team batting totals from boxscore for a single game.

    Batting totals use flat keys (ab, h, r, hr, bb, k). Doubles and triples
    are not in the totals row — sum them from individual batter rows instead.
    """
    rows = []
    try:
        box = statsapi.boxscore_data(game_id)
    except Exception as e:
        print(f"WARNING: Could not fetch boxscore for game {game_id}: {e}")
        return rows

    team_info = box.get("teamInfo", {})

    for side in ["home", "away"]:
        team_abbrev = team_info.get(side, {}).get("abbreviation", "")
        totals = box.get(f"{side}BattingTotals", {})
        # Sum doubles/triples from individual batters (skip header row)
        batters = [b for b in box.get(f"{side}Batters", []) if b.get("personId", 0) != 0]
        doubles = sum(_safe_int(b.get("doubles", 0)) for b in batters)
        triples = sum(_safe_int(b.get("triples", 0)) for b in batters)
        ab = _safe_int(totals.get("ab", 0))
        bb = _safe_int(totals.get("bb", 0))
        rows.append(
            {
                "team": team_abbrev,
                "game_id": str(game_id),
                "game_date": game_date,
                "season": season,
                "plate_appearances": ab + bb,
                "at_bats": ab,
                "hits": _safe_int(totals.get("h", 0)),
                "doubles": doubles,
                "triples": triples,
                "home_runs": _safe_int(totals.get("hr", 0)),
                "strikeouts": _safe_int(totals.get("k", 0)),
                "walks": bb,
            }
        )
    return rows


def handler(event, context):
    """Lambda handler.

    Accepts optional 'start_date' and 'end_date' (MM/DD/YYYY format).
    If omitted, defaults to yesterday (daily BAU mode).
    Both dates must be within the same year. Season is derived from the year.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%m/%d/%Y")
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    start_date = event.get("start_date", yesterday)
    end_date = event.get("end_date", today)

    start_year = int(start_date.split("/")[2])
    end_year = int(end_date.split("/")[2])
    if start_year != end_year:
        raise ValueError(f"start_date and end_date must be in the same year, got {start_year} and {end_year}")

    season = start_year

    print(f"Ingesting MLB Stats data for start_date={start_date}, end_date={end_date}, season={season}")

    games = statsapi.schedule(start_date=start_date, end_date=end_date)
    print(f"Found {len(games)} games for {start_date} to {end_date}")

    if not games:
        print("No games found, exiting")
        return {"status": "no_games", "start_date": start_date, "end_date": end_date}

    # --- Fetch probable pitcher IDs (numeric) from hydrated API ---
    pitcher_ids = fetch_probable_pitcher_ids(start_date, end_date)
    print(f"Fetched probable pitcher IDs for {len(pitcher_ids)} games")

    # --- Game Logs ---
    game_logs_key = f"{S3_PREFIX}/game_logs/{season}/game_logs.parquet"
    game_logs_df = build_game_logs(games, season, pitcher_ids)
    if len(game_logs_df) > 0:
        existing = read_existing(game_logs_key)
        merged = merge_and_deduplicate(existing, game_logs_df, "game_id")
        write_parquet(merged, game_logs_key)

    # --- Schedules ---
    schedules_key = f"{S3_PREFIX}/schedules/{season}/schedules.parquet"
    schedules_df = build_schedules(games, season)
    if len(schedules_df) > 0:
        existing = read_existing(schedules_key)
        if existing is not None and len(existing) > 0:
            combined = pl.concat([existing, schedules_df], how="diagonal_relaxed")
            schedules_df = combined.unique(
                subset=["team", "game_date"], keep="last"
            )
        write_parquet(schedules_df, schedules_key)

    # --- Pitcher Game Logs & Team Batting (from boxscores) ---
    completed_games = [
        g for g in games
        if g.get("status") == "Final" and g.get("game_type", "") in REGULAR_GAME_TYPES
    ]
    all_pitcher_rows = []
    all_batting_rows = []

    for g in completed_games:
        gid = g["game_id"]
        gdate = g["game_date"]
        all_pitcher_rows.extend(fetch_pitcher_game_logs(gid, gdate, season))
        all_batting_rows.extend(fetch_team_batting(gid, gdate, season))

    if all_pitcher_rows:
        pitcher_key = f"{S3_PREFIX}/pitcher_game_logs/{season}/pitcher_game_logs.parquet"
        pitcher_df = pl.DataFrame(all_pitcher_rows)
        existing = read_existing(pitcher_key)
        merged = merge_and_deduplicate(existing, pitcher_df, "game_id")
        # pitcher_game_logs can have multiple rows per game_id (one per pitcher)
        # deduplicate by pitcher_id + game_id instead
        if existing is not None and len(existing) > 0:
            combined = pl.concat([existing, pitcher_df], how="diagonal_relaxed")
            pitcher_df = combined.unique(
                subset=["pitcher_id", "game_id"], keep="last"
            )
        write_parquet(pitcher_df, pitcher_key)

    if all_batting_rows:
        batting_key = f"{S3_PREFIX}/team_batting/{season}/team_batting.parquet"
        batting_df = pl.DataFrame(all_batting_rows)
        existing = read_existing(batting_key)
        if existing is not None and len(existing) > 0:
            combined = pl.concat([existing, batting_df], how="diagonal_relaxed")
            batting_df = combined.unique(
                subset=["team", "game_id"], keep="last"
            )
        write_parquet(batting_df, batting_key)

    return {
        "status": "success",
        "start_date": start_date,
        "end_date": end_date,
        "season": season,
        "games_found": len(games),
        "games_completed": len(completed_games),
    }
