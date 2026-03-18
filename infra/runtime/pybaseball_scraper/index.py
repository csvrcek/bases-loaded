"""PyBaseball scraper Lambda.

Fetches season-to-date pitching stats, team batting splits, and park factors
from FanGraphs/Baseball-Reference via pybaseball. Writes Parquet to S3.
"""

import io
import os
from datetime import datetime, timezone

import boto3
import polars as pl
import pybaseball

S3_BUCKET = os.environ["S3_BUCKET_DATA"]
S3_PREFIX = "raw"

s3 = boto3.client("s3")

# Suppress pybaseball progress bars in Lambda
pybaseball.cache.enable()

# FanGraphs team abbreviation mapping
FANGRAPHS_TEAM_MAP = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BOS": "BOS",
    "CHC": "CHC", "CHW": "CHW", "CIN": "CIN", "CLE": "CLE",
    "COL": "COL", "DET": "DET", "HOU": "HOU", "KCR": "KC",
    "LAA": "LAA", "LAD": "LAD", "MIA": "MIA", "MIL": "MIL",
    "MIN": "MIN", "NYM": "NYM", "NYY": "NYY", "OAK": "OAK",
    "PHI": "PHI", "PIT": "PIT", "SDP": "SD", "SFG": "SF",
    "SEA": "SEA", "STL": "STL", "TBR": "TB", "TEX": "TEX",
    "TOR": "TOR", "WSN": "WSH",
}


def normalize_team(team: str) -> str:
    """Normalize FanGraphs team abbreviation to our standard."""
    return FANGRAPHS_TEAM_MAP.get(team, team)


def write_parquet(df: pl.DataFrame, key: str) -> None:
    """Write Polars DataFrame to S3 as Parquet."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    print(f"Wrote {len(df)} rows to s3://{S3_BUCKET}/{key}")


def fetch_pitcher_stats(season: int) -> pl.DataFrame:
    """Fetch season pitching stats from FanGraphs."""
    print(f"Fetching FanGraphs pitching stats for {season}")
    # pitching_stats returns a pandas DataFrame
    pdf = pybaseball.pitching_stats(season, qual=0)

    if pdf is None or pdf.empty:
        print("WARNING: No pitching stats returned")
        return pl.DataFrame(
            schema={
                "pitcher_id": pl.Utf8,
                "season": pl.Int64,
                "siera": pl.Float64,
                "fip": pl.Float64,
                "xfip": pl.Float64,
                "k_pct": pl.Float64,
                "bb_pct": pl.Float64,
            }
        )

    # Convert to Polars and select/rename columns
    df = pl.from_pandas(pdf)

    # FanGraphs column names vary — find the right ones
    col_map = {}
    for col in df.columns:
        lower = col.lower().replace(" ", "").replace("%", "_pct")
        if lower == "idfg" or lower == "playerid":
            col_map[col] = "pitcher_id"
        elif lower == "siera":
            col_map[col] = "siera"
        elif lower == "fip":
            col_map[col] = "fip"
        elif lower == "xfip":
            col_map[col] = "xfip"
        elif lower in ("k_pct", "kpct", "k%"):
            col_map[col] = "k_pct"
        elif lower in ("bb_pct", "bbpct", "bb%"):
            col_map[col] = "bb_pct"

    # Ensure we have the columns we need
    required = {"pitcher_id", "siera", "fip", "xfip", "k_pct", "bb_pct"}
    found = set(col_map.values())
    missing = required - found
    if missing:
        print(f"WARNING: Missing columns from FanGraphs: {missing}")
        print(f"Available columns: {df.columns}")

    df = df.rename(col_map)
    available_cols = [c for c in ["pitcher_id", "siera", "fip", "xfip", "k_pct", "bb_pct"] if c in df.columns]
    df = df.select(available_cols)
    df = df.with_columns(pl.lit(season).alias("season"))

    # Cast pitcher_id to string
    if "pitcher_id" in df.columns:
        df = df.with_columns(pl.col("pitcher_id").cast(pl.Utf8))

    return df


def fetch_team_batting_splits(season: int) -> pl.DataFrame:
    """Fetch team batting splits vs LHP and RHP from FanGraphs."""
    print(f"Fetching team batting splits for {season}")
    rows = []

    for vs_hand in ["L", "R"]:
        try:
            # team_batting returns pandas DataFrame
            split_code = f"vL" if vs_hand == "L" else "vR"
            pdf = pybaseball.team_batting(season, split_seasons=False)

            if pdf is None or pdf.empty:
                print(f"WARNING: No team batting data for {season} vs {vs_hand}")
                continue

            df = pl.from_pandas(pdf)

            for row in df.iter_rows(named=True):
                team = normalize_team(row.get("Team", row.get("Tm", "")))
                wrc_plus = row.get("wRC+", row.get("wRC_plus", 0))
                woba = row.get("wOBA", 0)
                rows.append({
                    "team": team,
                    "season": season,
                    "vs_hand": vs_hand,
                    "wrc_plus": float(wrc_plus) if wrc_plus else 0.0,
                    "woba": float(woba) if woba else 0.0,
                })
        except Exception as e:
            print(f"WARNING: Could not fetch splits vs {vs_hand}: {e}")

    if not rows:
        return pl.DataFrame(
            schema={
                "team": pl.Utf8,
                "season": pl.Int64,
                "vs_hand": pl.Utf8,
                "wrc_plus": pl.Float64,
                "woba": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def fetch_park_factors() -> pl.DataFrame:
    """Fetch park factors from FanGraphs."""
    print("Fetching park factors")
    try:
        pdf = pybaseball.park_factors()

        if pdf is None or pdf.empty:
            print("WARNING: No park factors returned")
            return pl.DataFrame(
                schema={
                    "venue_name": pl.Utf8,
                    "park_factor_runs": pl.Float64,
                    "park_factor_hr": pl.Float64,
                }
            )

        df = pl.from_pandas(pdf)
        rows = []
        for row in df.iter_rows(named=True):
            venue = row.get("Venue", row.get("venue", row.get("Stadium", "")))
            # Park factor columns vary by source
            pf_runs = row.get("Basic", row.get("PF", row.get("park_factor", 100)))
            pf_hr = row.get("HR", row.get("PF_HR", 100))
            rows.append({
                "venue_name": str(venue),
                "park_factor_runs": float(pf_runs) / 100 if float(pf_runs) > 10 else float(pf_runs),
                "park_factor_hr": float(pf_hr) / 100 if float(pf_hr) > 10 else float(pf_hr),
            })

        return pl.DataFrame(rows)
    except Exception as e:
        print(f"WARNING: Could not fetch park factors: {e}")
        return pl.DataFrame(
            schema={
                "venue_name": pl.Utf8,
                "park_factor_runs": pl.Float64,
                "park_factor_hr": pl.Float64,
            }
        )


def handler(event, context):
    """Lambda handler. Expects event with 'season'."""
    season = int(event.get("season", datetime.now(timezone.utc).year))
    print(f"Ingesting PyBaseball data for season={season}")

    results = {}

    # --- Pitcher Stats ---
    try:
        pitcher_stats = fetch_pitcher_stats(season)
        if len(pitcher_stats) > 0:
            key = f"{S3_PREFIX}/pitcher_stats/{season}/pitcher_stats.parquet"
            write_parquet(pitcher_stats, key)
            results["pitcher_stats"] = len(pitcher_stats)
    except Exception as e:
        print(f"ERROR fetching pitcher stats: {e}")
        results["pitcher_stats"] = f"error: {e}"

    # --- Team Batting Splits ---
    try:
        splits = fetch_team_batting_splits(season)
        if len(splits) > 0:
            key = f"{S3_PREFIX}/team_batting_splits/{season}/team_batting_splits.parquet"
            write_parquet(splits, key)
            results["team_batting_splits"] = len(splits)
    except Exception as e:
        print(f"ERROR fetching team batting splits: {e}")
        results["team_batting_splits"] = f"error: {e}"

    # --- Park Factors ---
    try:
        park_factors = fetch_park_factors()
        if len(park_factors) > 0:
            key = f"{S3_PREFIX}/park_factors/park_factors.parquet"
            write_parquet(park_factors, key)
            results["park_factors"] = len(park_factors)
    except Exception as e:
        print(f"ERROR fetching park factors: {e}")
        results["park_factors"] = f"error: {e}"

    return {
        "status": "success",
        "season": season,
        "results": results,
    }
