"""Processing pipeline configuration: S3 data contracts and constants.

S3 Data Contract (expected from ingestion layer):
──────────────────────────────────────────────────
game_logs/{season}/game_logs.parquet
    game_id, game_date, season, home_team, away_team, home_score, away_score,
    venue_name, home_sp_id, away_sp_id, status

pitcher_game_logs/{season}/pitcher_game_logs.parquet
    pitcher_id, game_id, game_date, season, team, role (SP/RP),
    handedness (L/R), is_closer (bool), innings_pitched, strikeouts,
    walks, earned_runs, hits_allowed, home_runs_allowed, pitches,
    batters_faced

pitcher_stats/{season}/pitcher_stats.parquet
    pitcher_id, season, siera, fip, xfip, k_pct, bb_pct
    (FanGraphs season-to-date, updated periodically by ingestion)

team_batting/{season}/team_batting.parquet
    team, game_id, game_date, season, plate_appearances, at_bats,
    hits, doubles, triples, home_runs, strikeouts, walks

team_batting_splits/{season}/team_batting_splits.parquet
    team, season, vs_hand (L/R), wrc_plus, woba
    (FanGraphs season-to-date splits, updated periodically)

weather/{season}/weather.parquet
    game_id, temp_f, wind_mph, wind_dir

park_factors/park_factors.parquet
    venue_name, park_factor_runs, park_factor_hr

schedules/{season}/schedules.parquet
    team, game_date, venue_name, venue_tz
"""

import os

# S3 bucket for raw data (set by Lambda environment variable)
S3_BUCKET_DATA = os.environ.get("S3_BUCKET_DATA", "bases-loaded-data")

# S3 prefix for raw ingestion data
S3_RAW_PREFIX = "raw"

# S3 paths relative to data bucket (format with season)
S3_PATHS = {
    "game_logs": f"{S3_RAW_PREFIX}/game_logs/{{season}}/game_logs.parquet",
    "pitcher_game_logs": f"{S3_RAW_PREFIX}/pitcher_game_logs/{{season}}/pitcher_game_logs.parquet",
    "pitcher_stats": f"{S3_RAW_PREFIX}/pitcher_stats/{{season}}/pitcher_stats.parquet",
    "team_batting": f"{S3_RAW_PREFIX}/team_batting/{{season}}/team_batting.parquet",
    "team_batting_splits": f"{S3_RAW_PREFIX}/team_batting_splits/{{season}}/team_batting_splits.parquet",
    "weather": f"{S3_RAW_PREFIX}/weather/{{season}}/weather.parquet",
    "park_factors": f"{S3_RAW_PREFIX}/park_factors/park_factors.parquet",
    "schedules": f"{S3_RAW_PREFIX}/schedules/{{season}}/schedules.parquet",
}

# FIP constant (league-average, approximate)
FIP_CONSTANT = 3.15

# Rolling window sizes (calendar days, for Polars period strings)
ROLLING_30D = "30d"
ROLLING_14D = "14d"
ROLLING_3D = "3d"

# Default fill values for null rolling stats
DEFAULT_REST_DAYS = 5
DEFAULT_PITCH_COUNT = 90
