"""Bullpen feature computation.

Computes per-game bullpen features for both home and away teams:
- bullpen_pitch_count_3d: Total reliever pitches over last 3 calendar days
- bullpen_xfip_14d: Rolling 14-day bullpen FIP (proxy for xFIP)
- closer_unavailable: 1 if primary closer pitched 2 consecutive days prior
"""

import polars as pl

from processing.config import FIP_CONSTANT, ROLLING_3D, ROLLING_14D

BULLPEN_FEATURE_COLS = [
    "bullpen_pitch_count_3d",
    "bullpen_xfip_14d",
    "closer_unavailable",
]


def compute_bullpen_features(
    game_logs: pl.DataFrame,
    pitcher_logs: pl.DataFrame,
) -> pl.DataFrame:
    """Compute bullpen features for all games.

    Returns DataFrame with columns: game_id + home_bullpen_* + away_bullpen_*
    """
    games = game_logs.with_columns(
        pl.col("game_date").str.to_date("%Y-%m-%d").alias("date")
    )

    rp_logs = (
        pitcher_logs.filter(pl.col("role").is_in(["RP", "CL"]))
        .with_columns(pl.col("game_date").str.to_date("%Y-%m-%d").alias("date"))
        .sort("date")
    )

    bp_pitches = _rolling_bullpen_pitches(rp_logs)
    bp_fip = _rolling_bullpen_fip(rp_logs)
    closer_avail = _closer_unavailability(pitcher_logs, games)

    # Create team-date anchors from game_logs
    home_dates = games.select(
        pl.col("home_team").alias("team"), "date", "game_id",
        pl.lit("home").alias("side"),
    )
    away_dates = games.select(
        pl.col("away_team").alias("team"), "date", "game_id",
        pl.lit("away").alias("side"),
    )
    team_dates = pl.concat([home_dates, away_dates])

    # Join all bullpen features
    team_bp = (
        team_dates
        .join(bp_pitches, on=["team", "date"], how="left")
        .join(bp_fip, on=["team", "date"], how="left")
        .join(closer_avail, on=["team", "date"], how="left")
        .with_columns(
            pl.col("bullpen_pitch_count_3d").fill_null(0),
            pl.col("closer_unavailable").fill_null(0),
        )
    )

    # Split into home and away
    home_bp = team_bp.filter(pl.col("side") == "home").select(
        "game_id",
        *[pl.col(c).alias(f"home_{c}") for c in BULLPEN_FEATURE_COLS],
    )
    away_bp = team_bp.filter(pl.col("side") == "away").select(
        "game_id",
        *[pl.col(c).alias(f"away_{c}") for c in BULLPEN_FEATURE_COLS],
    )

    return home_bp.join(away_bp, on="game_id")


def _rolling_bullpen_pitches(rp_logs: pl.DataFrame) -> pl.DataFrame:
    """Rolling 3-day total pitches by all relievers per team."""
    if len(rp_logs) == 0:
        return pl.DataFrame(
            schema={"team": pl.Utf8, "date": pl.Date, "bullpen_pitch_count_3d": pl.Int64}
        )

    daily_pitches = (
        rp_logs
        .group_by("team", "date")
        .agg(pl.col("pitches").sum().alias("daily_pitches"))
        .sort("date")
    )

    return (
        daily_pitches.rolling(
            index_column="date", period=ROLLING_3D, by="team", closed="left"
        )
        .agg(pl.col("daily_pitches").sum().alias("bullpen_pitch_count_3d"))
    )


def _rolling_bullpen_fip(rp_logs: pl.DataFrame) -> pl.DataFrame:
    """Rolling 14-day bullpen FIP (used as proxy for xFIP)."""
    if len(rp_logs) == 0:
        return pl.DataFrame(
            schema={"team": pl.Utf8, "date": pl.Date, "bullpen_xfip_14d": pl.Float64}
        )

    # Aggregate all RP appearances per team-date first
    daily_rp = (
        rp_logs
        .group_by("team", "date")
        .agg(
            pl.col("home_runs_allowed").sum().alias("hr"),
            pl.col("walks").sum().alias("bb"),
            pl.col("strikeouts").sum().alias("k"),
            pl.col("innings_pitched").sum().alias("ip"),
        )
        .sort("date")
    )

    return (
        daily_rp.rolling(
            index_column="date", period=ROLLING_14D, by="team", closed="left"
        )
        .agg(
            pl.col("hr").sum().alias("hr_sum"),
            pl.col("bb").sum().alias("bb_sum"),
            pl.col("k").sum().alias("k_sum"),
            pl.col("ip").sum().alias("ip_sum"),
        )
        .with_columns(
            pl.when(pl.col("ip_sum") > 0)
            .then(
                (13 * pl.col("hr_sum") + 3 * pl.col("bb_sum") - 2 * pl.col("k_sum"))
                / pl.col("ip_sum")
                + FIP_CONSTANT
            )
            .otherwise(None)
            .alias("bullpen_xfip_14d")
        )
        .select("team", "date", "bullpen_xfip_14d")
    )


def _closer_unavailability(
    pitcher_logs: pl.DataFrame,
    games: pl.DataFrame,
) -> pl.DataFrame:
    """Determine if team's closer is unavailable (pitched 2 consecutive prior days)."""
    closer_logs = (
        pitcher_logs.filter(
            (pl.col("role").is_in(["RP", "CL"])) & (pl.col("is_closer") == True)
        )
        .with_columns(pl.col("game_date").str.to_date("%Y-%m-%d").alias("date"))
        .select("team", "date")
        .unique()
    )

    if len(closer_logs) == 0:
        return pl.DataFrame(
            schema={"team": pl.Utf8, "date": pl.Date, "closer_unavailable": pl.Int32}
        )

    # Build all team-date pairs from games
    team_dates = pl.concat([
        games.select(pl.col("home_team").alias("team"), "date"),
        games.select(pl.col("away_team").alias("team"), "date"),
    ]).unique()

    # Check if closer pitched yesterday AND day before yesterday
    team_dates_check = team_dates.with_columns(
        (pl.col("date") - pl.duration(days=1)).alias("yesterday"),
        (pl.col("date") - pl.duration(days=2)).alias("day_before"),
    )

    # Left join for yesterday
    result = team_dates_check.join(
        closer_logs.with_columns(pl.lit(True).alias("pitched_d1")),
        left_on=["team", "yesterday"],
        right_on=["team", "date"],
        how="left",
    )

    # Left join for day before
    result = result.join(
        closer_logs.with_columns(pl.lit(True).alias("pitched_d2")),
        left_on=["team", "day_before"],
        right_on=["team", "date"],
        how="left",
    )

    return result.with_columns(
        (
            pl.col("pitched_d1").fill_null(False)
            & pl.col("pitched_d2").fill_null(False)
        )
        .cast(pl.Int32)
        .alias("closer_unavailable")
    ).select("team", pl.col("date").alias("date"), "closer_unavailable")
