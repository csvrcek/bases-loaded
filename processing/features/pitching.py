"""Starting pitcher feature computation.

Computes per-game SP features for both home and away teams:
- sp_sierra_std: Season-to-date SIERA (from FanGraphs)
- sp_fip_30d: Rolling 30-day FIP (from game logs)
- sp_k_minus_bb_14d: Rolling 14-day K% minus BB% (from game logs)
- sp_handedness: L or R
- sp_rest_days: Days since last start
- sp_last_pitch_count: Pitches thrown in last outing
"""

import polars as pl

from processing.config import (
    DEFAULT_PITCH_COUNT,
    DEFAULT_REST_DAYS,
    FIP_CONSTANT,
    ROLLING_14D,
    ROLLING_30D,
)

SP_FEATURE_COLS = [
    "sp_sierra_std",
    "sp_fip_30d",
    "sp_k_minus_bb_14d",
    "sp_handedness",
    "sp_rest_days",
    "sp_last_pitch_count",
]


def compute_sp_features(
    game_logs: pl.DataFrame,
    pitcher_logs: pl.DataFrame,
    pitcher_stats: pl.DataFrame,
) -> pl.DataFrame:
    """Compute starting pitcher features for all games.

    Returns DataFrame with columns: game_id + home_sp_* + away_sp_*
    """
    sp_logs = (
        pitcher_logs.filter(pl.col("role") == "SP")
        .with_columns(pl.col("game_date").str.to_date("%Y-%m-%d").alias("date"))
        .sort("date")
    )

    # --- Rolling 30d FIP: (13*HR + 3*BB - 2*K) / IP + constant ---
    fip_rolling = (
        sp_logs.rolling(
            index_column="date", period=ROLLING_30D, by="pitcher_id", closed="left"
        )
        .agg(
            pl.col("home_runs_allowed").sum().alias("hr_sum"),
            pl.col("walks").sum().alias("bb_sum"),
            pl.col("strikeouts").sum().alias("k_sum"),
            pl.col("innings_pitched").sum().alias("ip_sum"),
        )
        .with_columns(
            pl.when(pl.col("ip_sum") > 0)
            .then(
                (13 * pl.col("hr_sum") + 3 * pl.col("bb_sum") - 2 * pl.col("k_sum"))
                / pl.col("ip_sum")
                + FIP_CONSTANT
            )
            .otherwise(None)
            .alias("sp_fip_30d")
        )
        .select("pitcher_id", "date", "sp_fip_30d")
    )

    # --- Rolling 14d K% - BB% ---
    kbb_rolling = (
        sp_logs.rolling(
            index_column="date", period=ROLLING_14D, by="pitcher_id", closed="left"
        )
        .agg(
            pl.col("strikeouts").sum().alias("k_sum"),
            pl.col("walks").sum().alias("bb_sum"),
            pl.col("batters_faced").sum().alias("bf_sum"),
        )
        .with_columns(
            pl.when(pl.col("bf_sum") > 0)
            .then((pl.col("k_sum") - pl.col("bb_sum")) / pl.col("bf_sum"))
            .otherwise(None)
            .alias("sp_k_minus_bb_14d")
        )
        .select("pitcher_id", "date", "sp_k_minus_bb_14d")
    )

    # --- Rest days and last pitch count (shift within pitcher group) ---
    rest_pitch = sp_logs.sort("pitcher_id", "date").with_columns(
        (pl.col("date") - pl.col("date").shift(1).over("pitcher_id"))
        .dt.total_days()
        .alias("sp_rest_days"),
        pl.col("pitches").shift(1).over("pitcher_id").alias("sp_last_pitch_count"),
    )

    # --- Combine pitcher-level features ---
    pitcher_features = rest_pitch.select(
        "pitcher_id", "date", "handedness", "sp_rest_days", "sp_last_pitch_count"
    ).join(
        fip_rolling, on=["pitcher_id", "date"], how="left"
    ).join(
        kbb_rolling, on=["pitcher_id", "date"], how="left"
    )

    # Season-to-date SIERA from FanGraphs pitcher stats
    if len(pitcher_stats) > 0:
        pitcher_features = pitcher_features.join(
            pitcher_stats.select(
                "pitcher_id", pl.col("siera").alias("sp_sierra_std")
            ),
            on="pitcher_id",
            how="left",
        )
    else:
        pitcher_features = pitcher_features.with_columns(
            pl.lit(None).cast(pl.Float64).alias("sp_sierra_std")
        )

    pitcher_features = pitcher_features.rename(
        {"handedness": "sp_handedness"}
    ).with_columns(
        pl.col("sp_rest_days").fill_null(DEFAULT_REST_DAYS),
        pl.col("sp_last_pitch_count").fill_null(DEFAULT_PITCH_COUNT),
    )

    # --- Map to games (home and away) ---
    games = game_logs.with_columns(
        pl.col("game_date").str.to_date("%Y-%m-%d").alias("date")
    )

    home_sp = (
        games.select("game_id", "date", pl.col("home_sp_id").alias("pitcher_id"))
        .sort("date")
        .join_asof(
            pitcher_features.sort("date"),
            on="date",
            by="pitcher_id",
            strategy="backward",
        )
        .select("game_id", *[pl.col(c).alias(f"home_{c}") for c in SP_FEATURE_COLS])
    )

    away_sp = (
        games.select("game_id", "date", pl.col("away_sp_id").alias("pitcher_id"))
        .sort("date")
        .join_asof(
            pitcher_features.sort("date"),
            on="date",
            by="pitcher_id",
            strategy="backward",
        )
        .select("game_id", *[pl.col(c).alias(f"away_{c}") for c in SP_FEATURE_COLS])
    )

    return home_sp.join(away_sp, on="game_id")
