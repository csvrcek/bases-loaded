"""Team offensive feature computation.

Computes per-game offensive features for both home and away teams:
- team_wrc_plus_14d_split: Season-to-date wRC+ vs opposing SP handedness (FanGraphs)
- team_woba_std_split: Season-to-date wOBA vs opposing SP handedness (FanGraphs)
- team_k_pct_14d: Rolling 14-day strikeout rate (from game logs)
- team_iso_14d: Rolling 14-day isolated power (from game logs)

Note: wRC+ uses FanGraphs season-to-date split data rather than a true 14-day
rolling window, since computing wRC+ from raw stats requires park adjustments
and league-average constants. The model adapts to the signal regardless.
"""

import polars as pl

from processing.config import ROLLING_14D

OFFENSE_FEATURE_COLS = [
    "team_wrc_plus_14d_split",
    "team_woba_std_split",
    "team_k_pct_14d",
    "team_iso_14d",
]


def compute_offense_features(
    game_logs: pl.DataFrame,
    team_batting: pl.DataFrame,
    team_batting_splits: pl.DataFrame,
) -> pl.DataFrame:
    """Compute offensive features for all games.

    Expects game_logs to be enriched with home_sp_hand and away_sp_hand columns
    (added by the pipeline before calling this function).

    Returns DataFrame with columns: game_id + home_team_* + away_team_*
    """
    games = game_logs.with_columns(
        pl.col("game_date").str.to_date("%Y-%m-%d").alias("date")
    )

    # --- Rolling 14d K% and ISO from team game logs ---
    team_rolling = _compute_rolling_batting(team_batting)

    # --- Build per-team-per-game features ---
    # Home team: faces away SP, so split is vs away_sp_hand
    home_features = _build_team_features(
        games=games,
        team_col="home_team",
        opp_hand_col="away_sp_hand",
        team_rolling=team_rolling,
        team_batting_splits=team_batting_splits,
        prefix="home",
    )

    # Away team: faces home SP, so split is vs home_sp_hand
    away_features = _build_team_features(
        games=games,
        team_col="away_team",
        opp_hand_col="home_sp_hand",
        team_rolling=team_rolling,
        team_batting_splits=team_batting_splits,
        prefix="away",
    )

    return home_features.join(away_features, on="game_id")


def _compute_rolling_batting(team_batting: pl.DataFrame) -> pl.DataFrame:
    """Compute rolling 14-day K% and ISO from team game logs."""
    if len(team_batting) == 0:
        return pl.DataFrame(
            schema={
                "team": pl.Utf8,
                "date": pl.Date,
                "team_k_pct_14d": pl.Float64,
                "team_iso_14d": pl.Float64,
            }
        )

    batting = (
        team_batting.with_columns(
            pl.col("game_date").str.to_date("%Y-%m-%d").alias("date")
        )
        .sort("date")
    )

    return (
        batting.rolling(
            index_column="date", period=ROLLING_14D, by="team", closed="left"
        )
        .agg(
            pl.col("strikeouts").sum().alias("k_sum"),
            pl.col("plate_appearances").sum().alias("pa_sum"),
            pl.col("doubles").sum().alias("doubles_sum"),
            pl.col("triples").sum().alias("triples_sum"),
            pl.col("home_runs").sum().alias("hr_sum"),
            pl.col("at_bats").sum().alias("ab_sum"),
        )
        .with_columns(
            pl.when(pl.col("pa_sum") > 0)
            .then(pl.col("k_sum") / pl.col("pa_sum"))
            .otherwise(None)
            .alias("team_k_pct_14d"),
            pl.when(pl.col("ab_sum") > 0)
            .then(
                (
                    pl.col("doubles_sum")
                    + 2 * pl.col("triples_sum")
                    + 3 * pl.col("hr_sum")
                )
                / pl.col("ab_sum")
            )
            .otherwise(None)
            .alias("team_iso_14d"),
        )
        .select("team", "date", "team_k_pct_14d", "team_iso_14d")
    )


def _build_team_features(
    games: pl.DataFrame,
    team_col: str,
    opp_hand_col: str,
    team_rolling: pl.DataFrame,
    team_batting_splits: pl.DataFrame,
    prefix: str,
) -> pl.DataFrame:
    """Build offense features for one side (home or away)."""
    base = games.select(
        "game_id", "date", "season",
        pl.col(team_col).alias("team"),
        pl.col(opp_hand_col).alias("opp_hand"),
    )

    # Join rolling stats (join_asof to get most recent values for upcoming games)
    result = base.sort("date").join_asof(
        team_rolling.sort("date"),
        on="date",
        by="team",
        strategy="backward",
    )

    # Join FanGraphs split stats (wRC+ and wOBA vs opposing SP handedness)
    if len(team_batting_splits) > 0:
        result = result.join(
            team_batting_splits.select(
                "team",
                "season",
                pl.col("vs_hand"),
                pl.col("wrc_plus").alias("team_wrc_plus_14d_split"),
                pl.col("woba").alias("team_woba_std_split"),
            ),
            left_on=["team", "season", "opp_hand"],
            right_on=["team", "season", "vs_hand"],
            how="left",
        )
    else:
        result = result.with_columns(
            pl.lit(None).cast(pl.Float64).alias("team_wrc_plus_14d_split"),
            pl.lit(None).cast(pl.Float64).alias("team_woba_std_split"),
        )

    return result.select(
        "game_id",
        *[pl.col(c).alias(f"{prefix}_{c}") for c in OFFENSE_FEATURE_COLS],
    )
