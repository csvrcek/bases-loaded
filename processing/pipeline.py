"""Processing pipeline orchestration.

Loads raw data from S3, computes all feature categories, joins them into
per-game feature vectors, and writes to the GameDayState DynamoDB table.
"""

from decimal import Decimal

import boto3
import polars as pl

from shared.config import AWS_REGION, DYNAMODB_TABLE_GAME_DAY_STATE
from processing.loader import load_season_data
from processing.features.pitching import compute_sp_features
from processing.features.offense import compute_offense_features
from processing.features.bullpen import compute_bullpen_features
from processing.features.contextual import compute_contextual_features


def run_pipeline(season: int, target_date: str | None = None) -> pl.DataFrame:
    """Run the full feature engineering pipeline.

    Args:
        season: MLB season year (e.g. 2025).
        target_date: Optional ISO date string (YYYY-MM-DD). If provided, only
            games on this date are processed. If None, all games in the season
            are processed (backfill mode).

    Returns:
        DataFrame of processed game features that were written to DynamoDB.
    """
    # 1. Load raw data from S3
    data = load_season_data(season)

    if len(data["game_logs"]) == 0:
        print("No game logs found — nothing to process.")
        return pl.DataFrame()

    # 2. Enrich game_logs with SP handedness (needed by offense module)
    game_logs = _enrich_sp_handedness(data["game_logs"], data["pitcher_game_logs"])

    # 3. Compute each feature category
    print("Computing SP features...")
    sp_features = compute_sp_features(
        game_logs, data["pitcher_game_logs"], data["pitcher_stats"]
    )

    print("Computing offense features...")
    offense_features = compute_offense_features(
        game_logs, data["team_batting"], data["team_batting_splits"]
    )

    print("Computing bullpen features...")
    bullpen_features = compute_bullpen_features(
        game_logs, data["pitcher_game_logs"]
    )

    print("Computing contextual features...")
    contextual_features = compute_contextual_features(
        game_logs, data["weather"], data["park_factors"], data["schedules"]
    )

    # 4. Join all features on game_id
    features = (
        game_logs.select("game_id", "game_date", "home_score", "away_score")
        .join(sp_features, on="game_id", how="left")
        .join(offense_features, on="game_id", how="left")
        .join(bullpen_features, on="game_id", how="left")
        .join(contextual_features, on="game_id", how="left")
    )

    # 5. Compute target variable (only for completed games)
    features = features.with_columns(
        pl.when(
            pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null()
        )
        .then((pl.col("home_score") > pl.col("away_score")).cast(pl.Int32))
        .otherwise(None)
        .alias("target_home_win")
    ).drop("home_score", "away_score")

    # 6. Filter to target date if specified
    if target_date:
        features = features.filter(pl.col("game_date") == target_date)

    if len(features) == 0:
        print(f"No games to process for date={target_date}")
        return features

    # 7. Write to DynamoDB
    write_to_dynamodb(features, DYNAMODB_TABLE_GAME_DAY_STATE)

    return features


def _enrich_sp_handedness(
    game_logs: pl.DataFrame, pitcher_logs: pl.DataFrame
) -> pl.DataFrame:
    """Add home_sp_hand and away_sp_hand columns to game_logs."""
    if len(pitcher_logs) == 0:
        return game_logs.with_columns(
            pl.lit("R").alias("home_sp_hand"),
            pl.lit("R").alias("away_sp_hand"),
        )

    pitcher_hand = (
        pitcher_logs.filter(pl.col("role") == "SP")
        .select("pitcher_id", "handedness")
        .unique(subset=["pitcher_id"])
    )

    enriched = game_logs.join(
        pitcher_hand.select(
            pl.col("pitcher_id").alias("home_sp_id"),
            pl.col("handedness").alias("home_sp_hand"),
        ),
        on="home_sp_id",
        how="left",
    ).join(
        pitcher_hand.select(
            pl.col("pitcher_id").alias("away_sp_id"),
            pl.col("handedness").alias("away_sp_hand"),
        ),
        on="away_sp_id",
        how="left",
    )

    # Default to R if handedness unknown
    return enriched.with_columns(
        pl.col("home_sp_hand").fill_null("R"),
        pl.col("away_sp_hand").fill_null("R"),
    )


def write_to_dynamodb(df: pl.DataFrame, table_name: str) -> None:
    """Batch write game features to DynamoDB."""
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(table_name)

    items = df.to_dicts()
    with table.batch_writer() as batch:
        for row in items:
            batch.put_item(Item=_to_dynamodb_item(row))

    print(f"Wrote {len(items)} game features to {table_name}")


def _to_dynamodb_item(row: dict) -> dict:
    """Convert Python types to DynamoDB-compatible types."""
    item = {}
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, float):
            item[k] = Decimal(str(round(v, 6)))
        elif isinstance(v, int):
            item[k] = v
        else:
            item[k] = str(v)
    return item
