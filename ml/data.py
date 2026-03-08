"""Fetch training data from DynamoDB and return as a Polars DataFrame."""

import polars as pl

from shared.aws import scan_dynamodb_table
from shared.config import DYNAMODB_TABLE_GAME_DAY_STATE
from ml.config import TARGET_COL


def fetch_training_data() -> pl.DataFrame:
    """Scan the Game Day State table and return completed games as a Polars DataFrame."""
    raw_items = scan_dynamodb_table(DYNAMODB_TABLE_GAME_DAY_STATE)
    df = pl.from_dicts(raw_items)

    # Keep only completed games (target is populated)
    df = df.filter(pl.col(TARGET_COL).is_not_null())

    # Sort chronologically for time-based splitting
    df = df.sort("game_date")

    print(f"Loaded {len(df)} completed games from DynamoDB")
    return df
