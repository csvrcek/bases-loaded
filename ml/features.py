"""Feature preprocessing: encoding, null handling, and train/val splitting."""

import polars as pl

from ml.config import (
    ALL_NUMERIC_FEATURES,
    CATEGORICAL_FEATURES,
    CATEGORICAL_VALUES,
    NON_FEATURE_COLS,
    TARGET_COL,
)


def _one_hot_encode(df: pl.DataFrame) -> pl.DataFrame:
    """Deterministic one-hot encoding using known categorical values."""
    for col in CATEGORICAL_FEATURES:
        values = CATEGORICAL_VALUES.get(col, [])
        for val in values:
            dummy_col = f"{col}_{val}"
            if col in df.columns:
                df = df.with_columns(
                    (pl.col(col).cast(pl.Utf8) == val).cast(pl.Float64).alias(dummy_col)
                )
            else:
                df = df.with_columns(pl.lit(0.0).alias(dummy_col))
        if col in df.columns:
            df = df.drop(col)
    return df


def preprocess(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """Encode categoricals, handle nulls, and return a model-ready DataFrame.

    Returns:
        Tuple of (processed DataFrame, metadata dict with medians for each
        numeric feature).
    """
    # One-hot encode categorical columns using fixed value lists
    df = _one_hot_encode(df)

    # Fill numeric nulls with column median and collect medians for inference
    medians = {}
    numeric_cols = [c for c in ALL_NUMERIC_FEATURES if c in df.columns]
    for col in numeric_cols:
        median_val = df[col].median()
        medians[col] = float(median_val) if median_val is not None else 0.0
        null_count = df[col].null_count()
        if null_count > 0:
            null_pct = null_count / len(df)
            if null_pct > 0.10:
                print(f"WARNING: {col} has {null_pct:.1%} null values")
            df = df.with_columns(pl.col(col).fill_null(median_val))

    return df, {"medians": medians}


def split_features_target(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.Series]:
    """Separate feature matrix X from target vector y."""
    drop_cols = [c for c in NON_FEATURE_COLS if c in df.columns]
    y = df[TARGET_COL].cast(pl.Int32)
    X = df.drop(drop_cols)
    return X, y


def time_split(
    df: pl.DataFrame, ratio: float
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split DataFrame by time. Last `ratio` fraction becomes validation set."""
    split_idx = int(len(df) * (1 - ratio))
    train_df = df.head(split_idx)
    val_df = df.tail(len(df) - split_idx)
    print(f"Time split: {len(train_df)} train / {len(val_df)} validation games")
    return train_df, val_df
