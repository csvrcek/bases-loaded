"""Feature preprocessing: encoding, null handling, and train/val splitting."""

import polars as pl

from ml.config import (
    ALL_NUMERIC_FEATURES,
    CATEGORICAL_FEATURES,
    NON_FEATURE_COLS,
    TARGET_COL,
)


def preprocess(df: pl.DataFrame) -> pl.DataFrame:
    """Encode categoricals, handle nulls, and return a model-ready DataFrame."""
    # One-hot encode categorical columns
    for col in CATEGORICAL_FEATURES:
        if col in df.columns:
            dummies = df.select(pl.col(col).to_physical().cast(pl.Utf8)).to_dummies(separator="_")
            # Prefix dummy columns with original column name for clarity
            dummies = dummies.rename(
                {c: f"{col}_{c}" if not c.startswith(col) else c for c in dummies.columns}
            )
            df = pl.concat([df, dummies], how="horizontal")
            df = df.drop(col)

    # Fill numeric nulls with column median
    numeric_cols = [c for c in ALL_NUMERIC_FEATURES if c in df.columns]
    for col in numeric_cols:
        null_count = df[col].null_count()
        if null_count > 0:
            null_pct = null_count / len(df)
            if null_pct > 0.10:
                print(f"WARNING: {col} has {null_pct:.1%} null values")
            median_val = df[col].median()
            df = df.with_columns(pl.col(col).fill_null(median_val))

    return df


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
