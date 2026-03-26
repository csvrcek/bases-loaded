"""Inference-time feature preprocessing.

Mirrors ml/features.py::preprocess() but handles inference-specific
concerns: small batch sizes (null-fill with 0 instead of median),
and column alignment to match the trained model's expected features.
"""

import polars as pl

from ml.config import ALL_NUMERIC_FEATURES, CATEGORICAL_FEATURES, NON_FEATURE_COLS


def preprocess_for_inference(
    df: pl.DataFrame, expected_features: list[str]
) -> pl.DataFrame:
    """Preprocess features for inference and align to model's expected columns.

    Args:
        df: Raw feature DataFrame from DynamoDB (one row per game).
        expected_features: Feature names the trained model expects
            (from booster.feature_names).

    Returns:
        Model-ready DataFrame with columns matching expected_features.
    """
    # One-hot encode categorical columns (same logic as ml/features.py)
    for col in CATEGORICAL_FEATURES:
        if col in df.columns:
            dummies = (
                df.select(pl.col(col).to_physical().cast(pl.Utf8))
                .to_dummies(separator="_")
            )
            dummies = dummies.rename(
                {
                    c: f"{col}_{c}" if not c.startswith(col) else c
                    for c in dummies.columns
                }
            )
            df = pl.concat([df, dummies], how="horizontal")
            df = df.drop(col)

    # Fill numeric nulls with 0 (at inference time we can't compute a
    # meaningful median from a handful of games)
    numeric_cols = [c for c in ALL_NUMERIC_FEATURES if c in df.columns]
    for col in numeric_cols:
        if df[col].null_count() > 0:
            df = df.with_columns(pl.col(col).fill_null(0.0))

    # Drop non-feature columns
    drop_cols = [c for c in NON_FEATURE_COLS if c in df.columns]
    df = df.drop(drop_cols)

    # Align columns to match the trained model's expected features:
    # - Add missing columns as 0 (e.g. a one-hot category unseen at inference)
    # - Drop extra columns the model doesn't expect
    # - Reorder to match model's feature order
    for col in expected_features:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0.0).alias(col))

    df = df.select(expected_features)

    return df
