"""Inference-time feature preprocessing.

Mirrors ml/features.py::preprocess() but handles inference-specific
concerns: small batch sizes (null-fill with training medians instead of
recomputing), column alignment, and diagnostics for SNS notifications.
"""

import polars as pl

from ml.config import (
    ALL_NUMERIC_FEATURES,
    CATEGORICAL_FEATURES,
    CATEGORICAL_VALUES,
    NON_FEATURE_COLS,
)


def _one_hot_encode(df: pl.DataFrame) -> pl.DataFrame:
    """Deterministic one-hot encoding using known categorical values.

    Identical to ml/features._one_hot_encode — ensures training and
    inference produce the same dummy columns regardless of batch contents.
    """
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


def preprocess_for_inference(
    df: pl.DataFrame,
    expected_features: list[str],
    preprocessing_metadata: dict | None = None,
) -> tuple[pl.DataFrame, dict]:
    """Preprocess features for inference and align to model's expected columns.

    Args:
        df: Raw feature DataFrame from DynamoDB (one row per game).
        expected_features: Feature names the trained model expects
            (from booster.feature_names).
        preprocessing_metadata: Dict with "medians" key mapping feature
            names to their training-time median values. If None, falls
            back to 0.0 for null-fill.

    Returns:
        Tuple of (model-ready DataFrame, diagnostics dict).
    """
    medians = (preprocessing_metadata or {}).get("medians", {})

    # Collect diagnostics
    incoming_cols = set(df.columns) - set(NON_FEATURE_COLS)
    expected_numeric = set(ALL_NUMERIC_FEATURES)
    expected_categorical = set(CATEGORICAL_FEATURES)

    numeric_present = incoming_cols & expected_numeric
    numeric_missing = expected_numeric - incoming_cols
    categorical_present = incoming_cols & expected_categorical
    categorical_missing = expected_categorical - incoming_cols

    # One-hot encode categorical columns using fixed value lists
    df = _one_hot_encode(df)

    # Fill numeric nulls with training medians (fall back to 0.0)
    numeric_cols = [c for c in ALL_NUMERIC_FEATURES if c in df.columns]
    null_counts = {}
    for col in numeric_cols:
        if df[col].null_count() > 0:
            null_counts[col] = df[col].null_count()
            fill_value = medians.get(col, 0.0)
            df = df.with_columns(pl.col(col).fill_null(fill_value))

    # Drop non-feature columns
    drop_cols = [c for c in NON_FEATURE_COLS if c in df.columns]
    df = df.drop(drop_cols)

    # Align columns to match the trained model's expected features:
    # - Add missing columns (fill with training median if available, else 0)
    # - Drop extra columns the model doesn't expect
    # - Reorder to match model's feature order
    filled_from_alignment = []
    for col in expected_features:
        if col not in df.columns:
            fill_value = medians.get(col, 0.0)
            df = df.with_columns(pl.lit(fill_value).alias(col))
            filled_from_alignment.append(col)

    df = df.select(expected_features)

    diagnostics = {
        "numeric_features_present": len(numeric_present),
        "numeric_features_missing": sorted(numeric_missing),
        "categorical_present": sorted(categorical_present),
        "categorical_missing": sorted(categorical_missing),
        "null_counts": null_counts,
        "columns_filled_in_alignment": len(filled_from_alignment),
    }

    return df, diagnostics
