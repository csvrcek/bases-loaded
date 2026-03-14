#!/usr/bin/env python3
"""Weekly ML training pipeline entry point.

Run from repo root: python -m ml.train
"""

from ml.config import MAX_BRIER_SCORE, MAX_LOG_LOSS, VALIDATION_SPLIT_RATIO, XGBOOST_PARAM_SETS
from ml.data import fetch_training_data
from ml.features import preprocess, split_features_target, time_split
from ml.model import evaluate_model, save_model, train_model
from shared.aws import upload_to_s3
from shared.config import S3_BUCKET_MODELS, S3_MODEL_KEY


def passes_quality_gate(metrics: dict) -> bool:
    return metrics["log_loss"] <= MAX_LOG_LOSS and metrics["brier_score"] <= MAX_BRIER_SCORE


def main():
    # 1. Fetch data from DynamoDB
    df = fetch_training_data()

    # 2. Preprocess features
    df = preprocess(df)

    # 3. Time-based train/validation split
    train_df, val_df = time_split(df, VALIDATION_SPLIT_RATIO)
    X_train, y_train = split_features_target(train_df)
    X_val, y_val = split_features_target(val_df)

    # 4. Try each param set until one passes the quality gate
    for i, params in enumerate(XGBOOST_PARAM_SETS):
        print(f"\n--- Attempt {i + 1}/{len(XGBOOST_PARAM_SETS)} ---")
        booster = train_model(X_train, y_train, X_val, y_val, params)
        metrics = evaluate_model(booster, X_val, y_val)

        if passes_quality_gate(metrics):
            print("Model passed quality gate.")
            break
        print("Model did not pass quality gate, trying next param set...")
    else:
        raise RuntimeError(
            f"All {len(XGBOOST_PARAM_SETS)} param sets failed quality gate. "
            f"Last result: log_loss={metrics['log_loss']:.4f} (max {MAX_LOG_LOSS}), "
            f"brier_score={metrics['brier_score']:.4f} (max {MAX_BRIER_SCORE}). "
            f"Previous model retained."
        )

    # 5. Save and upload to S3
    local_path = save_model(booster, "/tmp/latest_model.json")
    upload_to_s3(local_path, S3_BUCKET_MODELS, S3_MODEL_KEY)
    print(f"Model uploaded to s3://{S3_BUCKET_MODELS}/{S3_MODEL_KEY}")


if __name__ == "__main__":
    main()
