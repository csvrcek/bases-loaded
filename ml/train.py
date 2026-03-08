#!/usr/bin/env python3
"""Weekly ML training pipeline entry point.

Run from repo root: python -m ml.train
"""

from ml.config import VALIDATION_SPLIT_RATIO, XGBOOST_PARAMS
from ml.data import fetch_training_data
from ml.features import preprocess, split_features_target, time_split
from ml.model import evaluate_model, save_model, train_model
from shared.aws import upload_to_s3
from shared.config import S3_BUCKET_MODELS, S3_MODEL_KEY


def main():
    # 1. Fetch data from DynamoDB
    df = fetch_training_data()

    # 2. Preprocess features
    df = preprocess(df)

    # 3. Time-based train/validation split
    train_df, val_df = time_split(df, VALIDATION_SPLIT_RATIO)
    X_train, y_train = split_features_target(train_df)
    X_val, y_val = split_features_target(val_df)

    # 4. Train XGBoost model
    booster = train_model(X_train, y_train, X_val, y_val, XGBOOST_PARAMS)

    # 5. Evaluate
    evaluate_model(booster, X_val, y_val)

    # 6. Save and upload to S3
    local_path = save_model(booster, "/tmp/latest_model.json")
    upload_to_s3(local_path, S3_BUCKET_MODELS, S3_MODEL_KEY)
    print(f"Model uploaded to s3://{S3_BUCKET_MODELS}/{S3_MODEL_KEY}")


if __name__ == "__main__":
    main()
