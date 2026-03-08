"""XGBoost model training, evaluation, and persistence."""

import polars as pl
import xgboost as xgb
from sklearn.metrics import brier_score_loss, log_loss

from ml.config import EARLY_STOPPING_ROUNDS, NUM_BOOST_ROUNDS


def train_model(
    X_train: pl.DataFrame,
    y_train: pl.Series,
    X_val: pl.DataFrame,
    y_val: pl.Series,
    params: dict,
) -> xgb.Booster:
    """Train an XGBoost model with early stopping on validation set."""
    dtrain = xgb.DMatrix(
        X_train.to_numpy(), label=y_train.to_numpy(), feature_names=X_train.columns
    )
    dval = xgb.DMatrix(
        X_val.to_numpy(), label=y_val.to_numpy(), feature_names=X_val.columns
    )

    booster = xgb.train(
        params,
        dtrain,
        num_boost_round=NUM_BOOST_ROUNDS,
        evals=[(dtrain, "train"), (dval, "val")],
        early_stopping_rounds=EARLY_STOPPING_ROUNDS,
        verbose_eval=25,
    )

    print(f"Training complete. Best iteration: {booster.best_iteration}")
    return booster


def evaluate_model(
    booster: xgb.Booster,
    X_val: pl.DataFrame,
    y_val: pl.Series,
) -> dict:
    """Evaluate model on validation set using Log Loss and Brier Score."""
    dval = xgb.DMatrix(X_val.to_numpy(), feature_names=X_val.columns)
    y_prob = booster.predict(dval)
    y_true = y_val.to_numpy()

    metrics = {
        "log_loss": log_loss(y_true, y_prob),
        "brier_score": brier_score_loss(y_true, y_prob),
    }

    print(f"Validation Log Loss:   {metrics['log_loss']:.4f}")
    print(f"Validation Brier Score: {metrics['brier_score']:.4f}")
    return metrics


def save_model(booster: xgb.Booster, path: str) -> str:
    """Save XGBoost model in JSON format."""
    booster.save_model(path)
    print(f"Model saved to {path}")
    return path
