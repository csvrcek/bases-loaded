"""Predict Lambda.

Triggered by a one-time EventBridge schedule ~60 min before the earliest
first pitch.  Loads the trained XGBoost model from S3, reads today's game
features from DynamoDB, runs inference, renders an HTML email with all
predictions, and dispatches it via SES.
"""

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import polars as pl
import xgboost as xgb

from inference.predict.email_renderer import render_prediction_email
from inference.predict.preprocess import preprocess_for_inference
from shared.config import AWS_REGION

S3_BUCKET_DATA = os.environ["S3_BUCKET_DATA"]
S3_BUCKET_MODELS = os.environ["S3_BUCKET_MODELS"]
S3_MODEL_KEY = os.environ.get("S3_MODEL_KEY", "latest_model.json")
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
SSM_SUBSCRIBERS_PARAM = os.environ.get(
    "SSM_SUBSCRIBERS_PARAM", "/bases-loaded/subscribers"
)
SSM_SENDER_PARAM = os.environ.get(
    "SSM_SENDER_PARAM", "/bases-loaded/ses-sender"
)

s3 = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
ssm = boto3.client("ssm", region_name=AWS_REGION)
ses = boto3.client("ses", region_name=AWS_REGION)


# ── Helpers ──────────────────────────────────────────────────────────


def _decimal_to_float(obj):
    """Recursively convert DynamoDB Decimals to Python float/int."""
    if isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    if isinstance(obj, dict):
        return {k: _decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimal_to_float(i) for i in obj]
    return obj


def load_model() -> xgb.Booster:
    """Download the latest model from S3 and return a Booster."""
    local_path = "/tmp/model.json"
    s3.download_file(S3_BUCKET_MODELS, S3_MODEL_KEY, local_path)
    booster = xgb.Booster()
    booster.load_model(local_path)
    print(f"Loaded model from s3://{S3_BUCKET_MODELS}/{S3_MODEL_KEY}")
    return booster


def fetch_features(game_date: str) -> list[dict]:
    """Query GameDayState by date via the GameDateIndex GSI."""
    table = dynamodb.Table(DYNAMODB_TABLE)
    items = []
    response = table.query(
        IndexName="GameDateIndex",
        KeyConditionExpression=boto3.dynamodb.conditions.Key("game_date").eq(
            game_date
        ),
    )
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.query(
            IndexName="GameDateIndex",
            KeyConditionExpression=boto3.dynamodb.conditions.Key("game_date").eq(
                game_date
            ),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response["Items"])

    return [_decimal_to_float(item) for item in items]


def load_slate(game_date: str) -> dict:
    """Read the slate JSON written by the slate fetcher."""
    key = f"inference/slate/{game_date}.json"
    resp = s3.get_object(Bucket=S3_BUCKET_DATA, Key=key)
    return json.loads(resp["Body"].read())


def get_ssm_param(name: str) -> str:
    """Read a plaintext SSM Parameter Store value."""
    resp = ssm.get_parameter(Name=name)
    return resp["Parameter"]["Value"]


def format_game_time(game_datetime_str: str) -> str:
    """Convert ISO datetime to a human-readable ET string."""
    if not game_datetime_str:
        return "TBD"
    try:
        dt = datetime.fromisoformat(game_datetime_str.replace("Z", "+00:00"))
        # Convert UTC to US/Eastern (EST = UTC-5, EDT = UTC-4)
        # Approximate: MLB season is entirely within EDT
        et = dt.utcoffset() is not None and dt or dt.replace(tzinfo=timezone.utc)
        et_hour = (et.hour - 4) % 24
        period = "AM" if et_hour < 12 else "PM"
        display_hour = et_hour % 12 or 12
        return f"{display_hour}:{et.minute:02d} {period} ET"
    except (ValueError, AttributeError):
        return "TBD"


def send_email(subject: str, html_body: str, sender: str, recipients: list[str]):
    """Send an HTML email via SES."""
    ses.send_email(
        Source=sender,
        Destination={"ToAddresses": recipients},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Html": {"Data": html_body, "Charset": "UTF-8"}},
        },
    )
    print(f"Sent email to {len(recipients)} recipients")


# ── Handler ──────────────────────────────────────────────────────────


def handler(event, context):
    """Run inference for today's games and email predictions."""
    game_date = event.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    print(f"Running inference for {game_date}")

    # 1. Load model
    booster = load_model()
    expected_features = booster.feature_names

    # 2. Fetch features from DynamoDB
    items = fetch_features(game_date)
    if not items:
        print(f"No features found in DynamoDB for {game_date}")
        return {"status": "no_features", "date": game_date}

    print(f"Found features for {len(items)} games")

    # 3. Load slate metadata for display info
    try:
        slate = load_slate(game_date)
        slate_lookup = {g["game_id"]: g for g in slate.get("games", [])}
    except Exception as e:
        print(f"WARNING: Could not load slate: {e}")
        slate_lookup = {}

    # 4. Preprocess and predict
    df = pl.DataFrame(items)
    game_ids = df["game_id"].to_list()

    X = preprocess_for_inference(df, expected_features)
    dmatrix = xgb.DMatrix(X.to_numpy(), feature_names=expected_features)
    home_win_probs = booster.predict(dmatrix)

    print(f"Generated predictions for {len(home_win_probs)} games")

    # 5. Build email data
    games_for_email = []
    for game_id, home_prob in zip(game_ids, home_win_probs):
        away_prob = 1.0 - float(home_prob)
        home_pct = round(float(home_prob) * 100)
        away_pct = 100 - home_pct

        meta = slate_lookup.get(game_id, {})

        games_for_email.append({
            "home_team": meta.get("home_team", "Home"),
            "away_team": meta.get("away_team", "Away"),
            "game_time": format_game_time(meta.get("game_datetime", "")),
            "venue_name": meta.get("venue_name", ""),
            "home_pitcher": meta.get("home_probable_pitcher", "TBD"),
            "away_pitcher": meta.get("away_probable_pitcher", "TBD"),
            "predictions": [
                {
                    "label": "Win Probability",
                    "home_value": f"{home_pct}%",
                    "away_value": f"{away_pct}%",
                    "home_pct": home_pct,
                    "away_pct": away_pct,
                },
            ],
        })

    # Sort by game time
    games_for_email.sort(key=lambda g: g["game_time"])

    # 6. Render email
    html = render_prediction_email(game_date, games_for_email)

    # 7. Send via SES
    try:
        sender = get_ssm_param(SSM_SENDER_PARAM)
        subscribers_str = get_ssm_param(SSM_SUBSCRIBERS_PARAM)
        recipients = [
            e.strip() for e in subscribers_str.split(",") if e.strip()
        ]
    except Exception as e:
        print(f"ERROR: Could not read SSM params: {e}")
        return {"status": "ssm_error", "error": str(e)}

    if not recipients:
        print("No subscribers configured — skipping email send")
        return {"status": "no_subscribers", "games": len(games_for_email)}

    subject = f"Bases Loaded: {len(games_for_email)} Game Predictions for {game_date}"
    send_email(subject, html, sender, recipients)

    return {
        "status": "success",
        "date": game_date,
        "games_predicted": len(games_for_email),
        "emails_sent": len(recipients),
    }
