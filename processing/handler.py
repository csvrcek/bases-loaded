"""Lambda handler for the processing pipeline."""

import json
import os
from datetime import date, timedelta

import boto3

from processing.pipeline import run_pipeline


def lambda_handler(event, context):
    """Process raw data from S3 into game-day features in DynamoDB.

    Event formats:
    - EventBridge cron (daily): {} or {"mode": "daily"}
    - Specific date: {"mode": "daily", "date": "2025-06-15", "season": 2025}
    - Full-season backfill: {"mode": "backfill", "season": 2025}
    """
    mode = event.get("mode", "daily")
    season = event.get("season", date.today().year)
    target_date = event.get("date")

    if mode == "daily" and not target_date:
        # Default: process yesterday's games (data ingested overnight)
        target_date = (date.today() - timedelta(days=1)).isoformat()

    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")

    try:
        if mode == "backfill":
            result = run_pipeline(season=season, target_date=None)
            msg = f"Backfill complete for {season}: {len(result)} games processed"
        else:
            result = run_pipeline(season=season, target_date=target_date)
            msg = f"Daily processing complete for {target_date}: {len(result)} games processed"

        print(msg)

        if sns_topic_arn:
            boto3.client("sns").publish(
                TopicArn=sns_topic_arn,
                Subject="Bases Loaded Processing: Success",
                Message=msg,
            )

        return {"statusCode": 200, "body": json.dumps({"message": msg})}

    except Exception as e:
        error_msg = f"Processing pipeline failed: {e}"
        print(f"ERROR: {error_msg}")

        if sns_topic_arn:
            boto3.client("sns").publish(
                TopicArn=sns_topic_arn,
                Subject="Bases Loaded Processing: FAILURE",
                Message=error_msg,
            )

        raise
