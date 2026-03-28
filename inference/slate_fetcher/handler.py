"""Slate fetcher Lambda.

Runs daily at 6 AM EST (11 AM UTC). Fetches today's MLB schedule,
writes a slate JSON to S3, and creates a one-time EventBridge Scheduler
task for 60 minutes before the earliest first pitch to trigger the
predict Lambda.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import statsapi

from shared.alerting import sns_alert

S3_BUCKET = os.environ["S3_BUCKET_DATA"]
PREDICT_FUNCTION_ARN = os.environ["PREDICT_FUNCTION_ARN"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]
SCHEDULER_GROUP = os.environ.get("SCHEDULER_GROUP", "bases-loaded-inference")

s3 = boto3.client("s3")
scheduler = boto3.client("scheduler")

REGULAR_GAME_TYPES = {"R", "F", "D", "L", "W"}


@sns_alert("Slate Fetcher")
def handler(event, context):
    """Fetch today's MLB slate and schedule the predict Lambda."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_slash = datetime.now(timezone.utc).strftime("%m/%d/%Y")

    print(f"Fetching MLB slate for {today}")
    games = statsapi.schedule(date=today_slash)

    # Filter to regular season + postseason, exclude completed games
    upcoming = []
    for g in games:
        if g.get("game_type", "") not in REGULAR_GAME_TYPES:
            continue
        if g.get("status", "") == "Final":
            continue
        upcoming.append(g)

    if not upcoming:
        print("No upcoming games today — nothing to schedule.")

        topic_arn = os.environ.get("SNS_TOPIC_ARN")
        if topic_arn:
            sns = boto3.client("sns")
            sns.publish(
                TopicArn=topic_arn,
                Subject="Bases Loaded: No Games Today",
                Message=f"No MLB games scheduled for {today}. The prediction pipeline will not run today.",
            )

        return {"status": "no_games", "date": today}

    # Build slate metadata for the predict Lambda
    slate = []
    earliest_start = None

    for g in upcoming:
        game_datetime_str = g.get("game_datetime", "")
        game_dt = None
        if game_datetime_str:
            try:
                game_dt = datetime.fromisoformat(
                    game_datetime_str.replace("Z", "+00:00")
                )
            except ValueError:
                pass

        if game_dt and (earliest_start is None or game_dt < earliest_start):
            earliest_start = game_dt

        slate.append({
            "game_id": str(g["game_id"]),
            "game_date": today,
            "home_team": g.get("home_name", ""),
            "away_team": g.get("away_name", ""),
            "home_team_abbrev": g.get("home_name", ""),
            "away_team_abbrev": g.get("away_name", ""),
            "venue_name": g.get("venue_name", ""),
            "game_datetime": game_datetime_str,
            "home_probable_pitcher": g.get("home_probable_pitcher", ""),
            "away_probable_pitcher": g.get("away_probable_pitcher", ""),
        })

    print(f"Found {len(slate)} upcoming games")

    # Write slate JSON to S3
    slate_key = f"inference/slate/{today}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=slate_key,
        Body=json.dumps({"date": today, "games": slate}),
        ContentType="application/json",
    )
    print(f"Wrote slate to s3://{S3_BUCKET}/{slate_key}")

    # Compute trigger time: 60 min before earliest first pitch
    now = datetime.now(timezone.utc)
    if earliest_start:
        trigger_time = earliest_start - timedelta(minutes=60)
        # If trigger time is in the past, run in 5 minutes
        if trigger_time <= now:
            trigger_time = now + timedelta(minutes=5)
    else:
        # No game times available — trigger in 30 minutes
        trigger_time = now + timedelta(minutes=30)

    trigger_iso = trigger_time.strftime("%Y-%m-%dT%H:%M:%S")
    schedule_name = f"bases-loaded-predict-{today}"

    print(f"Creating one-time schedule '{schedule_name}' at {trigger_iso}")

    # Delete existing schedule for today (idempotent re-runs)
    try:
        scheduler.delete_schedule(
            Name=schedule_name,
            GroupName=SCHEDULER_GROUP,
        )
        print(f"Deleted existing schedule '{schedule_name}'")
    except scheduler.exceptions.ResourceNotFoundException:
        pass

    scheduler.create_schedule(
        Name=schedule_name,
        GroupName=SCHEDULER_GROUP,
        ScheduleExpression=f"at({trigger_iso})",
        ScheduleExpressionTimezone="UTC",
        FlexibleTimeWindow={"Mode": "OFF"},
        ActionAfterCompletion="DELETE",
        Target={
            "Arn": PREDICT_FUNCTION_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": json.dumps({"date": today}),
        },
    )

    return {
        "status": "scheduled",
        "date": today,
        "games": len(slate),
        "trigger_time": trigger_iso,
    }
