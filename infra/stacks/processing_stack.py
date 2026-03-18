"""Processing stack: High-memory Docker Lambda for Polars-based feature engineering.

Reads raw data from S3, computes rolling stats and per-game feature vectors,
and writes the Game Day State to DynamoDB. Runs daily on an EventBridge schedule.
"""

from pathlib import Path

from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_sns as sns,
)
from constructs import Construct

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class ProcessingStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        data_bucket: s3.IBucket,
        game_day_table: dynamodb.ITable,
        notifications_topic: sns.ITopic,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # --- Processing Lambda (Docker image for Polars native deps) ---

        processing_fn = _lambda.DockerImageFunction(
            self,
            "ProcessingFunction",
            function_name="bases-loaded-processing",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(REPO_ROOT),
                file="processing/Dockerfile",
                exclude=[
                    ".git",
                    "*.pyc",
                    "__pycache__",
                    ".venv",
                    "cdk.out",
                    "node_modules",
                    "infra",
                    "ml",
                    "ingestion",
                    "inference",
                    "docs",
                    ".github",
                ],
            ),
            memory_size=3008,  # ~3 GB for Polars in-memory processing (Lambda max)
            timeout=Duration.minutes(10),
            environment={
                "S3_BUCKET_DATA": data_bucket.bucket_name,
                "DYNAMODB_TABLE": game_day_table.table_name,
                "SNS_TOPIC_ARN": notifications_topic.topic_arn,
            },
        )

        # Permissions
        data_bucket.grant_read(processing_fn)
        game_day_table.grant_read_write_data(processing_fn)
        notifications_topic.grant_publish(processing_fn)

        # --- EventBridge: run daily at 5 AM UTC (midnight EST, after ingestion) ---

        events.Rule(
            self,
            "DailyProcessingSchedule",
            schedule=events.Schedule.cron(hour="5", minute="0"),
            targets=[targets.LambdaFunction(processing_fn)],
        )
