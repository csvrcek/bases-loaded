"""Inference & Delivery stack: game-time predictions and email dispatch.

Daily at 6 AM EST (11 AM UTC), the slate fetcher Lambda fetches today's MLB
schedule and creates a one-time EventBridge Scheduler trigger for 60 minutes
before the earliest first pitch. The predict Lambda then loads the trained
model from S3, reads features from DynamoDB, generates win probabilities,
renders an HTML email, and sends it via SES.
"""

import json

from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_scheduler as scheduler,
    aws_sns as sns,
)
from constructs import Construct

from stacks import EXCLUDE_DIRS, REPO_ROOT

SCHEDULER_GROUP = "bases-loaded-inference"


class InferenceStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        data_bucket: s3.IBucket,
        models_bucket: s3.IBucket,
        game_day_table: dynamodb.ITable,
        notifications_topic: sns.ITopic,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # --- Predict Lambda (Docker — XGBoost + Polars + Jinja2) ---

        predict_fn = _lambda.DockerImageFunction(
            self,
            "PredictFunction",
            function_name="bases-loaded-predict",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(REPO_ROOT),
                file="inference/Dockerfile",
                cmd=["inference.predict.handler.handler"],
                exclude=[d for d in EXCLUDE_DIRS if d not in ("inference", "ml")],
            ),
            memory_size=1024,
            timeout=Duration.minutes(5),
            environment={
                "S3_BUCKET_DATA": data_bucket.bucket_name,
                "S3_BUCKET_MODELS": models_bucket.bucket_name,
                "S3_MODEL_KEY": "latest_model.json",
                "DYNAMODB_TABLE": game_day_table.table_name,
                "SSM_SUBSCRIBERS_PARAM": "/bases-loaded/subscribers",
                "SSM_SENDER_PARAM": "/bases-loaded/ses-sender",
            },
        )

        # Predict Lambda permissions
        data_bucket.grant_read(predict_fn)
        models_bucket.grant_read(predict_fn)
        game_day_table.grant_read_data(predict_fn)
        predict_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter"],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/bases-loaded/*",
                ],
            )
        )
        predict_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ses:SendEmail", "ses:SendRawEmail"],
                resources=["*"],
            )
        )

        # --- EventBridge Scheduler group for one-time inference triggers ---

        scheduler_group = scheduler.CfnScheduleGroup(
            self,
            "InferenceSchedulerGroup",
            name=SCHEDULER_GROUP,
        )

        # --- IAM role for EventBridge Scheduler to invoke predict Lambda ---

        scheduler_role = iam.Role(
            self,
            "InferenceSchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        predict_fn.grant_invoke(scheduler_role)

        # --- Slate Fetcher Lambda (Docker — statsapi) ---

        slate_fetcher_fn = _lambda.DockerImageFunction(
            self,
            "SlateFetcherFunction",
            function_name="bases-loaded-slate-fetcher",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(REPO_ROOT),
                file="inference/Dockerfile",
                cmd=["inference.slate_fetcher.handler.handler"],
                exclude=[d for d in EXCLUDE_DIRS if d not in ("inference", "ml")],
            ),
            memory_size=512,
            timeout=Duration.minutes(5),
            environment={
                "S3_BUCKET_DATA": data_bucket.bucket_name,
                "PREDICT_FUNCTION_ARN": predict_fn.function_arn,
                "SCHEDULER_ROLE_ARN": scheduler_role.role_arn,
                "SCHEDULER_GROUP": SCHEDULER_GROUP,
            },
        )

        # Slate fetcher permissions
        data_bucket.grant_read_write(slate_fetcher_fn)
        slate_fetcher_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "scheduler:CreateSchedule",
                    "scheduler:DeleteSchedule",
                ],
                resources=[
                    f"arn:aws:scheduler:{self.region}:{self.account}:schedule/{SCHEDULER_GROUP}/*",
                ],
            )
        )
        slate_fetcher_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[scheduler_role.role_arn],
            )
        )

        # --- EventBridge Scheduler: daily at 11 AM UTC (6 AM EST) ---

        daily_scheduler_role = iam.Role(
            self,
            "DailySlateFetcherRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        slate_fetcher_fn.grant_invoke(daily_scheduler_role)

        scheduler.CfnSchedule(
            self,
            "DailySlateFetchSchedule",
            schedule_expression="cron(0 11 * * ? *)",
            schedule_expression_timezone="UTC",
            description="Fetch today's MLB slate at 6 AM EST and schedule inference",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=slate_fetcher_fn.function_arn,
                role_arn=daily_scheduler_role.role_arn,
                input=json.dumps({}),
            ),
        )
