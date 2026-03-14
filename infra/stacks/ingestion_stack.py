"""Ingestion stack: Lambda scrapers orchestrated by Step Functions + EventBridge.

Three scrapers run in parallel via a Step Functions Express Workflow:
  - MLB Stats scraper (Docker Lambda): game logs, pitcher game logs, team batting, schedules
  - PyBaseball scraper (Docker Lambda): pitcher stats, batting splits, park factors
  - Weather scraper (Docker Lambda): game-day weather from OpenWeather API

Triggered daily at 8 AM UTC by an EventBridge Scheduler.
"""

import json
from pathlib import Path

from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_scheduler as scheduler,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct

RUNTIME_DIR = Path(__file__).resolve().parent.parent / "runtime"


class IngestionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        data_bucket: s3.IBucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # --- SNS Topic for ingestion notifications ---

        self.notifications_topic = sns.Topic(
            self,
            "IngestionNotifications",
            topic_name="bases-loaded-ingestion-notifications",
        )
        notification_email = self.node.try_get_context("notification_email")
        if notification_email:
            self.notifications_topic.add_subscription(
                subs.EmailSubscription(notification_email)
            )

        # --- MLB Stats Scraper (Docker Lambda) ---

        mlb_stats_fn = _lambda.DockerImageFunction(
            self,
            "MlbStatsScraper",
            function_name="bases-loaded-mlb-stats-scraper",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(RUNTIME_DIR / "mlb_stats_scraper"),
            ),
            memory_size=512,
            timeout=Duration.minutes(5),
            environment={
                "S3_BUCKET_DATA": data_bucket.bucket_name,
            },
        )
        data_bucket.grant_read_write(mlb_stats_fn)

        # --- PyBaseball Scraper (Docker Lambda) ---

        pybaseball_fn = _lambda.DockerImageFunction(
            self,
            "PyBaseballScraper",
            function_name="bases-loaded-pybaseball-scraper",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(RUNTIME_DIR / "pybaseball_scraper"),
            ),
            memory_size=1024,
            timeout=Duration.minutes(10),
            environment={
                "S3_BUCKET_DATA": data_bucket.bucket_name,
            },
        )
        data_bucket.grant_read_write(pybaseball_fn)

        # --- Weather Scraper (Docker Lambda) ---

        weather_fn = _lambda.DockerImageFunction(
            self,
            "WeatherScraper",
            function_name="bases-loaded-weather-scraper",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(RUNTIME_DIR / "weather_scraper"),
            ),
            memory_size=256,
            timeout=Duration.minutes(2),
            environment={
                "S3_BUCKET_DATA": data_bucket.bucket_name,
            },
        )
        data_bucket.grant_read_write(weather_fn)

        # SSM read permission for OpenWeather API key
        weather_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter"],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/bases-loaded/openweather-api-key",
                ],
            )
        )

        # --- Step Functions: Parallel execution of all 3 scrapers ---

        mlb_stats_task = tasks.LambdaInvoke(
            self,
            "InvokeMlbStats",
            lambda_function=mlb_stats_fn,
            retry_on_service_exceptions=True,
            result_path="$.mlbStats",
        )
        mlb_stats_task.add_retry(
            max_attempts=2,
            backoff_rate=2,
            interval=Duration.seconds(30),
        )

        pybaseball_task = tasks.LambdaInvoke(
            self,
            "InvokePyBaseball",
            lambda_function=pybaseball_fn,
            retry_on_service_exceptions=True,
            result_path="$.pyBaseball",
        )
        pybaseball_task.add_retry(
            max_attempts=2,
            backoff_rate=2,
            interval=Duration.seconds(30),
        )

        weather_task = tasks.LambdaInvoke(
            self,
            "InvokeWeather",
            lambda_function=weather_fn,
            retry_on_service_exceptions=True,
            result_path="$.weather",
        )
        weather_task.add_retry(
            max_attempts=2,
            backoff_rate=2,
            interval=Duration.seconds(30),
        )

        # SNS notification on completion
        notify_success = tasks.SnsPublish(
            self,
            "NotifySuccess",
            topic=self.notifications_topic,
            message=sfn.TaskInput.from_text("Ingestion pipeline completed successfully."),
            subject="Bases Loaded: Ingestion Complete",
        )

        # SNS notification on failure (used by Catch)
        notify_failure = tasks.SnsPublish(
            self,
            "NotifyFailure",
            topic=self.notifications_topic,
            message=sfn.TaskInput.from_text("Ingestion pipeline failed. Check Step Functions execution logs."),
            subject="Bases Loaded: Ingestion FAILED",
        )
        fail_state = sfn.Fail(self, "IngestionFailed", cause="One or more scrapers failed")

        parallel = sfn.Parallel(self, "RunAllScrapers")
        parallel.branch(mlb_stats_task)
        parallel.branch(pybaseball_task)
        parallel.branch(weather_task)

        parallel.add_catch(
            notify_failure.next(fail_state),
            result_path="$.error",
        )

        definition = parallel.next(notify_success)

        state_machine = sfn.StateMachine(
            self,
            "IngestionStateMachine",
            state_machine_name="bases-loaded-ingestion",
            state_machine_type=sfn.StateMachineType.EXPRESS,
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(15),
        )

        # --- EventBridge Scheduler: daily at 8 AM UTC ---

        scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )

        state_machine.grant_start_sync_execution(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "DailyIngestionSchedule",
            schedule_expression="cron(0 8 * * ? *)",
            schedule_expression_timezone="UTC",
            description="Trigger daily ingestion pipeline at 8 AM UTC",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({}),
            ),
        )
