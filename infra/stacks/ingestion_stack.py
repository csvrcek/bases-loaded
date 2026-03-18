"""Ingestion stack: Lambda scrapers orchestrated by Step Functions + EventBridge.

Daily scrapers (MLB Stats + Weather) run in parallel via Step Functions Express
Workflow at 8 AM UTC. PyBaseball scraper runs weekly (Mondays 7 AM UTC) since
FanGraphs stats are season-to-date aggregates that change slowly.
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
        notifications_topic: sns.ITopic,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

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

        # --- PyBaseball Scraper (Docker Lambda, weekly) ---

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

        # --- Step Functions: Daily ingestion (MLB Stats + Weather) ---

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
            topic=notifications_topic,
            message=sfn.TaskInput.from_text("Daily ingestion pipeline completed successfully."),
            subject="Bases Loaded: Daily Ingestion Complete",
        )

        # SNS notification on failure (used by Catch)
        notify_failure = tasks.SnsPublish(
            self,
            "NotifyFailure",
            topic=notifications_topic,
            message=sfn.TaskInput.from_text("Daily ingestion pipeline failed. Check Step Functions execution logs."),
            subject="Bases Loaded: Daily Ingestion FAILED",
        )
        fail_state = sfn.Fail(self, "IngestionFailed", cause="One or more scrapers failed")

        parallel = sfn.Parallel(self, "RunDailyScrapers")
        parallel.branch(mlb_stats_task)
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

        daily_scheduler_role = iam.Role(
            self,
            "DailySchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        state_machine.grant_start_sync_execution(daily_scheduler_role)

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
                role_arn=daily_scheduler_role.role_arn,
                input=json.dumps({}),
            ),
        )

        # --- EventBridge Scheduler: PyBaseball weekly on Mondays at 7 AM UTC ---

        weekly_scheduler_role = iam.Role(
            self,
            "WeeklySchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        pybaseball_fn.grant_invoke(weekly_scheduler_role)

        scheduler.CfnSchedule(
            self,
            "WeeklyPyBaseballSchedule",
            schedule_expression="cron(0 7 ? * MON *)",
            schedule_expression_timezone="UTC",
            description="Trigger weekly PyBaseball scraper on Mondays at 7 AM UTC (before training)",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=pybaseball_fn.function_arn,
                role_arn=weekly_scheduler_role.role_arn,
                input=json.dumps({}),
            ),
        )
