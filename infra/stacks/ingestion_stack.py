"""Ingestion stack: Lambda scrapers orchestrated by Step Functions + EventBridge.

Placeholder — Lambda functions and Step Function state machine will be defined
once the ingestion pillar code is built.
"""

from aws_cdk import (
    Stack,
    aws_s3 as s3,
)
from constructs import Construct


class IngestionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        data_bucket: s3.IBucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # TODO: Lambda functions for MLB Stats API, PyBaseball, OpenWeather
        # TODO: Step Functions Express Workflow to orchestrate scrapers
        # TODO: EventBridge daily cron rule to trigger the Step Function
        pass
