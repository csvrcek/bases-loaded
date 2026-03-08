"""Shared infrastructure: S3 buckets, DynamoDB table, and exports for other stacks."""

from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
)
from constructs import Construct


class SharedStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # --- S3 Buckets ---

        # Raw data lake for ingestion
        self.data_bucket = s3.Bucket(
            self,
            "DataBucket",
            bucket_name=f"bases-loaded-data-{self.account}",
            removal_policy=RemovalPolicy.RETAIN,
            versioned=True,
        )

        # Model artifact storage
        self.models_bucket = s3.Bucket(
            self,
            "ModelsBucket",
            bucket_name=f"bases-loaded-models-{self.account}",
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- DynamoDB ---

        # Game Day State feature store
        self.game_day_table = dynamodb.Table(
            self,
            "GameDayState",
            table_name="GameDayState",
            partition_key=dynamodb.Attribute(
                name="game_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # GSI for querying by date (needed for fetching daily slate)
        self.game_day_table.add_global_secondary_index(
            index_name="GameDateIndex",
            partition_key=dynamodb.Attribute(
                name="game_date", type=dynamodb.AttributeType.STRING
            ),
        )

        # --- Exports for cross-stack references ---

        CfnOutput(self, "DataBucketName", value=self.data_bucket.bucket_name)
        CfnOutput(self, "ModelsBucketName", value=self.models_bucket.bucket_name)
        CfnOutput(self, "GameDayTableName", value=self.game_day_table.table_name)
