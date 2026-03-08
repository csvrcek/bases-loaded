"""Inference & Delivery stack: game-time predictions and email dispatch.

Placeholder — Lambda functions and EventBridge Scheduler will be defined
once the inference pillar code is built.
"""

from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
)
from constructs import Construct


class InferenceStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        models_bucket: s3.IBucket,
        game_day_table: dynamodb.ITable,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # TODO: Morning Lambda (6 AM EST) to fetch MLB slate
        # TODO: Per-game EventBridge Scheduler tasks (t-60 min triggers)
        # TODO: Inference Lambda (load model from S3, read features from DynamoDB)
        # TODO: Jinja2 HTML email formatting
        # TODO: SES email dispatch
        pass
