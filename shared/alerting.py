"""Reusable SNS alerting decorator for Lambda handlers.

Wraps any Lambda handler to publish start, success, and failure
notifications to the shared SNS topic.
"""

import functools
import json
import os

import boto3


def _publish(sns_client, topic_arn: str, subject: str, message: str) -> None:
    """Publish to SNS, swallowing errors so alerting never breaks the pipeline."""
    try:
        sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except Exception as e:
        print(f"WARNING: Failed to publish SNS notification: {e}")


def sns_alert(component: str):
    """Decorator that sends SNS start/success/failure notifications.

    Usage::

        @sns_alert("Processing")
        def lambda_handler(event, context):
            ...

    Reads ``SNS_TOPIC_ARN`` from the environment. If the variable is unset
    the handler runs without notifications (useful for local testing).
    """

    def decorator(handler_fn):
        @functools.wraps(handler_fn)
        def wrapper(event, context):
            topic_arn = os.environ.get("SNS_TOPIC_ARN")
            sns = boto3.client("sns") if topic_arn else None

            if sns:
                _publish(
                    sns,
                    topic_arn,
                    f"Bases Loaded {component}: Started",
                    f"{component} started.",
                )

            try:
                result = handler_fn(event, context)

                if sns:
                    body = json.dumps(result, default=str) if result else ""
                    _publish(
                        sns,
                        topic_arn,
                        f"Bases Loaded {component}: Success",
                        f"{component} completed successfully.\n{body}",
                    )

                return result

            except Exception as e:
                if sns:
                    _publish(
                        sns,
                        topic_arn,
                        f"Bases Loaded {component}: FAILURE",
                        f"{component} failed: {e}",
                    )
                raise

        return wrapper

    return decorator
