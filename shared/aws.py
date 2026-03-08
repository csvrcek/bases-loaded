from decimal import Decimal

import boto3

from shared.config import AWS_REGION


def scan_dynamodb_table(table_name: str) -> list[dict]:
    """Full paginated scan of a DynamoDB table. Converts Decimal values to float/int."""
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(table_name)

    items = []
    response = table.scan()
    items.extend(response["Items"])

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response["Items"])

    return [_convert_decimals(item) for item in items]


def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    """Upload a local file to S3."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_file(local_path, bucket, key)


def _convert_decimals(obj):
    """Recursively convert DynamoDB Decimal types to Python float/int."""
    if isinstance(obj, Decimal):
        if obj == int(obj):
            return int(obj)
        return float(obj)
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_decimals(i) for i in obj]
    return obj
