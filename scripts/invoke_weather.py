"""Invoke the weather scraper Lambda for a specific season.

Usage:
    python3 scripts/invoke_weather.py 2025
"""

import json
import sys

import boto3
from botocore.config import Config

REGION = "us-east-2"

client = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=900))


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/invoke_weather.py <season>")
        sys.exit(1)

    season = int(sys.argv[1])
    print(f"Invoking weather scraper for {season}...")

    resp = client.invoke(
        FunctionName="bases-loaded-weather-scraper",
        InvocationType="RequestResponse",
        Payload=json.dumps({"season": season}),
    )
    result = json.loads(resp["Payload"].read())

    if resp.get("FunctionError"):
        print(f"ERROR: {result}")
        sys.exit(1)
    else:
        print(f"Result: {json.dumps(result, indent=2)}")


if __name__ == "__main__":
    main()
