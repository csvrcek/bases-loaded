"""Historical backfill orchestrator.

Invokes the deployed Lambda scrapers via boto3 to load 2020–2025 historical
data into S3. Run this locally after deploying the ingestion stack.

Usage:
    python scripts/backfill.py                  # all seasons 2020–2025
    python scripts/backfill.py --seasons 2024   # single season
    python scripts/backfill.py --seasons 2023 2024 2025  # specific seasons
"""

import argparse
import json
import sys
import time

import boto3

REGION = "us-east-2"
LAMBDA_CLIENT = boto3.client("lambda", region_name=REGION)

MLB_STATS_FN = "bases-loaded-mlb-stats-scraper"
PYBASEBALL_FN = "bases-loaded-pybaseball-scraper"
WEATHER_FN = "bases-loaded-weather-scraper"

DEFAULT_SEASONS = [2020, 2021, 2022, 2023, 2024, 2025]


def invoke(function_name: str, payload: dict) -> dict:
    """Invoke a Lambda function synchronously and return the parsed response."""
    print(f"  Invoking {function_name} with {json.dumps(payload)}")
    resp = LAMBDA_CLIENT.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),
    )
    result = json.loads(resp["Payload"].read())

    if resp.get("FunctionError"):
        print(f"  ERROR: {function_name} failed: {result}")
        return {"status": "error", "error": result}

    print(f"  Result: {json.dumps(result)}")
    return result


def backfill_season(season: int) -> None:
    """Run full backfill for a single season."""
    print(f"\n{'='*60}")
    print(f"Backfilling season {season}")
    print(f"{'='*60}")

    # 1. MLB Stats — full season in one call
    print(f"\n[1/3] MLB Stats scraper ({season})")
    invoke(MLB_STATS_FN, {
        "start_date": f"01/01/{season}",
        "end_date": f"12/31/{season}",
    })

    # 2. PyBaseball — already supports season param
    print(f"\n[2/3] PyBaseball scraper ({season})")
    invoke(PYBASEBALL_FN, {"season": season})

    # 3. Weather — reads game_logs from S3 for the full season
    print(f"\n[3/3] Weather scraper ({season})")
    invoke(WEATHER_FN, {"season": season})


def main():
    parser = argparse.ArgumentParser(description="Backfill historical MLB data (2020–2025)")
    parser.add_argument(
        "--seasons",
        type=int,
        nargs="+",
        default=DEFAULT_SEASONS,
        help="Seasons to backfill (default: 2020–2025)",
    )
    args = parser.parse_args()

    print(f"Starting backfill for seasons: {args.seasons}")
    start = time.time()

    for season in sorted(args.seasons):
        backfill_season(season)

    elapsed = time.time() - start
    print(f"\nBackfill complete! Processed {len(args.seasons)} seasons in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
