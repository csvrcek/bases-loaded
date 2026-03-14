"""Load Parquet data from S3 into Polars DataFrames."""

import io

import boto3
import polars as pl

from shared.config import AWS_REGION
from processing.config import S3_BUCKET_DATA, S3_PATHS


def load_parquet(bucket: str, key: str) -> pl.DataFrame:
    """Read a single Parquet file from S3 into a Polars DataFrame."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()
    return pl.read_parquet(io.BytesIO(data))


def load_season_data(season: int) -> dict[str, pl.DataFrame]:
    """Load all raw data for a season from S3.

    Returns a dict mapping dataset name to Polars DataFrame.
    Missing datasets return empty DataFrames (with a warning).
    """
    bucket = S3_BUCKET_DATA
    data = {}

    for name, path_template in S3_PATHS.items():
        key = path_template.format(season=season)
        try:
            data[name] = load_parquet(bucket, key)
            print(f"Loaded {name}: {len(data[name])} rows from s3://{bucket}/{key}")
        except Exception as e:
            print(f"WARNING: Could not load {name} for season {season}: {e}")
            data[name] = pl.DataFrame()

    return data
