#!/usr/bin/env python3
"""CDK app entry point for Bases Loaded infrastructure."""

import aws_cdk as cdk

from stacks.github_oidc_stack import GitHubOidcStack
from stacks.shared_stack import SharedStack
from stacks.ingestion_stack import IngestionStack
from stacks.processing_stack import ProcessingStack
from stacks.ml_stack import MlStack
from stacks.inference_stack import InferenceStack

app = cdk.App()
cdk.Tags.of(app).add("Project", "bases-loaded")

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-2",
)

# --- GitHub OIDC + deploy role ---
GitHubOidcStack(app, "BasesLoadedGitHubOidc", env=env)

# --- Shared resources (S3, DynamoDB) ---
shared = SharedStack(app, "BasesLoadedShared", env=env)

# --- Pillar stacks ---
IngestionStack(
    app,
    "BasesLoadedIngestion",
    data_bucket=shared.data_bucket,
    notifications_topic=shared.notifications_topic,
    env=env,
)

ProcessingStack(
    app,
    "BasesLoadedProcessing",
    data_bucket=shared.data_bucket,
    game_day_table=shared.game_day_table,
    notifications_topic=shared.notifications_topic,
    env=env,
)

MlStack(
    app,
    "BasesLoadedMl",
    models_bucket=shared.models_bucket,
    game_day_table=shared.game_day_table,
    notifications_topic=shared.notifications_topic,
    env=env,
)

InferenceStack(
    app,
    "BasesLoadedInference",
    data_bucket=shared.data_bucket,
    models_bucket=shared.models_bucket,
    game_day_table=shared.game_day_table,
    notifications_topic=shared.notifications_topic,
    env=env,
)

app.synth()
