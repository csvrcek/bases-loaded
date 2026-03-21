# Bases Loaded

A fully automated data ingestion and machine learning pipeline to predict Major Leaguge Baseball (MLB) game outcomes.

## Remaining Work

### Pillar 4 — Inference & Delivery

`inference/` and `infra/stacks/inference_stack.py` are stubs. Everything needed:

- [ ] **Slate fetcher Lambda** — 6 AM EST daily, hits MLB Stats API for today's games
- [ ] **Per-game scheduler** — creates EventBridge Scheduler tasks for t-60 min before each first pitch
- [ ] **Inference Lambda** — loads model from S3, reads features from DynamoDB, generates win probabilities
- [ ] **Jinja2 email template** — HTML email with matchup cards and win probabilities
- [ ] **SES dispatch** — sends email to subscribers
- [ ] **CDK `BasesLoadedInference` stack** — wires all of the above

---

### Event-Driven Ingestion → Processing Trigger

Currently Layer 1 (Ingestion) and Layer 2 (Processing) run on independent time-based schedules (8 AM UTC and 5 AM UTC respectively) with no dependency between them. The goal is for Layer 1's successful completion to directly trigger Layer 2.

**Approach:** The existing Step Functions state machine already ends with an SNS success notification. Add an EventBridge `PutEvents` task as the final step to emit a custom `IngestionCompleted` event. The Processing stack replaces its cron schedule with an EventBridge rule that listens for that event. The stacks stay fully decoupled — ingestion fires an event, processing listens for it.

No Lambda code changes required — infrastructure only.

#### Files to touch

| File | Change |
| --- | --- |
| `infra/stacks/ingestion_stack.py` | Add `tasks.EventBridgePutEvents` as the final Step Functions step, emitting `source: "bases-loaded.ingestion"` / `detail-type: "IngestionCompleted"` to the default EventBridge bus on success; grant state machine `events:PutEvents` |
| `infra/stacks/processing_stack.py` | Replace the `5 AM UTC` cron `events.Rule` with an EventBridge rule matching `source: ["bases-loaded.ingestion"]` / `detail-type: ["IngestionCompleted"]` that targets the processing Lambda |
| `infra/app.py` | No changes — stacks remain fully decoupled |

---

## Historical Backfill (required before first model training)

The model needs 2020–2025 historical game data loaded into S3 and DynamoDB before training can run.
`scripts/backfill.py` orchestrates the deployed Lambda scrapers to load all historical data.

### Prerequisites

1. Ingestion stack deployed: `cd infra && cdk deploy BasesLoadedIngestion`
2. AWS credentials configured with permission to invoke the Lambda functions
3. `boto3` installed locally: `pip install boto3`

### Running the backfill

```bash
# All seasons (2020–2025)
python scripts/backfill.py

# Specific seasons
python scripts/backfill.py --seasons 2024 2025

# Single season
python scripts/backfill.py --seasons 2025
```

For each season the script invokes three Lambdas in sequence:

1. **MLB Stats scraper** — fetches the full season schedule + boxscores via `start_date`/`end_date`
2. **PyBaseball scraper** — fetches season-to-date pitching stats, batting splits, and park factors
3. **Weather scraper** (backfill mode) — reads game_logs from S3 for venue mappings, fetches historical observations via Meteostat

### Verifying the backfill

Check that Parquet files were written to S3 for each season:

```bash
# List all raw data files
aws s3 ls s3://bases-loaded-data/raw/ --recursive --human-readable

# Spot-check a specific season
aws s3 ls s3://bases-loaded-data/raw/game_logs/2024/
aws s3 ls s3://bases-loaded-data/raw/pitcher_game_logs/2024/
aws s3 ls s3://bases-loaded-data/raw/team_batting/2024/
aws s3 ls s3://bases-loaded-data/raw/schedules/2024/
aws s3 ls s3://bases-loaded-data/raw/weather/2024/
aws s3 ls s3://bases-loaded-data/raw/pitcher_stats/2024/
aws s3 ls s3://bases-loaded-data/raw/team_batting_splits/2024/
aws s3 ls s3://bases-loaded-data/raw/park_factors/
```

Expected files per season: `game_logs.parquet`, `pitcher_game_logs.parquet`, `team_batting.parquet`, `schedules.parquet`, `weather.parquet`, `pitcher_stats.parquet`, `team_batting_splits.parquet`. Park factors are season-independent (single file).
