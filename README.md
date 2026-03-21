# Bases Loaded

A fully automated data ingestion and machine learning pipeline to predict Major Leaguge Baseball (MLB) game outcomes.

## TODO

- [ ] Event driven
  - [x] Layer 1 should trigger layer 2 — see plan below
  - [ ] Can layer 1 be triggered by an event as well?

---

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

### Relocate Scraper Code to `ingestion/`

The scraper Lambda code currently lives under `infra/runtime/` alongside the Docker build context, but the `ingestion/` top-level package is empty. For consistency with the other pillars (`processing/`, `ml/`), the business logic should live in `ingestion/` and `infra/runtime/` should only contain the Dockerfiles and entrypoints that import from it.

#### Ingestion files to touch

| Action | Detail |
| --- | --- |
| Move `infra/runtime/mlb_stats_scraper/index.py` → `ingestion/mlb_stats_scraper.py` | Core scraping logic |
| Move `infra/runtime/pybaseball_scraper/index.py` → `ingestion/pybaseball_scraper.py` | Core scraping logic |
| Move `infra/runtime/weather_scraper/index.py` → `ingestion/weather_scraper.py` | Core scraping logic |
| Move `infra/runtime/weather_scraper/venues.py` → `ingestion/venues.py` | Venue metadata |
| Update `infra/runtime/*/Dockerfile` | Install `ingestion/` package and import from it instead of defining logic inline |
| Update `infra/stacks/ingestion_stack.py` | Adjust Docker build context paths if needed |
| Add `ingestion/requirements.txt` and `ingestion/.venv/` | Match the per-pillar venv convention |

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

### Historical Backfill (required before first model training)

The model needs 2020–2024 historical game data loaded into S3 and DynamoDB before training can run.
The plan is to refactor the existing Lambda scrapers to support a backfill mode, then orchestrate them from a local script.

#### Lambda changes

**`infra/runtime/mlb_stats_scraper/index.py`**

- Add `mode: "backfill"` to the handler
- When backfill: accept `season` + `month` params, call `fetch_schedule(season, date=None)` (path already exists) and process all completed games for that month
- Chunking by month keeps each invocation under the 15-min Lambda timeout (~200 games vs ~2430 for a full season)
- Increase Lambda timeout from 5 min → 15 min in CDK (`infra/stacks/ingestion_stack.py`)

**`infra/runtime/pybaseball_scraper/index.py`**

- No changes needed — already fetches by season, just invoke with `{"season": YYYY}`

**`infra/runtime/weather_scraper/index.py`**

- Add `mode: "backfill"` to the handler — OpenWeather only provides forecasts so historical data needs a different source
- In backfill mode: read the season's `game_logs.parquet` from S3 to get `game_id → venue` mappings, then use **Meteostat** for historical observations
- Domed stadiums stay the same (zeroed out)
- Add `meteostat` to the weather scraper's `requirements.txt` and Docker image

#### New file: `scripts/backfill.py`

Thin local orchestrator — invokes the refactored Lambdas via boto3 in sequence, then triggers processing:

```python
for season in [2020, 2021, 2022, 2023, 2024]:
    for month in range(1, 13):
        invoke("MlbStatsScraper",  {"mode": "backfill", "season": season, "month": month})
    invoke("PybaseballScraper", {"season": season})
    invoke("WeatherScraper",    {"mode": "backfill", "season": season})
    invoke("ProcessingLambda",  {"mode": "backfill", "season": season})
```

#### Backfill files to touch

| File | Change |
| --- | --- |
| `infra/runtime/mlb_stats_scraper/index.py` | Add `mode: "backfill"` with month-chunked season fetch |
| `infra/runtime/weather_scraper/index.py` | Add `mode: "backfill"` using Meteostat |
| `infra/runtime/weather_scraper/requirements.txt` | Add `meteostat` |
| `infra/stacks/ingestion_stack.py` | Bump MLB Stats Lambda timeout to 15 min |
| `scripts/backfill.py` | New — boto3 orchestrator script |
