# CLAUDE.md

# currentDate
Today's date is 2026-03-07.

## Project: Bases Loaded
An automated MLB game outcome prediction pipeline that ingests baseball data, engineers features, trains an XGBoost model, and delivers win-probability predictions via email one hour before first pitch.

## Architecture
AWS-native, cost-optimized system with four pillars:

1. **Data Ingestion & Storage** — Lambda scrapers (MLB Stats API, PyBaseball, OpenWeather API) write daily deltas to S3. Step Functions (Express Workflows) orchestrated by EventBridge cron. Historical backfill runs locally then uploads to S3.
2. **Data Processing & Feature Engineering** — High-memory Lambda (2-4 GB) uses Polars for in-memory transforms. Writes finalized "Game Day State" features to DynamoDB.
3. **ML Training** — XGBoost trained weekly on EC2 Spot Instance via EventBridge trigger. Model artifact stored in S3 as `latest_model.json`. Evaluated with Log Loss and Brier Score.
4. **Inference & Delivery** — Morning Lambda fetches MLB slate at 6 AM EST, creates per-game EventBridge Scheduler tasks for t-60 min triggers. Inference Lambda loads model from S3, reads features from DynamoDB, generates win probabilities, formats HTML email via Jinja2, dispatches via Amazon SES.

## Key Data Sources
- **MLB Stats API** — live and historical game data
- **PyBaseball** — FanGraphs, Baseball-Reference, Statcast scraping
- **OpenWeather API** — stadium weather forecasts

## ML Features (see `docs/features.md` for full spec)
- **Target variable:** `target_home_win` (1 = home win, 0 = away win), one row per game
- **Feature categories:** Starting Pitching (SIERA, FIP, K-BB%, rest/pitch count), Offensive Splits (wRC+, wOBA, K%, ISO vs SP handedness), Bullpen (pitch count 3d, xFIP, closer availability), Contextual/Environmental (park factors, weather, travel fatigue)
- **Naming convention:** team-specific features prefixed with `home_` or `away_`

## Tech Stack
- **Language:** Python
- **Data processing:** Polars
- **ML:** XGBoost
- **Templating:** Jinja2
- **Cloud:** AWS (Lambda, Step Functions, S3, DynamoDB, EventBridge, SES, EC2 Spot)
- **IaC:** AWS CDK (Python)
- **CI/CD:** GitHub Actions

## Monorepo Structure
```
shared/          # Cross-pillar AWS utilities and config
ingestion/       # Pillar 1: Data ingestion (future)
processing/      # Pillar 2: Feature engineering (future)
ml/              # Pillar 3: ML training pipeline
inference/       # Pillar 4: Inference & delivery (future)
infra/           # AWS CDK app (all stacks)
.github/         # GitHub Actions deploy workflows
docs/            # Architecture and feature specs
```

## Commands
- **Run ML training:** `python -m ml.train` (from repo root)
- **Install ML deps:** `pip install -r ml/requirements.txt`
- **Install CDK deps:** `pip install -r infra/requirements.txt`
- **CDK synth:** `cd infra && cdk synth`
- **CDK deploy (all):** `cd infra && cdk deploy --all`
- **CDK deploy (one stack):** `cd infra && cdk deploy BasesLoadedMl`

## Deployment
- **IaC:** AWS CDK with one stack per pillar plus a shared stack
- **CDK stacks:** `BasesLoadedShared`, `BasesLoadedIngestion`, `BasesLoadedProcessing`, `BasesLoadedMl`, `BasesLoadedInference`
- **CI/CD:** GitHub Actions with path-filtered workflows — changes to a pillar only deploy that pillar's stack
- **Auth:** OIDC role assumption via `AWS_DEPLOY_ROLE_ARN` secret (no long-lived keys)
- All workflows also trigger on `shared/` changes since shared code affects all pillars

## Conventions
- Monorepo: each pillar is a top-level Python package with its own `requirements.txt`
- `shared/` contains cross-cutting AWS helpers (DynamoDB, S3) and central config
- Feature schema codified in `ml/config.py`, sourced from `docs/features.md`
- Run modules from repo root with `python -m <package>.<module>`
- S3 Standard for active data, Glacier for archival
- DynamoDB for low-latency feature reads at inference time
- Weekly model retraining (baseball dynamics change slowly)
- All inference runs serverless with zero idle cost
