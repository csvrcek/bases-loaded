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
