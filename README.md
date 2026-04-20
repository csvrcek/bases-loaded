# Bases Loaded

A fully automated data ingestion and machine learning pipeline to predict Major Leaguge Baseball (MLB) game outcomes.

## Remaining Work

- [ ] **Subscriber management** — build a self-service way for new users to subscribe to the prediction email list (e.g. a simple web form backed by API Gateway + Lambda that appends to the SSM subscriber list). Request SES production access before launch so unverified recipients can receive emails.
- [ ] **FanGraphs pitcher ID crosswalk** — PyBaseball scraper stores FanGraphs IDFG as `pitcher_id`, but `pitcher_game_logs` uses MLB Stats API numeric IDs. The SIERA join in `processing/features/pitching.py` silently returns NULLs because the ID systems don't match. Fix by adding a crosswalk table (e.g. from Chadwick Bureau register or `pybaseball.playerid_lookup()`) that maps IDFG → MLB Stats API ID during ingestion or processing.
