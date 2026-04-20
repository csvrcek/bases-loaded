# Bases Loaded

A fully automated data ingestion and machine learning pipeline to predict Major Leaguge Baseball (MLB) game outcomes.

## Remaining Work

- [ ] **Subscriber management** — build a self-service way for new users to subscribe to the prediction email list (e.g. a simple web form backed by API Gateway + Lambda that appends to the SSM subscriber list). Request SES production access before launch so unverified recipients can receive emails.
- [ ] **FanGraphs pitcher ID crosswalk** — PyBaseball scraper stores FanGraphs IDFG as `pitcher_id`, but `pitcher_game_logs` uses MLB Stats API numeric IDs. The SIERA join in `processing/features/pitching.py` silently returns NULLs because the ID systems don't match. Fix by adding a crosswalk table (e.g. from Chadwick Bureau register or `pybaseball.playerid_lookup()`) that maps IDFG → MLB Stats API ID during ingestion or processing. This also blocks `sp_sierra_std` from appearing in features.
- [ ] **SP handedness** — `pitcher_game_logs.handedness` is always empty because `statsapi.boxscore_data()` doesn't include it. Need to fetch handedness from the MLB Stats API people endpoint (`statsapi.get('person', {'personId': pid})`) or a roster lookup, then populate it during ingestion. Without this, `home_sp_handedness`/`away_sp_handedness` are empty and wRC+/wOBA split lookups can't match on opposing SP hand.
- [ ] **wRC+ and wOBA splits** — `team_wrc_plus_14d_split` and `team_woba_std_split` are NULL because they depend on both FanGraphs team batting split data and SP handedness (see above). Once SP handedness is populated and the PyBaseball scraper runs for 2026, these should flow through.
