# Bases Loaded

A fully automated data ingestion and machine learning pipeline to predict Major Leaguge Baseball (MLB) game outcomes.

## Remaining Work

- [ ] **SNS alerting** — add start, failure, and success notifications via the existing SNS topic to every Lambda and job across all pillars (ingestion scrapers, processing, ML training, slate fetcher, predict)
- [ ] **Subscriber management** — build a self-service way for new users to subscribe to the prediction email list (e.g. a simple web form backed by API Gateway + Lambda that appends to the SSM subscriber list). Request SES production access before launch so unverified recipients can receive emails.
