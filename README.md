# Bases Loaded

A fully automated data ingestion and machine learning pipeline to predict Major Leaguge Baseball (MLB) game outcomes.

## Remaining Work

- [ ] **SES setup**
  1. Open the [SES console](https://us-east-2.console.aws.amazon.com/ses/home?region=us-east-2#/identities) in us-east-2
  2. Click **Create identity** → choose **Email address** → enter the sender address → click **Create**
  3. Open the verification email and click the confirmation link
  4. (Production) Go to **Account dashboard** → **Request production access** to send to unverified recipients
- [ ] **SSM parameters**
  1. Open the [SSM Parameter Store console](https://us-east-2.console.aws.amazon.com/systems-manager/parameters?region=us-east-2)
  2. Create parameter `/bases-loaded/ses-sender` (String) → set value to the verified sender email from step above
  3. Create parameter `/bases-loaded/subscribers` (String) → set value to a comma-separated list of recipient emails (e.g. `alice@example.com,bob@example.com`)
- [ ] **Subscriber management** — build a self-service way for new users to subscribe to the prediction email list (e.g. a simple web form backed by API Gateway + Lambda that appends to the SSM subscriber list)
