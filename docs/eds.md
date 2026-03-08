# Project Overview
This document is an Engineering Design Specification (EDS) used for the Bases Loaded project. The objective of this project is to build a highly cost-optimized, automated data ingestion and machine learning pipeline to predict Major League Baseball (MLB) game outcomes and deliver those predictions via email exactly one hour before first pitch.

# System Architecture

## Overview
This AWS native system will utilize budget friendly tools, such as AWS Lambda functions, Step Functions and EC2 Spot Instances, maximizing the AWS Free Tier. It operates on a hybrid cycle: an overnight batch process for data ingestion and feature generation, paired with precision event-driven cycle for game-time inferences and email delivery

This architecture is divided into four cost-optimized pillars:
1. Data Ingestion & Lakehouse: Serverless scraping to cheap object storage
2. Data Processing: in-memory processing using fast, lightweight libraries
3. Machine Learning: EC2 Spot Instance training with basic artifact storage
4. Inference & Delivery: 100% serverless, zero-idle-cost prediction and email dispatch

## 1. Data Ingestion & Storage
Baseball prediction requires granular data, including pitch-by-pitch data (Statcast), game logs, plauyer stats, and weather forecasts

- Primary Data Sources: 
  - MLB Stats API (official live and historical data)
  - PyBaseball (Python library scraping FanGraphs, Baseball-Reference and StatCast)
  - OpenWeather API (stadium weather forecasts)
- Ingestion Compute: AWS Lambda. Scheduled functions will pull daily delta updates (i.e. newly completed games). For the initial historical data backfill, a local script will be ran on a local machine. The results will then be uploaded directly to S3.
- Storage (Data Lake): Amazon S3. We will use standard S3 for active data and utilize S3 Glacier for archiving old seasons if needed
- Orchestration: AWS Step Functions (Express Workflows) triggered by a daily Amazon EventBridge cron rule. Step Functions will provide excellent visual DAG (Directed Acyclic Graph) execution for the ingestion pipeline at a low cost.

## 2. Data Processing & Feature Engineering
Raw data much be aggregated to represent the *state* of a team entering a specific game. Baseball prediction is heavily dependent on rolling averages and contextual stats.
- Processing Engine: AWS Lambda configured with higher memory (e.g. 2GB-4GB)
- Data Transformation: we will use Polars, a fast DataFrame library written in Rust. Polars handles in-memory processing of gigabytes of tabular data in seconds, keeping Lambda execution times and costs extremely low.
- Feature Engineering: Polars processing layer will aggregate four categories of features: Starting Pitching context, Bullpen Fatigue, Offensive splits by handedness, and Environmental factors (weather/park effects)
- Feature Store: the processing Lambda writes a finalized "Game Day State" (the daily features for all teams) to an Amazon DynamoDB table. DynamoDB provides single-digit millisecond read times for the inference layer and easily fits within the AWS Free Tier.

## 3. Machine Learning Pipeline
Because baseball's underlying dynamics change slowly over a season, training a model every single day is unnecessary. Weekly or bi-weekly retrains are sufficient
- Algorithm: XGBoost. Gradient Boosted Trees consistently outperform deep neural networks on tabular sports data because they handle non-linear relationships and feature interactions exceptionally well
- Training Pipeline: to keep costs strictly minimized, training is decoupled from the daily cloud pipeline
  - The training will be done via an Amazon EventBridge rule to spin up a cheap Amazon EC2 Spot Instance once a week
  - The script trains the model, evaluates it (using Log Loss and Brier Score), and output a model artifact
- Model Registry: a simple S3 bucket acts as the registry. The latest, best-performing model is uploaded here as `latest_model.json`

## 4. Inference & Delivery (Email Report System)
The serving layer operates on a precise schedule dictated by the daily MLB slate, generating and emailing a report exactly one hour before first pitch. Because XGBoost models are highly compact, we can run inference directly inside a Lambda function
- Morning Game Slate Fetch: an AWS Lambda runs daily at 6:00 AM EST to query the MLB Stats API for the day's schedule and official start times
- Dynamic Event Scheduling: for each game on the slate, the morning Lambda creates a one-time Amazon EventBridge Scheduler task configured to trigger exactly 60 minutes before that specific game's scheduled start time
- Embedded Inference: at t-minus 60 minutes, the Inference Lambda is triggered:
  1. It downloads `latest_model.json` from S3 (caching it in memory for subsequent invocations that day) ***how can lambda access this cache in future invocations?***
  2. It queries DynamoDB for the daily features of the two teams playing
  3. It passes the feature vector to the local XGBoost model to generate win probability
- Reporting Formatting: using Jinja2, the Lambda formats the probability and key contextual drivers (i.e. weather, pitching advantages) into a clean HTML email template
- Email Dispatch: the Lambda passes the formatted HTML payload to Amazon SES