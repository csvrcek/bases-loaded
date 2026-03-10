import os

AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
DYNAMODB_TABLE_GAME_DAY_STATE = os.environ.get("DYNAMODB_TABLE", "GameDayState")
S3_BUCKET_MODELS = os.environ.get("S3_BUCKET_MODELS", "bases-loaded-models")
S3_MODEL_KEY = "latest_model.json"
