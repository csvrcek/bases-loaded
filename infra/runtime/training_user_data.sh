#!/bin/bash
set -euo pipefail

# Log everything for debugging
exec > /var/log/ml-training.log 2>&1

REGION="{region}"
SNS_TOPIC="{sns_topic_arn}"
export S3_BUCKET_MODELS="{s3_bucket_models}"
INSTANCE_ID=$(ec2-metadata -i | cut -d' ' -f2)

notify() {
    aws sns publish --region "$REGION" --topic-arn "$SNS_TOPIC" \
        --subject "Bases Loaded ML Pipeline: $1" --message "$2"
}

notify "Started" "Training pipeline started on instance $INSTANCE_ID."

trap 'notify "Failed" "Training pipeline failed on instance $INSTANCE_ID. Check /var/log/ml-training.log via SSM."' ERR

# Install dependencies
yum update -y
yum install -y python3.11 python3.11-pip git

# Clone repo and install deps
cd /tmp
git clone https://github.com/{repo_owner}/{repo_name}.git bases-loaded
cd bases-loaded
python3.11 -m pip install -r ml/requirements.txt

# Run training
python3.11 -m ml.train

notify "Completed" "Training pipeline completed successfully on instance $INSTANCE_ID."

echo "Training complete. Shutting down..."

# Self-terminate
aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region $REGION
