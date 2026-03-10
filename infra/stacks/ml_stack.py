"""ML Training stack: EC2 Spot Instance launched weekly via EventBridge.

The instance bootstraps itself via user data, runs the training pipeline,
uploads the model to S3, then terminates itself.
"""

from aws_cdk import (
    Duration,
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_scheduler as scheduler,
)
from constructs import Construct


TRAINING_USER_DATA = """#!/bin/bash
set -euo pipefail

# Log everything for debugging
exec > /var/log/ml-training.log 2>&1

echo "Starting ML training pipeline..."

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

echo "Training complete. Shutting down..."

# Self-terminate
INSTANCE_ID=$(ec2-metadata -i | cut -d' ' -f2)
aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region {region}
"""


class MlStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        models_bucket: s3.IBucket,
        game_day_table: dynamodb.ITable,
        repo_owner: str = "csvrcek",
        repo_name: str = "bases-loaded",
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # --- IAM Role for EC2 training instance ---

        training_role = iam.Role(
            self,
            "TrainingRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )

        # Read features from DynamoDB
        game_day_table.grant_read_data(training_role)

        # Write model artifact to S3
        models_bucket.grant_write(training_role)

        # Allow self-termination
        training_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ec2:TerminateInstances"],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "aws:ResourceTag/Project": "bases-loaded",
                    }
                },
            )
        )

        instance_profile = iam.CfnInstanceProfile(
            self,
            "TrainingInstanceProfile",
            roles=[training_role.role_name],
        )

        # --- Launch Template for Spot Instance ---

        user_data = ec2.UserData.for_linux()
        user_data.add_commands(
            TRAINING_USER_DATA.format(
                repo_owner=repo_owner,
                repo_name=repo_name,
                region=self.region,
            )
        )

        launch_template = ec2.LaunchTemplate(
            self,
            "TrainingLaunchTemplate",
            launch_template_name="bases-loaded-ml-training",
            instance_type=ec2.InstanceType("t3.medium"),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            user_data=user_data,
            role=training_role,
            spot_options=ec2.LaunchTemplateSpotOptions(
                request_type=ec2.SpotRequestType.ONE_TIME,
            ),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        20, volume_type=ec2.EbsDeviceVolumeType.GP3
                    ),
                )
            ],
        )

        # Tag for self-termination scoping
        from aws_cdk import Tags

        Tags.of(launch_template).add("Project", "bases-loaded")

        # --- EventBridge Scheduler: trigger weekly training ---

        # --- Lambda to launch the Spot Instance ---

        from aws_cdk import aws_lambda as _lambda

        launcher_role = iam.Role(
            self,
            "LauncherRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )

        launcher_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:RunInstances",
                    "ec2:CreateTags",
                    "iam:PassRole",
                ],
                resources=["*"],
            )
        )

        launcher_fn = _lambda.Function(
            self,
            "SpotLauncherFn",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_inline(
                f"""
import boto3
import os

ec2 = boto3.client("ec2")

def handler(event, context):
    response = ec2.run_instances(
        LaunchTemplate={{
            "LaunchTemplateName": "bases-loaded-ml-training",
        }},
        MinCount=1,
        MaxCount=1,
        TagSpecifications=[
            {{
                "ResourceType": "instance",
                "Tags": [
                    {{"Key": "Name", "Value": "bases-loaded-ml-training"}},
                    {{"Key": "Project", "Value": "bases-loaded"}},
                ],
            }}
        ],
    )
    instance_id = response["Instances"][0]["InstanceId"]
    print(f"Launched training instance: {{instance_id}}")
    return {{"instance_id": instance_id}}
"""
            ),
            role=launcher_role,
            timeout=Duration.seconds(30),
        )

        scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        launcher_fn.grant_invoke(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "WeeklyTrainingSchedule",
            schedule_expression="cron(0 6 ? * MON *)",
            schedule_expression_timezone="UTC",
            description="Trigger weekly ML model training every Monday at 6 AM UTC",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=launcher_fn.function_arn,
                role_arn=scheduler_role.role_arn,
            ),
        )
