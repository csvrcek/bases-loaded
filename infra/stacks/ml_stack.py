"""ML Training stack: EC2 Spot Instance launched weekly via EventBridge.

The instance bootstraps itself via user data, runs the training pipeline,
uploads the model to S3, then terminates itself.
"""

from pathlib import Path

from aws_cdk import (
    Duration,
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_scheduler as scheduler,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    Tags,
)
from constructs import Construct

RUNTIME_DIR = Path(__file__).resolve().parent.parent / "runtime"


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

        # --- SNS Topic for pipeline notifications ---

        self.notifications_topic = sns.Topic(
            self,
            "MlPipelineNotifications",
            topic_name="bases-loaded-ml-notifications",
        )
        notification_email = self.node.try_get_context("notification_email")
        if notification_email:
            self.notifications_topic.add_subscription(
                subs.EmailSubscription(notification_email)
            )

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

        # Publish training notifications
        self.notifications_topic.grant_publish(training_role)

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

        user_data_script = (RUNTIME_DIR / "training_user_data.sh").read_text()
        user_data_script = (
            user_data_script.replace("{repo_owner}", repo_owner)
            .replace("{repo_name}", repo_name)
            .replace("{region}", self.region)
            .replace("{sns_topic_arn}", self.notifications_topic.topic_arn)
        )
        user_data = ec2.UserData.for_linux()
        user_data.add_commands(user_data_script)

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

        Tags.of(launch_template).add("Project", "bases-loaded")

        # --- Lambda to launch the Spot Instance ---

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
            code=_lambda.Code.from_asset(str(RUNTIME_DIR / "spot_launcher")),
            role=launcher_role,
            timeout=Duration.seconds(30),
        )

        # --- EventBridge Scheduler: trigger weekly training ---

        scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        launcher_fn.grant_invoke(scheduler_role)

        scheduler.CfnSchedule(
            self,
            "WeeklyTrainingSchedule",
            schedule_expression="cron(0 11 ? * MON *)",
            schedule_expression_timezone="UTC",
            description="Trigger weekly ML model training every Monday at 11 AM UTC",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=launcher_fn.function_arn,
                role_arn=scheduler_role.role_arn,
            ),
        )
