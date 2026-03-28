import os

import boto3

ec2 = boto3.client("ec2")

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
sns = boto3.client("sns") if SNS_TOPIC_ARN else None


def _notify(subject, message):
    if sns:
        try:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
        except Exception as e:
            print(f"WARNING: Failed to publish SNS notification: {e}")


def handler(event, context):
    _notify("Bases Loaded ML Spot Launcher: Started", "ML Spot Launcher started.")

    try:
        response = ec2.run_instances(
            LaunchTemplate={
                "LaunchTemplateName": "bases-loaded-ml-training",
            },
            MinCount=1,
            MaxCount=1,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": "bases-loaded-ml-training"},
                        {"Key": "Project", "Value": "bases-loaded"},
                    ],
                }
            ],
        )
        instance_id = response["Instances"][0]["InstanceId"]
        print(f"Launched training instance: {instance_id}")

        _notify(
            "Bases Loaded ML Spot Launcher: Success",
            f"ML Spot Launcher completed successfully. Instance: {instance_id}",
        )

        return {"instance_id": instance_id}

    except Exception as e:
        _notify(
            "Bases Loaded ML Spot Launcher: FAILURE",
            f"ML Spot Launcher failed: {e}",
        )
        raise
