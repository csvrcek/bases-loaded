import boto3


ec2 = boto3.client("ec2")


def handler(event, context):
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
    return {"instance_id": instance_id}
