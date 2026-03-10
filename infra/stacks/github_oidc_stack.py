"""GitHub OIDC identity provider and deploy role for GitHub Actions CI/CD."""

from aws_cdk import (
    CfnOutput,
    Stack,
    aws_iam as iam,
)
from constructs import Construct


class GitHubOidcStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        repo_owner: str = "csvrcek",
        repo_name: str = "bases-loaded",
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # --- OIDC Identity Provider for GitHub Actions ---

        oidc_provider = iam.OpenIdConnectProvider(
            self,
            "GitHubOidcProvider",
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"],
        )

        # --- Deploy Role assumed by GitHub Actions ---

        deploy_role = iam.Role(
            self,
            "GitHubDeployRole",
            role_name="bases-loaded-github-deploy",
            assumed_by=iam.WebIdentityPrincipal(
                oidc_provider.open_id_connect_provider_arn,
                conditions={
                    "StringEquals": {
                        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                    },
                    "StringLike": {
                        "token.actions.githubusercontent.com:sub": f"repo:{repo_owner}/{repo_name}:*",
                    },
                },
            ),
            managed_policies=[
                # CDK needs CloudFormation, S3 (asset staging), SSM (lookups), IAM (resource roles)
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSCloudFormationFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name("IAMFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMReadOnlyAccess"
                ),
            ],
        )

        # S3 — CDK asset bucket + application buckets
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                resources=[
                    f"arn:aws:s3:::cdk-*-{self.account}-{self.region}",
                    f"arn:aws:s3:::cdk-*-{self.account}-{self.region}/*",
                    f"arn:aws:s3:::bases-loaded-*-{self.account}",
                    f"arn:aws:s3:::bases-loaded-*-{self.account}/*",
                ],
            )
        )

        # DynamoDB — GameDayState table
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/GameDayState",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/GameDayState/*",
                ],
            )
        )

        # EC2 — launch templates, spot instances (ML stack)
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ec2:*"],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "aws:ResourceTag/Project": "bases-loaded",
                    }
                },
            )
        )
        # EC2 describe actions (untagged, read-only)
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:Describe*",
                    "ec2:CreateLaunchTemplate",
                    "ec2:CreateLaunchTemplateVersion",
                    "ec2:DeleteLaunchTemplate",
                ],
                resources=["*"],
            )
        )

        # Lambda — deploy functions (launcher, inference, etc.)
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:*"],
                resources=[
                    f"arn:aws:lambda:{self.region}:{self.account}:function:BasesLoaded*",
                ],
            )
        )

        # EventBridge Scheduler — schedules for training + inference
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["scheduler:*"],
                resources=[
                    f"arn:aws:scheduler:{self.region}:{self.account}:schedule/*",
                ],
            )
        )

        # SES — inference delivery (future)
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ses:*"],
                resources=["*"],
            )
        )

        # --- Outputs ---

        CfnOutput(
            self,
            "DeployRoleArn",
            value=deploy_role.role_arn,
            description="ARN for GitHub Actions AWS_DEPLOY_ROLE_ARN secret",
        )
