from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    Tags,
    aws_budgets as budgets,
    aws_s3 as s3,
)
from constructs import Construct


class InfraStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs
    ) -> None:

        super().__init__(scope, construct_id, **kwargs)

        # Create the three layers of our data lake
        for layer in ["bronze", "silver", "gold"]:

            bucket = s3.Bucket(
                self,
                f"{layer.capitalize()}Bucket",

                # Prevent public access
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,

                # Encrypt objects using S3-managed keys
                encryption=s3.BucketEncryption.S3_MANAGED,

                # Require HTTPS
                enforce_ssl=True,

                # Preserve older versions of objects
                versioned=True,

                # Expire old versions and clean up failed uploads
                lifecycle_rules=[s3.LifecycleRule(
                    noncurrent_version_expiration=Duration.days(30),
                    abort_incomplete_multipart_upload_after=Duration.days(7),
                )],

                # Keep data if the stack is deleted
                removal_policy=RemovalPolicy.RETAIN,
            )

            # Tag resources for cost tracking
            Tags.of(bucket).add("project", "nyc-risk")
            Tags.of(bucket).add("sp", "SP0")
            Tags.of(bucket).add("layer", layer)

        # Cost guardrail: email when monthly spend passes $5
        budget_email = self.node.try_get_context("budget_email")
        if not budget_email:
            raise ValueError(
                "Missing context 'budget_email'. Pass it with: "
                "cdk deploy -c budget_email=you@example.com"
            )

        budgets.CfnBudget(
            self,
            "MonthlyCostBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name="nyc-risk-monthly",
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(amount=5, unit="USD"),
            ),
            notifications_with_subscribers=[
                budgets.CfnBudget.NotificationWithSubscribersProperty(
                    notification=budgets.CfnBudget.NotificationProperty(
                        notification_type="ACTUAL",
                        comparison_operator="GREATER_THAN",
                        threshold=5,
                        threshold_type="ABSOLUTE_VALUE",
                    ),
                    subscribers=[
                        budgets.CfnBudget.SubscriberProperty(
                            subscription_type="EMAIL",
                            address=budget_email,
                        )
                    ],
                )
            ],
        )
