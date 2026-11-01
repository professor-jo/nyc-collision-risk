# Review notes for InfraStack
#
# Fix first:
# - The ingestion Lambda is created inside the bucket loop, so the second pass
#   reuses the "CollisionIngestionLambda" ID and synth should fail. Move the
#   Lambda and its grant outside the loop.
# - Code.from_asset("..") packages the whole parent directory with no excludes,
#   which can include .git, virtualenvs, cdk.out, tests and .env files. Package
#   only ingestion/, or add exclude patterns.
#
# Check before relying on it:
# - Nothing in this stack invokes the Lambda. Add an EventBridge schedule if
#   ingestion should be recurring.
# - from_asset doesn't install dependencies. Anything the handler imports beyond
#   the standard library and boto3 needs bundling or a layer.
# - Reserved concurrency requires 100 concurrent executions to stay unreserved,
#   so it can fail on a new account with a low quota.
# - Nothing alerts on failed runs. Add a CloudWatch alarm on Lambda errors, and
#   consider a dead-letter queue if invocations will be async.
#
# Worth improving:
# - The budget alert only fires after spend passes $5. Add a forecasted alert or
#   a lower threshold.
# - The budget has no cost filter, so it tracks the whole AWS account.
# - Only the buckets are tagged. Use Tags.of(self) for project and sp, and keep
#   layer on each bucket.
# - Lambda logs are kept forever by default. Set a retention period.
# - Assigning bronze_bucket inside the loop relies on loop order. Store the
#   buckets in a dict instead.

from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    Tags,
    aws_budgets as budgets,
    aws_s3 as s3,
    aws_lambda as lambda_,
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

            if layer == "bronze":
                bronze_bucket = bucket
            # Tag resources for cost tracking
            Tags.of(bucket).add("project", "nyc-risk")
            Tags.of(bucket).add("sp", "SP0")
            Tags.of(bucket).add("layer", layer)

        # Create Lambda
        ingestion_lambda = lambda_.Function(
            self,
            "CollisionIngestionLambda",

            # Use Python 3.11 runtime
            runtime=lambda_.Runtime.PYTHON_3_12,


            # Package the ingestion/ directory as a zip
            code=lambda_.Code.from_asset("../lambda_package"),

            # Entry point for the Lambda
            handler="ingestion.lambda_handler.lambda_hanlder",
            timeout=Duration.minutes(5),
            memory_size=256,
            # Limit concurrent executions to 1
            reserved_concurrent_executions=1,

            # Environment variable for the Bronze bucket name
            environment={
                "BRONZE_BUCKET": bronze_bucket.bucket_name,
            },

            # Keep logs forever (default)
        )

        bronze_bucket.grant_read_write(ingestion_lambda)

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
