# InfraStack — NYC Collision Risk Pipeline
#
# P1 infrastructure:
# - S3 buckets for Bronze, Silver, and Gold data layers
# - Lambda for incremental and rolling-lookback ingestion
# - EventBridge for daily scheduled ingestion
# - DynamoDB for ingestion concurrency locking
# - CloudWatch alarms and SNS email notifications
# - AWS Budgets for cost monitoring
#
# Operational considerations:
# - Monitor Lambda execution duration and API retry behavior.
# - Monitor incomplete lookback executions.
# - Review AWS costs and service quotas periodically.

from pathlib import Path

from aws_cdk import (
    aws_cloudwatch as cloudwatch,
    aws_logs as logs,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    Duration,
    Stack,
    RemovalPolicy,
    Tags,
    aws_budgets as budgets,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_cloudwatch_actions as cw_actions,
    aws_events as events,
    aws_events_targets as targets,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_glue as glue,
    aws_s3_deployment as s3delpoy,
)

from constructs import Construct

# Resolved from this file, so the stack synthesizes from any working directory.
LAMBDA_PACKAGE_DIR = Path(__file__).resolve().parents[2] / "lambda_package"

# CDK needs to know where local PySpark script lives so we can upload it to S3 for
# Glue to execute
GLUE_SCRIPT_DIR = Path(__file__).resolve().parents[2] / "glue"

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


            if layer == "bronze":
                bronze_bucket = bucket
            elif layer == "silver":
                silver_bucket = bucket


            
            # Tag resources for cost tracking
            Tags.of(bucket).add("project", "nyc-risk")
            Tags.of(bucket).add("sp", "SP0")
            Tags.of(bucket).add("layer", layer)


        # IAM role assumed by AWS Glue jobs
        glue_role = iam.Role(
            self,
            "CollisionGlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )

        # Basic Glue permissions, including CloudWatch logging
        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        # Register the lakehouse database in Glue Data Catalog
        lakehouse_database = glue.CfnDatabase(
            self,
            "CollisionLakehouseDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="nyc_collision_lakehouse",
                description="Metadata catalog for NYC collision lakehouse tables",
            ),
        )

        # Upload local Glue scripts to the Silver S3 bucket
        glue_script_deployment = s3deploy.BucketDeployment(
            self,
            "GlueScriptDeployment",
            sources=[s3deeploy.Source.asset(str(GLUE_SCRIPT_DIR))],
            destination_bucket=silver_bucket,
            destination_key_prefix="glue/scripts",
            prune=False,
        )


        # Read raw collision data from Bronze
        bronze_bucket.grant_read(glue_role)

        # Write transformed data to Silver
        silver_bucket.grant_read_write(glue_role)


        # Create Lambda
        ingestion_lambda = lambda_.Function(
            self,
            "CollisionIngestionLambda",
            log_retention=logs.RetentionDays.ONE_WEEK,

            # Use Python 3.12 runtime
            runtime=lambda_.Runtime.PYTHON_3_12,


            # Package lambda_package/ as a zip. It is generated and not in Git:
            # the "build" command in cdk.json runs scripts/build_lambda_package.sh
            code=lambda_.Code.from_asset(str(LAMBDA_PACKAGE_DIR)),

            # Entry point for the Lambda
            handler="ingestion.lambda_handler.lambda_handler",
            timeout=Duration.minutes(5),
            memory_size=256,
            # Environment variable for the Bronze bucket name
            environment={
                "BRONZE_BUCKET": bronze_bucket.bucket_name,
            },

            # Keep logs forever (default)
        )

        # Grant Lambda access to S3
        bronze_bucket.grant_read_write(ingestion_lambda)

        # Create DynamoDB lock table
        # DynamoDB table is used to prevent overlapping ingestion runs (ran by lambda). The table has a single partition key "lock_id" and uses on-demand billing. The table is retained when the stack is deleted.
        ingestion_lock_table = dynamodb.Table(
            self,
            "IngestionLockTable",
            partition_key=dynamodb.Attribute(
                name="lock_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Allow Lambda to create, inspect, and release locks.
        ingestion_lock_table.grant_read_write_data(ingestion_lambda)

        # Tell Lambda which DynamoDB table stores ingestion locks.
        ingestion_lambda.add_environment(
            "INGESTION_LOCK_TABLE",
            ingestion_lock_table.table_name,
        )



        # Cloudwatch monitoring part.
        error_alarm = cloudwatch.Alarm(
            self,
            "IngestionErrorAlarm",
            metric=ingestion_lambda.metric_errors(
                period=Duration.minutes(5)
            ),
            threshold=1,
            evaluation_periods=1,
            alarm_description="ANYC collision ingestion Lambda failed",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        ingestion_rule = events.Rule(
            self,
            "DailyCollisionIngestion",
            schedule=events.Schedule.cron(
                minute="0",
                hour="0",
            ),
            enabled=True,
            description="Run NYC collision ingestion daily at 00:00 UTC",
        )

        ingestion_rule.add_target(
            targets.LambdaFunction(ingestion_lambda)
        )


        # Email when monthly spend passes $5 and for ssubscription
        budget_email = self.node.try_get_context("budget_email")
        if not budget_email:
            raise ValueError(
                "Missing context 'budget_email'. Pass it with: "
                "cdk deploy -c budget_email=you@example.com"
            )

        alert_topic = sns.Topic(
            self,
            "IngestionAlertTopic",
        )

        alert_topic.add_subscription(
            subscriptions.EmailSubscription(budget_email)
        )

        error_alarm.add_alarm_action(
            cw_actions.SnsAction(alert_topic)
        )

        # A single incomplete lookback is harmless, since the next run replays
        # it. Alert only when it stays incomplete three days in a row.
        lookback_incomplete_filter = ingestion_lambda.log_group.add_metric_filter(
            "LookbackIncompleteFilter",
            filter_pattern=logs.FilterPattern.literal(
                '"LOOKBACK_STATUS=incomplete"'
            ),
            metric_namespace="NycCollisionRisk",
            metric_name="LookbackIncomplete",
            metric_value="1",
        )

        lookback_incomplete_alarm = cloudwatch.Alarm(
            self,
            "LookbackIncompleteAlarm",
            metric=lookback_incomplete_filter.metric(
                period=Duration.days(1),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=3,
            alarm_description="NYC collision lookback incomplete 3 days in a row",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        lookback_incomplete_alarm.add_alarm_action(
            cw_actions.SnsAction(alert_topic)
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
