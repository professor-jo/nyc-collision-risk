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
    aws_s3_deployment as s3deploy,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
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
            sources=[s3deploy.Source.asset(str(GLUE_SCRIPT_DIR))],
            destination_bucket=silver_bucket,
            destination_key_prefix="glue/scripts",
            prune=False,
        )

        silver_job = glue.CfnJob(
            self,
            "SilverCollisionsJob",
            name="nyc-collision-silver-etl",
            # Uses our previously deployed IAM role
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                # Runs an Apache Spark ETL job
                name="glueetl",
                python_version="3",
                script_location=(
                    f"s3://{silver_bucket.bucket_name}/"
                    "glue/scripts/silver_collisions.py"
                ),
            ),
            glue_version="5.0",
            # Specifies the Glue worker type
            worker_type="G.1X",
            # Using small worker configuration
            number_of_workers=2,
            # Stops the job after 10 minutes to limit costs
            timeout=10,
            # Prevents automatic reruns during development.
            max_retries=0,
            # A backstop if something starts the job outside the pipeline.
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),


            # This passes Bronze S3 bucket name into the Glue job as
            # a runtime argument rather than hardcoding a bucket name in Python.
            default_arguments={
                "--BRONZE_BUCKET": bronze_bucket.bucket_name,
                "--SILVER_BUCKET": silver_bucket.bucket_name,


                "--additional-python-modules": "h3==4.3.1",

                # Enable Glue's built-in Apache Iceberg libraries.
                "--datalake-formats": "iceberg",

                # Configure Spark's Iceberg catalog.
                "--conf": (
                    "spark.sql.extensions="
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                    " --conf spark.sql.catalog.glue_catalog="
                    "org.apache.iceberg.spark.SparkCatalog"
                    " --conf spark.sql.catalog.glue_catalog.warehouse="
                    f"s3://{silver_bucket.bucket_name}/"
                    " --conf spark.sql.catalog.glue_catalog.catalog-impl="
                    "org.apache.iceberg.aws.glue.GlueCatalog"
                    " --conf spark.sql.catalog.glue_catalog.io-impl="
                    "org.apache.iceberg.aws.s3.S3FileIO"
                ),
            },
        )

        # Ensure the script is uploaded before the job is created
        silver_job.node.add_dependency(glue_script_deployment)

        # Read raw collision data from Bronze
        bronze_bucket.grant_read(glue_role)

        # Write transformed data to Silver
        silver_bucket.grant_read_write(glue_role)

        # 
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetPartitions",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/nyc_collision_lakehouse",
                    f"arn:aws:glue:{self.region}:{self.account}:table/nyc_collision_lakehouse/*",
                ],
            )
        )




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

        # ============================================================
        # P3.3 — Pipeline-level concurrency lock
        # ============================================================

        # This lock belongs to the whole Step Functions execution.
        pipeline_lock_id = "nyc_collision_pipeline"

        # A stopped or timed-out execution can't run its release step, so
        # the lock must expire on its own. It outlives the execution by
        # 5 minutes, the longest a Lambda already running can continue.
        pipeline_timeout = Duration.minutes(45)
        pipeline_lock_seconds = (
            pipeline_timeout.to_seconds() + Duration.minutes(5).to_seconds()
        )

        # Epoch seconds, the same format ingestion/lock.py uses.
        # This needs JSONata: classic JSONPath has no date functions.
        lock_now = (
            "$floor($toMillis($states.context.State.EnteredTime) / 1000)"
        )

        # Acquire when no lock exists, or take over one that has expired.
        acquire_pipeline_lock = tasks.CallAwsService.jsonata(
            self,
            "AcquirePipelineLock",
            service="dynamodb",
            action="putItem",
            parameters={
                "TableName": ingestion_lock_table.table_name,
                "Item": {
                    "lock_id": {
                        "S": pipeline_lock_id
                    },
                    "owner_id": {
                        "S": "{% $states.context.Execution.Id %}"
                    },
                    "expires_at": {
                        "N": (
                            f"{{% $string({lock_now} + "
                            f"{pipeline_lock_seconds:.0f}) %}}"
                        )
                    },
                },
                "ConditionExpression": (
                    "attribute_not_exists(lock_id) OR expires_at < :now"
                ),
                "ExpressionAttributeValues": {
                    ":now": {
                        "N": f"{{% $string({lock_now}) %}}"
                    },
                },
            },
            iam_resources=[
                ingestion_lock_table.table_arn
            ],
            # Pass the execution input through to the next state.
            outputs="{% $states.input %}",
        )

        # Delete the lock only if this execution still owns it, so one
        # execution can never remove another execution's lock.
        release_lock_parameters = {
            "TableName": ingestion_lock_table.table_name,
            "Key": {
                "lock_id": {
                    "S": pipeline_lock_id
                },
            },
            "ConditionExpression": "owner_id = :owner",
            "ExpressionAttributeValues": {
                ":owner": {
                    "S.$": "$$.Execution.Id"
                },
            },
        }

        release_pipeline_lock = tasks.CallAwsService(
            self,
            "ReleasePipelineLock",
            service="dynamodb",
            action="deleteItem",
            parameters=release_lock_parameters,
            iam_resources=[
                ingestion_lock_table.table_arn
            ],
            result_path=sfn.JsonPath.DISCARD,
        )

        pipeline_succeeded = sfn.Succeed(
            self,
            "PipelineSucceeded",
        )

        lock_already_held = sfn.Fail(
            self,
            "LockAlreadyHeld",
            error="PipelineLockUnavailable",
            cause="Another pipeline execution already owns the lock.",
        )

        # A scheduled run that finds the lock held is skipped on purpose.
        scheduled_run_skipped = sfn.Succeed(
            self,
            "ScheduledRunSkipped",
        )

        check_lock_conflict_trigger = sfn.Choice(
            self,
            "CheckLockConflictTrigger",
        )

        # A Choice state errors on a missing field, so check it exists.
        # With no trigger, such as a manual start, the conflict stays visible.
        check_lock_conflict_trigger.when(
            sfn.Condition.and_(
                sfn.Condition.is_present("$.trigger"),
                sfn.Condition.string_equals("$.trigger", "scheduled"),
            ),
            scheduled_run_skipped,
        )

        check_lock_conflict_trigger.otherwise(lock_already_held)

        # If acquisition fails because the lock is held, decide from the
        # trigger. Without outputs, the catch would replace the input
        # with the error and $.trigger would be lost.
        acquire_pipeline_lock.add_catch(
            check_lock_conflict_trigger,
            errors=[
                "DynamoDb.ConditionalCheckFailedException"
            ],
            outputs="{% $states.input %}",
        )

        initialize_ingestion = sfn.Pass(
            self,
            "InitializeIngestion",
            parameters={
                "attempt": 1,
                "max_attempts": 5,
                "bronze_changed_any": False,
            },
            result_path="$.ingestion_state",
        )

        invoke_ingestion = tasks.LambdaInvoke(
            self,
            "InvokeCollisionIngestion",
            lambda_function=ingestion_lambda,
            payload=sfn.TaskInput.from_object({
                "attempt": sfn.JsonPath.number_at(
                    "$.ingestion_state.attempt"
                ),
                "max_attempts": sfn.JsonPath.number_at(
                    "$.ingestion_state.max_attempts"
                ),
            }),
            payload_response_only=True,
            result_path="$.ingestion_result",
        )

        check_bronze_changed = sfn.Choice(
            self,
            "CheckBronzeChanged",
        )

        mark_bronze_changed = sfn.Pass(
            self,
            "MarkBronzeChanged",
            result=sfn.Result.from_boolean(True),
            result_path="$.ingestion_state.bronze_changed_any",
        )

        check_caught_up = sfn.Choice(
    self,
    "CheckCaughtUp",
)

        check_attempt_limit = sfn.Choice(
            self,
            "CheckAttemptLimit",
        )

        increment_attempt = sfn.Pass(
            self,
            "IncrementAttempt",
            parameters={
                "attempt.$": "States.MathAdd($.ingestion_state.attempt, 1)",
                "max_attempts.$": "$.ingestion_state.max_attempts",
                "bronze_changed_any.$": "$.ingestion_state.bronze_changed_any",
            },
            result_path="$.ingestion_state",
        )

        ingestion_complete = sfn.Pass(
            self,
            "IngestionComplete",
            result=sfn.Result.from_boolean(True),
            result_path="$.ingestion_state.caught_up",
        )

        ingestion_incomplete = sfn.Pass(
            self,
            "IngestionIncomplete",
            result=sfn.Result.from_boolean(False),
            result_path="$.ingestion_state.caught_up",
        )

        # If Lambda changed Bronze, remember it.
        check_bronze_changed.when(
            sfn.Condition.boolean_equals(
                "$.ingestion_result.bronze_changed",
                True,
            ),
            mark_bronze_changed,
        )

        # Otherwise, keep the previous cumulative value.
        check_bronze_changed.otherwise(check_caught_up)

        # After marking Bronze changed, check ingestion progress.
        mark_bronze_changed.next(check_caught_up)


        # If ingestion caught up, mark it complete.
        check_caught_up.when(
            sfn.Condition.boolean_equals(
                "$.ingestion_result.caught_up",
                True,
            ),
            ingestion_complete,
        )

        # Otherwise, check whether another attempt is allowed.
        check_caught_up.otherwise(check_attempt_limit)


        # Stop after five attempts.
        check_attempt_limit.when(
            sfn.Condition.number_greater_than_equals(
                "$.ingestion_state.attempt",
                5,
            ),
            ingestion_incomplete,
        )

        # Otherwise, increment the counter and invoke Lambda again.
        check_attempt_limit.otherwise(increment_attempt)

        increment_attempt.next(invoke_ingestion)

        # After each Lambda invocation, check whether Bronze changed.
        invoke_ingestion.next(check_bronze_changed)

        # ------------------------------------------------------------
        # P3.5 — Lambda failure path
        # ------------------------------------------------------------

        # The execution ends with the original Lambda error, so the
        # console shows IngestionLockHeld rather than a generic failure.
        pipeline_failed = sfn.Fail(
            self,
            "PipelineFailed",
            error_path="$.ingestion_error.Error",
            cause_path="$.ingestion_error.Cause",
        )

        # A separate state from ReleasePipelineLock, because a state has
        # only one next step and this path must end in failure.
        release_lock_after_failure = tasks.CallAwsService(
            self,
            "ReleasePipelineLockAfterFailure",
            service="dynamodb",
            action="deleteItem",
            parameters=release_lock_parameters,
            iam_resources=[
                ingestion_lock_table.table_arn
            ],
            result_path=sfn.JsonPath.DISCARD,
        )

        # A brief DynamoDB error must not leave the lock held.
        release_lock_after_failure.add_retry(
            errors=["States.ALL"],
            interval=Duration.seconds(2),
            max_attempts=3,
            backoff_rate=2,
        )

        # If the release still fails, report the Lambda error anyway.
        # A separate result_path keeps $.ingestion_error intact.
        release_lock_after_failure.add_catch(
            pipeline_failed,
            errors=["States.ALL"],
            result_path="$.release_error",
        )

        release_lock_after_failure.next(pipeline_failed)

        # Runs after the Lambda service retries on the task are used up.
        invoke_ingestion.add_catch(
            release_lock_after_failure,
            errors=["States.ALL"],
            result_path="$.ingestion_error",
        )




        

        # ------------------------------------------------------------
        # P3.7 — Conditional Silver job
        # ------------------------------------------------------------

        check_run_silver = sfn.Choice(
            self,
            "CheckRunSilver",
        )

        # Waits for the job to finish, so the lock stays held while
        # Glue writes to the Silver table.
        run_silver_job = tasks.GlueStartJobRun(
            self,
            "RunSilverJob",
            glue_job_name=silver_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.silver_result",
        )

        # Only a busy job is worth retrying. A failed job usually means
        # bad data, and running it again won't fix that.
        run_silver_job.add_retry(
            errors=["Glue.ConcurrentRunsExceededException"],
            interval=Duration.seconds(60),
            max_attempts=3,
            backoff_rate=1,
        )

        # Uses the same path as Lambda errors, so PipelineFailed reports
        # the real error from either component.
        run_silver_job.add_catch(
            release_lock_after_failure,
            errors=["States.ALL"],
            result_path="$.ingestion_error",
        )

        # Silver also runs when ingestion didn't catch up, so data from
        # those attempts isn't stranded in Bronze.
        ingestion_complete.next(check_run_silver)
        ingestion_incomplete.next(check_run_silver)

        check_run_silver.when(
            sfn.Condition.boolean_equals(
                "$.ingestion_state.bronze_changed_any",
                True,
            ),
            run_silver_job,
        )

        check_run_silver.otherwise(release_pipeline_lock)

        run_silver_job.next(release_pipeline_lock)

        # ------------------------------------------------------------
        # P3 — Replay and backfill modes
        # ------------------------------------------------------------

        # Replay reprocesses what is already in Bronze. It marks the run
        # as caught up so the final outcome check reports success.
        mark_replay_ready = sfn.Pass(
            self,
            "MarkReplayReady",
            result=sfn.Result.from_boolean(True),
            result_path="$.ingestion_state.caught_up",
        )

        mark_replay_ready.next(run_silver_job)

        invoke_backfill = tasks.LambdaInvoke(
            self,
            "InvokeBackfill",
            lambda_function=ingestion_lambda,
            payload=sfn.TaskInput.from_object({
                "mode": "backfill",
                "backfill_start": sfn.JsonPath.string_at("$.backfill_start"),
                "backfill_end": sfn.JsonPath.string_at("$.backfill_end"),
            }),
            payload_response_only=True,
            result_path="$.ingestion_result",
        )

        invoke_backfill.add_catch(
            release_lock_after_failure,
            errors=["States.ALL"],
            result_path="$.ingestion_error",
        )

        # The Lambda reports caught_up, so a backfill runs only once
        # and then follows the same Silver gate as normal ingestion.
        invoke_backfill.next(check_bronze_changed)

        def mode_is(mode):
            # A Choice state errors on a missing field, so check it exists.
            return sfn.Condition.and_(
                sfn.Condition.is_present("$.mode"),
                sfn.Condition.string_equals("$.mode", mode),
            )

        check_run_mode = sfn.Choice(
            self,
            "CheckRunMode",
        )

        check_run_mode.when(mode_is("replay"), mark_replay_ready)
        check_run_mode.when(mode_is("backfill"), invoke_backfill)
        check_run_mode.otherwise(invoke_ingestion)

        acquire_pipeline_lock.next(initialize_ingestion)
        initialize_ingestion.next(check_run_mode)

        invalid_backfill_input = sfn.Fail(
            self,
            "InvalidBackfillInput",
            error="InvalidBackfillInput",
            cause="Backfill runs need backfill_start and backfill_end.",
        )

        # Reject bad input before taking the lock. A missing path later
        # would stop the execution with the lock still held.
        validate_run_input = sfn.Choice(
            self,
            "ValidateRunInput",
        )

        validate_run_input.when(
            sfn.Condition.and_(
                mode_is("backfill"),
                sfn.Condition.not_(
                    sfn.Condition.and_(
                        sfn.Condition.is_present("$.backfill_start"),
                        sfn.Condition.is_present("$.backfill_end"),
                    ),
                ),
            ),
            invalid_backfill_input,
        )

        validate_run_input.otherwise(acquire_pipeline_lock)

        pipeline_definition = validate_run_input

        check_final_outcome = sfn.Choice(
            self,
            "CheckFinalOutcome",
        )

        ingestion_not_caught_up = sfn.Fail(
            self,
            "IngestionNotCaughtUp",
            error="IngestionIncomplete",
            cause="Ingestion did not catch up within five attempts.",
        )

        release_pipeline_lock.next(check_final_outcome)

        # A brief DynamoDB error must not fail a run whose work is done.
        release_pipeline_lock.add_retry(
            errors=["States.ALL"],
            interval=Duration.seconds(2),
            max_attempts=3,
            backoff_rate=2,
        )

        # The lock is already gone or no longer ours, which also happens
        # when a retry follows a delete that had actually succeeded.
        release_pipeline_lock.add_catch(
            check_final_outcome,
            errors=["DynamoDb.ConditionalCheckFailedException"],
            result_path="$.release_error",
        )

        check_final_outcome.when(
            sfn.Condition.boolean_equals(
                "$.ingestion_state.caught_up",
                True,
            ),
            pipeline_succeeded,
        )

        check_final_outcome.otherwise(ingestion_not_caught_up)

        pipeline_state_machine = sfn.StateMachine(
            self,
            "PipelineStateMachine",
            state_machine_name="nyc-collision-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(
                pipeline_definition
            ),
            timeout=pipeline_timeout,
        )

        # ------------------------------------------------------------
        # P3 — Pipeline failure alerts
        # ------------------------------------------------------------

        # Alarms cover every way an execution can end badly, including
        # timeouts, where no state in the workflow gets a chance to run.
        # Stopped executions are left out because a person chose to stop
        # them, and a skipped scheduled run counts as a success.
        pipeline_alarms = {
            "PipelineExecutionsFailedAlarm": (
                pipeline_state_machine.metric_failed,
                "NYC collision pipeline execution failed",
            ),
            "PipelineExecutionsTimedOutAlarm": (
                pipeline_state_machine.metric_timed_out,
                "NYC collision pipeline execution timed out",
            ),
        }

        for alarm_id, (metric, description) in pipeline_alarms.items():
            pipeline_alarm = cloudwatch.Alarm(
                self,
                alarm_id,
                metric=metric(
                    period=Duration.minutes(5),
                    statistic="Sum",
                ),
                threshold=1,
                evaluation_periods=1,
                alarm_description=description,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            )

            pipeline_alarm.add_alarm_action(
                cw_actions.SnsAction(alert_topic)
            )

        # ------------------------------------------------------------
        # P3 — Daily schedule
        # ------------------------------------------------------------

        # Holds schedule events that EventBridge could not deliver, such
        # as when the workflow cannot be started at all.
        schedule_dead_letter_queue = sqs.Queue(
            self,
            "PipelineScheduleDeadLetterQueue",
            retention_period=Duration.days(14),
            enforce_ssl=True,
        )

        # The state machine is the only target. Keeping the Lambda as
        # well would run ingestion twice each day.
        ingestion_rule.add_target(
            targets.SfnStateMachine(
                pipeline_state_machine,
                input=events.RuleTargetInput.from_object({
                    "trigger": "scheduled",
                }),
                dead_letter_queue=schedule_dead_letter_queue,
                retry_attempts=2,
            )
        )

        # A message in this queue means a daily run never started, which
        # the execution alarms can't see.
        schedule_delivery_alarm = cloudwatch.Alarm(
            self,
            "PipelineScheduleDeliveryAlarm",
            metric=(
                schedule_dead_letter_queue
                .metric_approximate_number_of_messages_visible(
                    period=Duration.minutes(5),
                    statistic="Maximum",
                )
            ),
            threshold=1,
            evaluation_periods=1,
            alarm_description="NYC collision pipeline schedule failed to start",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        schedule_delivery_alarm.add_alarm_action(
            cw_actions.SnsAction(alert_topic)
        )
