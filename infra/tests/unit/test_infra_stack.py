import json

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from infra.infra_stack import InfraStack


TEST_DBT_CLIENT_CIDR = "203.0.113.10/32"
TEST_CONTEXT = {
    "budget_email": "test@example.com",
    "dbt_client_cidr": TEST_DBT_CLIENT_CIDR,
}


@pytest.fixture(scope="module")
def template():
    """
    Synthesize the stack once and share the template 
    across all tests
    """
    app = cdk.App(context=TEST_CONTEXT)
    stack = InfraStack(app, "TestStack")
    return Template.from_stack(stack)

def test_creates_three_buckets(template):
    template.resource_count_is("AWS::S3::Bucket", 3)

def test_all_buckets_block_public_access(template):
    template.all_resources_properties("AWS::S3::Bucket", {
        "PublicAccessBlockConfiguration": {
            "BlockPublicAcls": True,
            "BlockPublicPolicy": True,
            "IgnorePublicAcls": True,
            "RestrictPublicBuckets": True
        }
    })

def test_all_buckets_have_encrypted_with_s3_managed_keys(template):
    template.all_resources_properties("AWS::S3::Bucket", {
        "BucketEncryption": {
            "ServerSideEncryptionConfiguration": Match.array_with([
                Match.object_like({
                    "ServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                })
            ])
        }
    })


def test_all_buckets_versioned(template):
    template.all_resources_properties("AWS::S3::Bucket", {
        "VersioningConfiguration": {"Status": "Enabled"}
    })

def test_all_buckets_have_lifecycle_rules(template):
    template.all_resources_properties("AWS::S3::Bucket", {
        "LifecycleConfiguration": {
            "Rules": Match.array_with([
                Match.object_like({
                    "Status": "Enabled",
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
                    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
                })
            ])
        }
    })

def test_all_buckets_retained_on_delete(template):
    template.all_resources("AWS::S3::Bucket", {
        "DeletionPolicy": "Retain",
        "UpdateReplacePolicy": "Retain",
    })

def test_all_buckets_deny_non_https(template):
    template.resource_count_is("AWS::S3::BucketPolicy", 3)
    template.all_resources_properties("AWS::S3::BucketPolicy", {
        "PolicyDocument": {
            "Statement": Match.array_with([
                Match.object_like({
                    "Effect": "Deny",
                    "Condition": {"Bool": {"aws:SecureTransport": "false"}
                    }
                })
            ])
        }
    })

@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_each_layer_bucket_is_tagged(template, layer):
    template.has_resource_properties("AWS::S3::Bucket", {
        "Tags": Match.array_with([
            {"Key": "layer", "Value": layer},
            {"Key":"project", "Value": "nyc-risk"}
        ])
    })

def test_monthly_budget_alerts_at_five_dollars(template):
    template.has_resource_properties("AWS::Budgets::Budget", {
        "Budget": {
            "BudgetType": "COST",
            "TimeUnit": "MONTHLY",
            "BudgetLimit": {"Amount": 5, "Unit": "USD"},
        },
        "NotificationsWithSubscribers": [{
            "Notification": {
                "NotificationType": "ACTUAL",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": 5,
                "ThresholdType": "ABSOLUTE_VALUE",
            },
            "Subscribers": [{"SubscriptionType": "EMAIL", "Address": "test@example.com"}],
        }],
    })


def test_lookback_incomplete_alarm_needs_three_days_in_a_row(template):
    # Must match the status text printed by ingestion/lookback_collisions.py.
    template.has_resource_properties("AWS::Logs::MetricFilter", {
        "FilterPattern": '"LOOKBACK_STATUS=incomplete"',
    })
    template.has_resource_properties("AWS::CloudWatch::Alarm", {
        "MetricName": "LookbackIncomplete",
        "Period": 86400,
        "EvaluationPeriods": 3,
        "Threshold": 1,
        "TreatMissingData": "notBreaching",
    })



def state_machine_definition(template):
    """Return the definition of the single state machine as a dict."""
    machines = template.find_resources("AWS::StepFunctions::StateMachine")
    assert len(machines) == 1

    definition = next(iter(machines.values()))["Properties"]["DefinitionString"]

    # ARNs and table names are CloudFormation references until deploy.
    text = "".join(
        part if isinstance(part, str) else "PLACEHOLDER"
        for part in definition["Fn::Join"][1]
    )
    return json.loads(text)


def state_machine_states(template):
    return state_machine_definition(template)["States"]


def test_pipeline_lock_can_be_recovered_after_expiry(template):
    acquire = state_machine_states(template)["AcquirePipelineLock"]
    arguments = acquire["Arguments"]

    assert acquire["QueryLanguage"] == "JSONata"
    assert arguments["ConditionExpression"] == (
        "attribute_not_exists(lock_id) OR expires_at < :now"
    )
    assert "$states.context.State.EnteredTime" in arguments["Item"]["expires_at"]["N"]
    assert "$states.context.State.EnteredTime" in (
        arguments["ExpressionAttributeValues"][":now"]["N"]
    )
    # A later state needs the input, and a JSONata task would replace it.
    assert acquire["Output"] == "{% $states.input %}"


def test_pipeline_lock_outlives_the_execution_timeout(template):
    definition = state_machine_definition(template)
    expires_at = definition["States"]["AcquirePipelineLock"]["Arguments"]["Item"]["expires_at"]["N"]

    assert definition["TimeoutSeconds"] == 45 * 60
    assert expires_at.endswith(" + 3000) %}")


def test_lambda_failure_releases_lock_then_fails(template):
    states = state_machine_states(template)

    assert states["InvokeCollisionIngestion"]["Catch"] == [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.ingestion_error",
        "Next": "ReleasePipelineLockAfterFailure",
    }]

    release = states["ReleasePipelineLockAfterFailure"]
    assert release["Next"] == "PipelineFailed"
    assert release["Parameters"] == states["ReleasePipelineLock"]["Parameters"]

    assert states["PipelineFailed"] == {
        "Type": "Fail",
        "ErrorPath": "$.ingestion_error.Error",
        "CausePath": "$.ingestion_error.Cause",
    }


def test_failed_release_still_reports_the_lambda_error(template):
    release = state_machine_states(template)["ReleasePipelineLockAfterFailure"]

    assert release["Retry"][0]["MaxAttempts"] == 3
    assert release["Catch"] == [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.release_error",
        "Next": "PipelineFailed",
    }]


def test_shared_release_separates_complete_from_incomplete(template):
    states = state_machine_states(template)

    assert states["IngestionComplete"]["Next"] == "CheckRunSilver"
    assert states["IngestionIncomplete"]["Next"] == "CheckRunSilver"
    assert states["CheckRunSilver"]["Default"] == "ReleasePipelineLock"
    assert states["RunSilverJob"]["Next"] == "ReleasePipelineLock"
    assert states["ReleasePipelineLock"]["Next"] == "CheckFinalOutcome"

    # The release must keep the input, or this check has nothing to read.
    assert states["ReleasePipelineLock"]["ResultPath"] is None

    outcome = states["CheckFinalOutcome"]
    assert outcome["Choices"] == [{
        "Variable": "$.ingestion_state.caught_up",
        "BooleanEquals": True,
        "Next": "PipelineSucceeded",
    }]
    assert outcome["Default"] == "IngestionNotCaughtUp"

    assert states["IngestionNotCaughtUp"] == {
        "Type": "Fail",
        "Error": "IngestionIncomplete",
        "Cause": "Ingestion did not catch up within five attempts.",
    }


def test_normal_release_retries_and_tolerates_a_missing_lock(template):
    release = state_machine_states(template)["ReleasePipelineLock"]

    assert release["Retry"][0]["ErrorEquals"] == ["States.ALL"]
    assert release["Retry"][0]["MaxAttempts"] == 3
    assert release["Catch"] == [{
        "ErrorEquals": ["DynamoDb.ConditionalCheckFailedException"],
        "ResultPath": "$.release_error",
        "Next": "CheckFinalOutcome",
    }]


def test_lock_conflict_keeps_input_and_checks_the_trigger(template):
    states = state_machine_states(template)

    assert states["AcquirePipelineLock"]["Catch"] == [{
        "ErrorEquals": ["DynamoDb.ConditionalCheckFailedException"],
        "Output": "{% $states.input %}",
        "Next": "CheckLockConflictTrigger",
    }]

    check = states["CheckLockConflictTrigger"]
    assert check["Choices"] == [{
        "And": [
            {"Variable": "$.trigger", "IsPresent": True},
            {"Variable": "$.trigger", "StringEquals": "scheduled"},
        ],
        "Next": "ScheduledRunSkipped",
    }]
    assert check["Default"] == "LockAlreadyHeld"

    assert states["ScheduledRunSkipped"]["Type"] == "Succeed"
    assert states["LockAlreadyHeld"]["Error"] == "PipelineLockUnavailable"


def run_workflow(template, lambda_outcomes, glue_outcome=None, execution_input=None):
    """
    Follow the generated transitions with fake Lambda and Glue outcomes.

    An outcome containing "Error" acts as a failed task. Retries aren't
    simulated. Returns the visited state names and the final data.
    """
    definition = state_machine_definition(template)
    states = definition["States"]
    outcomes = iter(lambda_outcomes)
    data = {"trigger": "manual", **(execution_input or {})}
    visited = []
    name = definition["StartAt"]

    def get(path):
        value = data
        for key in path[2:].split("."):
            value = value[key]
        return value

    def put(path, value):
        target = data
        keys = path[2:].split(".")
        for key in keys[:-1]:
            target = target.setdefault(key, {})
        target[keys[-1]] = value

    def rule_matches(rule):
        if "Not" in rule:
            return not rule_matches(rule["Not"])
        if "And" in rule:
            return all(rule_matches(part) for part in rule["And"])
        if "IsPresent" in rule:
            try:
                get(rule["Variable"])
                return rule["IsPresent"]
            except KeyError:
                return not rule["IsPresent"]

        value = get(rule["Variable"])
        if "StringEquals" in rule:
            return value == rule["StringEquals"]
        if "BooleanEquals" in rule:
            return value is rule["BooleanEquals"]
        return value >= rule["NumericGreaterThanEquals"]

    for _ in range(100):
        visited.append(name)
        state = states[name]
        kind = state["Type"]

        if kind in ("Succeed", "Fail"):
            return visited, data

        if kind == "Choice":
            name = next(
                (c["Next"] for c in state["Choices"] if rule_matches(c)),
                state["Default"],
            )
            continue

        if kind == "Pass":
            if "Parameters" in state:
                value = {}
                for key, item in state["Parameters"].items():
                    if not key.endswith(".$"):
                        value[key] = item
                    elif item.startswith("States.MathAdd"):
                        value[key[:-2]] = get(item.split("(")[1].split(",")[0]) + 1
                    else:
                        value[key[:-2]] = get(item)
            else:
                value = state["Result"]
            put(state["ResultPath"], value)
            name = state["Next"]
            continue

        # Task states.
        if name in ("InvokeCollisionIngestion", "InvokeBackfill"):
            outcome = next(outcomes)
        elif name == "RunSilverJob":
            outcome = glue_outcome or {"JobRunState": "SUCCEEDED"}
        else:
            outcome = {}

        if "Error" in outcome:
            catcher = next(
                c for c in state["Catch"]
                if "States.ALL" in c["ErrorEquals"]
                or outcome["Error"] in c["ErrorEquals"]
            )
            put(catcher["ResultPath"], outcome)
            name = catcher["Next"]
            continue

        if state.get("ResultPath"):
            put(state["ResultPath"], outcome)
        name = state["Next"]

    raise AssertionError(f"Workflow did not finish: {visited[-10:]}")


def ingestion(caught_up, bronze_changed):
    return {
        "statusCode": 200,
        "caught_up": caught_up,
        "bronze_changed": bronze_changed,
        "lookback_ran": caught_up,
        "lookback_complete": True if caught_up else None,
    }


def test_caught_up_with_bronze_changes_runs_silver(template):
    visited, data = run_workflow(template, [ingestion(True, True)])

    assert visited.index("RunSilverJob") < visited.index("ReleasePipelineLock")
    assert visited[-1] == "PipelineSucceeded"
    assert "ReleasePipelineLockAfterFailure" not in visited
    assert data["ingestion_state"]["bronze_changed_any"] is True


def test_caught_up_without_changes_skips_silver(template):
    visited, _ = run_workflow(template, [ingestion(True, False)])

    assert "RunSilverJob" not in visited
    assert "ReleasePipelineLock" in visited
    assert visited[-1] == "PipelineSucceeded"


def test_incomplete_with_changes_runs_silver_then_fails(template):
    # Only the first attempt wrote data, so the flag must carry forward.
    outcomes = [ingestion(False, True)] + [ingestion(False, False)] * 4

    visited, data = run_workflow(template, outcomes)

    assert visited.count("InvokeCollisionIngestion") == 5
    assert visited.index("RunSilverJob") < visited.index("ReleasePipelineLock")
    assert visited[-1] == "IngestionNotCaughtUp"
    assert data["ingestion_state"]["caught_up"] is False


@pytest.mark.parametrize(
    "error",
    ["Glue.ConcurrentRunsExceededException", "States.TaskFailed"],
)
def test_glue_failure_releases_lock_and_keeps_the_error(template, error):
    failure = {"Error": error, "Cause": "Glue cause"}

    visited, data = run_workflow(
        template,
        [ingestion(True, True)],
        glue_outcome=failure,
    )

    assert visited[-3:] == [
        "RunSilverJob",
        "ReleasePipelineLockAfterFailure",
        "PipelineFailed",
    ]
    assert "ReleasePipelineLock" not in visited
    assert data["ingestion_error"] == failure


def test_lambda_failure_never_reaches_silver(template):
    failure = {"Error": "IngestionLockHeld", "Cause": "Lambda cause"}

    visited, data = run_workflow(template, [failure])

    assert "RunSilverJob" not in visited
    assert visited[-2:] == ["ReleasePipelineLockAfterFailure", "PipelineFailed"]
    assert data["ingestion_error"] == failure


def test_silver_job_waits_and_retries_only_when_busy(template):
    job = state_machine_states(template)["RunSilverJob"]

    assert job["Resource"].endswith(":states:::glue:startJobRun.sync")
    assert [r["ErrorEquals"] for r in job["Retry"]] == [
        ["Glue.ConcurrentRunsExceededException"]
    ]


def test_worst_case_runtime_fits_the_execution_timeout(template):
    definition = state_machine_definition(template)

    functions = template.find_resources("AWS::Lambda::Function")
    lambda_seconds = next(
        f["Properties"]["Timeout"]
        for f in functions.values()
        if "lambda_handler" in f["Properties"].get("Handler", "")
    )
    jobs = template.find_resources("AWS::Glue::Job")
    glue_seconds = next(iter(jobs.values()))["Properties"]["Timeout"] * 60

    retry = definition["States"]["RunSilverJob"]["Retry"][0]
    busy_wait_seconds = retry["IntervalSeconds"] * retry["MaxAttempts"]

    worst_case = 5 * lambda_seconds + busy_wait_seconds + glue_seconds

    assert worst_case <= definition["TimeoutSeconds"]


@pytest.mark.parametrize(
    "metric_name",
    ["ExecutionsFailed", "ExecutionsTimedOut"],
)
def test_pipeline_alarm_emails_the_alert_topic(template, metric_name):
    alarms = template.find_resources("AWS::CloudWatch::Alarm", {
        "Properties": {
            "MetricName": metric_name,
            "Namespace": "AWS/States",
        },
    })

    assert len(alarms) == 1
    properties = next(iter(alarms.values()))["Properties"]

    assert properties["Threshold"] == 1
    assert properties["EvaluationPeriods"] == 1
    assert properties["TreatMissingData"] == "notBreaching"
    assert properties["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
    assert len(properties["AlarmActions"]) == 1
    assert "IngestionAlertTopic" in properties["AlarmActions"][0]["Ref"]


BACKFILL_INPUT = {
    "mode": "backfill",
    "backfill_start": "2026-10-01",
    "backfill_end": "2026-10-15",
}


def test_replay_skips_ingestion_and_runs_silver(template):
    visited, _ = run_workflow(template, [], execution_input={"mode": "replay"})

    assert "InvokeCollisionIngestion" not in visited
    assert "InvokeBackfill" not in visited
    assert visited.index("AcquirePipelineLock") < visited.index("RunSilverJob")
    assert visited.index("RunSilverJob") < visited.index("ReleasePipelineLock")
    assert visited[-1] == "PipelineSucceeded"


def test_replay_failure_releases_lock_and_keeps_the_error(template):
    failure = {"Error": "States.TaskFailed", "Cause": "Glue cause"}

    visited, data = run_workflow(
        template,
        [],
        glue_outcome=failure,
        execution_input={"mode": "replay"},
    )

    assert visited[-2:] == ["ReleasePipelineLockAfterFailure", "PipelineFailed"]
    assert data["ingestion_error"] == failure


def test_backfill_with_changes_runs_silver_once(template):
    visited, _ = run_workflow(
        template,
        [ingestion(True, True)],
        execution_input=BACKFILL_INPUT,
    )

    assert visited.count("InvokeBackfill") == 1
    assert "InvokeCollisionIngestion" not in visited
    assert "RunSilverJob" in visited
    assert visited[-1] == "PipelineSucceeded"


def test_backfill_without_changes_skips_silver(template):
    visited, _ = run_workflow(
        template,
        [ingestion(True, False)],
        execution_input=BACKFILL_INPUT,
    )

    assert "RunSilverJob" not in visited
    assert visited[-1] == "PipelineSucceeded"


def test_backfill_failure_releases_the_lock(template):
    failure = {"Error": "BackfillIncomplete", "Cause": "Lambda cause"}

    visited, data = run_workflow(
        template,
        [failure],
        execution_input=BACKFILL_INPUT,
    )

    assert "RunSilverJob" not in visited
    assert visited[-2:] == ["ReleasePipelineLockAfterFailure", "PipelineFailed"]
    assert data["ingestion_error"] == failure


@pytest.mark.parametrize("missing", ["backfill_start", "backfill_end"])
def test_invalid_backfill_is_rejected_before_taking_the_lock(template, missing):
    execution_input = {k: v for k, v in BACKFILL_INPUT.items() if k != missing}

    visited, _ = run_workflow(template, [], execution_input=execution_input)

    assert visited == ["ValidateRunInput", "InvalidBackfillInput"]


def test_backfill_payload_reaches_the_lambda(template):
    backfill = state_machine_states(template)["InvokeBackfill"]

    assert backfill["Parameters"] == {
        "mode": "backfill",
        "backfill_start.$": "$.backfill_start",
        "backfill_end.$": "$.backfill_end",
    }


def test_state_machine_has_its_production_name(template):
    template.has_resource_properties("AWS::StepFunctions::StateMachine", {
        "StateMachineName": "nyc-collision-pipeline",
    })


def test_glue_job_allows_one_concurrent_run(template):
    template.has_resource_properties("AWS::Glue::Job", {
        "Name": "nyc-collision-silver-etl",
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
    })


def test_schedule_starts_only_the_state_machine(template):
    rules = template.find_resources("AWS::Events::Rule")
    assert len(rules) == 1
    rule = next(iter(rules.values()))["Properties"]

    assert rule["ScheduleExpression"] == "cron(0 0 * * ? *)"
    assert len(rule["Targets"]) == 1

    target = rule["Targets"][0]
    assert "PipelineStateMachine" in target["Arn"]["Ref"]
    assert json.loads(target["Input"]) == {"trigger": "scheduled"}
    assert "DeadLetterConfig" in target
    assert target["RetryPolicy"]["MaximumRetryAttempts"] == 2


def test_eventbridge_can_no_longer_invoke_the_lambda(template):
    permissions = template.find_resources("AWS::Lambda::Permission", {
        "Properties": {"Principal": "events.amazonaws.com"},
    })

    assert permissions == {}


def test_undelivered_schedule_events_raise_an_alarm(template):
    template.resource_count_is("AWS::SQS::Queue", 1)
    template.has_resource_properties("AWS::CloudWatch::Alarm", {
        "MetricName": "ApproximateNumberOfMessagesVisible",
        "Namespace": "AWS/SQS",
        "Threshold": 1,
    })


def test_redshift_port_is_open_only_to_the_dbt_client(template):
    groups = template.find_resources("AWS::EC2::SecurityGroup")
    rules = [
        rule
        for group in groups.values()
        for rule in group["Properties"].get("SecurityGroupIngress", [])
    ]
    rules += [
        resource["Properties"]
        for resource in template.find_resources("AWS::EC2::SecurityGroupIngress").values()
    ]

    assert rules
    for rule in rules:
        assert rule.get("CidrIp") not in ("0.0.0.0/0", None)
        assert rule.get("CidrIpv6") is None
    assert [(r["FromPort"], r["ToPort"], r["CidrIp"]) for r in rules] == [
        (5439, 5439, TEST_DBT_CLIENT_CIDR)
    ]


def test_redshift_workgroup_is_public_in_three_subnets(template):
    workgroups = template.find_resources("AWS::RedshiftServerless::Workgroup")
    assert len(workgroups) == 1
    properties = next(iter(workgroups.values()))["Properties"]

    assert properties["PubliclyAccessible"] is True
    assert len(properties["SubnetIds"]) == 3
    assert len(properties["SecurityGroupIds"]) == 1
    template.resource_count_is("AWS::EC2::Subnet", 3)
    template.resource_count_is("AWS::EC2::NatGateway", 0)


@pytest.mark.parametrize(
    "cidr",
    [None, "", "0.0.0.0/0", "10.0.0.0/8", "203.0.113.0/23", "::/0", "not-an-ip", "203.0.113.10/24"],
)
def test_stack_refuses_a_missing_or_wide_dbt_client_cidr(cidr):
    context = {"budget_email": "test@example.com"}
    if cidr is not None:
        context["dbt_client_cidr"] = cidr
    app = cdk.App(context=context)

    with pytest.raises(ValueError, match="dbt_client_cidr"):
        InfraStack(app, "RejectedStack")
