import json

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from infra.infra_stack import InfraStack


@pytest.fixture(scope="module")
def template():
    """
    Synthesize the stack once and share the template 
    across all tests
    """
    app = cdk.App(context={"budget_email": "test@example.com"})
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
    assert release["Next"] == "IngestionFailed"
    assert release["Parameters"] == states["ReleasePipelineLock"]["Parameters"]

    assert states["IngestionFailed"] == {
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
        "Next": "IngestionFailed",
    }]


def test_shared_release_separates_complete_from_incomplete(template):
    states = state_machine_states(template)

    assert states["IngestionComplete"]["Next"] == "ReleasePipelineLock"
    assert states["IngestionIncomplete"]["Next"] == "ReleasePipelineLock"
    assert states["ReleasePipelineLock"]["Next"] == "CheckFinalOutcome"

    # The release must keep the input, or this check has nothing to read.
    assert states["ReleasePipelineLock"]["ResultPath"] is None

    outcome = states["CheckFinalOutcome"]
    assert outcome["Choices"] == [{
        "Variable": "$.ingestion_state.caught_up",
        "BooleanEquals": True,
        "Next": "LockTestSucceeded",
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
