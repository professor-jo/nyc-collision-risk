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
