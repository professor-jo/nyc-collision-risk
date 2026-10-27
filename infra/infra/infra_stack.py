from aws_cdk import (
    Stack,
    RemovalPolicy,
    Tags,
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

                # Keep data if the stack is deleted
                removal_policy=RemovalPolicy.RETAIN,
            )

            # Tag resources for cost tracking
            Tags.of(bucket).add("project", "nyc-risk")
            Tags.of(bucket).add("sp", "SP0")
            Tags.of(bucket).add("layer", layer)
