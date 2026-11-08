import os
import time
import uuid

import boto3
from botocore.exceptions import ClientError


LOCK_ID = "nyc_collision_ingestion"
LOCK_DURATION_SECONDS = 600
PIPELINE_LOCK_ID = "nyc_collision_pipeline"

def get_lock_table():
    '''
    Connect to the DynamoDB table configured by CDK.
    '''
    dynamodb = boto3.resource("dynamodb")
    table_name = os.environ["INGESTION_LOCK_TABLE"]

    return dynamodb.Table(table_name)




# DynamoDB creates the lock only when either
# 1. No lock exists
# 2. Previous lock has expired
def acquire_lock():
    '''
    Acquire the lock unless another active execution owns it.
    '''
    table = get_lock_table()

    owner_id = str(uuid.uuid4())
    current_time = int(time.time())
    expires_at = current_time + LOCK_DURATION_SECONDS

    try:
        table.put_item(
            Item={
                "lock_id":LOCK_ID,
                "owner_id":owner_id,
                "expires_at":expires_at,
            },
            # 
            ConditionExpression=(
                "attribute_not_exists(lock_id) "
                "OR expires_at < :now"
            ),
            ExpressionAttributeValues={
                ":now":current_time,
            },
        )

        return owner_id

    except ClientError as error:
        if (
            error.response["Error"]["Code"]
            =="ConditionalCheckFailedException"
        ):
            return None

        raise

def release_lock(owner_id):
    """
    Release the lock only if this execution still owns it.
    """
    table = get_lock_table()

    try:
        table.delete_item(
            Key={"lock_id": LOCK_ID},
            ConditionExpression="owner_id = :owner",
            ExpressionAttributeValues={
                ":owner": owner_id,
            },
        )

    except ClientError as error:
        if (
            error.response["Error"]["Code"]
            == "ConditionalCheckFailedException"
        ):
            return

        raise
