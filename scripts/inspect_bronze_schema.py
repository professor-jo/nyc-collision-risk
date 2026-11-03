
import json
from collections import Counter

import boto3
from botocore.config import Config


BUCKET = "infrastack-bronzebucketff082204-s2b6odvatfwt"
PREFIX = "collisions/"
PROFILE = "nyc-capstone"


def main():
    # Use the same AWS profile as our P1 ingestion project.
    session = boto3.Session(profile_name=PROFILE)
    s3 = session.client(
        "s3",
        config=Config(retries={"mode": "standard", "max_attempts": 3}),
    )

    # Counters for dataset-level statistics.
    total_records = 0
    total_files = 0
    field_present = Counter()
    field_missing = Counter()
    observed_types = {}

    # ListObjectsV2 returns up to 1,000 objects per page.
    # A paginator handles larger datasets automatically.
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            # Exclude the old manual sample file.
            if key.endswith("/sample_collisions.json"):
                continue

            response = s3.get_object(Bucket=BUCKET, Key=key)
            records = json.loads(response["Body"].read())

            if not isinstance(records, list):
                raise ValueError(f"Expected JSON array: {key}")

            total_files += 1

            for record in records:
                if not isinstance(record, dict):
                    raise ValueError(f"Expected JSON object in {key}")

                total_records += 1

                for field, value in record.items():
                    if value is None or value == "":
                        field_missing[field] += 1
                    else:
                        field_present[field] += 1

                        if field not in observed_types:
                            observed_types[field] = set()

                        observed_types[field].add(type(value).__name__)

    print(f"\nFiles inspected: {total_files}")
    print(f"Records inspected: {total_records}")
    print("\nFIELD PROFILE")
    print("-" * 85)

    all_fields = sorted(set(field_present) | set(field_missing))

    for field in all_fields:
        present = field_present[field]
        missing = total_records - present
        missing_pct = (
            missing / total_records * 100 if total_records else 0
        )

        types = ", ".join(sorted(observed_types.get(field, set())))

        print(
            f"{field:40} "
            f"missing={missing:6} "
            f"({missing_pct:5.1f}%) "
            f"types={types}"
        )


if __name__ == "__main__":
    main()
