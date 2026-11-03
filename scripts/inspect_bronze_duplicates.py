
import json
from collections import Counter, defaultdict

import boto3


BUCKET = "infrastack-bronzebucketff082204-s2b6odvatfwt"
PREFIX = "collisions/"
PROFILE = "nyc-capstone"


def main():
    session = boto3.Session(profile_name=PROFILE)
    s3 = session.client("s3")

    # Count occurrences of each collision ID.
    id_counts = Counter()

    # Store distinct record contents for each collision ID.
    record_versions = defaultdict(set)

    files_scanned = 0
    records_scanned = 0

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            if key.endswith("/sample_collisions.json"):
                continue

            response = s3.get_object(Bucket=BUCKET, Key=key)
            records = json.loads(response["Body"].read())

            files_scanned += 1

            for record in records:
                collision_id = record["collision_id"]

                # Normalize JSON so field order doesn't matter.
                normalized = json.dumps(
                    record,
                    sort_keys=True,
                    separators=(",", ":"),
                )

                id_counts[collision_id] += 1
                record_versions[collision_id].add(normalized)
                records_scanned += 1

    duplicate_ids = sum(
        count > 1 for count in id_counts.values()
    )

    changed_ids = [
        collision_id
        for collision_id, versions in record_versions.items()
        if len(versions) > 1
    ]

    print("\nBRONZE DUPLICATE PROFILE")
    print("-" * 45)
    print(f"Files scanned: {files_scanned}")
    print(f"Records scanned: {records_scanned}")
    print(f"Unique collision IDs: {len(id_counts)}")
    print(f"Collision IDs appearing multiple times: {duplicate_ids}")
    print(f"Collision IDs with different contents: {len(changed_ids)}")
    print(f"Duplicate record occurrences: {records_scanned - len(id_counts)}")

    print("\nSample IDs with changed contents:")
    for collision_id in changed_ids[:5]:
        print(collision_id)


if __name__ == "__main__":
    main()
