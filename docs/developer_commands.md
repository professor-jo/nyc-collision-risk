# Developer Commands

The commands to remember when working on the NYC Collision Risk project. Run them from the repository root unless a step says otherwise.

Everything runs in the `nyc-capstone` AWS profile in `us-east-1`.

## 1. Log in to AWS

```bash
source aws-login.sh
```

**What it does:** Checks whether your AWS SSO session is still valid and opens the browser login if it has expired. It also sets `AWS_PROFILE`, `AWS_DEFAULT_REGION` and `BRONZE_BUCKET` for the current terminal.

Use `source`, not `bash`, or the variables disappear when the script ends. The script is local only and isn't committed.

## 2. Set up Python

```bash
python -m venv .venv && source .venv/bin/activate
pip install boto3 -r requirements-lambda.txt -r infra/requirements.txt -r infra/requirements-dev.txt
```

**What it does:** Creates an isolated Python environment and installs the ingestion, CDK and test dependencies into it. You only need the first line once. After that, run `source .venv/bin/activate` in each new terminal.

## 3. Run the tests

```bash
bash scripts/build_lambda_package.sh
export BRONZE_BUCKET=test-bronze-bucket
python -m pytest tests/ infra/tests/ -q
```

**What it does:** Builds `lambda_package/`, which the infrastructure tests need, then runs the ingestion and infrastructure tests. AWS and the NYC API are mocked, so nothing touches real resources. The bucket name only needs to be set because the ingestion modules read it on import.

## 4. Infrastructure (AWS CDK)

Run these from the `infra/` folder:

```bash
cd infra
```

The CDK rebuilds `lambda_package/` automatically before each of these commands.

### Synth

```bash
cdk synth -c budget_email=you@example.com -c dbt_client_cidr=$(curl -s https://checkip.amazonaws.com)/32 --profile nyc-capstone
```

**What it does:** Converts your Python infrastructure code into an AWS CloudFormation template. It does not deploy anything. Use it to catch errors in the stack code early.

### Diff

```bash
cdk diff -c budget_email=you@example.com -c dbt_client_cidr=$(curl -s https://checkip.amazonaws.com)/32 --profile nyc-capstone
```

**What it does:** Compares your local infrastructure code with what is currently deployed in AWS and lists every resource that would be added, changed or replaced. It does not deploy anything. Run it before every deploy, and pay attention to IAM changes and anything marked "may be replaced".

### Deploy

```bash
cdk deploy -c budget_email=you@example.com -c dbt_client_cidr=$(curl -s https://checkip.amazonaws.com)/32 --profile nyc-capstone
```

**What it does:** Applies your infrastructure code to AWS. It uploads the Lambda code and the Glue script, then updates the CloudFormation stack to create, change or delete resources. **This changes real resources.** If the changes widen IAM permissions, it asks you to confirm first.

`budget_email` is where AWS Budgets and alarm emails are sent. The stack refuses to build without it.

`dbt_client_cidr` is the only address allowed to reach the Redshift endpoint on port 5439, so local dbt can connect. The command fills in your current public IP. The stack refuses to build without it, and rejects anything wider than a /24. When your IP changes, deploy again to update the security group.

## 5. Run the Silver Glue job

```bash
aws glue start-job-run --job-name nyc-collision-silver-etl \
  --profile nyc-capstone --region us-east-1
```

**What it does:** Starts the Silver ETL job, which reads Bronze, validates and deduplicates it, and merges the results into the `silver_collisions` Iceberg table. It prints a `JobRunId`, and each run costs a little money.

Check the run's status:

```bash
aws glue get-job-run --job-name nyc-collision-silver-etl --run-id <JobRunId> \
  --query 'JobRun.[JobRunState,ErrorMessage]' --output text \
  --profile nyc-capstone --region us-east-1
```

**What it does:** Shows whether the run is `RUNNING`, `SUCCEEDED` or `FAILED`, plus the error message if it failed.

Read the job's printed output:

```bash
aws logs tail /aws-glue/jobs/output --log-stream-name-prefix <JobRunId> \
  --profile nyc-capstone --region us-east-1
```

**What it does:** Prints the job's `print()` output. This includes the data quality results, `Missing S3 modification timestamps`, the deduplication counts and the Iceberg validation. Errors and stack traces go to `/aws-glue/jobs/error`.

Remember to run `cdk deploy` first after you edit `glue/silver_collisions.py`, or Glue will run the old script.

## 6. Run ingestion manually

Find the Lambda's name:

```bash
aws cloudformation describe-stack-resources --stack-name InfraStack \
  --query "StackResources[?ResourceType=='AWS::Lambda::Function'].PhysicalResourceId" \
  --output text --profile nyc-capstone --region us-east-1
```

Invoke it:

```bash
aws lambda invoke \
  --function-name <function-name> \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  --cli-connect-timeout 30 \
  --cli-read-timeout 360 \
  --profile nyc-capstone --region us-east-1 \
  response.json
```

**What it does:** Runs one ingestion immediately instead of waiting for the daily schedule, and writes the Lambda's response to `response.json`.

Keep `--cli-read-timeout 360`. Otherwise the CLI gives up after 60 seconds and retries, which starts a second run, and the lock rejects that run with a misleading `IngestionLockHeld` error.

## 7. Create the ingestion checkpoint (new environments only)

```bash
BRONZE_BUCKET=<bucket-name> python -m ingestion.checkpoint
```

**What it does:** Creates the starting checkpoint that ingestion needs, once, after the first deploy to a new AWS account. It refuses to overwrite an existing checkpoint, so it's safe to run by mistake.

## 8. Save your work

```bash
git status
git add <files>
git commit -m "Describe the change"
git push origin main
```

**What it does:** Shows what changed, records the change, and pushes it to GitHub. This project pushes straight to `main`. Pushing doesn't deploy anything, so still run `cdk deploy` to update AWS.
