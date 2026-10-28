# nyc-collision-risk

An end-to-end data project on NYC motor vehicle collision data, built on AWS.

- **Data engineering:** ingest raw collision data and move it through a bronze → silver → gold data lake on AWS
- **Data analysis:** SQL analysis of geographic hotspots, time patterns, risk factors and severity
- **Data science / ML:** forecast which areas are most likely to see collisions

All infrastructure is defined as code with the **AWS CDK (Python)** in [`infra/`](infra/).

## Tech stack

AWS CDK (Python) · Amazon S3 · AWS Budgets · pytest

## Project phases

| Phase | Scope | Status |
|-------|-------|--------|
| P0 | Infrastructure foundation | In progress |

---

## Progress log

### P0: Infrastructure foundation

I've implemented the following:

- Set up a CDK Python project to define all infrastructure as code
- Created three S3 buckets (bronze, silver, gold) for a medallion data lake
- Blocked all public access to the buckets to prevent accidental data exposure
- Encrypted data at rest with S3-managed keys and required HTTPS for encryption in transit
- Enabled versioning to recover from accidental overwrites or deletes
- Added lifecycle rules that expire old versions after 30 days and clean up failed uploads after 7 days, to control storage cost
- Set buckets to be retained if the stack is deleted, so data is never lost by accident
- Tagged every bucket with project and layer for cost tracking
- Added a $5/month AWS Budget with an email alert as a cost guardrail, with the email passed in at deploy time so it's never committed
- Wrote 11 CDK assertion unit tests to verify the security, retention, tagging and budget settings before anything is deployed
