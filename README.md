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

**Infrastructure as code**
- CDK Python project set up in `infra/`

**Data lake storage**
- Three S3 buckets for the medallion layers: bronze (raw), silver (cleaned), gold (analysis-ready)
- Block all public access
- Encryption at rest with S3-managed keys (SSE-S3)
- HTTPS required for all requests (encryption in transit)
- Versioning on
- Lifecycle rules: old object versions expire after 30 days; incomplete multipart uploads are removed after 7 days
- Buckets are retained if the stack is deleted
- Cost-allocation tags on every bucket (`project`, `sp`, `layer`)

**Cost guardrail**
- $5/month AWS Budget with an email alert
- Alert email passed in at deploy time, never committed to the repo

**Testing**
- 10 unit tests (pytest + CDK assertions) on the synthesized CloudFormation template, covering buckets, security settings, tags and the budget