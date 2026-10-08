# Puente cloud infrastructure

This public repository contains Puente's serverless definitions, retained legacy
services and account-checked retirement/recovery tools. The October 2026 work
retired unused execution while preserving the community-data reader required by
installed Collect clients.

| Service | Current intended state |
| --- | --- |
| Community-data reader (`s3-json-to-client`) | Keep active; preserve its endpoint and cached S3 data until clients have a compatible replacement. |
| Google Sheets ingestion | Retired: execution and refresh schedule disabled. Cached data no longer refreshes automatically. |
| SMS integrations | Retired: both AWS function executions disabled. |
| Dev and production exporter, ETL and analytics | Retired: execution and schedules disabled; definitions, APIs, stored data and backups retained for recovery. |

The six exporter/ETL/analytics templates declare zero reserved concurrency;
ETL/analytics schedules declare `DISABLED`. Their GitHub deployment workflows
are disabled, have no push triggers and have unconditional job holds in source.
Do not redeploy a retired stack merely to apply these settings: analytics has
database drift, and a full deployment could recreate unwanted paid resources.

## Operations and validation

- [Service lifecycle and owner decisions](docs/service-lifecycle.md)
- [AWS savings closeout and remaining scope](docs/aws-savings-closeout.md)
- [Retirement and recovery commands](ops/README.md)
- [Testing and rollback gates](docs/infrastructure-change-safety-plan.md)
- [Engineering review and mitigations](docs/retirement-review.md)
- [Map recovery from its verified private backup](ops/map-recovery.md)
- [Flask sizing evidence](docs/flask-sizing-evidence.md)

Run the mocked operation/failure tests locally:

```bash
python3 -m unittest discover -s ops -p 'test_*.py' -v
```

Verify the serverless execution restrictions against AWS without changing them:

```bash
python3 ops/verify_retirement_state.py \
  --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --result "$PRIVATE_RELEASE_DIR/retirement-verification.json"
```

This verifies control settings, not every application/client workflow. Keep
plans, billing evidence, raw logs, records and credentials outside this public
repository. Never restore the exposed Google credential from Git history.

## Changes and restoration

Use a feature branch and a reviewed PR. `master` requires the retirement CI check
and an up-to-date branch; review approvals are dismissed when new commits arrive.
Actions default to read-only permissions and cannot approve PRs. Secret scanning
and push protection are enabled. Production credentials are not available to the
validation workflow.

Restoration is a separate lifecycle decision. Reconcile stack drift, prepare
isolated staging and exercise recovery before changing execution or removing a
deployment hold. Repository changes do not themselves deploy AWS resources.
