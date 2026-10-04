# Reversible service retirement

These manifests record the intended disabled execution state for retired integrations and persistent development services. They do not delete Lambda definitions, API Gateway endpoints, IAM roles, storage or backups. This is a staged retirement with a retained rollback path, not a full infrastructure ownership migration.

- `retire-integrations.json`: disables Sheets ingestion, its refresh schedule and SMS functions. The community-data reader remains available because installed Collect clients call it.
- `retire-dev-execution.json`: disables the dev exporter, ETL and analytics functions and the two dev schedules. Production counterparts remain unchanged.
- `retire-prod-execution.json`: independently disables the production exporter, ETL and analytics functions and their two schedules, following the owner's subsequent decision to retire all three.
- All six dev/production CloudFormation templates now declare reserved concurrency zero; ETL and analytics schedules declare `DISABLED`. Reconcile existing stack drift before deploying them. Applying these files to the live stacks was intentionally avoided while analytics has deleted database resources.
- The three retired-service deployment workflows no longer trigger on branch pushes. Their live GitHub workflow states are disabled and recorded privately. Re-enabling deployment is a separate restoration decision; restoring Lambda execution alone does not re-enable CI deployment. The new retirement test workflow uses mocked AWS boundaries, read-only repository permissions and no deployment credentials.

The operation requires an explicit expected AWS account. It first records prior concurrency and schedule settings, rejects changed settings at apply time, verifies the result and attempts restoration if an operation fails. It never invokes a business Lambda, sends an SMS or deletes data. Use a deployment identity authorized only for these resources.

Keep plans and results outside this public repository. The plan is also the rollback input; protect it from edits, because rollback restores its saved values. Review any intervening operations before rolling back.

```bash
python3 ops/retire_integrations.py plan \
  --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --manifest ops/retire-integrations.json \
  --plan "$PRIVATE_RELEASE_DIR/integrations-before.json"

python3 ops/retire_integrations.py apply \
  --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --plan "$PRIVATE_RELEASE_DIR/integrations-before.json" \
  --result "$PRIVATE_RELEASE_DIR/integrations-after.json"

python3 ops/retire_integrations.py rollback \
  --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --plan "$PRIVATE_RELEASE_DIR/integrations-before.json" \
  --result "$PRIVATE_RELEASE_DIR/integrations-restored.json"
```

For the dev changes, make a separate plan using `ops/retire-dev-execution.json`. Keep separate before/after files for each operation. Never overwrite the original plan.

Use another separate plan with `ops/retire-prod-execution.json` for the production retirement. Rollback restores only that plan's target functions and schedules. Protected functions retain their settings at rollback time, including any independently authorized lifecycle changes made since the original plan.

Run the unit tests with:

```bash
python3 -m unittest discover -s ops -p 'test_*.py' -v
```

Coverage includes a read-only plan, account mismatch, protected-function exclusions, exact restoration of both reserved and unreserved concurrency, partial-failure rollback and pre-apply configuration drift. These tests use a fake AWS boundary; they are not a live production rollback rehearsal.

After applying an integration retirement, verify the protected reader returns the expected status, record count and response checksum using a private smoke test. Do not log the response body in public CI. These operations do not verify mobile end-to-end workflows or provision staging.

A complete shutdown of the community-data reader requires a compatible replacement for installed mobile clients. Disabling a Lambda does not revoke credentials embedded in its package; exposed provider credentials require separate provider-side revocation. Never put those values or backup packages into this repository.

## Confirmed-unused GraphQL compute

`pause_graphql.py` is a deliberately narrow operational retirement for the existing single-instance GraphQL Beanstalk environment. It discovers and records the instance, disk, Elastic IP, application version, scaling group and existing automation controls. It rejects unexpected ownership, capacity, active deployments and baseline probe failures. It does not resize or edit the Beanstalk-generated child stack.

The apply operation holds the pipeline's Deploy transition, disables managed updates, suspends the scaling processes that could replace/restart capacity, and stops the instance. EBS, the Elastic IP, source/configuration and resource definitions remain. They continue to incur storage/IP charges. A manually initiated Beanstalk deployment or environment termination can override these safeguards; keep the retired environment out of deployment workflows.

```bash
python3 ops/pause_graphql.py plan --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --plan "$PRIVATE_RELEASE_DIR/graphql-before.json"
python3 ops/pause_graphql.py apply --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --plan "$PRIVATE_RELEASE_DIR/graphql-before.json" \
  --result "$PRIVATE_RELEASE_DIR/graphql-paused.json"
python3 ops/pause_graphql.py rollback --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --plan "$PRIVATE_RELEASE_DIR/graphql-before.json" \
  --result "$PRIVATE_RELEASE_DIR/graphql-restored.json"
```

Recovery starts the retained instance, waits for a read-only GraphQL `__typename` probe, then restores the original scaling, managed-update and pipeline settings. If the application fails to recover, the automation safeguards remain held for investigation. Application identity, disk or unrelated scaling changes require review. Retaining an instance is not a backup against disk deletion; permanent retirement requires separate artifact/data retention decisions.

Before leaving the environment stopped, exercise apply → rollback → fresh plan → apply and record the result privately. Check Flask health, the website, map and the community reader alongside the operation. This probe establishes GraphQL process/query recovery; it does not certify every resolver, mobile workflow or peak-load behavior. Beanstalk health becoming degraded/severe while the retired instance is stopped is expected.

References: [AWS scaling-process suspension considerations](https://docs.aws.amazon.com/autoscaling/ec2/userguide/suspend-resume-considerations.html), [Beanstalk managed platform updates](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/environment-platform-update-managed.html).

## Owner-approved map retirement

`pause_map.py` provides the same `plan`, `apply` and `rollback` arguments for the map's existing load-balanced environment. It discovers every current instance instead of assuming a single instance, records capacity and disk identities, verifies healthy ALB targets plus the root/Dash layout/dependency endpoints, and rejects deployment or scaling drift. It requires managed updates to remain disabled, as they were in the reviewed baseline.

Apply disables the map repository's Beanstalk deployment workflow, suspends scaling/replacement and stops all recorded instances. Rollback starts the retained instances and requires every ALB target and the application probes to recover before restoring original automation. It preserves workflows or scaling processes that were already disabled. Use separate private map before/result files and rehearse recovery before the final pause.

The ALB, its hostname, security groups, disks and resource definitions remain. ALB, ALB public IPv4 and disk charges continue. Automatically assigned EC2 public addresses are released on stop and can change on recovery; the retained ALB route is the recovery endpoint. The pause does not certify a snapshot-based rebuild or a permanent ALB deletion. These operations must not be followed by an unrelated deployment to the retired environment.
