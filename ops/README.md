# Reversible service retirement

These manifests record the intended disabled execution state for retired integrations and persistent development services. They do not delete Lambda definitions, API Gateway endpoints, IAM roles, storage or backups. This is a staged retirement with a retained rollback path, not a full infrastructure ownership migration.

- `retire-integrations.json`: disables Sheets ingestion, its refresh schedule and SMS functions. The community-data reader remains available because installed Collect clients call it.
- `retire-dev-execution.json`: disables the dev exporter, ETL and analytics functions and the two dev schedules. Production counterparts remain unchanged.
- The three dev CloudFormation templates also declare reserved concurrency zero; the ETL and analytics schedules declare `DISABLED`. Reconcile existing stack drift before deploying them. Applying these files to the live stacks was intentionally avoided while analytics has deleted database resources.

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

Run the unit tests with:

```bash
python3 -m unittest discover -s ops -p 'test_*.py' -v
```

Coverage includes a read-only plan, account mismatch, protected-function exclusions, exact restoration of both reserved and unreserved concurrency, partial-failure rollback and pre-apply configuration drift. These tests use a fake AWS boundary; they are not a live production rollback rehearsal.

After applying an integration retirement, verify the protected reader returns the expected status, record count and response checksum using a private smoke test. Do not log the response body in public CI. These operations do not verify mobile end-to-end workflows or provision staging.

A complete shutdown of the community-data reader requires a compatible replacement for installed mobile clients. Disabling a Lambda does not revoke credentials embedded in its package; exposed provider credentials require separate provider-side revocation. Never put those values or backup packages into this repository.
