# GraphQL recovery from an independent private backup

The owner approved permanent GraphQL environment removal on November 5 and
accepted private-image recovery at a new address. Keep the verified image and
snapshots independently of the old environment. The earlier approximately
91-second retained-instance restart cannot recreate a deleted environment;
`pause_graphql.py rollback` rejects an absent original before making changes.

The owning stack reached `DELETE_COMPLETE` on November 5. Its instance, root
disk, Elastic IP, scaling group, launch template and security group were removed.
The image and completed snapshots remain private/unshared, the pipeline Deploy
transition remains held, and the active Flask, website and community reader
passed availability checks with the reader checksum unchanged. Private release
records contain the resource identities and verification results.

The October 11 isolated restore passed all three protocol probes and EC2/network
checks in 168.83 seconds (approximately three minutes). The original remained
stopped. After an interrupted handoff, the temporary test copy was left running;
it was discovered and its stack, instance, disk and security group were removed
and verified on November 5. The independent private backup remains available.
Use the paired execution/cleanup command below to avoid leaving another test
instance running when a probe completes or fails.

## Prepare and test the backup

1. Verify the expected AWS account, application/environment identities, stopped
   source instance, disk identities and deployed application version. Confirm
   managed updates and the CodePipeline Deploy transition are disabled, and all
   replacement/scaling processes remain suspended.
2. Preserve deployed source, environment configuration, pipeline configuration
   and the owning stack's resource inventory in a private release directory.
   The deployed package contains historical application configuration. Do not
   publish it or its credentials, or treat preservation as credential cleanup.
3. Create an EBS-backed image from the stopped original. Wait for an available
   private image and completed snapshots; verify neither image nor snapshots
   have sharing permissions. Keep the image independently of Beanstalk.
4. Preview `graphql-recovery-test.yaml` with the image, original VPC/public
   subnet and the operator's current IPv4 `/32`. The change set must add only
   `RecoveryInstance` and `RecoverySecurityGroup`.
5. Execute the isolated rehearsal with the paired command below. The copy has no instance profile, SSH access
   or production security group, and no useful outbound network access. IMDS
   requires tokens. Its boot script disables Beanstalk agents and starts the
   already-installed application and nginx without downloads or redeployment.
   Multipart user data first disables AL2's automatic package upgrades. Without
   that cloud-config part, the initial rehearsal waited for blocked repository
   downloads before the application startup script could run.
6. Use the previously previewed CREATE change set ARN and a new private directory:

```bash
python3 ops/rehearse_graphql_recovery.py \
  --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --change-set "$REVIEWED_CHANGE_SET_ARN" \
  --result-dir "$PRIVATE_RELEASE_DIR/new-graphql-rehearsal"
```

The runner requires the exact local template, a fresh two-resource change set,
one operator IPv4 address, and a private image whose test disk deletes on
termination. It executes the change set, waits for startup, runs the probe, and
deletes its exact stack in a `finally` block on success, failure, timeout or
handled SIGINT/SIGTERM. It verifies deletion before reporting success. A host
loss, SIGKILL or AWS teardown failure can still require manual cleanup using
the stack ARN saved privately; this is not an independent AWS expiry service.
On November 5, the paired runner also passed a complete live rehearsal: protocol,
isolation and EC2 checks passed in 290.16 seconds (approximately five minutes),
followed by automatic stack deletion verified before the command returned.
The suite includes mocked cleanup, interrupt, execution-response and scope tests.

For read-only diagnostics against an existing ready test stack, the probe can
also run separately. This standalone command does not clean up the stack:

```bash
python3 ops/probe_graphql_recovery.py \
  --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --timing "$PRIVATE_RELEASE_DIR/launch-timing.json" \
  --result "$PRIVATE_RELEASE_DIR/new-graphql-cold-recovery.json"
```

The standalone probe's optional timing file contains a UTC `started_at` recorded when the change set
was executed. Without it, timing starts at stack creation. Use a new result file.

The probe verifies stack ownership, selected image/VPC/subnet, private/unshared
image availability, absent IAM profile, exact ingress/egress and EC2 checks.
It sends three protocol-only queries: POST `__typename`, POST with variables and
aliases, and GET `__typename`. Requests use the verified instance IP, reject
redirects and ignore operator proxy settings. Only status, byte counts and hashes
are saved. The queries do not invoke application resolvers or retrieve records.

These checks prove startup and GraphQL protocol execution. They do not establish
database credentials/connectivity, business-resolver compatibility, TLS, public
routing, every client flow or peak-load capacity. Restoring service for users
requires separately reviewed dependency, routing and compatibility checks. Do
not grant the legacy test copy unrestricted outbound access or a production role
to make the rehearsal pass.

Delete the temporary stack after testing, including on failure. Confirm its
instance terminates and its disk and security group are removed. Preserve the
private backup. For any future retirement, preserve the original until its
owner accepts the changed recovery contract.

## Owner-approved permanent retirement

Removing the environment releases its Elastic IP and removes its retained
instance/disk. The old address and Beanstalk hostname cannot be guaranteed on
recovery. This trades the previous quick restart for an image restore at a new
address. The owner accepted this recovery contract on November 5 after both
private-image rehearsals; it supersedes the earlier retained-instance rollback.

Before removal, recheck backup privacy/availability, successful rehearsal
evidence, original resource/version identity, disabled deployment/replacement
controls and custom DNS references to the current address/hostname. Keep the
private stack inventory for cleanup verification.

Remove resources through Beanstalk's parent API. Resume only the Auto Scaling
`Terminate` process if needed for teardown; keep replacement/deployment held.
Never delete Beanstalk's generated child resources as an alternative to normal
parent-stack deletion, and never use `--no-terminate-resources`.

After termination, verify `DELETE_COMPLETE` and removal of its instance, root
disk, Elastic IP, scaling group and stack-owned network resources. Reconfirm
private backup availability and active Flask, website and community-reader
availability. Keep the pipeline deployment transition disabled.

`pause_graphql.py rollback` cannot recreate a deleted environment. It rejects
an absent original before mutations; follow this image procedure instead.
Image/VPC availability and future runtime compatibility affect recovery time.
No automatic public production endpoint is provisioned by the test template.

References: [AL2 launch-time package updates](https://docs.aws.amazon.com/linux/al2/ug/ec2.html),
[AL2 multipart user data](https://docs.aws.amazon.com/linux/al2/ug/amazon-linux-cloud-init.html).
