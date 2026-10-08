# Map recovery without the retired Beanstalk platform

The map's existing Python 3.8 / Amazon Linux 2 Beanstalk branch is retired. A
successful restart of its retained instance does not prove that Beanstalk can
recreate the environment. Keep the paused original intact until a recovery from
backup has been demonstrated and the changed recovery contract is accepted.

The final October 4 rehearsal passed automatically in approximately six minutes:
a fresh instance restored from the private image passed all nine application
probes and the isolation/EC2 checks. Temporary test resources were removed, with
cleanup and private-image availability reconfirmed October 8. The original
paused environment remains available for the earlier restart-based recovery.

## Backup and isolated rehearsal

1. Confirm the AWS account and that the source instance is stopped. Record the
   application version, environment configuration, disk identities, load-balancer
   settings and deployed source-bundle metadata in a private release directory.
2. Create an EBS-backed AMI of that stopped instance. Tag both image and snapshots
   for recovery, wait for `available`, and verify the image is private with no
   shared launch permissions. Never put the image identifiers, credentials,
   source package, configuration or application records into public CI artifacts.
3. Use `map-recovery-test.yaml` with the private AMI, an existing public subnet in
   the same VPC, and the release operator's current IPv4 `/32`. Preview the change
   set: it must add only a temporary EC2 instance and its security group.
4. Review the test instance's actual network controls. It has no instance role,
   no SSH access and no production security group. HTTP is restricted to the
   operator; an explicit unusable egress rule prevents default allow-all outbound
   access. IMDS requires tokens and supplies no instance-role credentials.
   The template's boot script disables Beanstalk control agents in the copy and
   explicitly enables/restarts the existing application and nginx services.
   This matters because the captured Beanstalk image leaves nginx disabled at
   boot; a raw instance launch alone did not restore HTTP service in rehearsal.
5. Launch the test copy without starting or modifying the retained original.
   Verify EC2 status, HTTP root, Dash layout and dependency JSON, and applicable
   read-only dashboard callbacks. Record timing and response metadata privately;
   do not publish medical/community records or response bodies.
6. Delete the temporary CloudFormation stack after the rehearsal. Confirm the
   test instance terminates and its root volume/security group are removed.
   Keep the recovery AMI and its snapshots private; they incur storage charges.

Example change-set creation (values supplied from the private release record):

```bash
aws cloudformation create-change-set \
  --region us-east-1 \
  --stack-name puente-map-isolated-recovery-test \
  --change-set-name initial-recovery-test \
  --change-set-type CREATE \
  --template-body file://ops/map-recovery-test.yaml \
  --parameters file://"$PRIVATE_RELEASE_DIR/recovery-parameters.json"
```

Review before execution. Do not treat template validation or a completed stack
as proof that the application recovered. The AMI includes legacy software and
may include old application configuration; do not grant it unrestricted network
access or a production IAM role as a shortcut to make a test pass.

After the stack reaches `CREATE_COMPLETE`, run the repeatable probe:

```bash
python3 ops/probe_map_recovery.py --expected-account "$EXPECTED_AWS_ACCOUNT" \
  --result "$PRIVATE_RELEASE_DIR/map-cold-recovery-probes.json"
```

The probe verifies the stack's instance identity, absent IAM profile, restricted
network rules, EC2 status checks, root page, Dash layout/dependencies, and all six
known read-only dashboard callbacks. Only response status, size and checksums
are saved. It does not verify client-side map-tile delivery or certify every
historic dataset. Use a new private result file for each run.

## What permanent retirement changes

Terminating the old Beanstalk environment removes its load balancer, instance and
associated resources. Do this through Beanstalk, not by deleting its generated
child resources individually. Preserve the verified AMI and source/configuration
records independently of the environment.

After termination, the existing Beanstalk hostname is not a guaranteed recovery
route. A cold restore creates a new instance/address and may require a new domain
or routing change. The isolated test deliberately does not create a public
production endpoint or TLS configuration. Image availability, VPC/subnet access,
operator access and future runtime compatibility also affect recovery time.

Permanent retirement therefore needs an explicit decision to accept slower
recovery and a potentially different URL. Until then, use `pause_map.py rollback`
and its private before-plan to recover the retained original route.

References: [Creating EBS-backed AMIs](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-an-ami-ebs.html),
[CloudFormation security-group egress behavior](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-ec2-securitygroup.html).
