# Map recovery without the retired Beanstalk platform

The map's existing Python 3.8 / Amazon Linux 2 Beanstalk branch is retired. A
successful restart of its retained instance did not prove that Beanstalk could
recreate the environment. Independent recovery was demonstrated before the
owner accepted permanent removal and recovery at a new URL on October 8.

The final October 4 rehearsal passed automatically in approximately six minutes:
a fresh instance restored from the private image passed all nine application
probes and the isolation/EC2 checks. Temporary test resources were removed, with
cleanup and private-image availability reconfirmed October 8. Keep the verified
private image and snapshots independently of the original environment.

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

The probe verifies the stack's instance/security-group ownership, selected image,
VPC/subnet identity, private/unshared image availability, absent IAM profile,
exact restricted network rules, EC2 status checks, root page, Dash layout/dependencies, and all six
known read-only dashboard callbacks. Only response status, size and checksums
are saved. HTTP uses a direct connection to the verified instance address and
rejects redirects; operator proxy settings cannot redirect the probe elsewhere.
It does not verify client-side map-tile delivery or certify every
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

The owner accepted slower recovery at a new URL on October 8. After termination,
`pause_map.py rollback` and its old before-plan cannot restore the original
environment. Use the private image and this isolated recovery procedure; public
routing/TLS needs a separate reviewed configuration before serving users again.

## Approved environment removal

Before removal, verify the environment/application identity, stopped instance,
private image and completed snapshots, successful recovery evidence, and absence
of custom DNS records referencing the old hostname or load balancer. Preserve a
private inventory of the generated CloudFormation stack's physical resources.

Keep replacement processes suspended, but resume the Auto Scaling `Terminate`
process so parent-stack deletion can terminate its stopped instance. Then remove
the environment through its owning API:

```bash
aws elasticbeanstalk terminate-environment --region us-east-1 \
  --environment-name puente-map-env --environment-id "$VERIFIED_ENVIRONMENT_ID" \
  --terminate-resources
```

Wait for termination and `DELETE_COMPLETE` on the generated stack. Verify the
instance, disk, Auto Scaling group, load balancer, target group and associated
security groups are removed, and that the private image/snapshots remain. Repeat
the active Flask, website and community-reader probes. Never use
`--no-terminate-resources`: it would leave the billable resources behind.

References: [Creating EBS-backed AMIs](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-an-ami-ebs.html),
[CloudFormation security-group egress behavior](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-ec2-securitygroup.html).
