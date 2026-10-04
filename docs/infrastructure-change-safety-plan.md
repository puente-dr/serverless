# Infrastructure testing and rollback plan

Status: proposed release gates, not an executed migration or a zero-downtime certification.

Goal: reduce cost and bring retained infrastructure under version control while preserving client behavior and data. A reviewer must be able to trace each change to its tests, deployed artifact, rollback target and operating owner. No production change may depend on an untested rollback.

## 1. Establish a reproducible baseline

Before changing capacity, runtime, routing, credentials or ownership:

- Inventory every endpoint and consumer: Manage exports, supported Collect versions, map embeds, GraphQL consumers, API Gateway clients, Google Sheets ingestion, scheduled ETL, analytics and SMS. Include external DNS, certificates, hard-coded environment URLs and direct load-balancer URLs. Mark unknown consumers as unresolved, not unused.
- Map each resource to its owning stack/service, source template, deployment workflow and accountable owner. Reconcile CloudFormation drift, including deleted analytics databases, before updating those stacks.
- Capture deployed application versions, immutable source bundles, dependency versions, Lambda versions/layers, configuration, scaling settings, listeners, certificates, DNS and TTLs. Keep operational identifiers and secret references in restricted release records; never publish secret values.
- Preserve recoverable state. Determine whether EC2 disks or local SQLite/files contain persistent data. Verify backups can actually be restored into an isolated environment. A CloudFormation rollback or Git revert cannot recover deleted data.
- Record 7 days of baseline traffic, successful business transactions, HTTP errors, export completion time, peak resident memory and scheduled job results. Extend observation through the longest relevant schedule/partner usage cycle. Historical ALB metrics alone do not identify consumers.
- Name a release operator and an independent observer when available. Record abort criteria, routing reversal, credentials required for rollback and the last known-good artifact. Verify access before the release.

Gate: no unresolved critical consumer, state ownership or restore question. The existing service remains running. Do not combine runtime upgrades, instance resizing and application behavior changes in one release.

## 2. Make public-repository CI safe before using it for deployment

The current workflows deploy on branch pushes; checked-in template changes can therefore be operational changes. Work on a feature branch. Establish the following before merging deployable infrastructure:

- Run unprivileged pull-request checks with synthetic fixtures and `contents: read`. Fork PRs receive no production credentials, OIDC deployment permissions or access to production data.
- Do not execute untrusted PR code in a privileged `pull_request_target` job. Require review of workflow, infrastructure and dependency changes; review the exact commit being released.
- Separate validation, deployment preview and execution. Use a protected production environment, serialized deployments and an explicit promotion gate. A documentation-only change must not redeploy the application.
- Use narrowly scoped GitHub OIDC roles with trust constrained to the intended repository and protected deployment context. Validate the replacement authentication path before retiring existing deployment credentials.
- Pin third-party Actions to reviewed immutable commits; use required CI checks on the production branch. Require secret scanning and push protection, and scan packaged artifacts as well as source/history. Treat findings as exposed even after deleting a file.
- Keep real records, billing exports, infrastructure evidence dumps, raw request logs, credentials and operational rollback bundles out of this public repository and public CI artifacts. Synthetic fixtures must not be disguised copies of production records.

Public repository visibility permits reading, forks and proposed PRs. Historical commit authorship does not grant write or deployment access. Audit direct grants, teams, organization base permissions, invitations, deploy keys and installed automation separately.

## 3. Required test matrix

| Surface | Validation | Required evidence |
| --- | --- | --- |
| Infrastructure | Template validation and linting, IAM review, expected resource ownership, deployed-template comparison, drift inspection, previewed additions/replacements/deletions | No unexpected replacements, deletions, public exposure or scheduled-job changes. Preview is necessary but does not prove runtime success. |
| Flask behavior | Run the existing aggregator `python -m pytest tests/` suite on the current runtime and candidate runtime if changing it; differential CSV, cleaning, custom fields, aliases, version endpoints and endpoint regression tests | All pass on the exact release commit. Local mocked tests are necessary but not evidence of network, TLS or upstream integration. |
| Flask integration | Candidate `/health`, legacy v1 JSON, v2/v3 CSV exports, canonical organization names/aliases/short codes, empty/missing-field/Unicode fixtures, custom-form filters and versions endpoints | Correct status, schema, CSV headers/row counts/content, CORS and intended access controls. Validate tenant boundaries and compare against frozen synthetic inputs. |
| User workflows | Manage login and each supported export flow; Collect supported-version checks, offline queue synchronization and submission into a dedicated test organization | No lost/duplicate submissions, download failures or client configuration changes. Test against isolated dependencies; do not create records in real partner organizations. |
| Capacity | Largest representative synthetic export, sustained observed peak concurrency, then 2x peak and recovery, plus concurrent small requests | No OOM/restart/timeouts; peak memory under 70% of usable RAM at normal peak, no sustained swap growth; p95 completion time within 10% of baseline and below client/server timeout margins. If concurrency is unknown, measure it before sizing. |
| Map / GraphQL | Discover required paths/queries, exercise known client flows and static assets, classify traffic using privacy-minimized logs | No retirement until owners/consumers are accounted for. Low CPU and old deployments do not establish disuse. |
| Exporter / ETL / analytics | Isolated staging buckets and database, representative synthetic events, schema/row-count comparison, retry and partial-failure tests | No duplicate outputs or missed jobs; replay/idempotency and recovery demonstrated. Never enable duplicate production schedules on a clone. |
| Sheets / SMS | Credentialed read using the replacement identity and a test sheet; isolated ingestion output; SMS provider sandbox or mocked sends | Successful authorization and equivalent transformed output; no production message sends during tests. |
| TLS / routing | Candidate tested under the real hostname semantics, certificates/SNI, redirects, CORS, upstream connectivity and DNS resolution | Both environments can serve the intended hostname during DNS convergence; all routing paths are understood. |
| Rollback | Route to candidate, deliberately trigger a synthetic failure, execute reversal and repeat user-flow probes | Timestamped recovery measurement, requests accounted for, old artifact intact. Repeat separately for credentials and scheduled jobs. |

Thresholds above are proposed acceptance criteria, not measured production SLOs. Tighten them if the baseline or client contract requires it. Any failure involving data correctness or tenant isolation is an immediate no-go, even if latency is acceptable.

## 4. Release without taking the existing service down

1. First codify the retained current behavior. Review an expected-no-change deployment preview or adopt/import resources only using a supported ownership procedure with its own rehearsal. Do not edit Beanstalk-generated child stacks independently.
2. Build a separate candidate environment from the recorded application artifact. Keep the current runtime and behavior while trialing a smaller instance. Apply a runtime upgrade in a later, independently tested release. Disable candidate production schedules and side effects; use isolated data.
3. Run the test matrix and a minimum 24-hour candidate soak, including burst exports and upstream-failure recovery. Capture RAM measurements; CPU alone cannot approve a smaller pandas export instance.
4. Freeze competing deployments for the release window. Confirm the pipeline will not overwrite the rollback environment or deploy to the wrong environment after a URL swap. Save its current target and establish the post-cutover target explicitly.
5. For Beanstalk, prefer a rehearsed blue/green URL swap where the actual routing supports it. A custom DNS alias directly targeting an ALB may require a different cutover. Do not assume a Beanstalk CNAME swap updates every consumer. Do not invent percentage-based canaries where no weighted routing exists.
6. Validate real-hostname TLS, CORS and safe business probes immediately after promotion. Keep both environments healthy through measured DNS/client cache convergence and the maximum in-flight export duration. Do not remove the old ALB or terminate old instances during this interval.
7. Observe actively for at least 60 minutes, then retain the warm rollback environment for at least 7 days and through the relevant complete job/partner cycle, whichever is longer. Temporary duplicate capacity is intentional. Record its cost and retirement date in the release record.
8. Decommission old resources only after the observation gate, confirmed backup/restore evidence and explicit release sign-off. Once warm capacity is removed, immediate rollback is no longer available; record the new restore-time objective.

Blue/green aims for uninterrupted service but cannot promise zero failed requests. DNS caching, long-running exports, persistent state and external dependencies must be tested. Existing single-instance services are not intrinsically highly available.

## 5. Rollback triggers and procedure

During promotion and the first hour, run synthetic probes every 30 seconds. Record results privately without response bodies containing personal data.

Immediate rollback triggers:

- Any export content mismatch, tenant-isolation failure, lost/duplicate write, authentication regression or certificate failure.
- Two consecutive failed synthetic business probes; an OOM or unexplained application restart under expected load.
- In a rolling five-minute window with at least 100 requests, 5xx exceeds both 1% and baseline by 0.5 percentage points. For lower traffic, rely on synthetic probes and investigate every new 5xx.
- Export p95 exceeds baseline by 20% for 10 minutes or any supported export hits a client timeout.
- A scheduled job misses its expected completion deadline or produces incorrect/duplicate output.

Procedure:

1. Stop promotion and competing deployment jobs; retain diagnostic metadata without logging sensitive payloads.
2. Verify the warm old environment is healthy, then reverse the exact routing operation rehearsed in staging. If health is not confirmed, use the tested alternate recovery path rather than blindly switching.
3. Leave the candidate running while cached DNS and in-flight requests drain; do not kill active exports. Check both routing destinations and real client flows during convergence.
4. Restore the pipeline deployment target and last known-good artifact/configuration. Reconcile IaC source to that intended state so the next release cannot reapply the failed change.
5. Validate export correctness, submissions, TLS and version checks. For jobs, pause the candidate scheduler, restore the previous target, and replay only missing work using the tested idempotency mechanism. Do not blindly rerun failed batches.
6. Record the failure, measured recovery time and remediation before another attempt.

Target: initiate routing reversal within 5 minutes of a trigger; measure actual end-to-end recovery in rehearsal. DNS-dependent recovery has no guaranteed five-minute completion until measured. Target zero data loss; keep database schemas and write formats backward-compatible. Snapshot restores are a separate disaster-recovery action, not the normal routing rollback.

## 6. Changes that need distinct treatment

- **Flask sizing:** preserve HTTPS termination and the old capacity during the trial. Removing the ALB is a separate architecture change with a separate test plan; it is not bundled with resizing.
- **Legacy-service retirement:** observe a full representative usage cycle, resolve external consumers and retain the current route until there is an agreed replacement or confirmed retirement. Do not deliberately shut a service down to discover whether anyone needs it.
- **Deleted analytics databases:** reconcile intended ownership and lifecycle before deployment. Do not restore a database or enable its jobs merely to eliminate drift. Reconcile snapshots and any retained outputs separately.
- **Credential rotation:** locate each consumer, provision a replacement through the provider, test it with least privilege, deploy the replacement, verify successful execution, then revoke the exposed credential promptly. Never use a publicly exposed key as the rollback credential. Preserve the ability to roll back application code while retaining the replacement secret. No secret values belong in this plan.
- **Support subscription:** keep paid support during risky infrastructure changes if it is part of the recovery strategy; downgrade afterward. Removing support saves money but is not a technical rollback mechanism.
- **Backup deletion:** exclude it from the first savings release. Data-retention approval and a tested alternative recovery copy are required; deletion has no ordinary rollback.

## 7. Completion evidence

A release record must identify the exact commit/artifacts, successful checks, representative test inputs, private metrics, reviewed infrastructure preview, routing and pipeline targets, rollback rehearsal result, owner, observation window and final resource inventory. Check actual billing after the changes settle. Mark each gate passed, failed or not run; do not label this plan itself as a passing test.

Current state when this plan was written: repository and AWS configuration were inspected; no candidate was provisioned, no load/integration/rollback rehearsal was run, and no service or credential was changed. Existing unit tests were inspected but not rerun for this documentation-only change. Production deployment is not yet cleared by this plan.

References: [AWS Beanstalk blue/green deployment](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/using-features.CNAMESwap.html), [Beanstalk configuration precedence](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/command-options.html), [CloudFormation drift detection and limitations](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-stack-drift.html), [GitHub repository roles](https://docs.github.com/en/organizations/managing-user-access-to-your-organizations-repositories/managing-repository-roles/repository-roles-for-an-organization).
