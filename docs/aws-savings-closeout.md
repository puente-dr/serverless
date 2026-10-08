# Serverless AWS savings closeout

Updated October 8, 2026. This closes the repository work for the owner-approved
serverless retirements; it does not claim that all AWS infrastructure has been
adopted into IaC or that every possible saving has been exhausted.

## Completed repository and execution work

- Ten Lambda functions have zero reserved concurrency and five schedules are
  disabled. Source templates and retirement manifests record the intended state.
- The active community-data reader and its cached S3 data remain available for
  installed Collect clients. Reader count/checksum checks remain private.
- Retired deployment workflows are disabled in GitHub and held in source. The
  validation workflow uses read-only permissions and no production credentials.
- PR #49 merged the operational tools, tests, lifecycle decisions and recovery
  documentation. Obsolete PR #48 (analytics development) and PR #35 (an old
  exporter dependency update) were closed; their branches/commits are preserved.
- The read-only verifier in `ops/verify_retirement_state.py` detects execution or
  schedule drift and refuses to certify a disabled protected reader. A failure
  requires investigation; the verifier never changes AWS resources.

## Cost attribution

The disabled Lambda workloads previously contributed negligible AWS charges.
Their retirement prevents unwanted execution and accidental restoration; it is
not the source of the account's major recurring reduction.

The material account savings came from downgrading paid AWS Support, stopping
the owner-retired GraphQL compute, and removing the map environment's compute,
load balancer and related billable resources. GraphQL retains its disk/address;
map retains its independently verified private backup. Those retained recovery
resources still incur charges. Account billing figures and estimated run-rate
calculations are recorded privately, not in this public repository.

No paid capacity reduction was applied to active Flask exports. Its synthetic
memory results make downsizing premature without production memory/concurrency
measurements, a separate Linux candidate and exercised routing rollback. The
website and community reader were also preserved.

## Deliberately deferred

- Full CloudFormation redeployment into drifted analytics stacks. The declared
  databases are absent; a deployment must not recreate them as part of closeout.
- Deletion of retained function definitions, APIs, stored data or legacy backups.
  Execution restrictions have a different recovery contract from deletion.
- Community-reader replacement and active Flask resizing. Both need their own
  compatibility and release gates.
- Provider credential cleanup, per the owner's October 8 direction to focus on
  AWS savings. Historical AWS/Google exposures are recorded privately; source
  removal or a denied lookup does not prove provider revocation. Unverified
  secret-scanning alerts remain open.

Future restoration or further resource removal must be an explicit lifecycle
change, with the tests and recovery contract recorded before execution.
