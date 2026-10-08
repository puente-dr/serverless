# Retirement review — October 8, 2026

Scope: the AWS retirement PR, its operational scripts, public CI boundaries,
retained service behavior and documented recovery contracts. The named senior
and staff review commands were not installed; the two perspectives were applied
directly by the same reviewer. This is not an independent second-person approval.

## Senior engineering review

- Fixed partial Lambda rollback ordering: a failed or unverified capacity restore
  now keeps schedules disabled. Explicit rollback verifies every restored
  function before enabling schedules. Tests cover failed recovery and successful
  API responses that do not produce the expected state.
- Added plan/manifest resource-scope checks before apply or rollback so extra
  functions, rules or protected-resource entries cannot be silently mutated.
- Replaced operational `assert` gates with explicit exceptions, preserving
  verification when Python optimization is enabled.
- Private evidence files are created exclusively with mode 0600, avoiding the
  interval before permissions were previously narrowed.
- Strengthened the map probe to bind its instance/security group to the actual
  stack resources, selected private image and VPC/subnet. Check exact egress
  controls, prevent HTTP redirects and bypass operator proxies. Added drift and
  image-sharing tests. The previous live restore remains the application recovery
  evidence; these additional guards have mocked boundary tests, not a new live
  restore rehearsal.

## Staff engineering review

- Added an unconditional source job hold to each retired deployment workflow.
  Re-enabling a workflow cannot run its legacy deployment job without a reviewed
  source change. This matters because full deployment into the drifted analytics
  stacks could recreate retired databases.
- Confirmed validation CI has read-only repository permission, no AWS identity,
  pinned checkout and no persisted checkout credentials. The merge does not
  deploy the changed templates or restart retired services.
- Confirmed the changed map recovery contract: its old environment is deleted,
  the verified image/snapshots remain private, and recovery uses a new URL. The
  old retained-instance restart procedure cannot recreate the environment.
- Kept Flask capacity/routing unchanged. Its 93 regression tests pass; synthetic
  memory results do not certify smaller production capacity. See the separate
  sizing evidence and release gates.
- A targeted scan of tracked files found no AWS access-key pattern, private-key
  block or Google service-account JSON. This is not comprehensive secret scanning.
  The removed Google key remains in old history/deployed copies and needs provider
  revocation. That separate credential remediation is still outstanding.

Thirty mocked tests pass in normal and optimized Python, including recovery
failure, identity drift, protected-resource and output-permission cases. Existing
live rehearsals and protected-endpoint checks remain bounded evidence; no claim
of complete user-flow coverage or guaranteed uninterrupted service is made.
