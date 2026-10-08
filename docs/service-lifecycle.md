# Service lifecycle decisions

Updated October 8, 2026. This records owner decisions and bounded evidence; absence of a metric is not proof that a feature has no users.

| Service | Intended state | Current action / unresolved condition |
| --- | --- | --- |
| Flask exports | Keep | Used by Manage; capacity and HTTPS unchanged. |
| GraphQL / Apollo | Retire, per owner | Compute stopped after a successful stop/restart rehearsal, including a read-only query and exact restoration of deployment/scaling controls in approximately 91 seconds. Disk and IP retained. |
| Map | Retire, per owner | Beanstalk environment and its compute, disk, load balancer and associated resources removed October 8. Verified private image and snapshots retained. Independent recovery passed nine application probes in approximately six minutes; owner accepted recovery at a new URL. |
| Production serverless exporter | Retire execution | Owner confirmed retirement. Function execution disabled; definition and API retained for rollback. |
| Production ETL | Retire execution | Owner confirmed retirement. Function execution and daily schedule disabled; definitions retained. |
| Production analytics | Retire execution | Owner confirmed retirement. Function execution and daily schedule disabled. Declared databases remain absent; no database recreated or backup deleted. |
| Sheets ingestion | Retire | Function execution and refresh schedule disabled, retained for rollback. No new automatic sheet-to-S3 refreshes. |
| Community-data reader | Replace before retirement | Collect still calls it for cached autofill data. Endpoint and S3 data preserved. |
| SMS | Retire | Both AWS SMS function executions disabled. Function definitions retained. External Twilio account/number billing was not changed. |
| Persistent dev exporter / ETL / analytics | Retire execution | Three functions and two schedules disabled. Templates reflect this; use isolated temporary staging when required. Stacks and stored data remain. |
| Website | Keep | Main website is served by S3/CloudFront; apex redirects to the www site. |
| Paid AWS support | Retire | Downgraded to Basic Support through the account console. |

Usage review window: July 1 through October 2, 2026, UTC (end exclusive October 3). Invocations made by audit smoke tests occur after that window. GitHub code search has indexing limits; code absence and low traffic do not establish consumer absence.

The retirement operations are in `ops/`. Private account evidence and rollback state are stored outside this public repository. Lambda changes are reversible execution restrictions and GraphQL retains its instance. Map removal uses the independently verified backup for recovery; its old instance and disk were deleted. No database or pre-existing backup was deleted. See `infrastructure-change-safety-plan.md` for the remaining release gates.

Validation completed October 4: 19 mocked operation/failure tests passed locally and in GitHub Actions; AWS accepted all six changed CloudFormation templates in syntax validation. Templates were not deployed against drifted stacks. Live stop/restart rehearsals passed for GraphQL and map before their final pauses. Final checks verified ten retired functions and five schedules disabled. Flask health, the website and community reader returned HTTP 200; the reader's count and checksum were unchanged. These checks are not a full client, resolver or load-test certification.

The retired-service deployment workflows are disabled in GitHub and changed to manual triggers in source. The previously tracked Google service-account credential file is removed from this branch and ignored. Removal does not revoke the credential or remove it from Git history or other deployed copies; provider-side revocation remains required. Do not restore the exposed credential when recovering retired integrations.

Independent map recovery on October 4: a fresh CloudFormation instance restored
from a private image passed the root page, layout/dependency JSON, all six known
read-only dashboard callbacks, EC2 checks and network/IAM isolation checks in
approximately six minutes. The recovery template explicitly starts nginx,
which the captured Beanstalk image leaves disabled at boot. No manual service
startup was needed in the final rehearsal. The temporary stacks, instances,
disks and security groups were removed; their absence and the private image's
availability were reconfirmed October 8. See `ops/map-recovery.md` for the tested
procedure and its routing limits.

October 8 verification: the map environment terminated and its generated stack
reached `DELETE_COMPLETE`. Its instance, root disk, scaling group, load balancer,
target group, security groups and launch template are removed. The image and
completed snapshots remain private and unshared. No custom DNS records in the
account pointed to the retired hostname/load balancer. GraphQL remains stopped;
Flask health, the website and the community reader returned HTTP 200, with the
reader count and checksum unchanged. Ten functions and five schedules remain
disabled. Twenty mocked operation/failure tests pass, including rejection of the
old map rollback when its environment no longer exists. Recovery now follows
`ops/map-recovery.md`; the earlier 66-second restart no longer applies to map.
