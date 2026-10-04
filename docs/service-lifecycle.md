# Service lifecycle decisions

Updated October 4, 2026. This records owner decisions and bounded evidence; absence of a metric is not proof that a feature has no users.

| Service | Intended state | Current action / unresolved condition |
| --- | --- | --- |
| Flask exports | Keep | Used by Manage; capacity and HTTPS unchanged. |
| GraphQL / Apollo | Retire, per owner | Compute stopped after a successful stop/restart rehearsal, including a read-only query and exact restoration of deployment/scaling controls in approximately 91 seconds. Disk and IP retained. |
| Map | Retire, per owner | Owner chose retirement after reviewing uncertain consumer evidence. Stop/restart rehearsal recovered the ALB targets and root/layout/dependency endpoints in approximately 66 seconds. Final compute pause in progress; disks, load balancer and hostname retained. |
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

The retirement operations are in `ops/`. Private account evidence and rollback state are stored outside this public repository. The initial changes are reversible execution restrictions; no resource definitions, databases or storage were deleted. See `infrastructure-change-safety-plan.md` for the remaining release gates.

Validation completed October 4: 19 mocked operation/failure tests passed; AWS accepted all six changed CloudFormation templates in syntax validation. Templates were not deployed against drifted stacks. After production Lambda retirement, Flask health, the website, map and community reader returned HTTP 200; the reader's count and checksum were unchanged. These checks are not a full client, resolver or load-test certification.

The retired-service deployment workflows are disabled in GitHub and changed to manual triggers in source. The previously tracked Google service-account credential file is removed from this branch and ignored. Removal does not revoke the credential or remove it from Git history or other deployed copies; provider-side revocation remains required. Do not restore the exposed credential when recovering retired integrations.
