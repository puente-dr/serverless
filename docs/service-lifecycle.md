# Service lifecycle decisions

Updated October 4, 2026. This records owner decisions and bounded evidence; absence of a metric is not proof that a feature has no users.

| Service | Intended state | Current action / unresolved condition |
| --- | --- | --- |
| Flask exports | Keep | Used by Manage; capacity and HTTPS unchanged. |
| GraphQL / Apollo | Unused, per owner | Still running. Prepare a recoverable Beanstalk retirement separately; do not count savings yet. |
| Map | Investigate | No callers found in searched Puente sources; request traffic remains unexplained. Retain pending consumer attribution. |
| Production serverless exporter | Investigate | No invocation datapoints in the review window, old execution logs, no endpoint consumers found in searches. Remains unchanged pending lifecycle decision. |
| Production ETL | Investigate | Daily delivery attempts failed throughout the review window. Remains unchanged pending intended-use decision. |
| Production analytics | Investigate | Databases declared by its stacks are absent. Scheduler deliveries do not reconcile with missing Lambda invocation datapoints and old logs. Remains unchanged. |
| Sheets ingestion | Retire | Function execution and refresh schedule disabled, retained for rollback. No new automatic sheet-to-S3 refreshes. |
| Community-data reader | Replace before retirement | Collect still calls it for cached autofill data. Endpoint and S3 data preserved. |
| SMS | Retire | Both AWS SMS function executions disabled. Function definitions retained. External Twilio account/number billing was not changed. |
| Persistent dev exporter / ETL / analytics | Retire execution | Three functions and two schedules disabled. Templates reflect this; use isolated temporary staging when required. Stacks and stored data remain. |
| Website | Keep | Main website is served by S3/CloudFront; apex redirects to the www site. |
| Paid AWS support | Retire | Downgraded to Basic Support through the account console. |

Usage review window: July 1 through October 2, 2026, UTC (end exclusive October 3). Invocations made by audit smoke tests occur after that window. GitHub code search has indexing limits; code absence and low traffic do not establish consumer absence.

The retirement operations are in `ops/`. Private account evidence and rollback state are stored outside this public repository. The initial changes are reversible execution restrictions; no resource definitions, databases or storage were deleted. See `infrastructure-change-safety-plan.md` for the remaining release gates.
