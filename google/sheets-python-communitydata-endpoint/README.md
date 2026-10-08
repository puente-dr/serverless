# Community-data functions

Updated October 8, 2026. These functions now have different lifecycle states.

## Retired Sheets ingestion

`googlesheets-to-s3` has zero reserved concurrency and its refresh schedule is
disabled. The separate Sheets-to-DynamoDB integration is also disabled. Cached
S3 data is retained, but automatic refresh from Google Sheets has stopped.

The previously tracked Google credential was exposed, removed from the current
source and ignored. It was not a safe example credential. Do not recover it from
Git history, copy it into a deployment package or invoke the retired ingester.
Provider-side revocation is separate work, deferred by the owner during the AWS
savings closeout.

If ingestion is deliberately restored later, use a legitimate replacement
identity and isolated staging output. Follow the [retirement/recovery procedure](../../ops/README.md)
and [testing gates](../../docs/infrastructure-change-safety-plan.md). Do not delete
the retained schedule or data as a shortcut to disabling execution.

## Active community-data reader

`s3-json-to-client` remains available because installed Collect clients use it
for cached autofill data. Preserve its API route and S3 object until a compatible
replacement reaches those clients. Disabling ingestion does not retire this
reader.

The configured GET route supplies these parameters to the Lambda:

- `bucket_name`: the retained S3 bucket.
- `key`: the JSON object's key.
- `parameter`: a property to return, or `all` for the entire cached dataset.

Use the existing configured endpoint from the private operational record.
Verify its status and private count/checksum against the baseline without logging
response bodies to public CI. Its availability does not imply the cached data is
being refreshed or that every supported mobile flow has been tested.
