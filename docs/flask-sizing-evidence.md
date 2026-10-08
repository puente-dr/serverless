# Flask capacity review — October 8, 2026

Decision: keep production on its current t3.medium capacity. The available
evidence does not justify a move to t3.small yet. No capacity, runtime, routing
or deployment setting was changed during this review.

The deployed application version identifies source commit
`8059c17ebf95e269aa73466c6962a70f56f0874a`. A private comparison of its source
bundle with the local aggregator checkout found 42 Python, requirements and
Procfile files byte-identical, with no differences among those compared files.
The Procfile uses Gunicorn with preload. The live platform is Python 3.8 on
Amazon Linux 2; local deployment configuration selects a different platform, so
an ordinary deployment must not be assumed to preserve the runtime.

## Completed checks

The existing aggregator regression suite passed all 93 tests on Python 3.8.20
with its pinned application dependencies and production network access blocked.
The suite covers differential cleaning, endpoint contracts, organization aliases,
custom fields, age calculation and the SQLite versions model. It emitted 100
existing deprecation warnings. This is application regression evidence, not a
Linux candidate, real upstream integration or production load test.

Synthetic requests to the actual Flask v3 export endpoint produced HTTP 200 and
the expected CSV row counts, preserving the synthetic organization boundary:

| Synthetic records | Peak process RSS (MiB) | Trial elapsed time (seconds) |
| --- | ---: | ---: |
| 10,000 | 179 | 0.44 |
| 50,000 | 588 | 2.38 |
| 100,000 | 1,152 | 4.77 |
| 250,000 | 2,614 | 12.06 |

These fresh-process trials ran on macOS ARM64 with upstream requests replaced by
synthetic JSON. RSS includes fixture creation and CSV validation, not just the
application's allocations. Elapsed time includes those steps too and cannot
predict AWS latency. Actual production record sizes and peak concurrency remain
unknown; the trial sizes are stress scenarios, not observed partner workloads.
Two parallel 100,000-record trial processes summed approximately 2,309 MiB of
individual peak RSS; that sum is not a measurement of simultaneous live memory
or the production Gunicorn worker configuration.

[AWS specifies 2 GiB for t3.small and 4 GiB for t3.medium](https://aws.amazon.com/ec2/instance-types/t3/).
The larger synthetic case exceeds a small instance's total RAM even before
reserving headroom for its OS and other processes. Low historical CPU utilization
therefore cannot establish that halving RAM is safe. The review found no CWAgent
memory metrics or SSM-managed instance through which memory could be measured
without establishing another access/telemetry path.

## Gates before resizing

Measure actual peak export size, worker/concurrency settings and memory over a
representative usage cycle. Trial the exact deployed artifact on a separate
Linux candidate, including large supplementary/custom-field exports and upstream
failures. Apply the capacity thresholds, candidate soak and exercised routing
rollback in [the infrastructure safety plan](infrastructure-change-safety-plan.md).
Keep the current instance capacity and HTTPS routing until those gates pass.

Raw source bundles, detailed evidence and operational metadata remain outside
this public repository. No production records were used in these local trials.
