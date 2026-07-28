# Task 3 Real Download Columns

Measurements were taken against sqlite 3.45.1 with 8,000 synthetic history rows
and 2,000 running task rows on the same machine used for the test run.

| Path | Before | After |
| --- | ---: | ---: |
| `load_history_page(50, search="needle")` | 19.136 ms | 8.383 ms |
| `load_history_page(50, source_key="example")` | 1.756 ms | 0.290 ms |
| `fail_running_tasks("interrupted")` for 2,000 running rows | 53.973 ms | 11.290 ms |

The repository tests assert the query plans for the source, media-id, and path
lookups so the real-column paths stay exercised.
