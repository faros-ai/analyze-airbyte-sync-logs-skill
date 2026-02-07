---
name: analyze-sync-logs
description: Generate a human-readable report from Airbyte sync log(s). Use when analyzing connector sync failures, comparing sync runs, or debugging connector issues.
allowed-tools: Bash(python3:*)
---

Analyze the provided Airbyte sync log file(s) and present a concise diagnostic report.

Arguments: $ARGUMENTS (one or more log file paths)

Steps:

1. For each log file, run: `python3 scripts/analyze-sync-logs.py <file>`
2. Parse the JSON output from each run
3. Present a report for each log covering:
   - Sync status, duration, start/end times
   - Source and destination versions and images
   - Key source config values that are present (e.g. url, cutoff_days, start_date, bucket_id/bucket_total, page_size)
   - Key destination config values that are present (e.g. origin, edition, graph, graphql_api)
   - Catalog: number of streams, list stream names with sync modes
   - Records per stream (source reads) with total
   - Destination stats: total read/processed/written/skipped/errored
   - Top destination models by write count
   - Errors and warnings (full messages)
4. If multiple logs are provided, add a comparison section:
   - Flag any differences in versions, config, or catalog
   - Show record count changes per stream (delta and direction)
   - Show model write count changes (top movers)
   - Highlight anything that could explain failures or anomalies (e.g. large record count swings, new errors, status changes)
5. End with a brief "Key observations" section calling out anything notable
