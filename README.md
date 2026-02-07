# Troubleshooting Airbyte Connector Syncs

An AI coding assistant skill to generate human-readable diagnostic reports from Airbyte connector sync logs.

## Overview

Parses Airbyte sync log files and produces structured reports with:
- Sync status, duration, and timing
- Source and destination versions
- Configuration summaries
- Per-stream record counts
- Destination write statistics by model
- State tracking (initial and final)
- Errors and warnings

## Installation

Clone the skill into your project:

```bash
git clone https://github.com/faros-ai/airbyte-analyze-sync-logs-skill .claude/skills/analyze-sync-logs
```

Or add as a submodule:

```bash
git submodule add https://github.com/faros-ai/airbyte-analyze-sync-logs-skill .claude/skills/analyze-sync-logs
```

## Usage

Ask your AI assistant to analyze a sync log:

- "Analyze the sync log at `/path/to/sync_log.txt`"
- "What happened in this sync?"
- "Compare these two sync logs"
- "Were there any errors in this sync?"

## Output

The skill extracts structured data from the log file, which the AI assistant then uses to generate a human-readable report tailored to your question. Reports typically include:

- Sync status summary (success/failure, duration)
- Record counts per stream
- Destination write statistics
- Errors and warnings with context
- State changes between syncs
- Comparisons when analyzing multiple logs

## Common Queries

| Question | What to Ask |
|----------|-------------|
| Did the sync succeed? | "What's the sync status?" |
| How many records synced? | "Show records per stream" |
| Any errors? | "Were there any errors?" |
| Compare two syncs | "Compare these logs and highlight differences" |
| Check state preservation | "What's the initial and final state for X stream?" |
| Connector versions | "What source and destination versions were used?" |

## License

Apache 2.0 - see [LICENSE](LICENSE)
