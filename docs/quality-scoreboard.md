# Quality Scoreboard

- Last updated: 2026-02-22T01:39:07.328Z
- Window: trailing 7 days
- Store scope: continente
- Pipeline runs in scope: 28 (previous window: 0)
- Self-improvement batches in scope: 6 (previous window: 0)

## Core metrics

| Metric | Target | Current | Trend | Notes |
| --- | --- | --- | --- | --- |
| `fallback_category_rate` | <= 0.0600 | 0.0313 | n/a | From pipeline run stats |
| `needs_review_rate` | <= 0.3500 | 0.4621 | n/a | From pipeline run stats |
| `auto_accepted_rate` | >= 0.8000 (canary) | 0.5838 | n/a | Canary-labeled runs only |
| `gate_pass_rate` | increasing | 0.0000 | n/a | From self-improvement batch summaries |
| `auto_applied_updates_count` | stable growth | 0 | stable | Sum across batch summaries |
| `rollbacks_triggered` | low/stable | 0 | stable | From rollback events |

## Documentation health

| Metric | Target | Current | Trend | Notes |
| --- | --- | --- | --- | --- |
| `docs:check` errors | 0 | 0 | n/a | Blocking doc correctness issues |
| `docs:check` warnings | downward trend | 0 | n/a | Non-blocking cleanup items |
| Broken doc path refs | 0 | 0 | n/a | `BROKEN_PATH_REF` findings |
| Unknown documented scripts | 0 | 0 | n/a | `UNKNOWN_SCRIPT_REF` findings |
| Agent docs tracked | increasing | 30 | n/a | Markdown files under `docs/agents/` |
| Oversize docs | 0 | 0 | n/a | `DOC_TOO_LARGE` findings |
| Missing card sections | 0 | 0 | n/a | `MISSING_REQUIRED_SECTION` findings |
| Module card coverage | 1.0000 | 1.0000 | n/a | Required module cards present |
| Owner coverage | 1.0000 | 1.0000 | n/a | Owner key mapped to ownership map |

## Open debt queue

- Keep this list short and specific.
- Move completed items into the decisions log if they change policy.

1. Needs-review rate is above target (0.4621 > 0.3500).
2. Canary auto-accepted rate is below threshold (0.5838 < 0.8000).
3. Batch gate pass rate is low (0.0000).

