# Quality Scoreboard

- Last updated: 2026-02-21T16:11:47.355Z
- Window: trailing 7 days
- Store scope: continente
- Pipeline runs in scope: 12 (previous window: 0)
- Self-improvement batches in scope: 0 (previous window: 0)

## Core metrics

| Metric | Target | Current | Trend | Notes |
| --- | --- | --- | --- | --- |
| `fallback_category_rate` | <= 0.0600 | 0.0814 | n/a | From pipeline run stats |
| `needs_review_rate` | <= 0.3500 | 0.6151 | n/a | From pipeline run stats |
| `auto_accepted_rate` | >= 0.8000 (canary) | 0.3071 | n/a | Canary-labeled runs only |
| `gate_pass_rate` | increasing | n/a | n/a | From self-improvement batch summaries |
| `auto_applied_updates_count` | stable growth | 0 | stable | Sum across batch summaries |
| `rollbacks_triggered` | low/stable | 0 | stable | From rollback events |

## Documentation health

| Metric | Target | Current | Trend | Notes |
| --- | --- | --- | --- | --- |
| `docs:check` errors | 0 | 0 | n/a | Blocking doc correctness issues |
| `docs:check` warnings | downward trend | 0 | n/a | Non-blocking cleanup items |
| Broken doc path refs | 0 | 0 | n/a | `BROKEN_PATH_REF` findings |
| Unknown documented scripts | 0 | 0 | n/a | `UNKNOWN_SCRIPT_REF` findings |

## Open debt queue

- Keep this list short and specific.
- Move completed items into the decisions log if they change policy.

1. Fallback rate is above target (0.0814 > 0.0600).
2. Needs-review rate is above target (0.6151 > 0.3500).
3. Canary auto-accepted rate is below threshold (0.3071 < 0.8000).

