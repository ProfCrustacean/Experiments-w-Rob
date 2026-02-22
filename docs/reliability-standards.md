# Reliability Standards

Safety and quality standards for pipeline execution and learning loops.

## Required Standards

- Blocking docs gate on PRs.
- Daily docs-garden schedule.
- Harness checks before auto-apply.
- Bounded retries and bounded loop counts.

## Quality Targets

- `HARNESS_MAX_FALLBACK_RATE`
- `HARNESS_MAX_NEEDS_REVIEW_RATE`
- `CANARY_AUTO_ACCEPT_THRESHOLD`
- `SELF_IMPROVE_GATE_MIN_SAMPLE_SIZE`

See live values in `docs/quality-scoreboard.md`.

## Docs Reliability Requirements

- Required docs must exist and be linked.
- Agent maps/task/module cards must remain navigable.
- Owner coverage and module-card coverage must remain complete.

## Escalation

- If `npm run docs:check` fails, fix in the same change set.
- If metrics regress, create/update debt items in `docs/quality-scoreboard.md`.

## Deep Links

- Docs policy: `docs/governance/docs-policy.md`
- Docs check spec: `docs/governance/docs-check-spec.md`

