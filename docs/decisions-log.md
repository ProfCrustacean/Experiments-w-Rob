# Decisions Log

Concise record of accepted operational and architecture decisions.

## 2026-02-21: Self-improvement execution mode

- Status: accepted
- Decision: queued async worker for loop batches
- Why: predictable throughput and easier recovery control
- Tradeoff: slower wall-clock completion than parallel loops

## 2026-02-21: Loop retry policy

- Status: accepted
- Decision: retry each failed loop once, then continue
- Why: catches transient failures without blocking whole batches
- Tradeoff: persistent failures still consume one extra attempt

## 2026-02-21: Tiered canary auto-apply

- Status: accepted
- Decision: full apply / partial low-risk / no-apply by gate band
- Why: balance safety with iteration velocity
- Tradeoff: status interpretation is more nuanced

## 2026-02-22: Agent-first docs architecture

- Status: accepted
- Decision: split into maps + task cards + module cards with strict checks
- Why: reduce navigation cost for agents and operators
- Tradeoff: more files to maintain

## 2026-02-22: Docs governance cadence

- Status: accepted
- Decision: blocking docs checks in CI and daily docs-garden refresh
- Why: keep docs synchronized with fast code changes
- Tradeoff: stricter PR discipline required

