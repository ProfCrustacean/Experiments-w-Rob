# Decisions Log

Lightweight architecture and policy decisions.

## 2026-02-21: Self-improvement execution mode

- Status: accepted
- Decision: use queued async worker processing for loop batches
- Why: predictable throughput and easier operational control
- Tradeoff: slower wall-clock completion than parallel loops

## 2026-02-21: Loop retry policy

- Status: accepted
- Decision: retry each failed loop once, then continue
- Why: catches transient failures without blocking whole batches
- Tradeoff: persistent failures still consume one extra run attempt

## 2026-02-21: Auto-apply learning policy

- Status: accepted
- Decision: apply learning updates only when all gates pass
- Why: protects data quality and limits unsafe drift
- Tradeoff: some useful proposals remain queued longer

## 2026-02-21: Structural change cap

- Status: accepted
- Decision: max 2 structural applies per loop
- Why: allows aggressive improvement while bounding blast radius
- Tradeoff: structural backlog can build during high-change periods

## 2026-02-21: Docs governance

- Status: accepted
- Decision: enforce docs checks in CI and weekly docs-garden schedule
- Why: keep documentation aligned with fast-moving pipeline changes
- Tradeoff: occasional noisy failures when docs lag behind code
