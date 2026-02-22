# Docs Policy

Policy baseline for agent-first documentation quality.

## Scope

Applies to:

- `README.md`
- `docs/*.md`
- `docs/agents/**/*.md`
- `docs/governance/*.md`

## Required Shapes

- Root index: `docs/index.md`
- Agent index: `docs/agents/index.md`
- Ownership map: `docs/agents/maps/ownership-map.md`
- Task cards under `docs/agents/task-cards/`
- Module cards under `docs/agents/module-cards/`

## Micro-Doc Standard

Task and module cards must include exactly these section headers:

- `Purpose`
- `When To Use`
- `Inputs`
- `Outputs`
- `Steps`
- `Failure Signals`
- `Related Files`
- `Related Commands`
- `Last Verified`

## Size Limits

- Task cards: 140 lines max
- Module cards: 120 lines max
- Map docs: 180 lines max
- Entrypoint docs (`README.md` and top-level docs): 220 lines max

## Ownership

Area ownership keys are defined in `docs/agents/maps/ownership-map.md`.
Every task card, module card, and map doc must include an `Owner: <key>` line.

## Enforcement

- Required command: `npm run docs:check`
- Scoreboard refresh: `npm run docs:scoreboard`
- Docs gate remains blocking in CI.

