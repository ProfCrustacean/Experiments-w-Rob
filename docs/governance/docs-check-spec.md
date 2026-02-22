# Docs Check Specification

Definition of validations performed by `npm run docs:check`.

## Existing Validation Families

- Missing required top-level docs
- Broken path references
- Unknown script references
- Env var docs drift
- Scoreboard freshness

## Agent-First Validation Families

- `MISSING_AGENT_INDEX`
- `MISSING_REQUIRED_SECTION`
- `DOC_TOO_LARGE`
- `DOC_POLLUTED_CONTENT`
- `MODULE_CARD_MISSING`
- `TASK_CARD_ORPHANED`
- `OWNER_MISSING`
- `OWNER_UNMAPPED`
- `ARCHITECTURE_DRIFT`

## Coverage Metrics in Summary

- `agentDocCount`
- `oversizeDocCount`
- `missingSectionCount`
- `moduleCoverageRate`
- `ownerCoverageRate`

## Notes

- `docs:check` remains the single blocking docs command.
- `docs:scoreboard` consumes docs-check summary and publishes trend visibility.
