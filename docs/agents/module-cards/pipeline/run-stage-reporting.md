# Module Card: run-stage-reporting

Owner: pipeline-flow-owner

## Purpose

Generate QA report rows and confusion hotlist artifact outputs.

## When To Use

After product persistence and before quality metrics/finalization.

## Inputs

Run id, output dir, products, enrichments, assignments map, QA sample size.

## Outputs

QA rows/result metadata and confusion hotlist metadata/content paths.

## Steps

1. Build QA rows from products and enrichments.\n2. Write QA report CSV.\n3. Build confusion hotlist rows.\n4. Write confusion hotlist CSV.

## Failure Signals

QA report write failure, confusion hotlist file write failure, empty artifact outputs when data exists.

## Related Files

- 
- 

## Related Commands

- 
> experiments-w-rob@0.1.0 pipeline
> tsx src/cli/pipeline.ts\n- 
> experiments-w-rob@0.1.0 qa:evaluate
> tsx src/cli/qa-evaluate.ts

## Last Verified

- 2026-02-22

