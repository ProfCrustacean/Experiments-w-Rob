import path from "node:path";
import { closePool } from "../db/client.js";
import { getConfig } from "../config.js";
import { runPipeline } from "../pipeline/run.js";
import { getPipelineRunById } from "../pipeline/persist.js";
import { buildCanarySubset, writeCanaryState } from "../canary/select-subset.js";
import { isGatePassing, readAutoAcceptedRateFromStats } from "../canary/gate.js";

async function main(): Promise<boolean> {
  const config = getConfig();
  if (!config.CATALOG_INPUT_PATH) {
    throw new Error("Missing CATALOG_INPUT_PATH in environment.");
  }
  if (!config.STORE_ID) {
    throw new Error("Missing STORE_ID in environment.");
  }

  const cwd = process.cwd();
  const inputPath = path.resolve(cwd, config.CATALOG_INPUT_PATH);
  const outputDir = path.resolve(cwd, config.OUTPUT_DIR);
  const subsetPath = path.resolve(cwd, config.CANARY_SUBSET_PATH);
  const statePath = path.resolve(cwd, config.CANARY_STATE_PATH);
  const runLabel = `canary-${new Date().toISOString().replace(/[:.]/g, "-")}`;

  const subset = await buildCanarySubset({
    inputPath,
    outputDir,
    subsetPath,
    statePath,
    sampleSize: config.CANARY_SAMPLE_SIZE,
    fixedRatio: config.CANARY_FIXED_RATIO,
    randomSeed: config.CANARY_RANDOM_SEED,
    storeId: config.STORE_ID,
  });

  const runSummary = await runPipeline({
    inputPath: subset.subsetPath,
    storeId: config.STORE_ID,
    runLabel,
  });

  const runRecord = await getPipelineRunById(runSummary.runId);
  if (!runRecord) {
    throw new Error(`Could not load run stats for canary run ${runSummary.runId}.`);
  }

  const autoAcceptedRate = readAutoAcceptedRateFromStats(runRecord.stats);
  const threshold = config.CANARY_AUTO_ACCEPT_THRESHOLD;

  const confusionArtifact = runSummary.artifacts.find(
    (artifact) => artifact.format === "confusion-csv" || artifact.key === "confusion_hotlist_csv",
  );

  if (!confusionArtifact) {
    throw new Error(`Canary run ${runSummary.runId} did not produce a confusion hotlist artifact.`);
  }

  const canaryHotlistPath = path.resolve(outputDir, confusionArtifact.fileName);
  const canaryState = await writeCanaryState({
    statePath,
    runId: runSummary.runId,
    hotlistPath: canaryHotlistPath,
  });

  const passed = isGatePassing(autoAcceptedRate, threshold);

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        runId: runSummary.runId,
        runLabel,
        storeId: config.STORE_ID,
        canary: {
          requested: subset.sampleSizeRequested,
          used: subset.sampleSizeUsed,
          totalAvailable: subset.totalAvailable,
          fixedTarget: subset.fixedTarget,
          fixedSelected: subset.fixedSelected,
          randomSelected: subset.randomSelected,
          subsetPath: subset.subsetPath,
          hotlistSource: subset.hotlistSource,
          hotlistPathUsedForSelection: subset.hotlistPath,
          warnings: subset.warnings,
        },
        rollingHotlist: {
          statePath,
          lastCanaryHotlistPath: canaryState.lastCanaryHotlistPath,
          updatedAt: canaryState.updatedAt,
        },
        gate: {
          metric: "auto_accepted_rate",
          value: autoAcceptedRate,
          threshold,
          passed,
        },
      },
      null,
      2,
    ),
  );

  return passed;
}

main()
  .then(async (passed) => {
    await closePool();
    if (!passed) {
      process.exitCode = 1;
    }
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("Canary run failed:", error);
    await closePool();
    process.exitCode = 1;
  });
