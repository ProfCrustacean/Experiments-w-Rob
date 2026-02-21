import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { getConfig } from "../config.js";
import { enqueueSelfImprovementBatch } from "../pipeline/persist.js";
import { parseSelfImprovementPhrase } from "../pipeline/self-improvement-orchestrator.js";
import type { SelfImproveLoopType } from "../types.js";
import { parseArgs } from "../utils/cli.js";

function parseLoopType(value: string | boolean | undefined): SelfImproveLoopType {
  if (value === "canary" || value === "full") {
    return value;
  }
  throw new Error("Invalid --type value. Use --type canary or --type full.");
}

function parseCount(
  value: string | boolean | undefined,
  maxLoops: number,
): number {
  if (typeof value !== "string") {
    throw new Error("Missing required argument --count.");
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid --count value. Use a positive integer.");
  }
  if (parsed > maxLoops) {
    throw new Error(`--count ${parsed} exceeds max allowed ${maxLoops}.`);
  }
  return parsed;
}

async function main(): Promise<void> {
  const config = getConfig();
  const args = parseArgs(process.argv.slice(2));

  let loopType: SelfImproveLoopType;
  let count: number;

  if (typeof args.phrase === "string" && args.phrase.trim().length > 0) {
    const intent = parseSelfImprovementPhrase(args.phrase, config.SELF_IMPROVE_MAX_LOOPS);
    if (intent.kind !== "enqueue") {
      throw new Error("The provided --phrase is not an enqueue command.");
    }
    loopType = intent.loopType;
    count = intent.count;
  } else {
    loopType = parseLoopType(args.type);
    count = parseCount(args.count, config.SELF_IMPROVE_MAX_LOOPS);
  }

  await runMigrations();
  const batch = await enqueueSelfImprovementBatch({
    requestedCount: count,
    loopType,
    maxLoopsCap: config.SELF_IMPROVE_MAX_LOOPS,
    retryLimit: config.SELF_IMPROVE_RETRY_LIMIT,
    autoApplyPolicy: config.SELF_IMPROVE_AUTO_APPLY_POLICY,
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        batchId: batch.id,
        loopType: batch.loopType,
        requestedCount: batch.requestedCount,
        maxLoopsCap: batch.maxLoopsCap,
        retryLimit: batch.retryLimit,
        autoApplyPolicy: batch.autoApplyPolicy,
        status: batch.status,
        createdAt: batch.createdAt,
      },
      null,
      2,
    ),
  );
}

main()
  .then(async () => {
    await closePool();
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("Self-improvement enqueue failed:", error);
    await closePool();
    process.exitCode = 1;
  });
