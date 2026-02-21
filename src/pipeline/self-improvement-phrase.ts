import type { SelfImproveLoopType } from "../types.js";

export type SelfImprovementPhraseIntent =
  | {
      kind: "enqueue";
      count: number;
      loopType: SelfImproveLoopType;
    }
  | {
      kind: "status_all";
    }
  | {
      kind: "status_one";
      batchId: string;
    };

export function parseSelfImprovementPhrase(
  rawPhrase: string,
  maxLoops: number,
): SelfImprovementPhraseIntent {
  const phrase = rawPhrase.trim();

  if (/^show\s+self-improvement\s+batches$/i.test(phrase)) {
    return {
      kind: "status_all",
    };
  }

  const showBatchMatch = phrase.match(/^show\s+self-improvement\s+batch\s+([A-Za-z0-9-]+)$/i);
  if (showBatchMatch) {
    return {
      kind: "status_one",
      batchId: showBatchMatch[1],
    };
  }

  const enqueueMatch = phrase.match(/^run\s+(\d+)\s+self-improvement\s+(canary|full)\s+loops?$/i);
  if (enqueueMatch) {
    const count = Number(enqueueMatch[1]);
    const loopType = enqueueMatch[2].toLowerCase() as SelfImproveLoopType;

    if (!Number.isInteger(count) || count <= 0) {
      throw new Error("Loop count must be a positive integer.");
    }
    if (count > maxLoops) {
      throw new Error(
        `Requested count ${count} exceeds max allowed ${maxLoops}. Please request ${maxLoops} or fewer loops.`,
      );
    }

    return {
      kind: "enqueue",
      count,
      loopType,
    };
  }

  if (
    /^run\s+\d+\s+self-improvement/i.test(phrase) ||
    /^show\s+self-improvement/i.test(phrase)
  ) {
    throw new Error(
      "Ambiguous self-improvement phrase. Use 'run <N> self-improvement <canary|full> loops', 'show self-improvement batches', or 'show self-improvement batch <id>'.",
    );
  }

  throw new Error(
    "Unrecognized phrase. Supported commands: run <N> self-improvement <canary|full> loops; show self-improvement batches; show self-improvement batch <id>.",
  );
}
