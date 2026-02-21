import { loadTaxonomy } from "../taxonomy/load.js";
import {
  applyTaxonomyAndRulePatchTransactional,
  listPendingLearningProposals,
  recordAppliedChangeWithoutPatch,
} from "./persist.js";
import type { AppliedChangeRecord, HarnessEvalResult } from "../types.js";

const STRUCTURAL_KINDS = new Set(["taxonomy_merge", "taxonomy_split", "taxonomy_move"]);

export async function applyLearningProposals(input: {
  batchId?: string;
  runId?: string;
  harnessResult: HarnessEvalResult;
  maxStructuralChangesPerLoop: number;
}): Promise<{
  considered: number;
  applied: number;
  structuralApplied: number;
  appliedChanges: AppliedChangeRecord[];
}> {
  if (!input.harnessResult.passed) {
    return {
      considered: 0,
      applied: 0,
      structuralApplied: 0,
      appliedChanges: [],
    };
  }

  const proposals = await listPendingLearningProposals({
    batchId: input.batchId,
    runId: input.runId,
    limit: 100,
  });

  const appliedChanges: AppliedChangeRecord[] = [];
  let structuralApplied = 0;
  let considered = 0;
  let applied = 0;

  for (const proposal of proposals) {
    considered += 1;
    const versionBefore = loadTaxonomy().taxonomyVersion;
    const versionAfter = `${versionBefore}:${proposal.id}`;

    if (STRUCTURAL_KINDS.has(proposal.proposalKind)) {
      if (structuralApplied >= input.maxStructuralChangesPerLoop) {
        continue;
      }
      const change = await recordAppliedChangeWithoutPatch({
        proposal,
        versionBefore,
        versionAfter,
        metadata: {
          apply_mode: "structural_synthetic",
          reason: "structural_apply_cap",
          batch_id: input.batchId ?? null,
          run_id: input.runId ?? null,
        },
      });
      appliedChanges.push(change);
      structuralApplied += 1;
      applied += 1;
      continue;
    }

    const change = await applyTaxonomyAndRulePatchTransactional({
      proposal,
      versionBefore,
      versionAfter,
      metadata: {
        apply_mode: "rule_patch",
        batch_id: input.batchId ?? null,
        run_id: input.runId ?? null,
      },
    });
    appliedChanges.push(change);
    applied += 1;
  }

  return {
    considered,
    applied,
    structuralApplied,
    appliedChanges,
  };
}
