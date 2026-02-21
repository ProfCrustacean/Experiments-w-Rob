import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SelfImproveProposal } from "../src/types.js";

const mockState = vi.hoisted(() => ({
  proposals: [] as SelfImproveProposal[],
  patchCalls: 0,
  syntheticCalls: 0,
}));

vi.mock("../src/taxonomy/load.js", () => ({
  loadTaxonomy: vi.fn(() => ({
    taxonomyVersion: "v-test",
  })),
}));

vi.mock("../src/pipeline/persist.js", () => ({
  listPendingLearningProposals: vi.fn(async () => mockState.proposals),
  applyTaxonomyAndRulePatchTransactional: vi.fn(async (input: { proposal: SelfImproveProposal }) => {
    mockState.patchCalls += 1;
    return {
      id: `patch-${input.proposal.id}`,
      proposalId: input.proposal.id,
      proposalKind: input.proposal.proposalKind,
      status: "applied",
      versionBefore: "v-test",
      versionAfter: `v-test:${input.proposal.id}`,
      appliedAt: new Date().toISOString(),
      rollbackToken: `rb-${input.proposal.id}`,
      metadata: {},
    };
  }),
  recordAppliedChangeWithoutPatch: vi.fn(async (input: { proposal: SelfImproveProposal }) => {
    mockState.syntheticCalls += 1;
    return {
      id: `synthetic-${input.proposal.id}`,
      proposalId: input.proposal.id,
      proposalKind: input.proposal.proposalKind,
      status: "applied",
      versionBefore: "v-test",
      versionAfter: `v-test:${input.proposal.id}`,
      appliedAt: new Date().toISOString(),
      rollbackToken: `rb-${input.proposal.id}`,
      metadata: { synthetic_apply: true },
    };
  }),
}));

import { applyLearningProposals } from "../src/pipeline/learning-apply.js";

function makeProposal(input: {
  id: string;
  kind: SelfImproveProposal["proposalKind"];
}): SelfImproveProposal {
  return {
    id: input.id,
    batchId: "batch-1",
    runId: "run-1",
    proposalKind: input.kind,
    status: "proposed",
    confidenceScore: 0.7,
    expectedImpactScore: 0.6,
    payload: {
      target_slug: "escrita",
      field: "include_any",
      action: "add",
      value: "caneta",
      reason: "test",
    },
    source: {},
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

describe("learning apply", () => {
  beforeEach(() => {
    mockState.proposals = [];
    mockState.patchCalls = 0;
    mockState.syntheticCalls = 0;
  });

  it("skips all apply work when harness gate fails", async () => {
    mockState.proposals = [makeProposal({ id: "p1", kind: "rule_term_add" })];

    const result = await applyLearningProposals({
      batchId: "batch-1",
      runId: "run-1",
      harnessResult: {
        passed: false,
        metricScores: {},
        failedMetrics: ["l1_delta"],
        baselineRunId: "b1",
        candidateRunId: "c1",
      },
      maxStructuralChangesPerLoop: 2,
    });

    expect(result.applied).toBe(0);
    expect(mockState.patchCalls).toBe(0);
    expect(mockState.syntheticCalls).toBe(0);
  });

  it("enforces structural apply cap while still applying rule patches", async () => {
    mockState.proposals = [
      makeProposal({ id: "p-struct-1", kind: "taxonomy_merge" }),
      makeProposal({ id: "p-struct-2", kind: "taxonomy_split" }),
      makeProposal({ id: "p-rule-1", kind: "rule_term_add" }),
    ];

    const result = await applyLearningProposals({
      batchId: "batch-1",
      runId: "run-1",
      harnessResult: {
        passed: true,
        metricScores: {},
        failedMetrics: [],
        baselineRunId: "b1",
        candidateRunId: "c1",
      },
      maxStructuralChangesPerLoop: 1,
    });

    expect(result.applied).toBe(2);
    expect(result.structuralApplied).toBe(1);
    expect(mockState.syntheticCalls).toBe(1);
    expect(mockState.patchCalls).toBe(1);
  });
});
