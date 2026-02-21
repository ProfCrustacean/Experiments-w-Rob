import { beforeEach, describe, expect, it, vi } from "vitest";

const mockState = vi.hoisted(() => ({
  appliedChanges: [
    {
      id: "applied-1",
      proposalId: "proposal-1",
      proposalKind: "rule_term_add",
      status: "applied",
      versionBefore: "v1",
      versionAfter: "v2",
      appliedAt: new Date().toISOString(),
      rollbackToken: "rb-1",
      metadata: {
        batch_id: "batch-1",
        run_id: "run-1",
      },
    },
  ],
  rollbackCalls: 0,
}));

vi.mock("../src/pipeline/persist.js", () => ({
  listRecentAppliedChanges: vi.fn(async () => mockState.appliedChanges),
  rollbackAppliedChangeTransactional: vi.fn(async (input: { appliedChangeId: string }) => {
    mockState.rollbackCalls += 1;
    const match = mockState.appliedChanges.find((change) => change.id === input.appliedChangeId);
    if (!match) {
      throw new Error("missing change");
    }
    return {
      ...match,
      status: "rolled_back",
    };
  }),
}));

import { rollbackByAppliedChangeId, rollbackOnHarnessDegrade } from "../src/pipeline/learning-rollback.js";

describe("learning rollback", () => {
  beforeEach(() => {
    mockState.rollbackCalls = 0;
    mockState.appliedChanges = [
      {
        id: "applied-1",
        proposalId: "proposal-1",
        proposalKind: "rule_term_add",
        status: "applied",
        versionBefore: "v1",
        versionAfter: "v2",
        appliedAt: new Date().toISOString(),
        rollbackToken: "rb-1",
        metadata: {
          batch_id: "batch-1",
          run_id: "run-1",
        },
      },
    ];
  });

  it("does not rollback when harness passes", async () => {
    const result = await rollbackOnHarnessDegrade({
      batchId: "batch-1",
      runId: "run-1",
      harnessResult: {
        passed: true,
        metricScores: {},
        failedMetrics: [],
        baselineRunId: "b1",
        candidateRunId: "c1",
      },
      watchLoops: 2,
      rollbackOnDegrade: true,
    });

    expect(result.rolledBack).toBe(false);
    expect(mockState.rollbackCalls).toBe(0);
  });

  it("rolls back most recent scoped applied change when harness degrades", async () => {
    const result = await rollbackOnHarnessDegrade({
      batchId: "batch-1",
      runId: "run-2",
      harnessResult: {
        passed: false,
        metricScores: {},
        failedMetrics: ["l1_delta"],
        baselineRunId: "b1",
        candidateRunId: "c2",
      },
      watchLoops: 2,
      rollbackOnDegrade: true,
    });

    expect(result.rolledBack).toBe(true);
    expect(result.change?.status).toBe("rolled_back");
    expect(mockState.rollbackCalls).toBe(1);
  });

  it("can rollback by explicit applied change id", async () => {
    const result = await rollbackByAppliedChangeId({
      appliedChangeId: "applied-1",
      reason: "manual_test",
    });

    expect(result.status).toBe("rolled_back");
    expect(mockState.rollbackCalls).toBe(1);
  });
});
