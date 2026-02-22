import { describe, expect, it } from "vitest";
import { __test_only_decideLearningApplyMode } from "../src/pipeline/self-improvement-loop.js";

describe("self-improvement canary apply policy", () => {
  it("uses full apply when canary quality gate passes", () => {
    const decision = __test_only_decideLearningApplyMode({
      loopType: "canary",
      autoApplyPolicy: "if_gate_passes",
      harnessPassed: true,
      qualityGatePassed: true,
      qualityGateBasePassed: true,
      highSeveritySchemaViolations: false,
      canaryAutoAcceptedRate: 0.78,
      canaryFullApplyThreshold: 0.75,
      canaryPartialApplyThreshold: 0.7,
      canaryRetryDegradeMode: false,
      maxStructuralChangesPerLoop: 2,
    });

    expect(decision.mode).toBe("full");
    expect(decision.shouldApply).toBe(true);
    expect(decision.maxStructuralChangesPerLoop).toBe(2);
  });

  it("uses partial low-risk apply in the incremental band", () => {
    const decision = __test_only_decideLearningApplyMode({
      loopType: "canary",
      autoApplyPolicy: "if_gate_passes",
      harnessPassed: true,
      qualityGatePassed: false,
      qualityGateBasePassed: true,
      highSeveritySchemaViolations: false,
      canaryAutoAcceptedRate: 0.72,
      canaryFullApplyThreshold: 0.75,
      canaryPartialApplyThreshold: 0.7,
      canaryRetryDegradeMode: false,
      maxStructuralChangesPerLoop: 2,
    });

    expect(decision.mode).toBe("partial_low_risk");
    expect(decision.shouldApply).toBe(true);
    expect(decision.maxStructuralChangesPerLoop).toBe(0);
  });

  it("disables apply below partial threshold", () => {
    const decision = __test_only_decideLearningApplyMode({
      loopType: "canary",
      autoApplyPolicy: "if_gate_passes",
      harnessPassed: true,
      qualityGatePassed: false,
      qualityGateBasePassed: true,
      highSeveritySchemaViolations: false,
      canaryAutoAcceptedRate: 0.69,
      canaryFullApplyThreshold: 0.75,
      canaryPartialApplyThreshold: 0.7,
      canaryRetryDegradeMode: false,
      maxStructuralChangesPerLoop: 2,
    });

    expect(decision.mode).toBe("none");
    expect(decision.shouldApply).toBe(false);
  });

  it("disables apply when harness fails even inside apply band", () => {
    const decision = __test_only_decideLearningApplyMode({
      loopType: "canary",
      autoApplyPolicy: "if_gate_passes",
      harnessPassed: false,
      qualityGatePassed: true,
      qualityGateBasePassed: true,
      highSeveritySchemaViolations: false,
      canaryAutoAcceptedRate: 0.8,
      canaryFullApplyThreshold: 0.75,
      canaryPartialApplyThreshold: 0.7,
      canaryRetryDegradeMode: false,
      maxStructuralChangesPerLoop: 2,
    });

    expect(decision.mode).toBe("none");
    expect(decision.shouldApply).toBe(false);
  });

  it("keeps structural cap at zero in retry degrade mode", () => {
    const decision = __test_only_decideLearningApplyMode({
      loopType: "canary",
      autoApplyPolicy: "if_gate_passes",
      harnessPassed: true,
      qualityGatePassed: true,
      qualityGateBasePassed: true,
      highSeveritySchemaViolations: false,
      canaryAutoAcceptedRate: 0.8,
      canaryFullApplyThreshold: 0.75,
      canaryPartialApplyThreshold: 0.7,
      canaryRetryDegradeMode: true,
      maxStructuralChangesPerLoop: 2,
    });

    expect(decision.mode).toBe("full");
    expect(decision.maxStructuralChangesPerLoop).toBe(0);
  });
});
