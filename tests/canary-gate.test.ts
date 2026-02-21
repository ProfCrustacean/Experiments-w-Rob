import { describe, expect, it } from "vitest";
import {
  isGatePassing,
  readAutoAcceptedRateFromStats,
} from "../src/canary/gate.js";

describe("canary gate", () => {
  it("passes when auto-accept rate meets the threshold", () => {
    expect(isGatePassing(0.8, 0.8)).toBe(true);
    expect(isGatePassing(0.91, 0.8)).toBe(true);
  });

  it("fails when auto-accept rate is below threshold", () => {
    expect(isGatePassing(0.7999, 0.8)).toBe(false);
  });

  it("reads auto_accepted_rate from numeric or string run stats", () => {
    expect(readAutoAcceptedRateFromStats({ auto_accepted_rate: 0.82 })).toBe(0.82);
    expect(readAutoAcceptedRateFromStats({ auto_accepted_rate: "0.81" })).toBe(0.81);
  });

  it("throws a clear error when auto_accepted_rate is missing", () => {
    expect(() => readAutoAcceptedRateFromStats({})).toThrow(
      "auto_accepted_rate is missing from run stats",
    );
  });
});
