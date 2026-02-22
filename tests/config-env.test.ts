import { afterEach, describe, expect, it, vi } from "vitest";

const ORIGINAL_ENV = { ...process.env };

afterEach(() => {
  process.env = { ...ORIGINAL_ENV };
  vi.resetModules();
});

describe("config environment parsing", () => {
  it("parses explicit boolean false values correctly", async () => {
    process.env.DATABASE_URL = "postgresql://user:password@localhost:5432/supplies";
    process.env.ATTRIBUTE_SECOND_PASS_ENABLED = "false";
    process.env.SELF_IMPROVE_ROLLBACK_ON_DEGRADE = "false";
    process.env.SELF_IMPROVE_CANARY_RETRY_DEGRADE_ENABLED = "0";

    const { getConfig } = await import("../src/config.js");
    const config = getConfig();

    expect(config.ATTRIBUTE_SECOND_PASS_ENABLED).toBe(false);
    expect(config.SELF_IMPROVE_ROLLBACK_ON_DEGRADE).toBe(false);
    expect(config.SELF_IMPROVE_CANARY_RETRY_DEGRADE_ENABLED).toBe(false);
  });

  it("rejects canary partial apply threshold above full apply threshold", async () => {
    process.env.DATABASE_URL = "postgresql://user:password@localhost:5432/supplies";
    process.env.CANARY_AUTO_ACCEPT_THRESHOLD = "0.75";
    process.env.SELF_IMPROVE_CANARY_PARTIAL_APPLY_THRESHOLD = "0.8";

    const { getConfig } = await import("../src/config.js");
    expect(() => getConfig()).toThrow(
      /SELF_IMPROVE_CANARY_PARTIAL_APPLY_THRESHOLD/,
    );
  });
});
