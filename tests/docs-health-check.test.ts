import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { runDocsChecks } from "../src/docs/health-check.js";

const REQUIRED_SECTIONS = [
  "Purpose",
  "When To Use",
  "Inputs",
  "Outputs",
  "Steps",
  "Failure Signals",
  "Related Files",
  "Related Commands",
  "Last Verified",
] as const;

function buildMicroDoc(owner: string): string {
  const sections = REQUIRED_SECTIONS.map((section) => `## ${section}\n\ncontent\n`).join("\n");
  return `# Card\n\nOwner: ${owner}\n\n${sections}`;
}

async function write(root: string, relativePath: string, content: string): Promise<void> {
  const absolute = path.join(root, relativePath);
  await mkdir(path.dirname(absolute), { recursive: true });
  await writeFile(absolute, content, "utf8");
}

async function createBaseFixture(): Promise<string> {
  const root = await mkdtemp(path.join(tmpdir(), "docs-health-"));

  await write(
    root,
    "package.json",
    JSON.stringify(
      {
        name: "fixture",
        scripts: {
          "docs:check": "echo ok",
          pipeline: "echo ok",
          canary: "echo ok",
          "self-improve:worker": "echo ok",
          "harness:eval": "echo ok",
          "learn:rollback": "echo ok",
          "report:list": "echo ok",
        },
      },
      null,
      2,
    ),
  );

  await write(
    root,
    "src/config.ts",
    [
      "export const envSchema = {",
      "  DATABASE_URL: \"\",",
      "  OPENAI_API_KEY: \"\",",
      "  STORE_ID: \"\",",
      "};",
    ].join("\n"),
  );

  await write(
    root,
    ".env.example",
    ["DATABASE_URL=postgres://example", "OPENAI_API_KEY=example", "STORE_ID=continente"].join("\n"),
  );

  await write(
    root,
    "README.md",
    [
      "# Fixture",
      "docs/index.md",
      "docs/self-improvement-runbook.md",
      "docs/system-architecture.md",
      "docs/reliability-standards.md",
      "docs/decisions-log.md",
      "docs/quality-scoreboard.md",
      "docs/governance/docs-policy.md",
      "docs/governance/docs-check-spec.md",
      "docs/agents/index.md",
    ].join("\n"),
  );

  await write(root, "docs/index.md", "# Index\n");
  await write(root, "docs/self-improvement-runbook.md", "# Runbook\n");
  await write(root, "docs/system-architecture.md", "# Architecture\n");
  await write(root, "docs/reliability-standards.md", "# Reliability\n");
  await write(root, "docs/decisions-log.md", "# Decisions\n");
  await write(
    root,
    "docs/quality-scoreboard.md",
    `# Quality\n\n- Last updated: ${new Date().toISOString()}\n`,
  );
  await write(root, "docs/governance/docs-policy.md", "# Policy\n");
  await write(root, "docs/governance/docs-check-spec.md", "# Spec\n");

  await write(
    root,
    "docs/agents/index.md",
    [
      "# Agent Index",
      "Owner: quality-tooling-owner",
      "docs/agents/task-cards/run-catalog-pipeline.md",
      "docs/agents/module-cards/pipeline/run-stage-startup.md",
      "docs/agents/module-cards/pipeline/run-support.md",
    ].join("\n"),
  );

  await write(
    root,
    "docs/agents/maps/ownership-map.md",
    [
      "# Ownership",
      "Owner: governance-policy-owner",
      "- `pipeline-flow-owner`: pipeline",
      "- `self-improvement-flow-owner`: loop",
      "- `quality-tooling-owner`: quality",
      "- `governance-policy-owner`: governance",
    ].join("\n"),
  );

  await write(
    root,
    "docs/agents/maps/system-map.md",
    [
      "# System Map",
      "Owner: governance-policy-owner",
      "- src/pipeline/run-stage-startup.ts",
    ].join("\n"),
  );

  await write(
    root,
    "docs/agents/task-cards/run-catalog-pipeline.md",
    buildMicroDoc("pipeline-flow-owner"),
  );

  await write(
    root,
    "docs/agents/module-cards/pipeline/run-stage-startup.md",
    buildMicroDoc("pipeline-flow-owner"),
  );
  await write(
    root,
    "docs/agents/module-cards/pipeline/run-support.md",
    buildMicroDoc("pipeline-flow-owner"),
  );

  await write(root, "src/pipeline/run-stage-startup.ts", "export const startup = true;\n");
  await write(root, "src/pipeline/run-support.ts", "export const support = true;\n");

  return root;
}

describe("docs health checks", () => {
  it("passes for a valid agent-first fixture", async () => {
    const root = await createBaseFixture();
    try {
      const summary = await runDocsChecks({ cwd: root });
      expect(summary.errors).toBe(0);
      expect(summary.warnings).toBe(0);
      expect(summary.moduleCoverageRate).toBe(1);
      expect(summary.ownerCoverageRate).toBe(1);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("flags missing required section", async () => {
    const root = await createBaseFixture();
    try {
      await write(
        root,
        "docs/agents/task-cards/run-catalog-pipeline.md",
        "# Card\n\nOwner: pipeline-flow-owner\n\n## Purpose\n\nOnly one section\n",
      );
      const summary = await runDocsChecks({ cwd: root });
      expect(summary.findings.some((finding) => finding.code === "MISSING_REQUIRED_SECTION")).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("flags oversize docs", async () => {
    const root = await createBaseFixture();
    try {
      const content = `${buildMicroDoc("pipeline-flow-owner")}\n${new Array(200).fill("extra").join("\n")}`;
      await write(root, "docs/agents/task-cards/run-catalog-pipeline.md", content);
      const summary = await runDocsChecks({ cwd: root });
      expect(summary.findings.some((finding) => finding.code === "DOC_TOO_LARGE")).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("flags polluted command or log output in cards", async () => {
    const root = await createBaseFixture();
    try {
      await write(
        root,
        "docs/agents/module-cards/pipeline/run-stage-startup.md",
        `${buildMicroDoc("pipeline-flow-owner")}\n> experiments-w-rob@0.1.0 pipeline\n`,
      );
      const summary = await runDocsChecks({ cwd: root });
      expect(summary.findings.some((finding) => finding.code === "DOC_POLLUTED_CONTENT")).toBe(
        true,
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("flags missing module card when run-stage source exists", async () => {
    const root = await createBaseFixture();
    try {
      await rm(path.join(root, "docs/agents/module-cards/pipeline/run-stage-startup.md"), {
        force: true,
      });
      const summary = await runDocsChecks({ cwd: root });
      expect(summary.findings.some((finding) => finding.code === "MODULE_CARD_MISSING")).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("flags missing and unmapped owners", async () => {
    const root = await createBaseFixture();
    try {
      await write(
        root,
        "docs/agents/task-cards/run-catalog-pipeline.md",
        buildMicroDoc("unknown-owner"),
      );
      await write(
        root,
        "docs/agents/module-cards/pipeline/run-stage-startup.md",
        buildMicroDoc("pipeline-flow-owner").replace("Owner: pipeline-flow-owner\n\n", ""),
      );

      const summary = await runDocsChecks({ cwd: root });
      expect(summary.findings.some((finding) => finding.code === "OWNER_UNMAPPED")).toBe(true);
      expect(summary.findings.some((finding) => finding.code === "OWNER_MISSING")).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("flags architecture drift for missing source in system map", async () => {
    const root = await createBaseFixture();
    try {
      await write(
        root,
        "docs/agents/maps/system-map.md",
        "# System Map\n\nOwner: governance-policy-owner\n\n- src/pipeline/does-not-exist.ts\n",
      );
      const summary = await runDocsChecks({ cwd: root });
      expect(summary.findings.some((finding) => finding.code === "ARCHITECTURE_DRIFT")).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
