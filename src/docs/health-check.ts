import { access, readdir, readFile } from "node:fs/promises";
import path from "node:path";

export type DocsFindingSeverity = "error" | "warn";

export interface DocsFinding {
  severity: DocsFindingSeverity;
  code: string;
  message: string;
}

export interface DocsCheckSummary {
  checkedAt: string;
  filesScanned: string[];
  errors: number;
  warnings: number;
  findings: DocsFinding[];
  agentDocCount: number;
  oversizeDocCount: number;
  missingSectionCount: number;
  moduleCoverageRate: number;
  ownerCoverageRate: number;
}

export interface DocsCheckOptions {
  cwd?: string;
  maxScoreboardAgeHours?: number;
  skipScoreboardFreshness?: boolean;
}

export const REQUIRED_DOC_PATHS = [
  "docs/index.md",
  "docs/self-improvement-runbook.md",
  "docs/system-architecture.md",
  "docs/reliability-standards.md",
  "docs/decisions-log.md",
  "docs/quality-scoreboard.md",
  "docs/governance/docs-policy.md",
  "docs/governance/docs-check-spec.md",
] as const;

const AGENT_INDEX_PATH = "docs/agents/index.md";
const OWNERSHIP_MAP_PATH = "docs/agents/maps/ownership-map.md";
const SYSTEM_MAP_PATH = "docs/agents/maps/system-map.md";

const REQUIRED_SECTION_HEADERS = [
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

const REQUIRED_SELF_IMPROVEMENT_MODULES = [
  "self-improvement-orchestrator",
  "self-improvement-loop",
  "learning-proposal-generator",
  "learning-apply",
  "learning-rollback",
  "persist-self-improvement",
] as const;

const LINE_LIMITS = {
  taskCard: 140,
  moduleCard: 120,
  mapDoc: 180,
  entrypoint: 220,
} as const;

function addFinding(
  findings: DocsFinding[],
  severity: DocsFindingSeverity,
  code: string,
  message: string,
): void {
  findings.push({
    severity,
    code,
    message,
  });
}

function normalizeFilePath(filePath: string): string {
  return filePath.split(path.sep).join("/");
}

async function pathExists(targetPath: string): Promise<boolean> {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}

function extractCommandRefs(markdown: string): string[] {
  const commands = new Set<string>();
  const regex = /npm\s+run\s+([a-zA-Z0-9:-]+)/g;

  for (const match of markdown.matchAll(regex)) {
    if (match[1]) {
      commands.add(match[1]);
    }
  }

  return [...commands];
}

function extractPathRefs(markdown: string): string[] {
  const refs = new Set<string>();
  const regex = /`((?:docs|src|\.github)\/[^`]+)`/g;

  for (const match of markdown.matchAll(regex)) {
    const ref = match[1]?.trim();
    if (!ref || ref.includes("<") || ref.includes(">") || ref.includes("*")) {
      continue;
    }
    refs.add(ref);
  }

  return [...refs];
}

function extractDocumentedEnvVars(markdown: string): string[] {
  const envVars = new Set<string>();
  const bulletRegex = /^\s*-\s*`([A-Z][A-Z0-9_]+)`/gm;

  for (const match of markdown.matchAll(bulletRegex)) {
    const key = match[1];
    if (key) {
      envVars.add(key);
    }
  }

  return [...envVars];
}

function extractConfigEnvKeys(configSource: string): Set<string> {
  const keys = new Set<string>();
  const lineRegex = /^\s*([A-Z][A-Z0-9_]+):\s*/gm;

  for (const match of configSource.matchAll(lineRegex)) {
    const key = match[1];
    if (key) {
      keys.add(key);
    }
  }

  return keys;
}

function extractEnvExampleKeys(envExample: string): Set<string> {
  const keys = new Set<string>();
  const lineRegex = /^([A-Z][A-Z0-9_]+)=/gm;

  for (const match of envExample.matchAll(lineRegex)) {
    const key = match[1];
    if (key) {
      keys.add(key);
    }
  }

  return keys;
}

function parseScoreboardUpdatedAt(markdown: string): Date | null {
  const match = markdown.match(/^-\s*Last updated:\s*(.+)$/m);
  if (!match?.[1]) {
    return null;
  }

  const parsed = new Date(match[1].trim());
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function extractOwnerFromDoc(markdown: string): string | null {
  const match = markdown.match(/^Owner:\s*([a-z0-9][a-z0-9-_]*)\s*$/im);
  return match?.[1] ?? null;
}

function extractOwnerKeysFromMap(markdown: string): Set<string> {
  const keys = new Set<string>();
  const regex = /-\s*`([a-z0-9][a-z0-9-_]*)`\s*:/g;
  for (const match of markdown.matchAll(regex)) {
    const key = match[1];
    if (key) {
      keys.add(key);
    }
  }
  return keys;
}

function extractArchitecturePaths(markdown: string): Set<string> {
  const paths = new Set<string>();
  const regex = /src\/[A-Za-z0-9_./-]+\.ts/g;
  for (const match of markdown.matchAll(regex)) {
    const value = match[0];
    if (value) {
      paths.add(value);
    }
  }
  return paths;
}

function lineCount(markdown: string): number {
  return markdown.split(/\r?\n/).length;
}

function missingRequiredSections(markdown: string): string[] {
  const missing: string[] = [];
  for (const section of REQUIRED_SECTION_HEADERS) {
    if (!markdown.includes(`## ${section}`)) {
      missing.push(section);
    }
  }
  return missing;
}

function detectDocPollution(markdown: string): string[] {
  const signals: string[] = [];
  if (/^>\s*experiments-w-rob@/m.test(markdown)) {
    signals.push("shell_output");
  }
  if (/^\{"timestamp":"\d{4}-\d{2}-\d{2}T/m.test(markdown)) {
    signals.push("json_log_output");
  }
  if (/^\[(sampling|embeddings|attribute_batches|attribute_second_pass)\]/m.test(markdown)) {
    signals.push("runtime_progress_output");
  }
  if (/^\s*RUN\s+v\d+/m.test(markdown) || /^\s*Test Files\s+\d+/m.test(markdown)) {
    signals.push("test_runner_output");
  }
  if (/^\s*-\s*$/m.test(markdown)) {
    signals.push("empty_bullet_placeholder");
  }
  if (/\\n/.test(markdown)) {
    signals.push("escaped_newline_literal");
  }
  return signals;
}

function isTaskCard(filePath: string): boolean {
  return filePath.startsWith("docs/agents/task-cards/");
}

function isModuleCard(filePath: string): boolean {
  return filePath.startsWith("docs/agents/module-cards/");
}

function isMapDoc(filePath: string): boolean {
  return filePath.startsWith("docs/agents/maps/");
}

function isTopLevelDocsFile(filePath: string): boolean {
  return /^docs\/[^/]+\.md$/.test(filePath);
}

function requiresOwner(filePath: string): boolean {
  return isTaskCard(filePath) || isModuleCard(filePath) || isMapDoc(filePath);
}

function lineLimitFor(filePath: string): number | null {
  if (isTaskCard(filePath)) {
    return LINE_LIMITS.taskCard;
  }
  if (isModuleCard(filePath)) {
    return LINE_LIMITS.moduleCard;
  }
  if (isMapDoc(filePath)) {
    return LINE_LIMITS.mapDoc;
  }
  if (filePath === "README.md" || isTopLevelDocsFile(filePath)) {
    return LINE_LIMITS.entrypoint;
  }
  return null;
}

async function listMarkdownFilesInDocsRecursive(dirPath: string): Promise<string[]> {
  if (!(await pathExists(dirPath))) {
    return [];
  }

  const entries = await readdir(dirPath, {
    withFileTypes: true,
  });

  const files: string[] = [];
  for (const entry of entries) {
    const absolute = path.join(dirPath, entry.name);
    const normalized = normalizeFilePath(absolute);
    if (entry.isDirectory()) {
      files.push(...(await listMarkdownFilesInDocsRecursive(normalized)));
      continue;
    }
    if (entry.isFile() && normalized.endsWith(".md")) {
      files.push(normalized);
    }
  }

  return files.sort((left, right) => left.localeCompare(right));
}

async function expectedModuleCardPaths(): Promise<string[]> {
  const expected: string[] = [];

  const pipelineDir = "src/pipeline";
  if (await pathExists(pipelineDir)) {
    const entries = await readdir(pipelineDir, {
      withFileTypes: true,
    });

    for (const entry of entries) {
      if (!entry.isFile()) {
        continue;
      }
      if (!/^run-stage-.*\.ts$/.test(entry.name) && entry.name !== "run-support.ts") {
        continue;
      }
      const base = entry.name.replace(/\.ts$/, "");
      expected.push(`docs/agents/module-cards/pipeline/${base}.md`);
    }
  }

  for (const moduleName of REQUIRED_SELF_IMPROVEMENT_MODULES) {
    const sourcePath = `src/pipeline/${moduleName}.ts`;
    if (!(await pathExists(sourcePath))) {
      continue;
    }
    expected.push(`docs/agents/module-cards/self-improvement/${moduleName}.md`);
  }

  expected.sort((left, right) => left.localeCompare(right));
  return expected;
}

export async function runDocsChecks(options?: DocsCheckOptions): Promise<DocsCheckSummary> {
  const previousCwd = process.cwd();
  if (options?.cwd) {
    process.chdir(options.cwd);
  }

  try {
    const findings: DocsFinding[] = [];

    const readmePath = "README.md";
    const configPath = "src/config.ts";
    const packagePath = "package.json";
    const envExamplePath = ".env.example";
    const scoreboardPath = "docs/quality-scoreboard.md";
    const maxScoreboardAgeHours = options?.maxScoreboardAgeHours ?? 48;

    const docsFiles = await listMarkdownFilesInDocsRecursive("docs");
    const markdownFiles = [readmePath, ...docsFiles];

    const markdownByFile = new Map<string, string>();
    for (const filePath of markdownFiles) {
      if (!(await pathExists(filePath))) {
        continue;
      }
      const content = await readFile(filePath, "utf8");
      markdownByFile.set(filePath, content);
    }

    const readme = markdownByFile.get(readmePath) ?? "";
    const packageJson = JSON.parse(await readFile(packagePath, "utf8")) as {
      scripts?: Record<string, string>;
    };
    const scripts = packageJson.scripts ?? {};
    const configSource = await readFile(configPath, "utf8");
    const envExample = await readFile(envExamplePath, "utf8");

    for (const requiredDoc of REQUIRED_DOC_PATHS) {
      if (!(await pathExists(requiredDoc))) {
        addFinding(findings, "error", "MISSING_REQUIRED_DOC", `${requiredDoc} is missing.`);
      }
    }

    if (!(await pathExists(AGENT_INDEX_PATH))) {
      addFinding(
        findings,
        "error",
        "MISSING_AGENT_INDEX",
        `${AGENT_INDEX_PATH} is required for agent-first navigation.`,
      );
    }

    const allCommandRefs = new Set<string>();
    const allPathRefs = new Set<string>();
    const allEnvVars = new Set<string>();
    const envReferenceDocs = new Set<string>([
      "README.md",
      "docs/self-improvement-runbook.md",
      "docs/reliability-standards.md",
    ]);

    for (const [filePath, content] of markdownByFile.entries()) {
      for (const command of extractCommandRefs(content)) {
        allCommandRefs.add(command);
      }
      for (const ref of extractPathRefs(content)) {
        allPathRefs.add(ref);
      }
      if (envReferenceDocs.has(filePath)) {
        for (const envVar of extractDocumentedEnvVars(content)) {
          allEnvVars.add(envVar);
        }
      }
    }

    for (const command of allCommandRefs) {
      if (!scripts[command]) {
        addFinding(
          findings,
          "error",
          "UNKNOWN_SCRIPT_REF",
          `Documentation references 'npm run ${command}', but package.json has no '${command}' script.`,
        );
      }
    }

    for (const ref of allPathRefs) {
      if (!(await pathExists(ref))) {
        addFinding(
          findings,
          "error",
          "BROKEN_PATH_REF",
          `Documentation references '${ref}', but that path does not exist.`,
        );
      }
    }

    const configKeys = extractConfigEnvKeys(configSource);
    const envExampleKeys = extractEnvExampleKeys(envExample);

    for (const envVar of allEnvVars) {
      if (!configKeys.has(envVar)) {
        addFinding(
          findings,
          "warn",
          "ENV_NOT_IN_CONFIG",
          `Documentation references '${envVar}', but it is not defined in src/config.ts env schema.`,
        );
      }

      if (!envExampleKeys.has(envVar)) {
        addFinding(
          findings,
          "warn",
          "ENV_NOT_IN_EXAMPLE",
          `Documentation references '${envVar}', but it is not present in .env.example.`,
        );
      }
    }

    const requiredReadmeRefs = [...REQUIRED_DOC_PATHS, AGENT_INDEX_PATH];
    for (const requiredRef of requiredReadmeRefs) {
      if (readme.includes(requiredRef)) {
        continue;
      }
      addFinding(
        findings,
        "warn",
        "README_DOC_MAP_GAP",
        `README.md does not reference ${requiredRef}.`,
      );
    }

    const agentDocCount = docsFiles.filter((filePath) => filePath.startsWith("docs/agents/")).length;

    const oversizeDocPaths = new Set<string>();
    let missingSectionCount = 0;
    let ownerRequiredCount = 0;
    let ownerMappedCount = 0;

    const ownershipMapContent = markdownByFile.get(OWNERSHIP_MAP_PATH) ?? "";
    const ownershipKeys = extractOwnerKeysFromMap(ownershipMapContent);

    for (const [filePath, content] of markdownByFile.entries()) {
      const lineLimit = lineLimitFor(filePath);
      if (lineLimit !== null) {
        const lines = lineCount(content);
        if (lines > lineLimit) {
          oversizeDocPaths.add(filePath);
          addFinding(
            findings,
            "error",
            "DOC_TOO_LARGE",
            `${filePath} has ${lines} lines and exceeds the ${lineLimit} line limit.`,
          );
        }
      }

      if (isTaskCard(filePath) || isModuleCard(filePath)) {
        const missingSections = missingRequiredSections(content);
        if (missingSections.length > 0) {
          missingSectionCount += missingSections.length;
          addFinding(
            findings,
            "error",
            "MISSING_REQUIRED_SECTION",
            `${filePath} is missing required section(s): ${missingSections.join(", ")}.`,
          );
        }

        const pollutionSignals = detectDocPollution(content);
        if (pollutionSignals.length > 0) {
          addFinding(
            findings,
            "error",
            "DOC_POLLUTED_CONTENT",
            `${filePath} contains command/log output artifacts: ${pollutionSignals.join(", ")}.`,
          );
        }
      }

      if (requiresOwner(filePath)) {
        ownerRequiredCount += 1;
        const owner = extractOwnerFromDoc(content);
        if (!owner) {
          addFinding(
            findings,
            "error",
            "OWNER_MISSING",
            `${filePath} is missing an 'Owner: <key>' line.`,
          );
          continue;
        }

        if (!ownershipKeys.has(owner)) {
          addFinding(
            findings,
            "error",
            "OWNER_UNMAPPED",
            `${filePath} uses owner '${owner}', which is not defined in ${OWNERSHIP_MAP_PATH}.`,
          );
          continue;
        }

        ownerMappedCount += 1;
      }
    }

    const taskCardFiles = docsFiles.filter((filePath) => isTaskCard(filePath));
    const agentsIndexContent = markdownByFile.get(AGENT_INDEX_PATH);
    if (agentsIndexContent) {
      for (const taskCardPath of taskCardFiles) {
        if (!agentsIndexContent.includes(taskCardPath)) {
          addFinding(
            findings,
            "error",
            "TASK_CARD_ORPHANED",
            `${taskCardPath} is not linked from ${AGENT_INDEX_PATH}.`,
          );
        }
      }
    }

    const expectedModuleCards = await expectedModuleCardPaths();
    let moduleCardsPresent = 0;
    for (const moduleCardPath of expectedModuleCards) {
      if (await pathExists(moduleCardPath)) {
        moduleCardsPresent += 1;
        continue;
      }
      addFinding(
        findings,
        "error",
        "MODULE_CARD_MISSING",
        `${moduleCardPath} is required because the source module exists.`,
      );
    }

    const architectureMap = markdownByFile.get(SYSTEM_MAP_PATH);
    if (architectureMap) {
      const architecturePaths = extractArchitecturePaths(architectureMap);
      for (const sourcePath of architecturePaths) {
        if (await pathExists(sourcePath)) {
          continue;
        }
        addFinding(
          findings,
          "error",
          "ARCHITECTURE_DRIFT",
          `${SYSTEM_MAP_PATH} references '${sourcePath}', but that source file does not exist.`,
        );
      }
    }

    if (!options?.skipScoreboardFreshness && (await pathExists(scoreboardPath))) {
      const scoreboard = await readFile(scoreboardPath, "utf8");
      const updatedAt = parseScoreboardUpdatedAt(scoreboard);
      if (!updatedAt) {
        addFinding(
          findings,
          "warn",
          "SCOREBOARD_TIMESTAMP_MISSING",
          "docs/quality-scoreboard.md is missing a valid '- Last updated: <ISO timestamp>' line.",
        );
      } else {
        const ageMs = Date.now() - updatedAt.getTime();
        const ageHours = ageMs / (1000 * 60 * 60);
        if (ageHours > maxScoreboardAgeHours) {
          addFinding(
            findings,
            "error",
            "SCOREBOARD_STALE",
            `docs/quality-scoreboard.md is ${ageHours.toFixed(1)}h old and exceeds the ${maxScoreboardAgeHours}h freshness SLA.`,
          );
        }
      }
    }

    const errors = findings.filter((finding) => finding.severity === "error").length;
    const warnings = findings.filter((finding) => finding.severity === "warn").length;

    const moduleCoverageRate =
      expectedModuleCards.length === 0 ? 1 : moduleCardsPresent / expectedModuleCards.length;
    const ownerCoverageRate = ownerRequiredCount === 0 ? 1 : ownerMappedCount / ownerRequiredCount;

    return {
      checkedAt: new Date().toISOString(),
      filesScanned: markdownFiles,
      errors,
      warnings,
      findings,
      agentDocCount,
      oversizeDocCount: oversizeDocPaths.size,
      missingSectionCount,
      moduleCoverageRate,
      ownerCoverageRate,
    };
  } finally {
    if (options?.cwd) {
      process.chdir(previousCwd);
    }
  }
}

export function toMarkdownReport(summary: DocsCheckSummary): string {
  const lines: string[] = [];
  lines.push("# Docs Health Report");
  lines.push("");
  lines.push(`- Checked at: ${summary.checkedAt}`);
  lines.push(`- Files scanned: ${summary.filesScanned.length}`);
  lines.push(`- Errors: ${summary.errors}`);
  lines.push(`- Warnings: ${summary.warnings}`);
  lines.push(`- Agent docs tracked: ${summary.agentDocCount}`);
  lines.push(`- Oversize docs: ${summary.oversizeDocCount}`);
  lines.push(`- Missing required sections: ${summary.missingSectionCount}`);
  lines.push(`- Module coverage rate: ${summary.moduleCoverageRate.toFixed(4)}`);
  lines.push(`- Owner coverage rate: ${summary.ownerCoverageRate.toFixed(4)}`);
  lines.push("");

  if (summary.findings.length === 0) {
    lines.push("No findings.");
  } else {
    lines.push("## Findings");
    lines.push("");
    for (const finding of summary.findings) {
      lines.push(`- [${finding.severity.toUpperCase()}] ${finding.code}: ${finding.message}`);
    }
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

export function countFindingsByCode(summary: DocsCheckSummary): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const finding of summary.findings) {
    counts[finding.code] = (counts[finding.code] ?? 0) + 1;
  }
  return counts;
}
