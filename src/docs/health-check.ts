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
}

export interface DocsCheckOptions {
  cwd?: string;
  maxScoreboardAgeHours?: number;
  skipScoreboardFreshness?: boolean;
}

export const REQUIRED_DOC_PATHS = [
  "docs/self-improvement-runbook.md",
  "docs/system-architecture.md",
  "docs/reliability-standards.md",
  "docs/decisions-log.md",
  "docs/quality-scoreboard.md",
] as const;

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
    if (!ref || ref.includes("<") || ref.includes(">")) {
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

async function listMarkdownFilesInDocs(): Promise<string[]> {
  const docsDir = "docs";
  if (!(await pathExists(docsDir))) {
    return [];
  }

  const entries = await readdir(docsDir, {
    withFileTypes: true,
  });

  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".md"))
    .map((entry) => path.join(docsDir, entry.name));
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

    const docsFiles = await listMarkdownFilesInDocs();
    const markdownFiles = [readmePath, ...docsFiles];

    const markdownByFile = new Map<string, string>();
    for (const filePath of markdownFiles) {
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

    const allCommandRefs = new Set<string>();
    const allPathRefs = new Set<string>();
    const allEnvVars = new Set<string>();

    for (const content of markdownByFile.values()) {
      for (const command of extractCommandRefs(content)) {
        allCommandRefs.add(command);
      }
      for (const ref of extractPathRefs(content)) {
        allPathRefs.add(ref);
      }
      for (const envVar of extractDocumentedEnvVars(content)) {
        allEnvVars.add(envVar);
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

    for (const requiredDoc of REQUIRED_DOC_PATHS) {
      if (readme.includes(requiredDoc)) {
        continue;
      }
      addFinding(
        findings,
        "warn",
        "README_DOC_MAP_GAP",
        `README.md does not reference ${requiredDoc}.`,
      );
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

    return {
      checkedAt: new Date().toISOString(),
      filesScanned: markdownFiles,
      errors,
      warnings,
      findings,
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
