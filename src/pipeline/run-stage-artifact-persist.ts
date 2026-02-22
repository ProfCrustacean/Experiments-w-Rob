import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { RunLogger } from "../logging/run-logger.js";
import {
  cleanupExpiredRunArtifacts,
  cleanupExpiredRunLogs,
  upsertRunArtifact,
} from "./persist.js";
import type { RunArtifactKey } from "./run-artifacts.js";

export interface PersistedArtifactItem {
  key: RunArtifactKey;
  fileName: string;
  mimeType: string;
  content: Buffer;
}

export interface ArtifactPersistenceResult {
  cleanedArtifacts: number;
  cleanedLogs: number;
}

export async function runArtifactPersistAndCleanupStages(input: {
  runId: string;
  outputDir: string;
  artifacts: PersistedArtifactItem[];
  artifactsExpireAt: Date;
  artifactRetentionHours: number;
  traceRetentionHours: number;
  logger: RunLogger;
  stageTimingsMs: Record<string, number>;
}): Promise<ArtifactPersistenceResult> {
  input.logger.info("pipeline", "stage.started", "Starting artifact persistence stage.", {
    stage_name: "artifact_persist",
  });
  const artifactPersistStart = Date.now();
  await mkdir(input.outputDir, { recursive: true });

  for (const artifact of input.artifacts) {
    const localPath = path.join(input.outputDir, artifact.fileName);
    await writeFile(localPath, artifact.content);
    await upsertRunArtifact({
      runId: input.runId,
      artifactKey: artifact.key,
      fileName: artifact.fileName,
      mimeType: artifact.mimeType,
      content: artifact.content,
      expiresAt: input.artifactsExpireAt,
    });
  }

  input.stageTimingsMs.artifact_persist_ms = Date.now() - artifactPersistStart;
  input.logger.info("pipeline", "stage.completed", "Artifact persistence stage completed.", {
    stage_name: "artifact_persist",
    elapsed_ms: input.stageTimingsMs.artifact_persist_ms,
    artifact_count: input.artifacts.length,
  });

  input.logger.info("pipeline", "stage.started", "Starting cleanup stage.", {
    stage_name: "cleanup",
  });
  const artifactCleanupStart = Date.now();
  const cleanedArtifacts = await cleanupExpiredRunArtifacts(input.artifactRetentionHours);
  const cleanedLogs = await cleanupExpiredRunLogs(input.traceRetentionHours);
  input.stageTimingsMs.artifact_cleanup_ms = Date.now() - artifactCleanupStart;
  input.logger.info("persistence", "cleanup.logs.completed", "Expired trace logs cleanup completed.", {
    deleted_count: cleanedLogs,
    retention_hours: input.traceRetentionHours,
  });
  input.logger.info("pipeline", "stage.completed", "Cleanup stage completed.", {
    stage_name: "cleanup",
    elapsed_ms: input.stageTimingsMs.artifact_cleanup_ms,
    artifact_cleanup_deleted_count: cleanedArtifacts,
    log_cleanup_deleted_count: cleanedLogs,
  });

  return {
    cleanedArtifacts,
    cleanedLogs,
  };
}
