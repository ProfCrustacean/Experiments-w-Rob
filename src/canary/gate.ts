export function readAutoAcceptedRateFromStats(stats: Record<string, unknown>): number {
  const raw = stats.auto_accepted_rate;
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return raw;
  }

  if (typeof raw === "string") {
    const parsed = Number(raw);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  throw new Error("auto_accepted_rate is missing from run stats; cannot evaluate canary gate.");
}

export function isGatePassing(autoAcceptedRate: number, threshold: number): boolean {
  return autoAcceptedRate >= threshold;
}
