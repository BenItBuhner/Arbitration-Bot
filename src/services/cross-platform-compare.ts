/**
 * Cross-platform outcome comparison: normalize UP/DOWN vs YES/NO,
 * compute favored outcome, and track accuracy (favored-outcome match %).
 */

import type { MarketSnapshot } from "./market-data-hub";

export type NormalizedOutcome = "UP" | "DOWN" | "UNKNOWN";

/**
 * Map platform outcome labels to shared UP/DOWN.
 * Polymarket: Up/Down. Kalshi: Yes/No (or yes_sub_title / no_sub_title with "above"/"below" etc).
 */
export function normalizeOutcomeLabel(
  label: string,
  _marketName?: string,
): NormalizedOutcome {
  const lower = (label ?? "").trim().toLowerCase();
  if (lower === "up" || lower === "yes") return "UP";
  if (lower === "down" || lower === "no") return "DOWN";
  if (
    lower.includes("above") ||
    lower.includes("up") ||
    lower.includes("over") ||
    lower.includes("greater")
  ) {
    return "UP";
  }
  if (
    lower.includes("below") ||
    lower.includes("down") ||
    lower.includes("under") ||
    lower.includes("less")
  ) {
    return "DOWN";
  }
  return "UNKNOWN";
}

/**
 * Compute favored outcome from snapshot: spot vs threshold -> UP or DOWN.
 * Threshold is priceToBeat if present, otherwise referencePrice (same for Poly/Kalshi).
 */
export function computeFavoredOutcome(
  snapshot: MarketSnapshot,
): NormalizedOutcome {
  const threshold =
    snapshot.priceToBeat > 0 ? snapshot.priceToBeat : snapshot.referencePrice;
  const priceValue =
    snapshot.provider === "kalshi" &&
    snapshot.kalshiUnderlyingValue != null &&
    Number.isFinite(snapshot.kalshiUnderlyingValue) &&
    snapshot.kalshiUnderlyingValue > 0
      ? snapshot.kalshiUnderlyingValue
      : snapshot.cryptoPrice;

  if (threshold <= 0 || !Number.isFinite(priceValue) || priceValue <= 0) {
    return "UNKNOWN";
  }
  return priceValue >= threshold ? "UP" : "DOWN";
}

/**
 * Mid of best bid/ask for market odds (0-1). Null if either missing.
 */
export function computeOddsMid(
  bestBid: number | null,
  bestAsk: number | null,
): number | null {
  if (bestBid == null || bestAsk == null) return null;
  if (!Number.isFinite(bestBid) || !Number.isFinite(bestAsk)) return null;
  const mid = (bestBid + bestAsk) / 2;
  return Math.max(0, Math.min(1, mid));
}

const ROLLING_WINDOW = 200;

export interface AccuracyState {
  matchCount: number;
  totalCount: number;
  lastPairs: Array<{ poly: NormalizedOutcome; kalshi: NormalizedOutcome }>;
}

export function createAccuracyState(): AccuracyState {
  return { matchCount: 0, totalCount: 0, lastPairs: [] };
}

export function updateAccuracy(
  state: AccuracyState,
  polyOutcome: NormalizedOutcome,
  kalshiOutcome: NormalizedOutcome,
): void {
  if (polyOutcome === "UNKNOWN" || kalshiOutcome === "UNKNOWN") return;
  state.totalCount += 1;
  if (polyOutcome === kalshiOutcome) state.matchCount += 1;
  state.lastPairs.push({ poly: polyOutcome, kalshi: kalshiOutcome });
  if (state.lastPairs.length > ROLLING_WINDOW) state.lastPairs.shift();
}

export function getAccuracyPercent(state: AccuracyState): number | null {
  if (state.totalCount === 0) return null;
  return (100 * state.matchCount) / state.totalCount;
}

export function getWindowAccuracyPercent(
  state: AccuracyState,
  windowSize: number = 100,
): number | null {
  const slice =
    state.lastPairs.length <= windowSize
      ? state.lastPairs
      : state.lastPairs.slice(-windowSize);
  if (slice.length === 0) return null;
  const matches = slice.filter((p) => p.poly === p.kalshi).length;
  return (100 * matches) / slice.length;
}
