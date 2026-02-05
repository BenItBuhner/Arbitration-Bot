import type { MarketSnapshot } from "./market-data-hub";
import { extractOutcomes, getMarketBySlug } from "./market-service";
import {
  normalizeOutcomeLabel,
  type NormalizedOutcome,
} from "./cross-platform-compare";
import type { KalshiClient } from "../clients/kalshi/kalshi-client";

export interface OfficialOutcomeResult {
  outcome: NormalizedOutcome | null;
  outcomeSource: string | null;
  finalPrice: number | null;
  finalPriceSource: string | null;
}

export interface FinalPriceResult {
  value: number | null;
  source: string;
  points: number;
  coverageMs: number | null;
  windowMs: number;
}

export function resolveThreshold(snapshot: MarketSnapshot): {
  value: number | null;
  source: string;
} {
  if (snapshot.priceToBeat > 0) {
    return { value: snapshot.priceToBeat, source: "price_to_beat" };
  }
  if (snapshot.referencePrice > 0) {
    return { value: snapshot.referencePrice, source: snapshot.referenceSource };
  }
  const labelCandidates = [snapshot.upOutcome, snapshot.downOutcome];
  for (const label of labelCandidates) {
    if (!label) continue;
    const lower = label.toLowerCase();
    if (!lower.includes("price to beat")) continue;
    const parsed = parseNumeric(label);
    if (parsed !== null && parsed > 0) {
      return { value: parsed, source: "label" };
    }
  }
  return { value: null, source: snapshot.referenceSource ?? "missing" };
}

export function resolveCloseTimeMs(
  snapshot: MarketSnapshot,
  now: number,
): number | null {
  if (
    snapshot.marketCloseTimeMs !== null &&
    snapshot.marketCloseTimeMs !== undefined &&
    Number.isFinite(snapshot.marketCloseTimeMs)
  ) {
    return snapshot.marketCloseTimeMs;
  }
  if (
    snapshot.timeLeftSec !== null &&
    snapshot.timeLeftSec !== undefined &&
    Number.isFinite(snapshot.timeLeftSec)
  ) {
    return now + snapshot.timeLeftSec * 1000;
  }
  return null;
}

export function computeFinalPrice(
  snapshot: MarketSnapshot,
  now: number,
  options: {
    windowMs: number;
    minPoints: number;
    allowStaleAfterMs: number;
  },
): FinalPriceResult {
  const closeTimeMs = resolveCloseTimeMs(snapshot, now);
  const allowStale =
    closeTimeMs === null ? true : now - closeTimeMs > options.allowStaleAfterMs;

  if (
    snapshot.provider === "kalshi" &&
    snapshot.kalshiUnderlyingValue != null &&
    Number.isFinite(snapshot.kalshiUnderlyingValue) &&
    snapshot.kalshiUnderlyingValue > 0
  ) {
    const ts = snapshot.kalshiUnderlyingTs ?? null;
    const withinWindow =
      closeTimeMs !== null && ts !== null
        ? Math.abs(closeTimeMs - ts) <= options.windowMs
        : false;
    if (withinWindow || allowStale || closeTimeMs === null) {
      return {
        value: snapshot.kalshiUnderlyingValue,
        source: withinWindow ? "kalshi_underlying" : "kalshi_underlying_stale",
        points: 1,
        coverageMs: null,
        windowMs: options.windowMs,
      };
    }
  }

  if (closeTimeMs !== null && snapshot.priceHistoryWithTs) {
    const windowStart = closeTimeMs - options.windowMs;
    const points = snapshot.priceHistoryWithTs.filter(
      (p) => p.ts >= windowStart && p.ts <= closeTimeMs,
    );
    if (points.length >= options.minPoints) {
      const first = points[0];
      const last = points[points.length - 1];
      if (!first || !last) {
        return {
          value: null,
          source: "pending_final_price",
          points: 0,
          coverageMs: null,
          windowMs: options.windowMs,
        };
      }
      const sum = points.reduce((acc, p) => acc + p.price, 0);
      const avg = sum / points.length;
      const coverage = last.ts - first.ts;
      return {
        value: avg,
        source: "spot_avg_window",
        points: points.length,
        coverageMs: coverage,
        windowMs: options.windowMs,
      };
    }
  }

  if (closeTimeMs !== null && !allowStale) {
    return {
      value: null,
      source: "pending_final_price",
      points: 0,
      coverageMs: null,
      windowMs: options.windowMs,
    };
  }

  const spot = snapshot.cryptoPrice > 0 ? snapshot.cryptoPrice : null;
  if (spot != null) {
    return {
      value: spot,
      source: allowStale ? "spot_last_stale" : "spot_last",
      points: 1,
      coverageMs: null,
      windowMs: options.windowMs,
    };
  }

  return {
    value: null,
    source: "missing",
    points: 0,
    coverageMs: null,
    windowMs: options.windowMs,
  };
}

export function computeOutcomeFromValues(
  price: number | null,
  threshold: number | null,
): NormalizedOutcome {
  if (
    price === null ||
    threshold === null ||
    !Number.isFinite(price) ||
    !Number.isFinite(threshold) ||
    threshold <= 0
  ) {
    return "UNKNOWN";
  }
  return price >= threshold ? "UP" : "DOWN";
}

export async function fetchPolymarketOfficialOutcome(
  slug: string,
): Promise<OfficialOutcomeResult> {
  const market = await getMarketBySlug(slug);
  if (!market) {
    return {
      outcome: null,
      outcomeSource: null,
      finalPrice: null,
      finalPriceSource: null,
    };
  }
  const record = market as unknown as Record<string, unknown>;
  const outcomes = extractOutcomes(market);

  const outcomeKeys = [
    "resolution",
    "resolvedOutcome",
    "resolved_outcome",
    "result",
    "winningOutcome",
    "winning_outcome",
    "finalOutcome",
    "final_outcome",
    "outcome",
    "answer",
  ];
  let outcome: NormalizedOutcome | null = null;
  let outcomeSource: string | null = null;
  for (const key of outcomeKeys) {
    if (!(key in record)) continue;
    const candidate = normalizeOutcomeValue(record[key], outcomes);
    if (candidate) {
      outcome = candidate;
      outcomeSource = key;
      break;
    }
  }

  if (!outcome) {
    const prices = parseOutcomePrices(
      record.outcomePrices ?? record.outcome_prices,
    );
    if (prices && outcomes.length === prices.length) {
      let maxIdx = 0;
      let maxVal = prices[0] ?? 0;
      for (let i = 1; i < prices.length; i += 1) {
        if ((prices[i] ?? 0) > maxVal) {
          maxVal = prices[i] ?? 0;
          maxIdx = i;
        }
      }
      if (maxVal >= 0.95) {
        const candidate = normalizeOutcomeValue(maxIdx, outcomes);
        if (candidate) {
          outcome = candidate;
          outcomeSource = "outcomePrices";
        }
      }
    }
  }

  const priceKeys = [
    "finalPrice",
    "final_price",
    "settlement_price",
    "settlementPrice",
    "final_value",
    "finalValue",
    "settlement_value",
    "resolution_price",
    "close_price",
    "closing_price",
  ];
  const numeric = findNumericByKeys(record, priceKeys);
  return {
    outcome,
    outcomeSource,
    finalPrice: numeric.value,
    finalPriceSource: numeric.key,
  };
}

export async function fetchKalshiOfficialOutcome(
  client: KalshiClient,
  ticker: string,
): Promise<OfficialOutcomeResult> {
  const market = await client.getMarket(ticker);
  if (!market) {
    return {
      outcome: null,
      outcomeSource: null,
      finalPrice: null,
      finalPriceSource: null,
    };
  }
  const record = market as Record<string, unknown>;

  const outcomeKeys = [
    "result",
    "market_result",
    "settlement_result",
    "final_result",
    "resolution",
    "resolved_outcome",
    "settled_outcome",
    "outcome",
  ];
  let outcome: NormalizedOutcome | null = null;
  let outcomeSource: string | null = null;
  for (const key of outcomeKeys) {
    if (!(key in record)) continue;
    const candidate = normalizeOutcomeValue(record[key], ["Yes", "No"]);
    if (candidate) {
      outcome = candidate;
      outcomeSource = key;
      break;
    }
  }

  const priceKeys = [
    "settlement_price",
    "settlement_price_dollars",
    "settlement_price_cents",
    "final_price",
    "final_price_dollars",
    "final_price_cents",
    "settlement_value",
    "final_value",
    "resolution_price",
    "close_price",
    "closing_price",
  ];
  const numeric = findNumericByKeys(record, priceKeys);
  return {
    outcome,
    outcomeSource,
    finalPrice: numeric.value,
    finalPriceSource: numeric.key,
  };
}

function parseNumeric(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const cleaned = value.replace(/[$,%]/g, "").replace(/,/g, "").trim();
    if (!cleaned) return null;
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return parseNumeric(
      record.value ?? record.display_value ?? record.close ?? record.open,
    );
  }
  return null;
}

function findNumericByKeys(
  record: Record<string, unknown>,
  keys: string[],
): { value: number | null; key: string | null } {
  for (const key of keys) {
    if (!(key in record)) continue;
    const parsed = parseNumeric(record[key]);
    if (parsed !== null && Number.isFinite(parsed)) {
      return { value: parsed, key };
    }
  }
  return { value: null, key: null };
}

function normalizeOutcomeValue(
  value: unknown,
  outcomes?: string[],
): NormalizedOutcome | null {
  if (typeof value === "string") {
    const normalized = normalizeOutcomeLabel(value);
    return normalized === "UNKNOWN" ? null : normalized;
  }
  if (typeof value === "boolean") {
    return value ? "UP" : "DOWN";
  }
  if (typeof value === "number" && outcomes && outcomes.length > 0) {
    const idx = Math.round(value);
    if (idx >= 0 && idx < outcomes.length) {
      const normalized = normalizeOutcomeLabel(outcomes[idx] ?? "");
      return normalized === "UNKNOWN" ? null : normalized;
    }
  }
  return null;
}

function parseOutcomePrices(value: unknown): number[] | null {
  if (!value) return null;
  if (Array.isArray(value)) {
    const parsed = value
      .map((v) => parseNumeric(v))
      .filter((v) => v != null) as number[];
    return parsed.length > 0 ? parsed : null;
  }
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) {
        const nums = parsed
          .map((v) => parseNumeric(v))
          .filter((v) => v != null) as number[];
        return nums.length > 0 ? nums : null;
      }
    } catch {
      return null;
    }
  }
  return null;
}
