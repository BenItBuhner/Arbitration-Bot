/**
 * Cross-platform outcome analysis: compare Polymarket vs Kalshi for the same coin/timeframe.
 * Runs both hubs side-by-side, normalizes outcomes, and tracks resolution match % per market.
 */

import { existsSync, mkdirSync } from "fs";
import { join } from "path";
import { MarketDataHub, type MarketSnapshot } from "../services/market-data-hub";
import { KalshiMarketDataHub } from "../services/kalshi-market-data-hub";
import { RunLogger } from "../services/run-logger";
import { loadProviderConfig, normalizeCoinKey } from "../services/profile-config";
import { getKalshiEnvConfig } from "../clients/kalshi/kalshi-config";
import { KalshiClient } from "../clients/kalshi/kalshi-client";
import type { CoinSymbol } from "../services/auto-market";
import { parseUpDownSlugStartMs } from "../services/auto-market";
import {
  computeOddsMid,
  createAccuracyState,
  updateAccuracy,
  type NormalizedOutcome,
  type AccuracyState,
} from "../services/cross-platform-compare";
import { CrossPlatformDashboard } from "../cli/cross-platform-dashboard";
import { setupCoinNavigation } from "../cli/coin-navigation";
import { selectMany } from "../cli/prompts";
import {
  computeFinalPrice,
  computeOutcomeFromValues,
  fetchKalshiOfficialOutcome,
  fetchPolymarketOfficialOutcome,
  resolveCloseTimeMs,
  resolveThreshold,
} from "../services/outcome-resolution";

const ODDS_HISTORY_LIMIT = 180;
const RENDER_INTERVAL_MS = 500;
const MISMATCH_HISTORY_TAIL = 6;
const FINAL_WINDOW_MS = parseEnvNumber("CROSS_ANALYSIS_FINAL_WINDOW_MS", 60_000, 10_000);
const FINAL_GRACE_MS = parseEnvNumber("CROSS_ANALYSIS_FINAL_GRACE_MS", 120_000, 30_000);
const FINAL_MIN_POINTS = parseEnvNumber("CROSS_ANALYSIS_FINAL_MIN_POINTS", 3, 1);
const OFFICIAL_MAX_WAIT_MS = parseEnvNumber(
  "CROSS_ANALYSIS_OFFICIAL_WAIT_MS",
  45_000,
  5_000,
);
const FINAL_STALE_AFTER_MS = Math.min(FINAL_GRACE_MS, OFFICIAL_MAX_WAIT_MS);
const FINAL_PRICE_OPTIONS = {
  windowMs: FINAL_WINDOW_MS,
  minPoints: FINAL_MIN_POINTS,
  allowStaleAfterMs: FINAL_STALE_AFTER_MS,
};
const MATCH_TIME_TOLERANCE_MS = parseEnvNumber(
  "CROSS_ANALYSIS_MATCH_TIME_TOLERANCE_MS",
  120_000,
  10_000,
);
const SLOT_DURATION_MS = parseEnvNumber(
  "CROSS_ANALYSIS_SLOT_DURATION_MS",
  15 * 60 * 1000,
  60 * 1000,
);
const SLOT_TOLERANCE_MS = parseEnvNumber(
  "CROSS_ANALYSIS_SLOT_TOLERANCE_MS",
  90_000,
  10_000,
);
const STALE_LOG_INTERVAL_MS = parseEnvNumber(
  "CROSS_ANALYSIS_STALE_LOG_MS",
  30_000,
  5_000,
);
const PRICE_STALE_MS = parseEnvNumber(
  "CROSS_ANALYSIS_PRICE_STALE_MS",
  20_000,
  5_000,
);
const BOOK_STALE_MS = parseEnvNumber(
  "CROSS_ANALYSIS_BOOK_STALE_MS",
  30_000,
  5_000,
);
const SUMMARY_LOG_INTERVAL_MS = parseEnvNumber(
  "CROSS_ANALYSIS_SUMMARY_LOG_MS",
  300_000,
  60_000,
);
const OFFICIAL_RETRY_BASE_MS = parseEnvNumber(
  "CROSS_ANALYSIS_OFFICIAL_RETRY_BASE_MS",
  10_000,
  2_000,
);
const OFFICIAL_RETRY_MAX_MS = parseEnvNumber(
  "CROSS_ANALYSIS_OFFICIAL_RETRY_MAX_MS",
  120_000,
  10_000,
);
const OFFICIAL_RETRY_LIMIT = parseEnvNumber(
  "CROSS_ANALYSIS_OFFICIAL_RETRIES",
  0,
  0,
);

function parseEnvFlag(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const normalized = raw.trim().toLowerCase();
  if (["false", "0", "off", "no"].includes(normalized)) return false;
  if (["true", "1", "on", "yes"].includes(normalized)) return true;
  return defaultValue;
}

function parseEnvNumber(
  name: string,
  defaultValue: number,
  minValue: number,
): number {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.max(minValue, parsed);
}

function formatMaybe(value: number | null | undefined, decimals: number): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "n/a";
  return value.toFixed(decimals);
}

function formatIso(ms: number | null | undefined): string {
  if (!ms || !Number.isFinite(ms)) return "n/a";
  return new Date(ms).toISOString();
}

function tail(values: number[] | undefined, limit: number): number[] {
  if (!values || values.length === 0) return [];
  return values.length <= limit ? values.slice() : values.slice(-limit);
}

function resolveSlotStartMs(
  snapshot: MarketSnapshot,
  now: number,
  durationMs: number,
): number | null {
  if (snapshot.provider === "polymarket") {
    const parsed = parseUpDownSlugStartMs(snapshot.slug);
    if (parsed) return parsed;
  }
  const closeTime = resolveCloseTimeMs(snapshot, now);
  if (closeTime !== null) {
    return closeTime - durationMs;
  }
  return null;
}

function computeAgeMs(value: number | null | undefined, now: number): number | null {
  if (!value || !Number.isFinite(value) || value <= 0) return null;
  return Math.max(0, now - value);
}

function getOrderBookSummary(snapshot: MarketSnapshot, tokenId?: string | null): {
  bestBid: number | null;
  bestAsk: number | null;
  mid: number | null;
  lastTrade: number | null;
  totalBidValue: number | null;
  totalAskValue: number | null;
  bidLevels: number | null;
  askLevels: number | null;
} {
  if (!tokenId) {
    return {
      bestBid: null,
      bestAsk: null,
      mid: null,
      lastTrade: null,
      totalBidValue: null,
      totalAskValue: null,
      bidLevels: null,
      askLevels: null,
    };
  }
  const bestBid = snapshot.bestBid.get(tokenId) ?? null;
  const bestAsk = snapshot.bestAsk.get(tokenId) ?? null;
  const mid = computeOddsMid(bestBid, bestAsk);
  const book = snapshot.orderBooks.get(tokenId);
  return {
    bestBid,
    bestAsk,
    mid,
    lastTrade: book?.lastTrade ?? null,
    totalBidValue: book?.totalBidValue ?? null,
    totalAskValue: book?.totalAskValue ?? null,
    bidLevels: book?.bids.length ?? null,
    askLevels: book?.asks.length ?? null,
  };
}

function getNextRunDir(rootDir: string): { runDir: string; runId: string } {
  let index = 1;
  while (true) {
    const name = index === 1 ? "run" : `run${index}`;
    const candidate = join(rootDir, name);
    if (!existsSync(candidate)) {
      mkdirSync(candidate, { recursive: true });
      return { runDir: candidate, runId: name };
    }
    index += 1;
  }
}

export interface CrossPlatformAnalysisRouteOptions {
  coins?: CoinSymbol[];
  headless?: boolean;
  headlessSummary?: boolean;
}

interface LastComparison {
  polyOutcome: NormalizedOutcome;
  kalshiOutcome: NormalizedOutcome;
  matched: boolean;
  comparedAtMs: number;
  polySlug: string;
  kalshiSlug: string;
}

interface PairSnapshots {
  coin: CoinSymbol;
  polySnap: MarketSnapshot;
  kalshiSnap: MarketSnapshot;
  lastSeenMs: number;
  polyClosePrice?: number | null;
  polyClosePriceSource?: string | null;
  polyClosePricePoints?: number | null;
  polyClosePriceCoverageMs?: number | null;
  polyClosePriceWindowMs?: number | null;
  polyCloseThreshold?: number | null;
  polyCloseThresholdSource?: string | null;
  polyCloseCapturedMs?: number | null;
  kalshiClosePrice?: number | null;
  kalshiClosePriceSource?: string | null;
  kalshiClosePricePoints?: number | null;
  kalshiClosePriceCoverageMs?: number | null;
  kalshiClosePriceWindowMs?: number | null;
  kalshiCloseThreshold?: number | null;
  kalshiCloseThresholdSource?: string | null;
  kalshiCloseCapturedMs?: number | null;
  polyOutcome?: NormalizedOutcome | null;
  kalshiOutcome?: NormalizedOutcome | null;
  unknownLogged?: boolean;
  polyOfficialOutcome?: NormalizedOutcome | null;
  polyOfficialOutcomeSource?: string | null;
  polyOfficialFinalPrice?: number | null;
  polyOfficialFinalPriceSource?: string | null;
  polyOfficialFetchAttempts?: number;
  polyOfficialFetchPending?: boolean;
  polyOfficialFetchLastMs?: number | null;
  kalshiOfficialOutcome?: NormalizedOutcome | null;
  kalshiOfficialOutcomeSource?: string | null;
  kalshiOfficialFinalPrice?: number | null;
  kalshiOfficialFinalPriceSource?: string | null;
  kalshiOfficialFetchAttempts?: number;
  kalshiOfficialFetchPending?: boolean;
  kalshiOfficialFetchLastMs?: number | null;
}

export async function crossPlatformAnalysisRoute(
  options: CrossPlatformAnalysisRouteOptions = {},
): Promise<void> {
  const polyConfig = loadProviderConfig("polymarket");
  const kalshiConfigResult = loadProviderConfig("kalshi");
  const summaryOnly = options.headlessSummary === true;
  const headless = options.headless || summaryOnly;
  const availableCoins = polyConfig.coinOptions.filter((c) =>
    kalshiConfigResult.coinOptions.includes(c),
  );
  let coins: CoinSymbol[] = [];

  if (options.coins && options.coins.length > 0) {
    coins = options.coins.filter((c) => availableCoins.includes(c));
    if (coins.length === 0) {
      console.log("No valid coins matched --coins.");
      return;
    }
  } else if (process.stdin.isTTY && !headless) {
    const selectedCoinsRaw = await selectMany(
      "Select coins",
      availableCoins.map((coin) => ({
        title: coin.toUpperCase(),
        value: coin.toUpperCase(),
      })),
    );
    if (selectedCoinsRaw.length === 0) {
      console.log("No coins selected. Exiting.");
      return;
    }
    coins = selectedCoinsRaw
      .map((coin) => normalizeCoinKey(coin))
      .filter((coin): coin is CoinSymbol => !!coin);
    if (coins.length === 0) {
      console.log("No valid coins selected. Exiting.");
      return;
    }
  } else {
    coins = availableCoins;
  }

  if (coins.length === 0) {
    console.log(
      "No coins in common between Polymarket and Kalshi config. Add at least one (e.g. eth) to both providers in config.json.",
    );
    return;
  }

  let kalshiEnvConfig;
  try {
    kalshiEnvConfig = getKalshiEnvConfig(process.env);
  } catch (e) {
    console.log(
      "Kalshi env not configured. Set KALSHI_API_KEY, KALSHI_PRIVATE_KEY_PATH, and optionally KALSHI_ENV.",
    );
    return;
  }
  const kalshiOutcomeClient = new KalshiClient(kalshiEnvConfig);

  const kalshiSelectors = kalshiConfigResult.kalshiSelectorsByCoin;
  if (!kalshiSelectors) {
    console.log("Kalshi market selectors missing in config.");
    return;
  }

  const logsRoot = join(process.cwd(), "logs", "cross-platform");
  if (!existsSync(logsRoot)) {
    mkdirSync(logsRoot, { recursive: true });
  }
  const { runDir, runId } = getNextRunDir(logsRoot);
  const systemLogger = new RunLogger(join(runDir, "system.log"), 200, {
    stdout: summaryOnly,
  });
  const debugLogger = summaryOnly
    ? new RunLogger(join(runDir, "debug.log"))
    : systemLogger;
  const mismatchLogger = new RunLogger(join(runDir, "mismatch.log"));
  const verboseAllFinals = parseEnvFlag("CROSS_ANALYSIS_VERBOSE", false);
  const verboseMismatch = parseEnvFlag("CROSS_ANALYSIS_MISMATCH_VERBOSE", true);
  systemLogger.log(
    `Cross-platform analysis run ${runId} started for coins: ${coins.join(", ")}`,
  );
  if (summaryOnly) {
    systemLogger.log(
      "Headless summary mode enabled (system.log concise, debug.log verbose).",
    );
  }

  const polyHub = new MarketDataHub(debugLogger);
  const kalshiHub = new KalshiMarketDataHub(
    debugLogger,
    kalshiEnvConfig,
    kalshiSelectors,
  );

  await polyHub.start(coins);
  await kalshiHub.start(coins);

  const accuracyByCoin = new Map<CoinSymbol, AccuracyState>();
  for (const coin of coins) {
    accuracyByCoin.set(coin, createAccuracyState());
  }
  const lastComparisonByCoin = new Map<CoinSymbol, LastComparison>();
  const pairSnapshotsByKey = new Map<string, PairSnapshots>();
  const activePairKeyByCoin = new Map<CoinSymbol, string>();
  const pendingPairKeys = new Set<string>();
  const comparedPairKeys = new Set<string>();
  let lastSummaryLogMs = 0;
  const lastStaleLogByCoin = new Map<CoinSymbol, number>();
  const staleStateByCoin = new Map<CoinSymbol, boolean>();

  const polyOddsHistoryByCoin = new Map<CoinSymbol, number[]>();
  const kalshiOddsHistoryByCoin = new Map<CoinSymbol, number[]>();
  for (const coin of coins) {
    polyOddsHistoryByCoin.set(coin, []);
    kalshiOddsHistoryByCoin.set(coin, []);
  }

  const dashboard = headless ? null : new CrossPlatformDashboard();
  let activeCoinIndex = 0;
  const cleanupNavigation = dashboard
    ? setupCoinNavigation(
        coins.length,
        () => activeCoinIndex,
        (next) => {
          activeCoinIndex = next;
        },
      )
    : () => {};

  const timer = setInterval(() => {
    try {
      const now = Date.now();
      const polySnapshots = polyHub.getSnapshots();
      const kalshiSnapshots = kalshiHub.getSnapshots();

    const isSnapshotClosed = (snapshot: MarketSnapshot): boolean => {
      if (
        snapshot.timeLeftSec !== null &&
        snapshot.timeLeftSec !== undefined &&
        snapshot.timeLeftSec <= 0
      ) {
        return true;
      }
      if (
        snapshot.marketCloseTimeMs !== null &&
        snapshot.marketCloseTimeMs !== undefined &&
        now >= snapshot.marketCloseTimeMs
      ) {
        return true;
      }
      return false;
    };

    const logOutcomeAnalytics = (
      entry: PairSnapshots,
      pairKey: string,
      polyOutcome: NormalizedOutcome | null,
      kalshiOutcome: NormalizedOutcome | null,
      matched: boolean,
      level: "INFO" | "WARN",
    ): void => {
      const polyThreshold =
        entry.polyCloseThreshold ?? resolveThreshold(entry.polySnap).value;
      const kalshiThreshold =
        entry.kalshiCloseThreshold ?? resolveThreshold(entry.kalshiSnap).value;
      const polyPrice =
        entry.polyClosePrice ??
        computeFinalPrice(entry.polySnap, now, FINAL_PRICE_OPTIONS).value;
      const kalshiPrice =
        entry.kalshiClosePrice ??
        computeFinalPrice(entry.kalshiSnap, now, FINAL_PRICE_OPTIONS).value;

      const polyUpBook = getOrderBookSummary(
        entry.polySnap,
        entry.polySnap.upTokenId,
      );
      const polyDownBook = getOrderBookSummary(
        entry.polySnap,
        entry.polySnap.downTokenId,
      );
      const kalshiYesBook = getOrderBookSummary(entry.kalshiSnap, "YES");
      const kalshiNoBook = getOrderBookSummary(entry.kalshiSnap, "NO");

      const polyCloseIso = formatIso(entry.polySnap.marketCloseTimeMs ?? null);
      const kalshiCloseIso = formatIso(entry.kalshiSnap.marketCloseTimeMs ?? null);
      const closeDeltaSec =
        entry.polySnap.marketCloseTimeMs && entry.kalshiSnap.marketCloseTimeMs
          ? Math.round(
              (entry.kalshiSnap.marketCloseTimeMs -
                entry.polySnap.marketCloseTimeMs) /
                1000,
            )
          : null;

      const spotTailPoly = tail(entry.polySnap.priceHistory, MISMATCH_HISTORY_TAIL);
      const spotTailKalshi = tail(
        entry.kalshiSnap.priceHistory,
        MISMATCH_HISTORY_TAIL,
      );
      const polyOddsTail = tail(
        polyOddsHistoryByCoin.get(entry.coin),
        MISMATCH_HISTORY_TAIL,
      );
      const kalshiOddsTail = tail(
        kalshiOddsHistoryByCoin.get(entry.coin),
        MISMATCH_HISTORY_TAIL,
      );

      const json = {
        coin: entry.coin,
        pairKey,
        matched,
        polyOutcome,
        kalshiOutcome,
        polyOfficialOutcome: entry.polyOfficialOutcome ?? null,
        polyOfficialOutcomeSource: entry.polyOfficialOutcomeSource ?? null,
        kalshiOfficialOutcome: entry.kalshiOfficialOutcome ?? null,
        kalshiOfficialOutcomeSource: entry.kalshiOfficialOutcomeSource ?? null,
        polyOfficialFinalPrice: entry.polyOfficialFinalPrice ?? null,
        polyOfficialFinalPriceSource: entry.polyOfficialFinalPriceSource ?? null,
        kalshiOfficialFinalPrice: entry.kalshiOfficialFinalPrice ?? null,
        kalshiOfficialFinalPriceSource: entry.kalshiOfficialFinalPriceSource ?? null,
        polySlug: entry.polySnap.slug,
        kalshiSlug: entry.kalshiSnap.slug ?? entry.kalshiSnap.marketTicker ?? null,
        polySeries: entry.polySnap.seriesSlug ?? null,
        kalshiSeries: entry.kalshiSnap.seriesSlug ?? null,
        polyEvent: entry.polySnap.eventTicker ?? null,
        kalshiEvent: entry.kalshiSnap.eventTicker ?? null,
        polyCloseTimeIso: polyCloseIso,
        kalshiCloseTimeIso: kalshiCloseIso,
        closeTimeDeltaSec: closeDeltaSec,
        polyTimeLeftSec: entry.polySnap.timeLeftSec ?? null,
        kalshiTimeLeftSec: entry.kalshiSnap.timeLeftSec ?? null,
        polyThreshold,
        polyThresholdSource:
          entry.polyCloseThresholdSource ?? resolveThreshold(entry.polySnap).source,
        polyFinalPricePoints:
          entry.polyClosePricePoints ??
          computeFinalPrice(entry.polySnap, now, FINAL_PRICE_OPTIONS).points,
        polyFinalPriceCoverageMs:
          entry.polyClosePriceCoverageMs ??
          computeFinalPrice(entry.polySnap, now, FINAL_PRICE_OPTIONS).coverageMs,
        polyFinalPriceWindowMs:
          entry.polyClosePriceWindowMs ??
          computeFinalPrice(entry.polySnap, now, FINAL_PRICE_OPTIONS).windowMs,
        kalshiThreshold,
        kalshiThresholdSource:
          entry.kalshiCloseThresholdSource ??
          resolveThreshold(entry.kalshiSnap).source,
        kalshiFinalPricePoints:
          entry.kalshiClosePricePoints ??
          computeFinalPrice(entry.kalshiSnap, now, FINAL_PRICE_OPTIONS).points,
        kalshiFinalPriceCoverageMs:
          entry.kalshiClosePriceCoverageMs ??
          computeFinalPrice(entry.kalshiSnap, now, FINAL_PRICE_OPTIONS).coverageMs,
        kalshiFinalPriceWindowMs:
          entry.kalshiClosePriceWindowMs ??
          computeFinalPrice(entry.kalshiSnap, now, FINAL_PRICE_OPTIONS).windowMs,
        polyPriceUsed: polyPrice,
        polyPriceSource:
          entry.polyClosePriceSource ??
          computeFinalPrice(entry.polySnap, now, FINAL_PRICE_OPTIONS).source,
        kalshiPriceUsed: kalshiPrice,
        kalshiPriceSource:
          entry.kalshiClosePriceSource ??
          computeFinalPrice(entry.kalshiSnap, now, FINAL_PRICE_OPTIONS).source,
        polyPriceDelta:
          polyPrice != null && polyThreshold != null ? polyPrice - polyThreshold : null,
        polyPriceDeltaPct:
          polyPrice != null && polyThreshold != null && polyThreshold !== 0
            ? ((polyPrice - polyThreshold) / polyThreshold) * 100
            : null,
        kalshiPriceDelta:
          kalshiPrice != null && kalshiThreshold != null
            ? kalshiPrice - kalshiThreshold
            : null,
        kalshiPriceDeltaPct:
          kalshiPrice != null && kalshiThreshold != null && kalshiThreshold !== 0
            ? ((kalshiPrice - kalshiThreshold) / kalshiThreshold) * 100
            : null,
        polySpotPrice: entry.polySnap.cryptoPrice ?? null,
        kalshiSpotPrice: entry.kalshiSnap.cryptoPrice ?? null,
        polySpotAgeSec: entry.polySnap.cryptoPriceTimestamp
          ? Math.round((now - entry.polySnap.cryptoPriceTimestamp) / 1000)
          : null,
        kalshiSpotAgeSec: entry.kalshiSnap.cryptoPriceTimestamp
          ? Math.round((now - entry.kalshiSnap.cryptoPriceTimestamp) / 1000)
          : null,
        kalshiUnderlyingValue: entry.kalshiSnap.kalshiUnderlyingValue ?? null,
        kalshiUnderlyingTs: formatIso(entry.kalshiSnap.kalshiUnderlyingTs ?? null),
        kalshiLastPrice: entry.kalshiSnap.kalshiLastPrice ?? null,
        kalshiMarketPrice: entry.kalshiSnap.kalshiMarketPrice ?? null,
        polyBook: polyUpBook,
        polyDownBook,
        kalshiYesBook,
        kalshiNoBook,
        polyDataStatus: entry.polySnap.dataStatus,
        kalshiDataStatus: entry.kalshiSnap.dataStatus,
        polyOddsTail,
        kalshiOddsTail,
        polySpotTail: spotTailPoly,
        kalshiSpotTail: spotTailKalshi,
      };

      const prefix = matched ? "FINAL_MATCH" : "FINAL_MISMATCH";
      mismatchLogger.log(
        `${prefix} ${entry.coin.toUpperCase()} pair=${pairKey} poly=${polyOutcome} kalshi=${kalshiOutcome}`,
        level,
      );
      mismatchLogger.log(
        `${prefix} thresholds poly=${formatMaybe(polyThreshold, 2)} (${json.polyThresholdSource}) kalshi=${formatMaybe(kalshiThreshold, 2)} (${json.kalshiThresholdSource})`,
        level,
      );
      mismatchLogger.log(
        `${prefix} prices poly=${formatMaybe(polyPrice, 2)} (${json.polyPriceSource}) pts=${json.polyFinalPricePoints ?? "n/a"} covMs=${json.polyFinalPriceCoverageMs ?? "n/a"} kalshi=${formatMaybe(kalshiPrice, 2)} (${json.kalshiPriceSource}) pts=${json.kalshiFinalPricePoints ?? "n/a"} covMs=${json.kalshiFinalPriceCoverageMs ?? "n/a"}`,
        level,
      );
      mismatchLogger.log(
        `${prefix} deltas poly=${formatMaybe(json.polyPriceDelta ?? null, 2)} (${formatMaybe(json.polyPriceDeltaPct ?? null, 2)}%) kalshi=${formatMaybe(json.kalshiPriceDelta ?? null, 2)} (${formatMaybe(json.kalshiPriceDeltaPct ?? null, 2)}%)`,
        level,
      );
      mismatchLogger.log(
        `${prefix} official poly=${json.polyOfficialOutcome ?? "n/a"} (${json.polyOfficialOutcomeSource ?? "n/a"}) kalshi=${json.kalshiOfficialOutcome ?? "n/a"} (${json.kalshiOfficialOutcomeSource ?? "n/a"})`,
        level,
      );
      mismatchLogger.log(
        `${prefix} odds polyBid=${formatMaybe(polyUpBook.bestBid, 4)} polyAsk=${formatMaybe(polyUpBook.bestAsk, 4)} kalshiBid=${formatMaybe(kalshiYesBook.bestBid, 4)} kalshiAsk=${formatMaybe(kalshiYesBook.bestAsk, 4)}`,
        level,
      );
      mismatchLogger.log(
        `${prefix} liquidity polyYesBid=${formatMaybe(polyUpBook.totalBidValue, 2)} polyYesAsk=${formatMaybe(polyUpBook.totalAskValue, 2)} kalshiYesBid=${formatMaybe(kalshiYesBook.totalBidValue, 2)} kalshiYesAsk=${formatMaybe(kalshiYesBook.totalAskValue, 2)}`,
        level,
      );
      mismatchLogger.log(
        `${prefix} liquidity polyNoBid=${formatMaybe(polyDownBook.totalBidValue, 2)} polyNoAsk=${formatMaybe(polyDownBook.totalAskValue, 2)} kalshiNoBid=${formatMaybe(kalshiNoBook.totalBidValue, 2)} kalshiNoAsk=${formatMaybe(kalshiNoBook.totalAskValue, 2)}`,
        level,
      );
      mismatchLogger.log(
        `${prefix} close poly=${polyCloseIso} kalshi=${kalshiCloseIso} deltaSec=${closeDeltaSec ?? "n/a"}`,
        level,
      );
      mismatchLogger.log(
        `${prefix} spotTail poly=[${spotTailPoly.map((v) => formatMaybe(v, 2)).join(", ")}] kalshi=[${spotTailKalshi.map((v) => formatMaybe(v, 2)).join(", ")}]`,
        level,
      );
      mismatchLogger.log(
        `${prefix} oddsTail poly=[${polyOddsTail.map((v) => formatMaybe(v, 4)).join(", ")}] kalshi=[${kalshiOddsTail.map((v) => formatMaybe(v, 4)).join(", ")}]`,
        level,
      );
      mismatchLogger.log(`${prefix}_JSON ${JSON.stringify(json)}`, level);
    };

    const tryFinalizePair = (pairKey: string, entry: PairSnapshots): boolean => {
      if (comparedPairKeys.has(pairKey)) return true;
      const polyClosed = isSnapshotClosed(entry.polySnap);
      const kalshiClosed = isSnapshotClosed(entry.kalshiSnap);

      if (polyClosed && entry.polyOutcome == null) {
        maybeFetchPolymarketOfficial(entry);
      }
      if (kalshiClosed && entry.kalshiOutcome == null) {
        maybeFetchKalshiOfficial(entry);
      }

      if (polyClosed && entry.polyOutcome == null) {
        const threshold = resolveThreshold(entry.polySnap);
        const finalPrice = computeFinalPrice(
          entry.polySnap,
          now,
          FINAL_PRICE_OPTIONS,
        );
        const polyCloseTimeMs = resolveCloseTimeMs(entry.polySnap, now);
        const polyAttempts = entry.polyOfficialFetchAttempts ?? 0;
        const allowComputed =
          (OFFICIAL_RETRY_LIMIT > 0 &&
            polyAttempts >= OFFICIAL_RETRY_LIMIT) ||
          (polyCloseTimeMs !== null &&
            now - polyCloseTimeMs > OFFICIAL_MAX_WAIT_MS);
        entry.polyClosePrice = finalPrice.value;
        entry.polyClosePriceSource = finalPrice.source;
        entry.polyClosePricePoints = finalPrice.points;
        entry.polyClosePriceCoverageMs = finalPrice.coverageMs;
        entry.polyClosePriceWindowMs = finalPrice.windowMs;
        entry.polyCloseThreshold = threshold.value;
        entry.polyCloseThresholdSource = threshold.source;
        entry.polyCloseCapturedMs = now;
        if (
          allowComputed &&
          finalPrice.value !== null &&
          threshold.value !== null &&
          threshold.value > 0
        ) {
          entry.polyOutcome = computeOutcomeFromValues(
            finalPrice.value,
            threshold.value,
          );
        }
      }

      if (kalshiClosed && entry.kalshiOutcome == null) {
        const threshold = resolveThreshold(entry.kalshiSnap);
        const finalPrice = computeFinalPrice(
          entry.kalshiSnap,
          now,
          FINAL_PRICE_OPTIONS,
        );
        const kalshiCloseTimeMs = resolveCloseTimeMs(entry.kalshiSnap, now);
        const kalshiAttempts = entry.kalshiOfficialFetchAttempts ?? 0;
        const allowComputed =
          (OFFICIAL_RETRY_LIMIT > 0 &&
            kalshiAttempts >= OFFICIAL_RETRY_LIMIT) ||
          (kalshiCloseTimeMs !== null &&
            now - kalshiCloseTimeMs > OFFICIAL_MAX_WAIT_MS);
        entry.kalshiClosePrice = finalPrice.value;
        entry.kalshiClosePriceSource = finalPrice.source;
        entry.kalshiClosePricePoints = finalPrice.points;
        entry.kalshiClosePriceCoverageMs = finalPrice.coverageMs;
        entry.kalshiClosePriceWindowMs = finalPrice.windowMs;
        entry.kalshiCloseThreshold = threshold.value;
        entry.kalshiCloseThresholdSource = threshold.source;
        entry.kalshiCloseCapturedMs = now;
        if (
          allowComputed &&
          finalPrice.value !== null &&
          threshold.value !== null &&
          threshold.value > 0
        ) {
          entry.kalshiOutcome = computeOutcomeFromValues(
            finalPrice.value,
            threshold.value,
          );
        }
      }

      if (!polyClosed || !kalshiClosed) {
        return false;
      }

      const polyCloseTimeMs = resolveCloseTimeMs(entry.polySnap, now);
      const kalshiCloseTimeMs = resolveCloseTimeMs(entry.kalshiSnap, now);
      const closeDeltaMs =
        polyCloseTimeMs && kalshiCloseTimeMs
          ? Math.abs(polyCloseTimeMs - kalshiCloseTimeMs)
          : null;

      if (closeDeltaMs !== null && closeDeltaMs > MATCH_TIME_TOLERANCE_MS) {
        comparedPairKeys.add(pairKey);
        systemLogger.log(
          `FINAL outcome skipped for ${entry.coin.toUpperCase()} pair=${pairKey} (close time delta ${(closeDeltaMs / 1000).toFixed(
            1,
          )}s exceeds tolerance)`,
          "WARN",
        );
        mismatchLogger.log(
          `FINAL_SKIPPED ${entry.coin.toUpperCase()} pair=${pairKey} closeDeltaSec=${(
            closeDeltaMs / 1000
          ).toFixed(1)} polyClose=${formatIso(polyCloseTimeMs)} kalshiClose=${formatIso(
            kalshiCloseTimeMs,
          )}`,
          "WARN",
        );
        return true;
      }
      const polyWithinGrace =
        polyCloseTimeMs !== null && now - polyCloseTimeMs < FINAL_GRACE_MS;
      const kalshiWithinGrace =
        kalshiCloseTimeMs !== null && now - kalshiCloseTimeMs < FINAL_GRACE_MS;

      if (
        entry.polyOutcome == null ||
        entry.kalshiOutcome == null ||
        entry.polyOutcome === "UNKNOWN" ||
        entry.kalshiOutcome === "UNKNOWN"
      ) {
        if (polyWithinGrace || kalshiWithinGrace) {
          return false;
        }
        if (!entry.unknownLogged) {
          entry.unknownLogged = true;
          logDebug(
            `FINAL outcome unresolved for ${entry.coin.toUpperCase()} pair=${pairKey} poly=${entry.polyOutcome ?? "null"} kalshi=${entry.kalshiOutcome ?? "null"} (threshold/price missing)`,
            "WARN",
          );
          if (verboseMismatch) {
            logOutcomeAnalytics(
              entry,
              pairKey,
              entry.polyOutcome ?? null,
              entry.kalshiOutcome ?? null,
              false,
              "WARN",
            );
          }
        }
        return false;
      }

      const polyOutcome = entry.polyOutcome;
      const kalshiOutcome = entry.kalshiOutcome;
      const matched = polyOutcome === kalshiOutcome;

      const acc = accuracyByCoin.get(entry.coin);
      if (acc) updateAccuracy(acc, polyOutcome, kalshiOutcome);
      comparedPairKeys.add(pairKey);
      lastComparisonByCoin.set(entry.coin, {
        polyOutcome,
        kalshiOutcome,
        matched,
        comparedAtMs: now,
        polySlug: entry.polySnap.slug,
        kalshiSlug:
          entry.kalshiSnap.slug ?? entry.kalshiSnap.marketTicker ?? "kalshi",
      });

      systemLogger.log(
        `FINAL outcome ${entry.coin.toUpperCase()} ${matched ? "matched" : "mismatched"} pair=${pairKey} poly=${polyOutcome} kalshi=${kalshiOutcome}`,
        matched ? "INFO" : "WARN",
      );
      if (!matched && verboseMismatch) {
        logOutcomeAnalytics(entry, pairKey, polyOutcome, kalshiOutcome, false, "WARN");
      } else if (matched && verboseAllFinals) {
        logOutcomeAnalytics(entry, pairKey, polyOutcome, kalshiOutcome, true, "INFO");
      }

      return true;
    };

    const shouldAttemptOfficialFetch = (
      attempts: number,
      lastMs: number | null | undefined,
    ): boolean => {
      if (OFFICIAL_RETRY_LIMIT > 0 && attempts >= OFFICIAL_RETRY_LIMIT) {
        return false;
      }
      if (!lastMs) return true;
      const delay = Math.min(
        OFFICIAL_RETRY_MAX_MS,
        OFFICIAL_RETRY_BASE_MS * 2 ** Math.max(0, attempts - 1),
      );
      return now - lastMs >= delay;
    };

    const isPlausibleUnderlying = (
      price: number,
      threshold: number | null,
    ): boolean => {
      if (threshold && threshold > 100 && price < 10) return false;
      return true;
    };

    const logDebug = (message: string, level: "INFO" | "WARN" | "ERROR" = "INFO") => {
      debugLogger.log(message, level);
    };

    const maybeFetchPolymarketOfficial = (entry: PairSnapshots): void => {
      if (entry.polyOfficialFetchPending) return;
      if (entry.polyOfficialOutcome) return;
      const attempts = entry.polyOfficialFetchAttempts ?? 0;
      if (!shouldAttemptOfficialFetch(attempts, entry.polyOfficialFetchLastMs)) return;

      entry.polyOfficialFetchPending = true;
      entry.polyOfficialFetchAttempts = attempts + 1;
      entry.polyOfficialFetchLastMs = now;

      void fetchPolymarketOfficialOutcome(entry.polySnap.slug)
        .then((result) => {
          entry.polyOfficialFetchPending = false;
          if (result.outcome) {
            entry.polyOfficialOutcome = result.outcome;
            entry.polyOfficialOutcomeSource = result.outcomeSource;
          }
          if (result.finalPrice != null) {
            entry.polyOfficialFinalPrice = result.finalPrice;
            entry.polyOfficialFinalPriceSource = result.finalPriceSource;
          }

          const threshold = resolveThreshold(entry.polySnap).value;
          if (entry.polyOfficialOutcome && entry.polyOutcome == null) {
            entry.polyOutcome = entry.polyOfficialOutcome;
            entry.polyCloseCapturedMs = now;
            logDebug(
              `OFFICIAL Polymarket outcome ${entry.coin.toUpperCase()} ${entry.polyOfficialOutcome} (${entry.polyOfficialOutcomeSource ?? "unknown"}) slug=${entry.polySnap.slug}`,
            );
          } else if (
            entry.polyOutcome == null &&
            entry.polyOfficialFinalPrice != null &&
            isPlausibleUnderlying(entry.polyOfficialFinalPrice, threshold)
          ) {
            entry.polyClosePrice = entry.polyOfficialFinalPrice;
            entry.polyClosePriceSource = `official:${entry.polyOfficialFinalPriceSource ?? "final_price"}`;
            entry.polyCloseThreshold = threshold;
            entry.polyCloseThresholdSource = resolveThreshold(entry.polySnap).source;
            entry.polyCloseCapturedMs = now;
            entry.polyOutcome = computeOutcomeFromValues(
              entry.polyOfficialFinalPrice,
              threshold,
            );
            logDebug(
              `OFFICIAL Polymarket final price ${entry.coin.toUpperCase()} ${formatMaybe(entry.polyOfficialFinalPrice, 2)} (${entry.polyOfficialFinalPriceSource ?? "unknown"}) slug=${entry.polySnap.slug}`,
            );
          }
        })
        .catch(() => {
          entry.polyOfficialFetchPending = false;
        });
    };

    const maybeFetchKalshiOfficial = (entry: PairSnapshots): void => {
      if (entry.kalshiOfficialFetchPending) return;
      if (entry.kalshiOfficialOutcome) return;
      const attempts = entry.kalshiOfficialFetchAttempts ?? 0;
      if (!shouldAttemptOfficialFetch(attempts, entry.kalshiOfficialFetchLastMs)) return;

      entry.kalshiOfficialFetchPending = true;
      entry.kalshiOfficialFetchAttempts = attempts + 1;
      entry.kalshiOfficialFetchLastMs = now;

      const ticker =
        entry.kalshiSnap.marketTicker ?? entry.kalshiSnap.slug ?? "";
      if (!ticker) {
        entry.kalshiOfficialFetchPending = false;
        return;
      }

      void fetchKalshiOfficialOutcome(kalshiOutcomeClient, ticker)
        .then((result) => {
          entry.kalshiOfficialFetchPending = false;
          if (result.outcome) {
            entry.kalshiOfficialOutcome = result.outcome;
            entry.kalshiOfficialOutcomeSource = result.outcomeSource;
          }
          if (result.finalPrice != null) {
            entry.kalshiOfficialFinalPrice = result.finalPrice;
            entry.kalshiOfficialFinalPriceSource = result.finalPriceSource;
          }

          const threshold = resolveThreshold(entry.kalshiSnap).value;
          if (entry.kalshiOfficialOutcome && entry.kalshiOutcome == null) {
            entry.kalshiOutcome = entry.kalshiOfficialOutcome;
            entry.kalshiCloseCapturedMs = now;
            logDebug(
              `OFFICIAL Kalshi outcome ${entry.coin.toUpperCase()} ${entry.kalshiOfficialOutcome} (${entry.kalshiOfficialOutcomeSource ?? "unknown"}) ticker=${ticker}`,
            );
          } else if (
            entry.kalshiOutcome == null &&
            entry.kalshiOfficialFinalPrice != null &&
            isPlausibleUnderlying(entry.kalshiOfficialFinalPrice, threshold)
          ) {
            entry.kalshiClosePrice = entry.kalshiOfficialFinalPrice;
            entry.kalshiClosePriceSource = `official:${entry.kalshiOfficialFinalPriceSource ?? "final_price"}`;
            entry.kalshiCloseThreshold = threshold;
            entry.kalshiCloseThresholdSource = resolveThreshold(entry.kalshiSnap).source;
            entry.kalshiCloseCapturedMs = now;
            entry.kalshiOutcome = computeOutcomeFromValues(
              entry.kalshiOfficialFinalPrice,
              threshold,
            );
            logDebug(
              `OFFICIAL Kalshi final price ${entry.coin.toUpperCase()} ${formatMaybe(entry.kalshiOfficialFinalPrice, 2)} (${entry.kalshiOfficialFinalPriceSource ?? "unknown"}) ticker=${ticker}`,
            );
          }
        })
        .catch(() => {
          entry.kalshiOfficialFetchPending = false;
        });
    };

    for (const coin of coins) {
      const polySnap = polySnapshots.get(coin);
      const kalshiSnap = kalshiSnapshots.get(coin);

      if (polySnap) {
        const upTokenId = polySnap.upTokenId;
        const bid = upTokenId ? polySnap.bestBid.get(upTokenId) ?? null : null;
        const ask = upTokenId ? polySnap.bestAsk.get(upTokenId) ?? null : null;
        const mid = computeOddsMid(bid ?? null, ask ?? null);
        if (mid != null) {
          const arr = polyOddsHistoryByCoin.get(coin)!;
          arr.push(mid);
          if (arr.length > ODDS_HISTORY_LIMIT) arr.shift();
        }
      }

      if (kalshiSnap) {
        const arr = kalshiOddsHistoryByCoin.get(coin)!;
        if (
          arr.length === 0 &&
          kalshiSnap.kalshiMarketPriceHistory &&
          kalshiSnap.kalshiMarketPriceHistory.length > 0
        ) {
          arr.push(
            ...kalshiSnap.kalshiMarketPriceHistory.slice(-ODDS_HISTORY_LIMIT),
          );
        }
        const odds =
          kalshiSnap.kalshiMarketPrice ??
          kalshiSnap.kalshiLastPrice ??
          null;
        if (odds != null) {
          const last = arr[arr.length - 1];
          if (last !== odds) {
            arr.push(odds);
            if (arr.length > ODDS_HISTORY_LIMIT) arr.shift();
          }
        }
      }

      const staleReasons: string[] = [];
      if (!polySnap) {
        staleReasons.push("poly_missing");
      }
      if (!kalshiSnap) {
        staleReasons.push("kalshi_missing");
      }
      if (polySnap) {
        if (polySnap.dataStatus !== "healthy") {
          staleReasons.push(`poly_status:${polySnap.dataStatus}`);
        }
        const priceAge = computeAgeMs(polySnap.cryptoPriceTimestamp, now);
        if (priceAge === null) {
          staleReasons.push("poly_price_missing");
        } else if (priceAge > PRICE_STALE_MS) {
          staleReasons.push(`poly_price_stale:${Math.round(priceAge / 1000)}s`);
        }
        const bookAge = computeAgeMs(polySnap.lastBookUpdateMs, now);
        if (bookAge !== null && bookAge > BOOK_STALE_MS) {
          staleReasons.push(`poly_book_stale:${Math.round(bookAge / 1000)}s`);
        }
      }
      if (kalshiSnap) {
        if (kalshiSnap.dataStatus !== "healthy") {
          staleReasons.push(`kalshi_status:${kalshiSnap.dataStatus}`);
        }
        const priceAge = computeAgeMs(kalshiSnap.cryptoPriceTimestamp, now);
        if (priceAge === null) {
          staleReasons.push("kalshi_price_missing");
        } else if (priceAge > PRICE_STALE_MS) {
          staleReasons.push(
            `kalshi_price_stale:${Math.round(priceAge / 1000)}s`,
          );
        }
        const bookAge = computeAgeMs(kalshiSnap.lastBookUpdateMs, now);
        if (bookAge !== null && bookAge > BOOK_STALE_MS) {
          staleReasons.push(`kalshi_book_stale:${Math.round(bookAge / 1000)}s`);
        }
      }

      if (polySnap && kalshiSnap) {
        const polySlotStart = resolveSlotStartMs(polySnap, now, SLOT_DURATION_MS);
        const kalshiSlotStart = resolveSlotStartMs(
          kalshiSnap,
          now,
          SLOT_DURATION_MS,
        );
        if (polySlotStart !== null && kalshiSlotStart !== null) {
          const slotDelta = Math.abs(polySlotStart - kalshiSlotStart);
          if (slotDelta > SLOT_TOLERANCE_MS) {
            staleReasons.push(
              `slot_mismatch:${Math.round(slotDelta / 1000)}s`,
            );
          }
        } else {
          if (polySlotStart === null) staleReasons.push("poly_slot_missing");
          if (kalshiSlotStart === null) staleReasons.push("kalshi_slot_missing");
        }
      }

      if (staleReasons.length > 0) {
        const lastLog = lastStaleLogByCoin.get(coin) ?? 0;
        if (now - lastLog >= STALE_LOG_INTERVAL_MS) {
          systemLogger.log(
            `STALE_DATA ${coin.toUpperCase()} waiting for fresh markets (${staleReasons.join(
              ", ",
            )})`,
            "WARN",
          );
          lastStaleLogByCoin.set(coin, now);
        }
        staleStateByCoin.set(coin, true);
        continue;
      }

      if (staleStateByCoin.get(coin)) {
        systemLogger.log(
          `STALE_DATA_RESOLVED ${coin.toUpperCase()} markets back in sync`,
          "INFO",
        );
        staleStateByCoin.set(coin, false);
      }

      if (polySnap && kalshiSnap) {
        const pairKey = `${polySnap.slug}|${kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi"}`;
        const existing = pairSnapshotsByKey.get(pairKey);
        if (existing) {
          existing.polySnap = polySnap;
          existing.kalshiSnap = kalshiSnap;
          existing.lastSeenMs = now;
        } else {
          pairSnapshotsByKey.set(pairKey, {
            coin,
            polySnap,
            kalshiSnap,
            lastSeenMs: now,
          });
        }

        const lastPairKey = activePairKeyByCoin.get(coin);
        if (lastPairKey && lastPairKey !== pairKey) {
          pendingPairKeys.add(lastPairKey);
        }
        activePairKeyByCoin.set(coin, pairKey);

        const currentEntry = pairSnapshotsByKey.get(pairKey);
        if (currentEntry) {
          if (tryFinalizePair(pairKey, currentEntry)) {
            pairSnapshotsByKey.delete(pairKey);
          }
        }
      }
    }

    for (const pendingKey of pendingPairKeys) {
      const entry = pairSnapshotsByKey.get(pendingKey);
      if (!entry) {
        pendingPairKeys.delete(pendingKey);
        continue;
      }
      if (tryFinalizePair(pendingKey, entry)) {
        pendingPairKeys.delete(pendingKey);
        pairSnapshotsByKey.delete(pendingKey);
      }
    }

    if (summaryOnly && now - lastSummaryLogMs >= SUMMARY_LOG_INTERVAL_MS) {
      for (const coin of coins) {
        const acc = accuracyByCoin.get(coin);
        const total = acc?.totalCount ?? 0;
        const matches = acc?.matchCount ?? 0;
        const rate = total > 0 ? ((100 * matches) / total).toFixed(1) : "n/a";
        const last = lastComparisonByCoin.get(coin);
        const lastLabel = last
          ? `${last.polyOutcome}/${last.kalshiOutcome} (${last.matched ? "match" : "mismatch"})`
          : "pending";
        systemLogger.log(
          `SUMMARY ${coin.toUpperCase()} matches=${matches}/${total} rate=${rate}% last=${lastLabel}`,
        );
      }
      lastSummaryLogMs = now;
    }

      if (dashboard) {
        const activeCoin =
          coins[activeCoinIndex] ?? coins[0] ?? ("eth" as CoinSymbol);
        dashboard.update({
          polySnapshots,
          kalshiSnapshots,
          coins,
          activeCoin,
          activeCoinIndex,
          coinCount: coins.length,
          accuracyByCoin,
          polyOddsHistoryByCoin,
          kalshiOddsHistoryByCoin,
          lastComparisonByCoin,
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      systemLogger.log(`ANALYSIS_LOOP_ERROR ${message}`, "ERROR");
    }
  }, RENDER_INTERVAL_MS);

  process.on("SIGINT", () => {
    clearInterval(timer);
    cleanupNavigation();
    polyHub.stop();
    kalshiHub.stop();
    process.exit(0);
  });
}
