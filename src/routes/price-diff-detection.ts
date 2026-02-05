/**
 * Price diff detection: compare Polymarket vs Kalshi opposing outcomes
 * and flag large price gaps in real-time.
 */

import { existsSync, mkdirSync } from "fs";
import { join } from "path";
import type { CoinSymbol } from "../services/auto-market";
import { MarketDataHub, type MarketSnapshot } from "../services/market-data-hub";
import { KalshiMarketDataHub } from "../services/kalshi-market-data-hub";
import { RunLogger } from "../services/run-logger";
import { loadProviderConfig, normalizeCoinKey } from "../services/profile-config";
import { getKalshiEnvConfig } from "../clients/kalshi/kalshi-config";
import { computeOddsMid } from "../services/cross-platform-compare";
import { PriceDiffDashboard } from "../cli/price-diff-dashboard";
import { promptConfirm, promptText, selectMany } from "../cli/prompts";
import { setupCoinNavigation } from "../cli/coin-navigation";
import {
  computeFillEstimate,
  randomDelayMs,
  resolveAsks,
  type FillEstimate,
} from "../services/arbitrage-fill";

const ODDS_HISTORY_LIMIT = 180;
const RENDER_INTERVAL_MS = 300;
const FLAG_LOG_INTERVAL_MS = parseEnvNumber(
  "PRICE_DIFF_LOG_INTERVAL_MS",
  10_000,
  1_000,
);
const SUMMARY_LOG_INTERVAL_MS = parseEnvNumber(
  "PRICE_DIFF_SUMMARY_LOG_MS",
  300_000,
  60_000,
);
const BOOK_STALE_MS = parseEnvNumber("PRICE_DIFF_BOOK_STALE_MS", 30_000, 5_000);
const STALE_LOG_INTERVAL_MS = parseEnvNumber(
  "PRICE_DIFF_STALE_LOG_MS",
  30_000,
  5_000,
);
const RECENT_LOG_LIMIT = 5;
const EXEC_DELAY_MIN_ENV = parseEnvNumber("EXECUTION_DELAY_MIN_MS", 250, 0);
const EXEC_DELAY_MAX_ENV = parseEnvNumber("EXECUTION_DELAY_MAX_MS", 300, 0);
const { min: EXEC_DELAY_MIN_MS, max: EXEC_DELAY_MAX_MS } = resolveDelayRange(
  EXEC_DELAY_MIN_ENV,
  EXEC_DELAY_MAX_ENV,
);

function parseEnvNumber(
  name: string,
  defaultValue: number,
  minValue: number,
): number {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const cleaned = raw.replace(/[$,]/g, "").trim();
  const parsed = Number(cleaned);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.max(minValue, parsed);
}

function resolveDelayRange(
  minMs: number,
  maxMs: number,
): { min: number; max: number } {
  const min = Math.max(0, Math.floor(minMs));
  const max = Math.max(min, Math.floor(maxMs));
  return { min, max };
}

function parseEnvOptionalNumber(name: string): number | null {
  const raw = process.env[name];
  if (!raw) return null;
  const cleaned = raw.replace(/[$,]/g, "").trim();
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseInputNumber(value: string | null | undefined): number | null {
  if (!value) return null;
  const cleaned = value.replace(/[$,]/g, "").trim();
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatMaybe(value: number | null | undefined, decimals: number): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "n/a";
  return value.toFixed(decimals);
}

function computeAgeMs(value: number | null | undefined, now: number): number | null {
  if (!value || !Number.isFinite(value) || value <= 0) return null;
  return Math.max(0, now - value);
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

export interface PriceDiffDetectionRouteOptions {
  coins?: CoinSymbol[];
  headless?: boolean;
  headlessSummary?: boolean;
  realisticFill?: boolean;
  fillUsd?: number;
}

interface FillConfirm extends FillEstimate {
  confirmedAtMs: number;
  delayMs: number;
  gapDelta: number;
}

interface PriceDiffState {
  polyUpAsk: number | null;
  polyDownAsk: number | null;
  kalshiYesAsk: number | null;
  kalshiNoAsk: number | null;
  costPolyUpKalshiNo: number | null;
  costPolyDownKalshiYes: number | null;
  gapPolyUpKalshiNo: number | null;
  gapPolyDownKalshiYes: number | null;
  abovePolyUpKalshiNo: boolean;
  abovePolyDownKalshiYes: boolean;
  realisticUpNo?: FillEstimate | null;
  realisticDownYes?: FillEstimate | null;
  confirmUpNo?: FillConfirm | null;
  confirmDownYes?: FillConfirm | null;
  updatedAtMs: number;
}

interface DiffLogState {
  lastAbove: boolean;
  lastLogMs: number;
}

interface MarketDiffCounts {
  totalMarkets: number;
  matchedMarkets: number;
  upNoMarkets: number;
  downYesMarkets: number;
}

interface PendingConfirm {
  marketKey: string;
  candidate: FillEstimate;
  originalGap: number;
  delayMs: number;
  timer: NodeJS.Timeout;
  committedAtMs: number;
}

export async function priceDiffDetectionRoute(
  options: PriceDiffDetectionRouteOptions = {},
): Promise<void> {
  const polyConfig = loadProviderConfig("polymarket");
  const kalshiConfigResult = loadProviderConfig("kalshi");
  const summaryOnly = options.headlessSummary === true;
  const headless = options.headless || summaryOnly;
  const availableCoins = polyConfig.coinOptions.filter((c) =>
    kalshiConfigResult.coinOptions.includes(c),
  );
  let coins: CoinSymbol[] = [];
  const envFillUsd = parseEnvOptionalNumber("PRICE_DIFF_FILL_USD");
  let realisticFill = options.realisticFill;
  let fillUsd = options.fillUsd;

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

  if (realisticFill === undefined) {
    if (process.stdin.isTTY && !headless) {
      const response = await promptConfirm(
        "Enable realistic fill simulation?",
        false,
      );
      if (response === null) {
        console.log("Prompt cancelled. Exiting.");
        return;
      }
      realisticFill = response;
    } else {
      realisticFill = false;
    }
  }

  if (realisticFill) {
    if (!fillUsd || !Number.isFinite(fillUsd) || fillUsd <= 0) {
      if (process.stdin.isTTY && !headless) {
        const initial =
          envFillUsd && envFillUsd > 0 ? String(envFillUsd) : "100";
        const response = await promptText("Fill budget (USD)", {
          initial,
          validate: (value) =>
            parseInputNumber(value) && parseInputNumber(value)! > 0
              ? true
              : "Enter a positive USD amount",
        });
        if (response === null) {
          console.log("Prompt cancelled. Exiting.");
          return;
        }
        const parsed = parseInputNumber(response);
        if (parsed && parsed > 0) {
          fillUsd = parsed;
        }
      } else if (envFillUsd && envFillUsd > 0) {
        fillUsd = envFillUsd;
      }
    }
  }

  let kalshiEnvConfig;
  try {
    kalshiEnvConfig = getKalshiEnvConfig(process.env);
  } catch {
    console.log(
      "Kalshi env not configured. Set KALSHI_API_KEY, KALSHI_PRIVATE_KEY_PATH, and optionally KALSHI_ENV.",
    );
    return;
  }

  const kalshiSelectors = kalshiConfigResult.kalshiSelectorsByCoin;
  if (!kalshiSelectors) {
    console.log("Kalshi market selectors missing in config.");
    return;
  }

  const logsRoot = join(process.cwd(), "logs", "price-diff");
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

  const recentLogsByCoin = new Map<CoinSymbol, string[]>();
  const pushRecentLog = (message: string, coin?: CoinSymbol): void => {
    const time = new Date().toLocaleTimeString();
    const entry = `[${time}] ${message}`;
    if (coin) {
      const logs = recentLogsByCoin.get(coin) ?? [];
      logs.push(entry);
      if (logs.length > RECENT_LOG_LIMIT) {
        logs.splice(0, logs.length - RECENT_LOG_LIMIT);
      }
      recentLogsByCoin.set(coin, logs);
      return;
    }
    for (const target of coins) {
      const logs = recentLogsByCoin.get(target) ?? [];
      logs.push(entry);
      if (logs.length > RECENT_LOG_LIMIT) {
        logs.splice(0, logs.length - RECENT_LOG_LIMIT);
      }
      recentLogsByCoin.set(target, logs);
    }
  };

  const threshold = parseEnvNumber("TEST_PRICE_DIFF_REQ", 0.07, 0);
  const polyOddsHistoryByCoin = new Map<CoinSymbol, number[]>();
  const kalshiOddsHistoryByCoin = new Map<CoinSymbol, number[]>();
  const diffByCoin = new Map<CoinSymbol, PriceDiffState>();
  const diffLogStateByCoin = new Map<
    CoinSymbol,
    { upNo: DiffLogState; downYes: DiffLogState }
  >();
  const confirmStateByCoin = new Map<
    CoinSymbol,
    {
      upNo?: FillConfirm | null;
      downYes?: FillConfirm | null;
      lastConfirmUpNoMs: number;
      lastConfirmDownYesMs: number;
    }
  >();
  const pendingConfirmByCoin = new Map<
    CoinSymbol,
    { upNo?: PendingConfirm; downYes?: PendingConfirm }
  >();
  const lastStaleLogByCoin = new Map<CoinSymbol, number>();
  const staleStateByCoin = new Map<CoinSymbol, boolean>();
  const diffCountByCoin = new Map<CoinSymbol, MarketDiffCounts>();
  const marketSeenByCoin = new Map<CoinSymbol, Set<string>>();
  const marketFlaggedByCoin = new Map<
    CoinSymbol,
    { upNo: Set<string>; downYes: Set<string> }
  >();
  const marketMatchedByCoin = new Map<CoinSymbol, Set<string>>();
  let lastSummaryLogMs = 0;

  for (const coin of coins) {
    recentLogsByCoin.set(coin, []);
    polyOddsHistoryByCoin.set(coin, []);
    kalshiOddsHistoryByCoin.set(coin, []);
    diffLogStateByCoin.set(coin, {
      upNo: { lastAbove: false, lastLogMs: 0 },
      downYes: { lastAbove: false, lastLogMs: 0 },
    });
    diffCountByCoin.set(coin, {
      totalMarkets: 0,
      matchedMarkets: 0,
      upNoMarkets: 0,
      downYesMarkets: 0,
    });
    marketSeenByCoin.set(coin, new Set());
    marketFlaggedByCoin.set(coin, { upNo: new Set(), downYes: new Set() });
    marketMatchedByCoin.set(coin, new Set());
    confirmStateByCoin.set(coin, {
      upNo: null,
      downYes: null,
      lastConfirmUpNoMs: 0,
      lastConfirmDownYesMs: 0,
    });
    pendingConfirmByCoin.set(coin, {});
  }

  systemLogger.log(
    `Price diff detection run ${runId} started for coins: ${coins.join(", ")} (threshold=${threshold.toFixed(4)})`,
  );
  pushRecentLog(
    `Run started for coins: ${coins.join(", ")} (threshold=${threshold.toFixed(4)})`,
  );
  if (summaryOnly) {
    systemLogger.log(
      "Headless summary mode enabled (system.log concise, debug.log verbose).",
    );
    pushRecentLog("Headless summary mode enabled.");
  }

  if (realisticFill) {
    if (!fillUsd || !Number.isFinite(fillUsd) || fillUsd <= 0) {
      systemLogger.log(
        "REALISTIC_FILL disabled: no valid USD budget provided (set PRICE_DIFF_FILL_USD or --fill-usd)",
        "WARN",
      );
      pushRecentLog("REALISTIC_FILL disabled (missing budget)");
      realisticFill = false;
    } else {
      systemLogger.log(
        `REALISTIC_FILL enabled (budget=$${fillUsd.toFixed(2)})`,
      );
      pushRecentLog(`REALISTIC_FILL enabled ($${fillUsd.toFixed(2)})`);
    }
  }

  const polyHub = new MarketDataHub(debugLogger, {
    requireCryptoPrice: false,
  });
  const kalshiHub = new KalshiMarketDataHub(
    debugLogger,
    kalshiEnvConfig,
    kalshiSelectors,
    {
      requireCryptoPrice: false,
    },
  );

  await polyHub.start(coins);
  await kalshiHub.start(coins);

  const dashboard = headless ? null : new PriceDiffDashboard();
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

  const logFlag = (
    coin: CoinSymbol,
    label: string,
    gap: number,
    cost: number,
    polyAsk: number | null,
    kalshiAsk: number | null,
    polySlug: string,
    kalshiSlug: string,
  ): void => {
    const cheaper =
      polyAsk != null && kalshiAsk != null
        ? polyAsk < kalshiAsk
          ? "poly"
          : polyAsk > kalshiAsk
            ? "kalshi"
            : "equal"
        : "n/a";
    systemLogger.log(
      `PRICE_DIFF ${coin.toUpperCase()} ${label} gap=${gap.toFixed(4)} cost=${cost.toFixed(4)} threshold=${threshold.toFixed(4)} polyAsk=${formatMaybe(
        polyAsk,
        4,
      )} kalshiAsk=${formatMaybe(kalshiAsk, 4)} cheaper=${cheaper} poly=${polySlug} kalshi=${kalshiSlug}`,
    );
    pushRecentLog(
      `PRICE_DIFF ${coin.toUpperCase()} ${label} gap=${gap.toFixed(4)} cost=${cost.toFixed(4)} threshold=${threshold.toFixed(4)} polyAsk=${formatMaybe(
        polyAsk,
        4,
      )} kalshiAsk=${formatMaybe(kalshiAsk, 4)} cheaper=${cheaper}`,
      coin,
    );
  };

  const logRealisticCandidate = (
    coin: CoinSymbol,
    label: string,
    estimate: FillEstimate,
    polySlug: string,
    kalshiSlug: string,
  ): void => {
    const message = `REALISTIC_FILL_CANDIDATE ${coin.toUpperCase()} ${label} shares=${estimate.shares} avgPoly=${estimate.avgPoly.toFixed(
      4,
    )} avgKalshi=${estimate.avgKalshi.toFixed(4)} gap=${estimate.gap.toFixed(
      4,
    )} cost=${estimate.totalCost.toFixed(2)} budget=${(fillUsd ?? 0).toFixed(
      2,
    )} poly=${polySlug} kalshi=${kalshiSlug}`;
    if (summaryOnly) {
      debugLogger.log(message);
    } else {
      systemLogger.log(message);
    }
    pushRecentLog(
      `REALISTIC_FILL_CANDIDATE ${coin.toUpperCase()} ${label} shares=${estimate.shares} gap=${estimate.gap.toFixed(
        4,
      )} cost=${estimate.totalCost.toFixed(2)}`,
      coin,
    );
  };

  const logRealisticConfirm = (
    coin: CoinSymbol,
    label: string,
    confirm: FillConfirm,
    polySlug: string,
    kalshiSlug: string,
  ): void => {
    const message = `REALISTIC_FILL_CONFIRM ${coin.toUpperCase()} ${label} shares=${confirm.shares} avgPoly=${confirm.avgPoly.toFixed(
      4,
    )} avgKalshi=${confirm.avgKalshi.toFixed(4)} gap=${confirm.gap.toFixed(
      4,
    )} gapDelta=${confirm.gapDelta.toFixed(4)} cost=${confirm.totalCost.toFixed(
      2,
    )} delayMs=${confirm.delayMs} poly=${polySlug} kalshi=${kalshiSlug}`;
    systemLogger.log(message);
    if (summaryOnly) {
      debugLogger.log(message);
    }
    pushRecentLog(
      `REALISTIC_FILL_CONFIRM ${coin.toUpperCase()} ${label} gap=${confirm.gap.toFixed(
        4,
      )} Δ=${confirm.gapDelta.toFixed(4)} cost=${confirm.totalCost.toFixed(2)}`,
      coin,
    );
  };

  const logRealisticExecuted = (
    coin: CoinSymbol,
    label: string,
    confirm: FillConfirm,
    originalGap: number,
    slippage: number,
    fillSource: string,
    polySlug: string,
    kalshiSlug: string,
  ): void => {
    const slippageLabel =
      slippage >= 0 ? `+${slippage.toFixed(4)}` : slippage.toFixed(4);
    const message = `REALISTIC_FILL_EXECUTED ${coin.toUpperCase()} ${label} shares=${confirm.shares} avgPoly=${confirm.avgPoly.toFixed(
      4,
    )} avgKalshi=${confirm.avgKalshi.toFixed(4)} gap=${confirm.gap.toFixed(
      4,
    )} origGap=${originalGap.toFixed(4)} slippage=${slippageLabel} cost=${confirm.totalCost.toFixed(
      2,
    )} delayMs=${confirm.delayMs} fill=${fillSource} poly=${polySlug} kalshi=${kalshiSlug}`;
    systemLogger.log(message);
    if (summaryOnly) {
      debugLogger.log(message);
    }
    pushRecentLog(
      `EXECUTED ${coin.toUpperCase()} ${label} gap=${confirm.gap.toFixed(
        4,
      )} slip=${slippageLabel} cost=${confirm.totalCost.toFixed(2)}`,
      coin,
    );
  };

  const computeRealisticEstimate = (
    polySnap: MarketSnapshot | undefined,
    kalshiSnap: MarketSnapshot | undefined,
    polyTokenId: string | null | undefined,
    kalshiTokenId: string,
  ): FillEstimate | null => {
    if (!realisticFill || !fillUsd || fillUsd <= 0) return null;
    const polyAsks = resolveAsks(polySnap, polyTokenId);
    const kalshiAsks = resolveAsks(kalshiSnap, kalshiTokenId);
    if (polyAsks.length === 0 || kalshiAsks.length === 0) return null;
    return computeFillEstimate(polyAsks, kalshiAsks, fillUsd);
  };

  const scheduleRealisticConfirm = (
    coin: CoinSymbol,
    direction: "upNo" | "downYes",
    candidate: FillEstimate,
    marketKey: string,
  ): void => {
    const pending = pendingConfirmByCoin.get(coin);
    if (!pending) return;
    const delayMs = randomDelayMs(EXEC_DELAY_MIN_MS, EXEC_DELAY_MAX_MS);
    const committedAtMs = Date.now();
    const originalGap = candidate.gap;

    const timer = setTimeout(() => {
      const polySnap = polyHub.getSnapshots().get(coin);
      const kalshiSnap = kalshiHub.getSnapshots().get(coin);
      const currentKey =
        polySnap && kalshiSnap
          ? `${polySnap.slug}|${kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi"}`
          : null;

      // Execution delay model: We are already committed. No abort.
      // If market changed or no data, use original candidate as fallback.
      let actualFill: FillEstimate;
      let fillSource: "post-delay" | "original" | "market-changed";

      if (!polySnap || !kalshiSnap || !currentKey || currentKey !== marketKey) {
        actualFill = candidate;
        fillSource = "market-changed";
      } else {
        const polyTokenId =
          direction === "upNo" ? polySnap.upTokenId : polySnap.downTokenId;
        const kalshiTokenId = direction === "upNo" ? "NO" : "YES";
        const confirmEstimate = computeRealisticEstimate(
          polySnap,
          kalshiSnap,
          polyTokenId,
          kalshiTokenId,
        );
        if (confirmEstimate) {
          actualFill = confirmEstimate;
          fillSource = "post-delay";
        } else {
          actualFill = candidate;
          fillSource = "original";
        }
      }

      const slippage = actualFill.gap - originalGap;
      const confirm: FillConfirm = {
        ...actualFill,
        confirmedAtMs: Date.now(),
        delayMs,
        gapDelta: slippage,
      };

      const confirmState = confirmStateByCoin.get(coin);
      if (confirmState) {
        if (direction === "upNo") {
          confirmState.upNo = confirm;
          confirmState.lastConfirmUpNoMs = Date.now();
        } else {
          confirmState.downYes = confirm;
          confirmState.lastConfirmDownYesMs = Date.now();
        }
      }

      // Track all executed trades (regardless of gap) and count matches
      const counts = diffCountByCoin.get(coin);
      const marketFlags = marketFlaggedByCoin.get(coin);
      const matchedSet = marketMatchedByCoin.get(coin);
      if (counts && marketFlags && matchedSet) {
        // Count if original gap was above threshold (since that's what triggered the trade)
        if (originalGap >= threshold) {
          if (direction === "upNo" && !marketFlags.upNo.has(marketKey)) {
            marketFlags.upNo.add(marketKey);
            counts.upNoMarkets += 1;
          }
          if (direction === "downYes" && !marketFlags.downYes.has(marketKey)) {
            marketFlags.downYes.add(marketKey);
            counts.downYesMarkets += 1;
          }
          if (!matchedSet.has(marketKey)) {
            matchedSet.add(marketKey);
            counts.matchedMarkets += 1;
          }
        }
      }

      const label =
        direction === "upNo" ? "PolyUp_vs_KalshiNo" : "PolyDown_vs_KalshiYes";
      const slug1 = polySnap?.slug ?? "poly";
      const slug2 =
        kalshiSnap?.slug ?? kalshiSnap?.marketTicker ?? "kalshi";

      // Always log executed trade with slippage info
      logRealisticExecuted(
        coin,
        label,
        confirm,
        originalGap,
        slippage,
        fillSource,
        slug1,
        slug2,
      );

      pending[direction] = undefined;
    }, delayMs);

    pending[direction] = {
      marketKey,
      candidate,
      originalGap,
      delayMs,
      timer,
      committedAtMs,
    };
  };

  const timer = setInterval(() => {
    const now = Date.now();
    try {
      const polySnapshots = polyHub.getSnapshots();
      const kalshiSnapshots = kalshiHub.getSnapshots();

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
        if (!polySnap) staleReasons.push("poly_missing");
        if (!kalshiSnap) staleReasons.push("kalshi_missing");
        if (polySnap && polySnap.dataStatus !== "healthy") {
          staleReasons.push(`poly_status:${polySnap.dataStatus}`);
        }
        if (kalshiSnap && kalshiSnap.dataStatus !== "healthy") {
          staleReasons.push(`kalshi_status:${kalshiSnap.dataStatus}`);
        }
        if (polySnap) {
          const bookAge = computeAgeMs(polySnap.lastBookUpdateMs, now);
          if (bookAge !== null && bookAge > BOOK_STALE_MS) {
            staleReasons.push(`poly_book_stale:${Math.round(bookAge / 1000)}s`);
          }
        }
        if (kalshiSnap) {
          const bookAge = computeAgeMs(kalshiSnap.lastBookUpdateMs, now);
          if (bookAge !== null && bookAge > BOOK_STALE_MS) {
            staleReasons.push(`kalshi_book_stale:${Math.round(bookAge / 1000)}s`);
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
            pushRecentLog(
              `STALE_DATA ${coin.toUpperCase()} waiting (${staleReasons.join(", ")})`,
              coin,
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
          pushRecentLog(
            `STALE_DATA_RESOLVED ${coin.toUpperCase()} markets back in sync`,
            coin,
          );
          staleStateByCoin.set(coin, false);
        }

        if (!polySnap || !kalshiSnap) {
          continue;
        }

        const marketKey = `${polySnap.slug}|${kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi"}`;
        const seenKeys = marketSeenByCoin.get(coin);
        const counts = diffCountByCoin.get(coin);
        if (seenKeys && counts && !seenKeys.has(marketKey)) {
          seenKeys.add(marketKey);
          counts.totalMarkets += 1;
          pushRecentLog(
            `MARKET_NEW ${coin.toUpperCase()} total=${counts.totalMarkets} key=${marketKey}`,
            coin,
          );
        }

        const polyUpAsk =
          polySnap.upTokenId !== null && polySnap.upTokenId !== undefined
            ? polySnap.bestAsk.get(polySnap.upTokenId) ?? null
            : null;
        const polyDownAsk =
          polySnap.downTokenId !== null && polySnap.downTokenId !== undefined
            ? polySnap.bestAsk.get(polySnap.downTokenId) ?? null
            : null;
        const kalshiYesAsk = kalshiSnap.bestAsk.get("YES") ?? null;
        const kalshiNoAsk = kalshiSnap.bestAsk.get("NO") ?? null;

        const costPolyUpKalshiNo =
          polyUpAsk != null && kalshiNoAsk != null
            ? polyUpAsk + kalshiNoAsk
            : null;
        const costPolyDownKalshiYes =
          polyDownAsk != null && kalshiYesAsk != null
            ? polyDownAsk + kalshiYesAsk
            : null;
        const gapPolyUpKalshiNo =
          costPolyUpKalshiNo != null ? 1 - costPolyUpKalshiNo : null;
        const gapPolyDownKalshiYes =
          costPolyDownKalshiYes != null ? 1 - costPolyDownKalshiYes : null;

        const abovePolyUpKalshiNo =
          gapPolyUpKalshiNo != null && gapPolyUpKalshiNo >= threshold;
        const abovePolyDownKalshiYes =
          gapPolyDownKalshiYes != null && gapPolyDownKalshiYes >= threshold;

        const realisticUpNo = computeRealisticEstimate(
          polySnap,
          kalshiSnap,
          polySnap.upTokenId,
          "NO",
        );
        const realisticDownYes = computeRealisticEstimate(
          polySnap,
          kalshiSnap,
          polySnap.downTokenId,
          "YES",
        );
        const confirmState = confirmStateByCoin.get(coin);
        const confirmUpNo = confirmState?.upNo ?? null;
        const confirmDownYes = confirmState?.downYes ?? null;

        diffByCoin.set(coin, {
          polyUpAsk,
          polyDownAsk,
          kalshiYesAsk,
          kalshiNoAsk,
          costPolyUpKalshiNo,
          costPolyDownKalshiYes,
          gapPolyUpKalshiNo,
          gapPolyDownKalshiYes,
          abovePolyUpKalshiNo,
          abovePolyDownKalshiYes,
          realisticUpNo,
          realisticDownYes,
          confirmUpNo,
          confirmDownYes,
          updatedAtMs: now,
        });

        const logState = diffLogStateByCoin.get(coin)!;
        const marketFlags = marketFlaggedByCoin.get(coin)!;
        const matchedSet = marketMatchedByCoin.get(coin)!;
        const polySlug = polySnap.slug;
        const kalshiSlug =
          kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi";

        if (!realisticFill) {
          if (counts && abovePolyUpKalshiNo && !marketFlags.upNo.has(marketKey)) {
            marketFlags.upNo.add(marketKey);
            counts.upNoMarkets += 1;
            if (!matchedSet.has(marketKey)) {
              matchedSet.add(marketKey);
              counts.matchedMarkets += 1;
            }
          }
          if (
            counts &&
            abovePolyDownKalshiYes &&
            !marketFlags.downYes.has(marketKey)
          ) {
            marketFlags.downYes.add(marketKey);
            counts.downYesMarkets += 1;
            if (!matchedSet.has(marketKey)) {
              matchedSet.add(marketKey);
              counts.matchedMarkets += 1;
            }
          }

          if (gapPolyUpKalshiNo != null && costPolyUpKalshiNo != null) {
            const shouldLog =
              abovePolyUpKalshiNo &&
              (!logState.upNo.lastAbove ||
                now - logState.upNo.lastLogMs >= FLAG_LOG_INTERVAL_MS);
            if (shouldLog) {
              logFlag(
                coin,
                "PolyUp_vs_KalshiNo",
                gapPolyUpKalshiNo,
                costPolyUpKalshiNo,
                polyUpAsk,
                kalshiNoAsk,
                polySlug,
                kalshiSlug,
              );
              logState.upNo.lastLogMs = now;
              logState.upNo.lastAbove = true;
            } else if (!abovePolyUpKalshiNo) {
              logState.upNo.lastAbove = false;
            }
          } else {
            logState.upNo.lastAbove = false;
          }

          if (gapPolyDownKalshiYes != null && costPolyDownKalshiYes != null) {
            const shouldLog =
              abovePolyDownKalshiYes &&
              (!logState.downYes.lastAbove ||
                now - logState.downYes.lastLogMs >= FLAG_LOG_INTERVAL_MS);
            if (shouldLog) {
              logFlag(
                coin,
                "PolyDown_vs_KalshiYes",
                gapPolyDownKalshiYes,
                costPolyDownKalshiYes,
                polyDownAsk,
                kalshiYesAsk,
                polySlug,
                kalshiSlug,
              );
              logState.downYes.lastLogMs = now;
              logState.downYes.lastAbove = true;
            } else if (!abovePolyDownKalshiYes) {
              logState.downYes.lastAbove = false;
            }
          } else {
            logState.downYes.lastAbove = false;
          }
        } else {
          const pending = pendingConfirmByCoin.get(coin)!;
          const confirmState = confirmStateByCoin.get(coin)!;

          if (
            realisticUpNo &&
            realisticUpNo.gap >= threshold &&
            !pending.upNo &&
            now - confirmState.lastConfirmUpNoMs >= FLAG_LOG_INTERVAL_MS
          ) {
            logRealisticCandidate(
              coin,
              "PolyUp_vs_KalshiNo",
              realisticUpNo,
              polySlug,
              kalshiSlug,
            );
            scheduleRealisticConfirm(coin, "upNo", realisticUpNo, marketKey);
          }

          if (
            realisticDownYes &&
            realisticDownYes.gap >= threshold &&
            !pending.downYes &&
            now - confirmState.lastConfirmDownYesMs >= FLAG_LOG_INTERVAL_MS
          ) {
            logRealisticCandidate(
              coin,
              "PolyDown_vs_KalshiYes",
              realisticDownYes,
              polySlug,
              kalshiSlug,
            );
            scheduleRealisticConfirm(
              coin,
              "downYes",
              realisticDownYes,
              marketKey,
            );
          }
        }
      }

      if (summaryOnly && now - lastSummaryLogMs >= SUMMARY_LOG_INTERVAL_MS) {
        for (const coin of coins) {
          const counts = diffCountByCoin.get(coin);
          const diff = diffByCoin.get(coin);
          const total = counts?.totalMarkets ?? 0;
          const upNoRate =
            total > 0 ? ((100 * (counts?.upNoMarkets ?? 0)) / total).toFixed(1) : "n/a";
          const downYesRate =
            total > 0 ? ((100 * (counts?.downYesMarkets ?? 0)) / total).toFixed(1) : "n/a";
          const gapUpNo = realisticFill
            ? diff?.confirmUpNo?.gap ?? diff?.realisticUpNo?.gap ?? null
            : diff?.gapPolyUpKalshiNo ?? null;
          const gapDownYes = realisticFill
            ? diff?.confirmDownYes?.gap ?? diff?.realisticDownYes?.gap ?? null
            : diff?.gapPolyDownKalshiYes ?? null;
          systemLogger.log(
            `SUMMARY ${coin.toUpperCase()} markets=${total} upNo=${counts?.upNoMarkets ?? 0} (${upNoRate}%) downYes=${counts?.downYesMarkets ?? 0} (${downYesRate}%) gapUpNo=${formatMaybe(
              gapUpNo,
              4,
            )} gapDownYes=${formatMaybe(gapDownYes, 4)}`,
          );
        }
        lastSummaryLogMs = now;
      }

      if (dashboard) {
        const activeCoin =
          coins[activeCoinIndex] ?? coins[0] ?? ("eth" as CoinSymbol);
        const activeLogs = recentLogsByCoin.get(activeCoin) ?? [];
        dashboard.update({
          coins,
          activeCoin,
          activeCoinIndex,
          coinCount: coins.length,
          polySnapshots,
          kalshiSnapshots,
          polyOddsHistoryByCoin,
          kalshiOddsHistoryByCoin,
          diffByCoin,
          threshold,
          realisticFillEnabled: realisticFill,
          fillBudgetUsd: fillUsd ?? null,
          recentLogs: activeLogs,
          marketCountsByCoin: diffCountByCoin,
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      systemLogger.log(`PRICE_DIFF_LOOP_ERROR ${message}`, "ERROR");
      pushRecentLog(`PRICE_DIFF_LOOP_ERROR ${message}`);
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
