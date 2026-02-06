import type { CoinSymbol } from "./auto-market";
import type { MarketSnapshot } from "./market-data-hub";
import { RunLogger } from "./run-logger";
import type { ArbitrageCoinConfig } from "./arbitrage-config";
import {
  computeFillEstimate,
  randomDelayMs,
  resolveAsks,
  type FillEstimate,
} from "./arbitrage-fill";
import type { NormalizedOutcome } from "./cross-platform-compare";
import type { KalshiClient } from "../clients/kalshi/kalshi-client";
import {
  computeFinalPrice,
  computeOutcomeFromValues,
  fetchKalshiOfficialOutcome,
  fetchPolymarketOfficialOutcome,
  resolveCloseTimeMs,
  resolveThreshold,
} from "./outcome-resolution";

const DECISION_COOLDOWN_MS = 200;
const POSITION_MAX_AGE_MS = parseEnvNumber("ARB_POSITION_MAX_AGE_MS", 300_000, 30_000);
const POSITION_MAX_UNRESOLVED_MS = parseEnvNumber("ARB_POSITION_MAX_UNRESOLVED_MS", 600_000, 60_000);
const THRESHOLD_DIVERGENCE_MAX = parseEnvNumber("ARB_THRESHOLD_DIVERGENCE_PCT", 0.5, 0) / 100; // env is in %, internal is fraction
const EXEC_DELAY_MIN_ENV = parseEnvNumber("EXECUTION_DELAY_MIN_MS", 250, 0);
const EXEC_DELAY_MAX_ENV = parseEnvNumber("EXECUTION_DELAY_MAX_MS", 300, 0);
const { min: EXEC_DELAY_MIN_MS, max: EXEC_DELAY_MAX_MS } = resolveDelayRange(
  EXEC_DELAY_MIN_ENV,
  EXEC_DELAY_MAX_ENV,
);
const DEFAULT_TIE_EPSILON = 0.002;

const FINAL_WINDOW_MS = parseEnvNumber(
  "CROSS_ANALYSIS_FINAL_WINDOW_MS",
  60_000,
  10_000,
);
const FINAL_GRACE_MS = parseEnvNumber(
  "CROSS_ANALYSIS_FINAL_GRACE_MS",
  120_000,
  30_000,
);
const FINAL_MIN_POINTS = parseEnvNumber(
  "CROSS_ANALYSIS_FINAL_MIN_POINTS",
  3,
  1,
);
const OFFICIAL_MAX_WAIT_MS = parseEnvNumber(
  "CROSS_ANALYSIS_OFFICIAL_WAIT_MS",
  45_000,
  5_000,
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
const FINAL_STALE_AFTER_MS = Math.min(FINAL_GRACE_MS, OFFICIAL_MAX_WAIT_MS);
const FINAL_PRICE_OPTIONS = {
  windowMs: FINAL_WINDOW_MS,
  minPoints: FINAL_MIN_POINTS,
  allowStaleAfterMs: FINAL_STALE_AFTER_MS,
};
const DEFAULT_FILL_USD = parseEnvNumber("PRICE_DIFF_FILL_USD", 100, 1);

type ArbitrageDirection = "upNo" | "downYes";
type EstimateSource = "orderbook" | "best_ask";

export interface ArbitrageSummary {
  runtimeSec: number;
  totalTrades: number;
  wins: number;
  losses: number;
  totalProfit: number;
  openExposure: number;
}

export interface ArbitragePositionSummary {
  direction: ArbitrageDirection;
  shares: number;
  avgPoly: number;
  avgKalshi: number;
  costTotal: number;
  confirmedAtMs: number;
}

export interface ArbitrageMarketView {
  coin: CoinSymbol;
  marketName: string;
  polySlug: string;
  kalshiSlug: string;
  timeLeftSec: number | null;
  spotPrice: number | null;
  polyThreshold: number | null;
  kalshiThreshold: number | null;
  polyUpAsk: number | null;
  polyDownAsk: number | null;
  kalshiYesAsk: number | null;
  kalshiNoAsk: number | null;
  estimateUpNo: FillEstimate | null;
  estimateDownYes: FillEstimate | null;
  estimateUpNoSource: EstimateSource | null;
  estimateDownYesSource: EstimateSource | null;
  selectedDirection: ArbitrageDirection | null;
  pendingDirection: ArbitrageDirection | null;
  pendingDelayMs: number | null;
  position: ArbitragePositionSummary | null;
  lastResult: string | null;
  dataStatus: "healthy" | "stale" | "unknown";
}

interface PendingOrder {
  dueMs: number;
  direction: ArbitrageDirection;
  marketKey: string;
  candidate: FillEstimate;
  originalGap: number;
  polyTarget: NormalizedOutcome;
  kalshiTarget: NormalizedOutcome;
  delayMs: number;
  committedAtMs: number;
}

interface ArbitragePosition {
  marketKey: string;
  polySlug: string;
  kalshiSlug: string;
  direction: ArbitrageDirection;
  shares: number;
  avgPoly: number;
  avgKalshi: number;
  costTotal: number;
  actualGap: number;
  originalGap: number;
  slippage: number;
  committedAtMs: number;
  executedAtMs: number;
  delayMs: number;
  polyTarget: NormalizedOutcome;
  kalshiTarget: NormalizedOutcome;
  confirmedAtMs: number;
  polySnap: MarketSnapshot;
  kalshiSnap: MarketSnapshot;
  polyOutcome: NormalizedOutcome | null;
  kalshiOutcome: NormalizedOutcome | null;
  polyOfficialOutcome: NormalizedOutcome | null;
  kalshiOfficialOutcome: NormalizedOutcome | null;
  polyOfficialOutcomeSource: string | null;
  kalshiOfficialOutcomeSource: string | null;
  polyOfficialFinalPrice: number | null;
  kalshiOfficialFinalPrice: number | null;
  polyOfficialFinalPriceSource: string | null;
  kalshiOfficialFinalPriceSource: string | null;
  polyOfficialFetchAttempts: number;
  kalshiOfficialFetchAttempts: number;
  polyOfficialFetchPending: boolean;
  kalshiOfficialFetchPending: boolean;
  polyOfficialFetchLastMs: number | null;
  kalshiOfficialFetchLastMs: number | null;
  polyCloseCapturedMs: number | null;
  kalshiCloseCapturedMs: number | null;
  polyClosePrice: number | null;
  kalshiClosePrice: number | null;
  polyClosePriceSource: string | null;
  kalshiClosePriceSource: string | null;
  polyClosePricePoints: number | null;
  kalshiClosePricePoints: number | null;
  polyClosePriceCoverageMs: number | null;
  kalshiClosePriceCoverageMs: number | null;
  polyClosePriceWindowMs: number | null;
  kalshiClosePriceWindowMs: number | null;
  polyCloseThreshold: number | null;
  kalshiCloseThreshold: number | null;
  polyCloseThresholdSource: string | null;
  kalshiCloseThresholdSource: string | null;
  mismatchLogged: boolean;
}

interface CoinState {
  pendingOrder: PendingOrder | null;
  position: ArbitragePosition | null;
  lastDecisionMs: number;
  lastResult: string | null;
  lastSnapshots: { poly: MarketSnapshot; kalshi: MarketSnapshot } | null;
  lastEstimates: {
    upNo: FillEstimate | null;
    downYes: FillEstimate | null;
    upNoSource: EstimateSource | null;
    downYesSource: EstimateSource | null;
    selected: ArbitrageDirection | null;
  };
  lastSkipReason: string | null;
  lastSkipLogMs: number;
}

export interface ArbitrageEngineOptions {
  kalshiOutcomeClient: KalshiClient;
  mismatchLogger?: RunLogger;
  decisionLatencyMs?: number;
  headlessSummary?: boolean;
}

export class ArbitrageEngine {
  private name: string;
  private configs: Map<CoinSymbol, ArbitrageCoinConfig>;
  private logger: RunLogger;
  private kalshiOutcomeClient: KalshiClient;
  private mismatchLogger: RunLogger | null;
  private summary: ArbitrageSummary;
  private pnlHistory: number[];
  private startMs: number;
  private states: Map<CoinSymbol, CoinState>;
  private decisionLatencyMs: number | null;
  private summaryOnly: boolean;

  constructor(
    name: string,
    configs: Map<CoinSymbol, ArbitrageCoinConfig>,
    logger: RunLogger,
    options: ArbitrageEngineOptions,
    startTimeMs?: number,
  ) {
    this.name = name;
    this.configs = configs;
    this.logger = logger;
    this.kalshiOutcomeClient = options.kalshiOutcomeClient;
    this.mismatchLogger = options.mismatchLogger ?? null;
    this.summaryOnly = options.headlessSummary === true;
    this.startMs = startTimeMs ?? Date.now();
    this.summary = {
      runtimeSec: 0,
      totalTrades: 0,
      wins: 0,
      losses: 0,
      totalProfit: 0,
      openExposure: 0,
    };
    this.pnlHistory = [];
    this.states = new Map();
    this.decisionLatencyMs =
      typeof options.decisionLatencyMs === "number" &&
      Number.isFinite(options.decisionLatencyMs)
        ? Math.max(0, Math.floor(options.decisionLatencyMs))
        : null;
    for (const coin of configs.keys()) {
      this.states.set(coin, this.createState());
    }
  }

  getName(): string {
    return this.name;
  }

  getSummary(): ArbitrageSummary {
    return { ...this.summary };
  }

  getPnlHistory(): number[] {
    return [...this.pnlHistory];
  }

  getLogs(): string[] {
    return this.logger.getRecentLines();
  }

  getMarketViews(): ArbitrageMarketView[] {
    const views: ArbitrageMarketView[] = [];
    for (const [coin, state] of this.states.entries()) {
      const snapshots = state.lastSnapshots;
      if (!snapshots) continue;
      const polySnap = snapshots.poly;
      const kalshiSnap = snapshots.kalshi;
      const polyThreshold = resolveThreshold(polySnap).value;
      const kalshiThreshold = resolveThreshold(kalshiSnap).value;
      const timeLeft = resolveMinTimeLeft(polySnap, kalshiSnap);
      const polyUpAsk = polySnap.upTokenId
        ? polySnap.bestAsk.get(polySnap.upTokenId) ?? null
        : null;
      const polyDownAsk = polySnap.downTokenId
        ? polySnap.bestAsk.get(polySnap.downTokenId) ?? null
        : null;
      const kalshiYesAsk = kalshiSnap.bestAsk.get("YES") ?? null;
      const kalshiNoAsk = kalshiSnap.bestAsk.get("NO") ?? null;
      const dataStatus = resolveDataStatus(polySnap, kalshiSnap);
      views.push({
        coin,
        marketName: polySnap.marketName || kalshiSnap.marketName || "Market",
        polySlug: polySnap.slug,
        kalshiSlug: kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi",
        timeLeftSec: timeLeft,
        spotPrice: polySnap.cryptoPrice > 0 ? polySnap.cryptoPrice : null,
        polyThreshold,
        kalshiThreshold,
        polyUpAsk,
        polyDownAsk,
        kalshiYesAsk,
        kalshiNoAsk,
        estimateUpNo: state.lastEstimates.upNo,
        estimateDownYes: state.lastEstimates.downYes,
        estimateUpNoSource: state.lastEstimates.upNoSource,
        estimateDownYesSource: state.lastEstimates.downYesSource,
        selectedDirection: state.lastEstimates.selected,
        pendingDirection: state.pendingOrder?.direction ?? null,
        pendingDelayMs: state.pendingOrder?.delayMs ?? null,
        position: state.position
          ? {
              direction: state.position.direction,
              shares: state.position.shares,
              avgPoly: state.position.avgPoly,
              avgKalshi: state.position.avgKalshi,
              costTotal: state.position.costTotal,
              confirmedAtMs: state.position.confirmedAtMs,
            }
          : null,
        lastResult: state.lastResult,
        dataStatus,
      });
    }
    return views;
  }

  evaluate(
    polySnapshots: Map<CoinSymbol, MarketSnapshot>,
    kalshiSnapshots: Map<CoinSymbol, MarketSnapshot>,
    nowMs: number = Date.now(),
  ): void {
    this.summary.runtimeSec = (nowMs - this.startMs) / 1000;
    let openExposure = 0;

    for (const [coin, config] of this.configs.entries()) {
      try {
        this.evaluateCoin(coin, config, polySnapshots, kalshiSnapshots, nowMs);
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown error";
        this.logger.log(
          `${coin.toUpperCase()} EVAL_ERROR: ${message}`,
          "ERROR",
        );
      }
      const state = this.states.get(coin);
      if (state?.position) {
        openExposure += state.position.costTotal;
      }
    }

    this.summary.openExposure = openExposure;
    this.pnlHistory.push(this.summary.totalProfit);
    if (this.pnlHistory.length > 180) {
      this.pnlHistory.shift();
    }
  }

  private createState(): CoinState {
    return {
      pendingOrder: null,
      position: null,
      lastDecisionMs: 0,
      lastResult: null,
      lastSnapshots: null,
      lastEstimates: {
        upNo: null,
        downYes: null,
        upNoSource: null,
        downYesSource: null,
        selected: null,
      },
      lastSkipReason: null,
      lastSkipLogMs: 0,
    };
  }

  private evaluateCoin(
    coin: CoinSymbol,
    config: ArbitrageCoinConfig,
    polySnapshots: Map<CoinSymbol, MarketSnapshot>,
    kalshiSnapshots: Map<CoinSymbol, MarketSnapshot>,
    nowMs: number,
  ): void {
    const state = this.states.get(coin) ?? this.createState();
    const polySnap = polySnapshots.get(coin);
    const kalshiSnap = kalshiSnapshots.get(coin);

    if (polySnap && kalshiSnap) {
      state.lastSnapshots = { poly: polySnap, kalshi: kalshiSnap };
    }

    if (state.position) {
      if (polySnap && kalshiSnap) {
        const currentKey = buildMarketKey(polySnap, kalshiSnap);
        if (currentKey === state.position.marketKey) {
          state.position.polySnap = polySnap;
          state.position.kalshiSnap = kalshiSnap;
        }
      }
      this.maybeResolvePosition(state, nowMs);
      this.states.set(coin, state);
      return;
    }

    if (state.pendingOrder) {
      if (
        !polySnap ||
        !kalshiSnap ||
        buildMarketKey(polySnap, kalshiSnap) !== state.pendingOrder.marketKey
      ) {
        this.logger.log(
          `${coin.toUpperCase()} pending order canceled (market changed)`,
          "WARN",
        );
        state.pendingOrder = null;
      } else if (nowMs >= state.pendingOrder.dueMs) {
        this.confirmPendingOrder(state, config, polySnap, kalshiSnap, nowMs);
      }
      this.states.set(coin, state);
      return;
    }

    if (!polySnap || !kalshiSnap) {
      this.skipCoin(state, coin, "missing_snapshot", nowMs);
      return;
    }

    const timeLeft = resolveMinTimeLeft(polySnap, kalshiSnap);
    const fillBudget = resolveFillBudget(config);

    // Always compute display estimates when possible (even if trade not allowed yet).
    if (fillBudget) {
      const rawUpNo = computeDisplayEstimate(
        "upNo",
        polySnap,
        kalshiSnap,
        fillBudget,
      );
      const rawDownYes = computeDisplayEstimate(
        "downYes",
        polySnap,
        kalshiSnap,
        fillBudget,
      );
      state.lastEstimates = {
        upNo: rawUpNo?.estimate ?? null,
        downYes: rawDownYes?.estimate ?? null,
        upNoSource: rawUpNo?.source ?? null,
        downYesSource: rawDownYes?.source ?? null,
        selected: null,
      };
    } else {
      state.lastEstimates = {
        upNo: null,
        downYes: null,
        upNoSource: null,
        downYesSource: null,
        selected: null,
      };
    }

    if (timeLeft === null) {
      this.skipCoin(state, coin, "time_unknown", nowMs);
      return;
    }
    if (timeLeft <= 0) {
      this.skipCoin(state, coin, "market_closed", nowMs);
      return;
    }
    if (timeLeft > config.tradeAllowedTimeLeft) {
      this.skipCoin(state, coin, `waiting (${Math.round(timeLeft)}s left, allowed<=${config.tradeAllowedTimeLeft}s)`, nowMs);
      return;
    }
    if (config.tradeStopTimeLeft !== null && timeLeft <= config.tradeStopTimeLeft) {
      this.skipCoin(state, coin, `stopped (${Math.round(timeLeft)}s left, stop<=${config.tradeStopTimeLeft}s)`, nowMs);
      return;
    }

    if (!isFreshEnough(config, polySnap, kalshiSnap, nowMs)) {
      this.skipCoin(state, coin, `stale_data (poly=${polySnap.dataStatus} kalshi=${kalshiSnap.dataStatus})`, nowMs);
      return;
    }

    if (!fillBudget) {
      this.skipCoin(state, coin, "no_fill_budget", nowMs);
      return;
    }

    // ── Threshold validation: both platforms must have resolvable thresholds ──
    const polyThreshold = resolveThreshold(polySnap);
    const kalshiThreshold = resolveThreshold(kalshiSnap);
    if (!polyThreshold.value || polyThreshold.value <= 0) {
      this.skipCoin(state, coin, `poly_threshold_missing (src=${polyThreshold.source})`, nowMs);
      return;
    }
    if (!kalshiThreshold.value || kalshiThreshold.value <= 0) {
      this.skipCoin(state, coin, `kalshi_threshold_missing (src=${kalshiThreshold.source})`, nowMs);
      return;
    }

    // ── Threshold divergence check ──────────────────────────────
    // The arbitrage assumes both platforms resolve the same direction.
    // If thresholds differ significantly, the outcome could diverge
    // (e.g., price between the two thresholds → one is UP, other is DOWN).
    // This would cause a TOTAL LOSS on both legs.
    const thresholdDivergence = Math.abs(polyThreshold.value - kalshiThreshold.value);
    const thresholdDivergencePct = thresholdDivergence / Math.min(polyThreshold.value, kalshiThreshold.value);
    if (THRESHOLD_DIVERGENCE_MAX > 0 && thresholdDivergencePct > THRESHOLD_DIVERGENCE_MAX) {
      this.skipCoin(
        state,
        coin,
        `threshold_divergence poly=${polyThreshold.value.toFixed(2)} kalshi=${kalshiThreshold.value.toFixed(2)} diff=${(thresholdDivergencePct * 100).toFixed(2)}%`,
        nowMs,
      );
      return;
    }

    const upNo = buildCandidate(
      "upNo",
      polySnap,
      kalshiSnap,
      fillBudget,
      config,
    );
    const downYes = buildCandidate(
      "downYes",
      polySnap,
      kalshiSnap,
      fillBudget,
      config,
    );

    const selected = chooseCandidate(upNo, downYes);
    state.lastEstimates.selected = selected?.direction ?? null;

    if (!selected) {
      this.states.set(coin, state);
      return;
    }

    if (nowMs - state.lastDecisionMs < DECISION_COOLDOWN_MS) {
      this.states.set(coin, state);
      return;
    }
    state.lastDecisionMs = nowMs;

    const delayMs =
      this.decisionLatencyMs !== null
        ? this.decisionLatencyMs
        : randomDelayMs(EXEC_DELAY_MIN_MS, EXEC_DELAY_MAX_MS);

    state.pendingOrder = {
      dueMs: nowMs + delayMs,
      direction: selected.direction,
      marketKey: buildMarketKey(polySnap, kalshiSnap),
      candidate: selected.estimate,
      originalGap: selected.estimate.gap,
      polyTarget: selected.polyTarget,
      kalshiTarget: selected.kalshiTarget,
      delayMs,
      committedAtMs: nowMs,
    };

    if (!this.summaryOnly) {
      this.logger.log(
        `${coin.toUpperCase()} ARB_CANDIDATE ${selected.direction} gap=${selected.estimate.gap.toFixed(
          4,
        )} shares=${selected.estimate.shares} cost=${selected.estimate.totalCost.toFixed(
          2,
        )} delay=${delayMs}ms polyThreshold=${polyThreshold.value} kalshiThreshold=${kalshiThreshold.value}`,
      );
    }
    this.states.set(coin, state);
  }

  /**
   * Log why a coin was skipped. Only logs when the reason changes or
   * every 30s to avoid spamming. This is invaluable for diagnosing
   * "why isn't it trading?" issues.
   */
  private skipCoin(
    state: CoinState,
    coin: CoinSymbol,
    reason: string,
    nowMs: number,
  ): void {
    // Log less frequently for expected skip states (waiting/stopped),
    // more frequently for actionable ones (stale_data, threshold_missing)
    const isExpected = reason.startsWith("waiting") || reason.startsWith("market_closed");
    const intervalMs = isExpected ? 120_000 : 30_000;
    const reasonChanged = state.lastSkipReason !== reason;
    const logDue = nowMs - state.lastSkipLogMs >= intervalMs;

    if (reasonChanged || logDue) {
      if (!this.summaryOnly) {
        this.logger.log(
          `${coin.toUpperCase()} SKIP: ${reason}`,
        );
      }
      state.lastSkipReason = reason;
      state.lastSkipLogMs = nowMs;
    }
    this.states.set(coin, state);
  }

  private confirmPendingOrder(
    state: CoinState,
    config: ArbitrageCoinConfig,
    polySnap: MarketSnapshot,
    kalshiSnap: MarketSnapshot,
    nowMs: number,
  ): void {
    const pending = state.pendingOrder;
    if (!pending) return;

    // ── Threshold re-validation at execution time ────────────────
    // If thresholds vanished during the delay, abort rather than
    // entering a position that can never resolve its outcome.
    const polyThresholdCheck = resolveThreshold(polySnap);
    const kalshiThresholdCheck = resolveThreshold(kalshiSnap);
    if (
      !polyThresholdCheck.value ||
      polyThresholdCheck.value <= 0 ||
      !kalshiThresholdCheck.value ||
      kalshiThresholdCheck.value <= 0
    ) {
      this.logger.log(
        `${polySnap.coin.toUpperCase()} PENDING_ABORT: threshold vanished during delay polyThreshold=${polyThresholdCheck.value ?? "null"} kalshiThreshold=${kalshiThresholdCheck.value ?? "null"}`,
        "WARN",
      );
      state.pendingOrder = null;
      return;
    }

    // ── Threshold divergence re-check at execution time ──────────
    const confirmDivergence = Math.abs(polyThresholdCheck.value - kalshiThresholdCheck.value);
    const confirmDivergencePct = confirmDivergence / Math.min(polyThresholdCheck.value, kalshiThresholdCheck.value);
    if (THRESHOLD_DIVERGENCE_MAX > 0 && confirmDivergencePct > THRESHOLD_DIVERGENCE_MAX) {
      this.logger.log(
        `${polySnap.coin.toUpperCase()} PENDING_ABORT: threshold diverged during delay poly=${polyThresholdCheck.value.toFixed(2)} kalshi=${kalshiThresholdCheck.value.toFixed(2)} diff=${(confirmDivergencePct * 100).toFixed(2)}%`,
        "WARN",
      );
      state.pendingOrder = null;
      return;
    }

    // Execution delay model: We are already committed. No abort possible.
    // Re-fetch post-delay books to determine actual fill.
    const fillBudget = resolveFillBudget(config);
    let postDelayCandidate: ReturnType<typeof buildCandidateNoValidation> = null;
    try {
      postDelayCandidate = fillBudget
        ? buildCandidateNoValidation(
            pending.direction,
            polySnap,
            kalshiSnap,
            fillBudget,
          )
        : null;
    } catch {
      // Post-delay book walk failed -- use original candidate
    }

    // Use post-delay fill if available, otherwise fallback to original candidate
    const actualFill = postDelayCandidate?.estimate ?? pending.candidate;
    const actualGap = actualFill.gap;
    const slippage = actualGap - pending.originalGap;

    // Safety: if the post-delay fill has a deeply negative gap, the
    // opportunity evaporated. In paper mode we log this as slippage data.
    // We still execute (committed), but log the warning.
    if (actualGap < -0.10) {
      this.logger.log(
        `${polySnap.coin.toUpperCase()} SLIPPAGE_WARNING: post-delay gap=${actualGap.toFixed(4)} (was ${pending.originalGap.toFixed(4)}) -- opportunity evaporated`,
        "WARN",
      );
    }

    state.position = {
      marketKey: pending.marketKey,
      polySlug: polySnap.slug,
      kalshiSlug: kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi",
      direction: pending.direction,
      shares: actualFill.shares,
      avgPoly: actualFill.avgPoly,
      avgKalshi: actualFill.avgKalshi,
      costTotal: actualFill.totalCost,
      actualGap,
      originalGap: pending.originalGap,
      slippage,
      committedAtMs: pending.committedAtMs,
      executedAtMs: nowMs,
      delayMs: pending.delayMs,
      polyTarget: pending.polyTarget,
      kalshiTarget: pending.kalshiTarget,
      confirmedAtMs: nowMs,
      polySnap,
      kalshiSnap,
      polyOutcome: null,
      kalshiOutcome: null,
      polyOfficialOutcome: null,
      kalshiOfficialOutcome: null,
      polyOfficialOutcomeSource: null,
      kalshiOfficialOutcomeSource: null,
      polyOfficialFinalPrice: null,
      kalshiOfficialFinalPrice: null,
      polyOfficialFinalPriceSource: null,
      kalshiOfficialFinalPriceSource: null,
      polyOfficialFetchAttempts: 0,
      kalshiOfficialFetchAttempts: 0,
      polyOfficialFetchPending: false,
      kalshiOfficialFetchPending: false,
      polyOfficialFetchLastMs: null,
      kalshiOfficialFetchLastMs: null,
      polyCloseCapturedMs: null,
      kalshiCloseCapturedMs: null,
      polyClosePrice: null,
      kalshiClosePrice: null,
      polyClosePriceSource: null,
      kalshiClosePriceSource: null,
      polyClosePricePoints: null,
      kalshiClosePricePoints: null,
      polyClosePriceCoverageMs: null,
      kalshiClosePriceCoverageMs: null,
      polyClosePriceWindowMs: null,
      kalshiClosePriceWindowMs: null,
      polyCloseThreshold: null,
      kalshiCloseThreshold: null,
      polyCloseThresholdSource: null,
      kalshiCloseThresholdSource: null,
      mismatchLogged: false,
    };
    state.pendingOrder = null;
    this.summary.totalTrades += 1;

    const slippageLabel =
      slippage >= 0 ? `+${slippage.toFixed(4)}` : slippage.toFixed(4);
    const fillSource = postDelayCandidate ? "post-delay" : "original";
    const polyThresholdVal = resolveThreshold(polySnap).value;
    const kalshiThresholdVal = resolveThreshold(kalshiSnap).value;
    this.logger.log(
      `${polySnap.coin.toUpperCase()} ARB_EXECUTED ${pending.direction} shares=${actualFill.shares} avgPoly=${actualFill.avgPoly.toFixed(
        4,
      )} avgKalshi=${actualFill.avgKalshi.toFixed(4)} cost=${actualFill.totalCost.toFixed(
        2,
      )} gap=${actualGap.toFixed(4)} origGap=${pending.originalGap.toFixed(4)} slippage=${slippageLabel} fill=${fillSource} polyThreshold=${polyThresholdVal ?? "n/a"} kalshiThreshold=${kalshiThresholdVal ?? "n/a"} dataStatus=${resolveDataStatus(polySnap, kalshiSnap)}`,
    );
  }

  private maybeResolvePosition(state: CoinState, nowMs: number): void {
    const position = state.position;
    if (!position) return;

    const polyClosed = isSnapshotClosed(position.polySnap, nowMs);
    const kalshiClosed = isSnapshotClosed(position.kalshiSnap, nowMs);

    if (polyClosed && position.polyOutcome === null) {
      this.maybeFetchPolymarketOfficial(position, nowMs);
    }
    if (kalshiClosed && position.kalshiOutcome === null) {
      this.maybeFetchKalshiOfficial(position, nowMs);
    }

    if (polyClosed && position.polyOutcome === null) {
      const threshold = resolveThreshold(position.polySnap);
      const finalPrice = computeFinalPrice(
        position.polySnap,
        nowMs,
        FINAL_PRICE_OPTIONS,
      );
      const polyCloseTimeMs = resolveCloseTimeMs(position.polySnap, nowMs);
      const allowComputed =
        (OFFICIAL_RETRY_LIMIT > 0 &&
          position.polyOfficialFetchAttempts >= OFFICIAL_RETRY_LIMIT) ||
        (polyCloseTimeMs !== null &&
          nowMs - polyCloseTimeMs > OFFICIAL_MAX_WAIT_MS);
      position.polyClosePrice = finalPrice.value;
      position.polyClosePriceSource = finalPrice.source;
      position.polyClosePricePoints = finalPrice.points;
      position.polyClosePriceCoverageMs = finalPrice.coverageMs;
      position.polyClosePriceWindowMs = finalPrice.windowMs;
      position.polyCloseThreshold = threshold.value;
      position.polyCloseThresholdSource = threshold.source;
      position.polyCloseCapturedMs = nowMs;
      if (
        allowComputed &&
        finalPrice.value !== null &&
        threshold.value !== null &&
        threshold.value > 0
      ) {
        position.polyOutcome = computeOutcomeFromValues(
          finalPrice.value,
          threshold.value,
        );
      }
    }

    if (kalshiClosed && position.kalshiOutcome === null) {
      const threshold = resolveThreshold(position.kalshiSnap);
      const finalPrice = computeFinalPrice(
        position.kalshiSnap,
        nowMs,
        FINAL_PRICE_OPTIONS,
      );
      const kalshiCloseTimeMs = resolveCloseTimeMs(position.kalshiSnap, nowMs);
      const allowComputed =
        (OFFICIAL_RETRY_LIMIT > 0 &&
          position.kalshiOfficialFetchAttempts >= OFFICIAL_RETRY_LIMIT) ||
        (kalshiCloseTimeMs !== null &&
          nowMs - kalshiCloseTimeMs > OFFICIAL_MAX_WAIT_MS);
      position.kalshiClosePrice = finalPrice.value;
      position.kalshiClosePriceSource = finalPrice.source;
      position.kalshiClosePricePoints = finalPrice.points;
      position.kalshiClosePriceCoverageMs = finalPrice.coverageMs;
      position.kalshiClosePriceWindowMs = finalPrice.windowMs;
      position.kalshiCloseThreshold = threshold.value;
      position.kalshiCloseThresholdSource = threshold.source;
      position.kalshiCloseCapturedMs = nowMs;
      if (
        allowComputed &&
        finalPrice.value !== null &&
        threshold.value !== null &&
        threshold.value > 0
      ) {
        position.kalshiOutcome = computeOutcomeFromValues(
          finalPrice.value,
          threshold.value,
        );
      }
    }

    // ── Force-resolve stuck positions ────────────────────────────
    // If both markets closed but we still can't resolve, eventually
    // we MUST clean up to avoid blocking the coin forever.
    const bothClosed = polyClosed && kalshiClosed;
    const positionAge = nowMs - position.confirmedAtMs;
    const needsForceResolve =
      bothClosed && positionAge > POSITION_MAX_AGE_MS;
    const needsAbsoluteForceResolve =
      positionAge > POSITION_MAX_UNRESOLVED_MS;

    if (
      (needsForceResolve || needsAbsoluteForceResolve) &&
      (position.polyOutcome === null || position.kalshiOutcome === null)
    ) {
      this.forceResolveOutcomes(position, nowMs);
    }

    if (
      position.polyOutcome === null ||
      position.kalshiOutcome === null ||
      position.polyOutcome === "UNKNOWN" ||
      position.kalshiOutcome === "UNKNOWN"
    ) {
      // If we're past absolute force resolve and STILL stuck, force UNKNOWN
      if (needsAbsoluteForceResolve) {
        if (position.polyOutcome === null) position.polyOutcome = "UNKNOWN";
        if (position.kalshiOutcome === null) position.kalshiOutcome = "UNKNOWN";
        this.logger.log(
          `${position.polySnap.coin.toUpperCase()} FORCED_TIMEOUT position age=${Math.round(positionAge / 1000)}s polyOutcome=${position.polyOutcome} kalshiOutcome=${position.kalshiOutcome}`,
          "WARN",
        );
      } else {
        return;
      }
    }

    const polyWin = position.polyOutcome === position.polyTarget;
    const kalshiWin = position.kalshiOutcome === position.kalshiTarget;
    const payout =
      (polyWin ? position.shares : 0) + (kalshiWin ? position.shares : 0);
    const netPnl = payout - position.costTotal;

    if (netPnl >= 0) {
      this.summary.wins += 1;
    } else {
      this.summary.losses += 1;
    }
    this.summary.totalProfit += netPnl;

    const posAgeSec = Math.round((nowMs - position.confirmedAtMs) / 1000);
    const result = `${netPnl >= 0 ? "WIN" : "LOSS"} net=${netPnl.toFixed(
      2,
    )} poly=${position.polyOutcome} kalshi=${position.kalshiOutcome}`;
    state.lastResult = result;
    this.logger.log(
      `${position.polySnap.coin.toUpperCase()} ARB_RESOLVED ${result} age=${posAgeSec}s payout=${payout.toFixed(2)} cost=${position.costTotal.toFixed(2)}`,
    );

    if (
      this.mismatchLogger &&
      position.polyOutcome !== position.kalshiOutcome &&
      !position.mismatchLogged
    ) {
      position.mismatchLogged = true;
      this.logMismatch(position, nowMs);
    }

    state.position = null;
  }

  private maybeFetchPolymarketOfficial(
    position: ArbitragePosition,
    nowMs: number,
  ): void {
    if (position.polyOfficialFetchPending) return;
    if (position.polyOfficialOutcome) return;
    if (
      !shouldAttemptOfficialFetch(
        position.polyOfficialFetchAttempts,
        position.polyOfficialFetchLastMs,
        nowMs,
      )
    )
      return;

    position.polyOfficialFetchPending = true;
    position.polyOfficialFetchAttempts += 1;
    position.polyOfficialFetchLastMs = nowMs;

    void fetchPolymarketOfficialOutcome(position.polySnap.slug)
      .then((result) => {
        position.polyOfficialFetchPending = false;
        if (result.outcome) {
          position.polyOfficialOutcome = result.outcome;
          position.polyOfficialOutcomeSource = result.outcomeSource;
        }
        if (result.finalPrice != null) {
          position.polyOfficialFinalPrice = result.finalPrice;
          position.polyOfficialFinalPriceSource = result.finalPriceSource;
        }

        const threshold = resolveThreshold(position.polySnap).value;
        if (position.polyOfficialOutcome && position.polyOutcome == null) {
          position.polyOutcome = position.polyOfficialOutcome;
          position.polyCloseCapturedMs = nowMs;
        } else if (
          position.polyOutcome == null &&
          position.polyOfficialFinalPrice != null &&
          isPlausibleUnderlying(position.polyOfficialFinalPrice, threshold)
        ) {
          position.polyClosePrice = position.polyOfficialFinalPrice;
          position.polyClosePriceSource = `official:${position.polyOfficialFinalPriceSource ?? "final_price"}`;
          position.polyCloseThreshold = threshold;
          position.polyCloseThresholdSource =
            resolveThreshold(position.polySnap).source;
          position.polyCloseCapturedMs = nowMs;
          position.polyOutcome = computeOutcomeFromValues(
            position.polyOfficialFinalPrice,
            threshold,
          );
        }
      })
      .catch((err) => {
        position.polyOfficialFetchPending = false;
        this.logger.log(
          `OFFICIAL_FETCH_ERROR poly slug=${position.polySlug}: ${err instanceof Error ? err.message : "unknown"}`,
          "WARN",
        );
      });
  }

  private maybeFetchKalshiOfficial(
    position: ArbitragePosition,
    nowMs: number,
  ): void {
    if (position.kalshiOfficialFetchPending) return;
    if (position.kalshiOfficialOutcome) return;
    if (
      !shouldAttemptOfficialFetch(
        position.kalshiOfficialFetchAttempts,
        position.kalshiOfficialFetchLastMs,
        nowMs,
      )
    )
      return;

    position.kalshiOfficialFetchPending = true;
    position.kalshiOfficialFetchAttempts += 1;
    position.kalshiOfficialFetchLastMs = nowMs;
    const ticker =
      position.kalshiSnap.marketTicker ?? position.kalshiSnap.slug ?? "";
    if (!ticker) {
      position.kalshiOfficialFetchPending = false;
      return;
    }

    void fetchKalshiOfficialOutcome(this.kalshiOutcomeClient, ticker)
      .then((result) => {
        position.kalshiOfficialFetchPending = false;
        if (result.outcome) {
          position.kalshiOfficialOutcome = result.outcome;
          position.kalshiOfficialOutcomeSource = result.outcomeSource;
        }
        if (result.finalPrice != null) {
          position.kalshiOfficialFinalPrice = result.finalPrice;
          position.kalshiOfficialFinalPriceSource = result.finalPriceSource;
        }

        const threshold = resolveThreshold(position.kalshiSnap).value;
        if (position.kalshiOfficialOutcome && position.kalshiOutcome == null) {
          position.kalshiOutcome = position.kalshiOfficialOutcome;
          position.kalshiCloseCapturedMs = nowMs;
        } else if (
          position.kalshiOutcome == null &&
          position.kalshiOfficialFinalPrice != null &&
          isPlausibleUnderlying(position.kalshiOfficialFinalPrice, threshold)
        ) {
          position.kalshiClosePrice = position.kalshiOfficialFinalPrice;
          position.kalshiClosePriceSource = `official:${position.kalshiOfficialFinalPriceSource ?? "final_price"}`;
          position.kalshiCloseThreshold = threshold;
          position.kalshiCloseThresholdSource =
            resolveThreshold(position.kalshiSnap).source;
          position.kalshiCloseCapturedMs = nowMs;
          position.kalshiOutcome = computeOutcomeFromValues(
            position.kalshiOfficialFinalPrice,
            threshold,
          );
        }
      })
      .catch((err) => {
        position.kalshiOfficialFetchPending = false;
        this.logger.log(
          `OFFICIAL_FETCH_ERROR kalshi ticker=${position.kalshiSlug}: ${err instanceof Error ? err.message : "unknown"}`,
          "WARN",
        );
      });
  }

  /**
   * Force-resolve outcomes when normal resolution has timed out.
   * Uses best-effort data: official outcomes, computed from price+threshold,
   * or copies from the platform that DID resolve.
   */
  private forceResolveOutcomes(
    position: ArbitragePosition,
    nowMs: number,
  ): void {
    const coin = position.polySnap.coin.toUpperCase();

    // Try to resolve poly outcome if still null
    if (position.polyOutcome === null) {
      if (position.polyOfficialOutcome) {
        position.polyOutcome = position.polyOfficialOutcome;
      } else {
        const threshold = resolveThreshold(position.polySnap).value;
        const price =
          position.polyClosePrice ??
          position.polySnap.cryptoPrice ??
          null;
        if (price && price > 0 && threshold && threshold > 0) {
          position.polyOutcome = computeOutcomeFromValues(price, threshold);
        } else if (position.kalshiOutcome && position.kalshiOutcome !== "UNKNOWN") {
          // Copy from Kalshi (same underlying asset, should agree)
          position.polyOutcome = position.kalshiOutcome;
          this.logger.log(
            `${coin} FORCE_RESOLVE: copied kalshi outcome (${position.kalshiOutcome}) to poly (threshold=${threshold}, price=${price})`,
            "WARN",
          );
        }
      }
    }

    // Try to resolve kalshi outcome if still null
    if (position.kalshiOutcome === null) {
      if (position.kalshiOfficialOutcome) {
        position.kalshiOutcome = position.kalshiOfficialOutcome;
      } else {
        const threshold = resolveThreshold(position.kalshiSnap).value;
        const price =
          position.kalshiClosePrice ??
          (position.kalshiSnap.kalshiUnderlyingValue ?? null) ??
          position.kalshiSnap.cryptoPrice ??
          null;
        if (price && price > 0 && threshold && threshold > 0) {
          position.kalshiOutcome = computeOutcomeFromValues(price, threshold);
        } else if (position.polyOutcome && position.polyOutcome !== "UNKNOWN") {
          // Copy from Poly (same underlying asset, should agree)
          position.kalshiOutcome = position.polyOutcome;
          this.logger.log(
            `${coin} FORCE_RESOLVE: copied poly outcome (${position.polyOutcome}) to kalshi (threshold=${threshold}, price=${price})`,
            "WARN",
          );
        }
      }
    }
  }

  private logMismatch(position: ArbitragePosition, nowMs: number): void {
    if (!this.mismatchLogger) return;
    const polyThreshold = resolveThreshold(position.polySnap).value;
    const kalshiThreshold = resolveThreshold(position.kalshiSnap).value;
    const polyPrice =
      position.polyClosePrice ??
      computeFinalPrice(position.polySnap, nowMs, FINAL_PRICE_OPTIONS).value;
    const kalshiPrice =
      position.kalshiClosePrice ??
      computeFinalPrice(position.kalshiSnap, nowMs, FINAL_PRICE_OPTIONS).value;
    const polyCloseIso = formatIso(
      resolveCloseTimeMs(position.polySnap, nowMs),
    );
    const kalshiCloseIso = formatIso(
      resolveCloseTimeMs(position.kalshiSnap, nowMs),
    );

    const json = {
      pairKey: position.marketKey,
      direction: position.direction,
      shares: position.shares,
      polyOutcome: position.polyOutcome,
      kalshiOutcome: position.kalshiOutcome,
      polyOfficialOutcome: position.polyOfficialOutcome,
      kalshiOfficialOutcome: position.kalshiOfficialOutcome,
      polyOfficialOutcomeSource: position.polyOfficialOutcomeSource,
      kalshiOfficialOutcomeSource: position.kalshiOfficialOutcomeSource,
      polyOfficialFinalPrice: position.polyOfficialFinalPrice,
      kalshiOfficialFinalPrice: position.kalshiOfficialFinalPrice,
      polyOfficialFinalPriceSource: position.polyOfficialFinalPriceSource,
      kalshiOfficialFinalPriceSource: position.kalshiOfficialFinalPriceSource,
      polySlug: position.polySlug,
      kalshiSlug: position.kalshiSlug,
      polyThreshold,
      kalshiThreshold,
      polyPriceUsed: polyPrice,
      kalshiPriceUsed: kalshiPrice,
      polyCloseTimeIso: polyCloseIso,
      kalshiCloseTimeIso: kalshiCloseIso,
      polyDataStatus: position.polySnap.dataStatus,
      kalshiDataStatus: position.kalshiSnap.dataStatus,
      thresholdDivergencePct:
        polyThreshold && kalshiThreshold && polyThreshold > 0 && kalshiThreshold > 0
          ? ((Math.abs(polyThreshold - kalshiThreshold) / Math.min(polyThreshold, kalshiThreshold)) * 100).toFixed(4)
          : null,
    };

    this.mismatchLogger.log(
      `ARB_MISMATCH ${position.polySnap.coin.toUpperCase()} pair=${position.marketKey} poly=${position.polyOutcome} kalshi=${position.kalshiOutcome}`,
      "WARN",
    );
    this.mismatchLogger.log(`ARB_MISMATCH_JSON ${JSON.stringify(json)}`, "WARN");
  }
}

interface Candidate {
  direction: ArbitrageDirection;
  estimate: FillEstimate;
  polyTarget: NormalizedOutcome;
  kalshiTarget: NormalizedOutcome;
}

interface DisplayEstimate {
  estimate: FillEstimate;
  source: EstimateSource;
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

function resolveDelayRange(
  minMs: number,
  maxMs: number,
): { min: number; max: number } {
  const min = Math.max(0, Math.floor(minMs));
  const max = Math.max(min, Math.floor(maxMs));
  return { min, max };
}

function resolveMinTimeLeft(
  polySnap: MarketSnapshot,
  kalshiSnap: MarketSnapshot,
): number | null {
  if (polySnap.timeLeftSec == null || kalshiSnap.timeLeftSec == null) return null;
  return Math.min(polySnap.timeLeftSec, kalshiSnap.timeLeftSec);
}

function resolveDataStatus(
  polySnap: MarketSnapshot,
  kalshiSnap: MarketSnapshot,
): "healthy" | "stale" | "unknown" {
  if (polySnap.dataStatus !== "healthy" || kalshiSnap.dataStatus !== "healthy") {
    return "stale";
  }
  return "healthy";
}

function buildMarketKey(
  polySnap: MarketSnapshot,
  kalshiSnap: MarketSnapshot,
): string {
  const kalshiKey = kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi";
  return `${polySnap.slug}|${kalshiKey}`;
}

function isSnapshotClosed(snapshot: MarketSnapshot, now: number): boolean {
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
}

function resolveFillBudget(config: ArbitrageCoinConfig): number | null {
  const configured = config.fillUsd && config.fillUsd > 0 ? config.fillUsd : null;
  const budget = Math.min(
    configured ?? DEFAULT_FILL_USD,
    config.maxSpendTotal,
  );
  if (!Number.isFinite(budget) || budget <= 0) return null;
  return budget;
}

function computeAgeSec(ts: number | null | undefined, nowMs: number): number | null {
  if (!ts || !Number.isFinite(ts)) return null;
  return Math.max(0, (nowMs - ts) / 1000);
}

function isFreshEnough(
  config: ArbitrageCoinConfig,
  polySnap: MarketSnapshot,
  kalshiSnap: MarketSnapshot,
  nowMs: number,
): boolean {
  if (polySnap.dataStatus !== "healthy" || kalshiSnap.dataStatus !== "healthy") {
    return false;
  }
  if (config.maxPriceStalenessSec != null) {
    const polyAge = computeAgeSec(polySnap.cryptoPriceTimestamp, nowMs);
    const kalshiAge = computeAgeSec(kalshiSnap.cryptoPriceTimestamp, nowMs);
    if (polyAge == null || kalshiAge == null) return false;
    if (polyAge > config.maxPriceStalenessSec) return false;
    if (kalshiAge > config.maxPriceStalenessSec) return false;
  }
  return true;
}

function buildCandidate(
  direction: ArbitrageDirection,
  polySnap: MarketSnapshot,
  kalshiSnap: MarketSnapshot,
  budgetUsd: number,
  config: ArbitrageCoinConfig,
): Candidate | null {
  const polyTokenId =
    direction === "upNo" ? polySnap.upTokenId : polySnap.downTokenId;
  const kalshiTokenId = direction === "upNo" ? "NO" : "YES";
  if (!polyTokenId) return null;

  const polyAsks = resolveAsks(polySnap, polyTokenId);
  const kalshiAsks = resolveAsks(kalshiSnap, kalshiTokenId);
  if (polyAsks.length === 0 || kalshiAsks.length === 0) return null;

  const estimate = computeFillEstimate(polyAsks, kalshiAsks, budgetUsd);
  if (!estimate) return null;
  if (estimate.totalCost < config.minSpendTotal) return null;
  if (estimate.totalCost > config.maxSpendTotal) return null;
  if (estimate.gap < config.minGap) return null;

  if (config.maxSpread != null) {
    const polySpread = computeSpread(polySnap, polyTokenId);
    const kalshiSpread = computeSpread(kalshiSnap, kalshiTokenId);
    if (polySpread == null || kalshiSpread == null) return null;
    if (polySpread > config.maxSpread) return null;
    if (kalshiSpread > config.maxSpread) return null;
  }

  if (config.minDepthValue != null) {
    const polyDepth = polySnap.orderBooks.get(polyTokenId)?.totalAskValue ?? null;
    const kalshiDepth = kalshiSnap.orderBooks.get(kalshiTokenId)?.totalAskValue ?? null;
    if (polyDepth == null || kalshiDepth == null) return null;
    if (polyDepth < config.minDepthValue) return null;
    if (kalshiDepth < config.minDepthValue) return null;
  }

  return {
    direction,
    estimate,
    polyTarget: direction === "upNo" ? "UP" : "DOWN",
    kalshiTarget: direction === "upNo" ? "DOWN" : "UP",
  };
}

/**
 * Build candidate without validation checks - used for post-delay fill
 * computation when we're already committed and cannot abort.
 */
function buildCandidateNoValidation(
  direction: ArbitrageDirection,
  polySnap: MarketSnapshot,
  kalshiSnap: MarketSnapshot,
  budgetUsd: number,
): Candidate | null {
  const polyTokenId =
    direction === "upNo" ? polySnap.upTokenId : polySnap.downTokenId;
  const kalshiTokenId = direction === "upNo" ? "NO" : "YES";
  if (!polyTokenId) return null;

  const polyAsks = resolveAsks(polySnap, polyTokenId);
  const kalshiAsks = resolveAsks(kalshiSnap, kalshiTokenId);
  if (polyAsks.length === 0 || kalshiAsks.length === 0) return null;

  const estimate = computeFillEstimate(polyAsks, kalshiAsks, budgetUsd);
  if (!estimate) return null;

  return {
    direction,
    estimate,
    polyTarget: direction === "upNo" ? "UP" : "DOWN",
    kalshiTarget: direction === "upNo" ? "DOWN" : "UP",
  };
}

function computeDisplayEstimate(
  direction: ArbitrageDirection,
  polySnap: MarketSnapshot,
  kalshiSnap: MarketSnapshot,
  budgetUsd: number,
): DisplayEstimate | null {
  const candidate = buildCandidateNoValidation(
    direction,
    polySnap,
    kalshiSnap,
    budgetUsd,
  );
  if (candidate?.estimate) {
    return { estimate: candidate.estimate, source: "orderbook" };
  }

  const polyTokenId =
    direction === "upNo" ? polySnap.upTokenId : polySnap.downTokenId;
  const kalshiTokenId = direction === "upNo" ? "NO" : "YES";
  if (!polyTokenId) return null;

  const polyAsk = polySnap.bestAsk.get(polyTokenId) ?? null;
  const kalshiAsk = kalshiSnap.bestAsk.get(kalshiTokenId) ?? null;
  if (polyAsk == null || kalshiAsk == null) return null;
  const costPerShare = polyAsk + kalshiAsk;
  if (!Number.isFinite(costPerShare) || costPerShare <= 0) return null;
  const shares = Math.floor(budgetUsd / costPerShare);
  if (shares <= 0) return null;

  const costPoly = shares * polyAsk;
  const costKalshi = shares * kalshiAsk;
  return {
    estimate: {
      shares,
      avgPoly: polyAsk,
      avgKalshi: kalshiAsk,
      costPoly,
      costKalshi,
      totalCost: costPoly + costKalshi,
      gap: 1 - costPerShare,
    },
    source: "best_ask",
  };
}

function computeSpread(
  snapshot: MarketSnapshot,
  tokenId: string,
): number | null {
  const bid = snapshot.bestBid.get(tokenId) ?? null;
  const ask = snapshot.bestAsk.get(tokenId) ?? null;
  if (bid == null || ask == null) return null;
  if (!Number.isFinite(bid) || !Number.isFinite(ask)) return null;
  return Math.max(0, ask - bid);
}

function chooseCandidate(
  upNo: Candidate | null,
  downYes: Candidate | null,
): Candidate | null {
  if (upNo && !downYes) return upNo;
  if (downYes && !upNo) return downYes;
  if (!upNo || !downYes) return null;

  const gapDelta = Math.abs(upNo.estimate.gap - downYes.estimate.gap);
  if (gapDelta <= DEFAULT_TIE_EPSILON) {
    const upPolyShare = upNo.estimate.costPoly / upNo.estimate.totalCost;
    const downPolyShare = downYes.estimate.costPoly / downYes.estimate.totalCost;
    if (upPolyShare > downPolyShare) return upNo;
    if (downPolyShare > upPolyShare) return downYes;
  }

  return upNo.estimate.gap >= downYes.estimate.gap ? upNo : downYes;
}

function shouldAttemptOfficialFetch(
  attempts: number,
  lastMs: number | null,
  nowMs: number,
): boolean {
  if (OFFICIAL_RETRY_LIMIT > 0 && attempts >= OFFICIAL_RETRY_LIMIT) {
    return false;
  }
  if (!lastMs) return true;
  const delay = Math.min(
    OFFICIAL_RETRY_MAX_MS,
    OFFICIAL_RETRY_BASE_MS * 2 ** Math.max(0, attempts - 1),
  );
  return nowMs - lastMs >= delay;
}

function isPlausibleUnderlying(
  price: number,
  threshold: number | null,
): boolean {
  if (threshold && threshold > 100 && price < 10) return false;
  return true;
}

function formatIso(ms: number | null | undefined): string {
  if (!ms || !Number.isFinite(ms)) return "n/a";
  return new Date(ms).toISOString();
}
