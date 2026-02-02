import type { MarketSnapshot, OrderBookLevel } from "./market-data-hub";
import type { SignalSnapshot, TokenSignal } from "./market-signals";
import type { CoinSymbol } from "./auto-market";
import { RunLogger } from "./run-logger";

function parseEnvFlag(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const normalized = raw.trim().toLowerCase();
  if (["false", "0", "off", "no"].includes(normalized)) return false;
  if (["true", "1", "on", "yes"].includes(normalized)) return true;
  return defaultValue;
}

const forcedMinConfidenceRaw = process.env.BACKTEST_FORCE_MIN_CONFIDENCE;
const FORCED_MIN_CONFIDENCE = Number.isFinite(Number(forcedMinConfidenceRaw))
  ? Number(forcedMinConfidenceRaw)
  : null;

const CONFIDENCE_WEIGHTS = {
  spread: 0.15,
  imbalance: 0.25,
  tradeFlow: 0.2,
  momentum: 0.25,
  staleness: 0.1,
  reference: 0.05,
};

const DEFAULT_EDGE_WEIGHTS = {
  gap: 0.3,
  depth: 0.15,
  imbalance: 0.15,
  velocity: 0.1,
  momentum: 0.15,
  volatility: 0.05,
  spread: 0.07,
  reference: 0.03,
};

const DEFAULT_EDGE_CAPS = {
  gap: 2,
  depth: 2,
  velocity: 2,
  momentum: 2,
  volatility: 2,
  spread: 1,
};

const DEFAULT_EDGE_TAU_SEC = 30;
const DEFAULT_GATE_MIN_MULTIPLIER = 0.35;
const DEFAULT_GATE_PER_SIGNAL_FLOOR = 0.4;

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function formatMetric(
  value: number | null | undefined,
  digits: number,
): string {
  if (value === null || value === undefined) return "n/a";
  if (!Number.isFinite(value)) return "n/a";
  return value.toFixed(digits);
}

export type SizeStrategy = "fixed" | "edge" | "depth" | "confidence";

export interface EdgeWeights {
  gap?: number;
  depth?: number;
  imbalance?: number;
  velocity?: number;
  momentum?: number;
  volatility?: number;
  spread?: number;
  reference?: number;
}

export interface EdgeCaps {
  gap?: number;
  depth?: number;
  velocity?: number;
  momentum?: number;
  volatility?: number;
  spread?: number;
}

export interface EdgeModelConfig {
  enabled?: boolean;
  weights?: EdgeWeights;
  caps?: EdgeCaps;
  stalenessTauSec?: number;
  requireSignals?: boolean;
  minScore?: number | null;
}

export type SizeModelMode = "legacy" | "edge_weighted";

export interface SizeModelConfig {
  mode?: SizeModelMode;
  edgeGamma?: number;
  minSizeFloor?: number;
  maxSizeCeil?: number;
  applyGateMultiplier?: boolean;
  confidenceWeight?: number;
  depthWeight?: number;
  spreadPenaltyWeight?: number;
}

export interface GateModelConfig {
  enabled?: boolean;
  minGateMultiplier?: number;
  perSignalFloor?: number;
  applyToSize?: boolean;
}

export interface LossGovernorConfig {
  enabled?: boolean;
  streakThreshold?: number;
  minDiffMultiplier?: number;
  sizeScaleMultiplier?: number;
}

export interface CrossModeOverrides {
  minDiffMultiplier?: number;
  maxShareMultiplier?: number;
  minShareMultiplier?: number;
  maxSpendMultiplier?: number;
  minConfidence?: number | null;
  minDepthValue?: number | null;
  minTradeVelocity?: number | null;
  sizeScaleMultiplier?: number;
  minRecoveryMultiple?: number | null;
  minLossToTrigger?: number | null;
}

export interface CrossModeConfig {
  splitTimeSec?: number;
  precision?: CrossModeOverrides;
  opportunistic?: CrossModeOverrides;
}

export interface TradeRule {
  tierSeconds: number;
  minimumPriceDifference: number;
  maximumSharePrice: number;
  minimumSharePrice: number;
  maximumSpend: number;
  minimumSpend: number;
  maxSpread?: number | null;
  minBookImbalance?: number | null;
  minDepthValue?: number | null;
  minTradeVelocity?: number | null;
  minMomentum?: number | null;
  minVolatility?: number | null;
  maxPriceStalenessSec?: number | null;
  minConfidence?: number | null;
  sizeStrategy?: SizeStrategy;
  sizeScale?: number | null;
  maxOpenExposure?: number | null;
}

export interface CrossTradeRule extends TradeRule {
  minRecoveryMultiple?: number | null;
  minLossToTrigger?: number | null;
}

export interface CrossTradeConfig {
  tradeAllowedTimeLeft: number;
  rules: CrossTradeRule[];
}

export interface TimedTradeConfig {
  tradeAllowedTimeLeft: number;
  rules: TradeRule[];
  cross?: CrossTradeConfig;
  edgeModel?: EdgeModelConfig;
  sizeModel?: SizeModelConfig;
  gateModel?: GateModelConfig;
  lossGovernor?: LossGovernorConfig;
  crossModes?: CrossModeConfig;
}

interface FakePosition {
  tokenId: string;
  outcome: string;
  shares: number;
  avgPrice: number;
  cost: number;
  openedAt: number;
}

interface CriteriaStatus {
  timeOk: boolean;
  gapOk: boolean;
  priceOk: boolean;
  spreadOk: boolean;
  depthOk: boolean;
  imbalanceOk: boolean;
  velocityOk: boolean;
  momentumOk: boolean;
  volatilityOk: boolean;
  stalenessOk: boolean;
  confidenceOk: boolean;
  exposureOk: boolean;
}

interface CriteriaLogState {
  timeOk: number;
  gapOk: number;
  priceOk: number;
  spreadOk: number;
  depthOk: number;
  imbalanceOk: number;
  velocityOk: number;
  momentumOk: number;
  volatilityOk: number;
  stalenessOk: number;
  confidenceOk: number;
  exposureOk: number;
}

interface PendingOrder {
  dueMs: number;
  tokenId: string;
  outcome: string;
  rule: TradeRule;
}

interface CoinTradeState {
  position: FakePosition | null;
  pendingOrder: PendingOrder | null;
  simulatedConsumption: Map<string, Map<number, number>>;
  simulatedBidConsumption: Map<string, Map<number, number>>;
  crossed: boolean;
  realizedPnl: number;
  lossStreak: number;
  marketHadTrade: boolean;
  marketTradeCounted: boolean;
  lastDecisionMs: number;
  lastSignalBlockMs: number;
  lastResolvedSlug: string | null;
  activeTierSec: number | null;
  criteriaStatus: CriteriaStatus;
  criteriaLastLoggedMs: CriteriaLogState;
  crossDebugLastMs: number;
  crossDebugLastKey: string | null;
}

interface SignalStats {
  samples: number;
  spreadSum: number;
  spreadCount: number;
  depthSum: number;
  depthCount: number;
  confidenceSum: number;
  confidenceCount: number;
}

export interface ProfileEngineOptions {
  advancedSignals?: boolean;
  signalDebug?: boolean;
  decisionLatencyMs?: number;
  crossDebug?: boolean;
  crossAllowNoFlip?: boolean;
}

export interface ProfileSummary {
  runtimeSec: number;
  totalTrades: number;
  wins: number;
  losses: number;
  totalProfit: number;
  openExposure: number;
}

export interface ProfileMarketView {
  coin: CoinSymbol;
  marketName: string;
  marketSlug: string;
  timeLeftSec: number | null;
  priceToBeat: number;
  referencePrice: number;
  referenceSource: "price_to_beat" | "historical" | "html" | "missing";
  cryptoPrice: number;
  priceDiff: number | null;
  favoredOutcome: string | null;
  bestAsk: number | null;
  bestBid: number | null;
  totalAskValue: number | null;
  totalBidValue: number | null;
  dataStatus: "unknown" | "healthy" | "stale";
  positionShares: number;
  positionCost: number;
  positionAvgPrice: number;
  crossed: boolean;
  realizedPnl: number;
  marketHadTrade: boolean;
  lastResult: string | null;
  signalSpread?: number | null;
  signalImbalance?: number | null;
  signalDepthValue?: number | null;
  signalConfidence?: number | null;
}

const CRITERIA_LOG_COOLDOWN_MS = 15000;
const CROSS_LOG_COOLDOWN_MS = 30000;

export class ProfileEngine {
  private name: string;
  private configs: Map<CoinSymbol, TimedTradeConfig>;
  private logger: RunLogger;
  private startMs = 0;
  private summary: ProfileSummary = {
    runtimeSec: 0,
    totalTrades: 0,
    wins: 0,
    losses: 0,
    totalProfit: 0,
    openExposure: 0,
  };
  private pnlHistory: number[] = [];
  private coinStates: Map<CoinSymbol, CoinTradeState> = new Map();
  private lastSnapshots: Map<CoinSymbol, MarketSnapshot> = new Map();
  private lastResultByCoin: Map<CoinSymbol, string | null> = new Map();
  private advancedSignals: boolean;
  private signalDebug: boolean;
  private crossDebug: boolean;
  private crossAllowNoFlip: boolean;
  private decisionLatencyMs: number | null;
  private signalStats: SignalStats = {
    samples: 0,
    spreadSum: 0,
    spreadCount: 0,
    depthSum: 0,
    depthCount: 0,
    confidenceSum: 0,
    confidenceCount: 0,
  };

  constructor(
    name: string,
    configs: Map<CoinSymbol, TimedTradeConfig>,
    logger: RunLogger,
    startTimeMs?: number,
    options: ProfileEngineOptions = {},
  ) {
    this.name = name;
    this.configs = configs;
    this.logger = logger;
    this.startMs = startTimeMs ?? Date.now();
    this.advancedSignals = options.advancedSignals === true;
    const envDebug = parseEnvFlag("BACKTEST_SIGNAL_DEBUG", false);
    this.signalDebug = options.signalDebug ?? envDebug;
    const envCrossDebug = parseEnvFlag("BACKTEST_CROSS_DEBUG", false);
    this.crossDebug = options.crossDebug ?? envCrossDebug;
    const envCrossAllowNoFlip = parseEnvFlag(
      "BACKTEST_CROSS_ALLOW_NO_FLIP",
      false,
    );
    this.crossAllowNoFlip = options.crossAllowNoFlip ?? envCrossAllowNoFlip;
    const rawLatency = options.decisionLatencyMs;
    this.decisionLatencyMs =
      typeof rawLatency === "number" && Number.isFinite(rawLatency)
        ? Math.max(0, Math.floor(rawLatency))
        : null;
    for (const coin of configs.keys()) {
      this.coinStates.set(coin, this.createCoinState());
    }
  }

  getName(): string {
    return this.name;
  }

  getSummary(): ProfileSummary {
    return { ...this.summary };
  }

  getPnlHistory(): number[] {
    return [...this.pnlHistory];
  }

  getLogs(): string[] {
    return this.logger.getRecentLines();
  }

  getSignalStats(): {
    samples: number;
    avgSpread: number | null;
    avgDepthValue: number | null;
    avgConfidence: number | null;
  } {
    return {
      samples: this.signalStats.samples,
      avgSpread:
        this.signalStats.spreadCount > 0
          ? this.signalStats.spreadSum / this.signalStats.spreadCount
          : null,
      avgDepthValue:
        this.signalStats.depthCount > 0
          ? this.signalStats.depthSum / this.signalStats.depthCount
          : null,
      avgConfidence:
        this.signalStats.confidenceCount > 0
          ? this.signalStats.confidenceSum / this.signalStats.confidenceCount
          : null,
    };
  }

  getPendingCoins(): Set<CoinSymbol> {
    const pending = new Set<CoinSymbol>();
    for (const [coin, state] of this.coinStates.entries()) {
      if (state.pendingOrder) {
        pending.add(coin);
      }
    }
    return pending;
  }

  getNextPendingTime(): number | null {
    let next: number | null = null;
    for (const state of this.coinStates.values()) {
      const due = state.pendingOrder?.dueMs;
      if (due === undefined) continue;
      if (next === null || due < next) {
        next = due;
      }
    }
    return next;
  }

  evaluate(
    snapshots: Map<CoinSymbol, MarketSnapshot>,
    nowMs: number = Date.now(),
    coinFilter?: Set<CoinSymbol>,
  ): void {
    this.summary.runtimeSec = (nowMs - this.startMs) / 1000;

    let openExposure = 0;

    for (const [coin, config] of this.configs.entries()) {
      const coinState = this.coinStates.get(coin);
      if (coinState?.position) {
        openExposure += coinState.position.cost;
      }

      if (coinFilter && !coinFilter.has(coin)) {
        continue;
      }

      const snapshot = snapshots.get(coin);
      if (!snapshot) continue;

      const previous = this.lastSnapshots.get(coin);
      if (previous && previous.slug !== snapshot.slug) {
        this.resolveMarket(coin, previous);
        this.resetCoinState(coin);
        this.logger.clearRecentLines();
        this.logger.log(
          `${coin.toUpperCase()} new market detected (${snapshot.slug}), recent logs cleared.`,
        );
      }

      if (
        coinState?.pendingOrder &&
        nowMs >= coinState.pendingOrder.dueMs &&
        !coinState.position
      ) {
        const pending = coinState.pendingOrder;
        coinState.pendingOrder = null;
        this.executeTrade(
          coin,
          snapshot,
          pending.tokenId,
          pending.outcome,
          pending.rule,
          config,
          nowMs,
        );
      }

      this.evaluateMarket(coin, snapshot, config, nowMs);

      this.lastSnapshots.set(coin, snapshot);
    }

    this.summary.openExposure = openExposure;
    this.pnlHistory.push(this.summary.totalProfit);
    if (this.pnlHistory.length > 180) {
      this.pnlHistory.shift();
    }
  }

  getMarketViews(): ProfileMarketView[] {
    const views: ProfileMarketView[] = [];
    for (const [coin, snapshot] of this.lastSnapshots.entries()) {
      const state = this.coinStates.get(coin);
      const lastResult = this.lastResultByCoin.get(coin) || null;
      const config = this.configs.get(coin) || null;

      const threshold =
        snapshot.priceToBeat > 0
          ? snapshot.priceToBeat
          : snapshot.referencePrice;
      const hasPrice = threshold > 0 && snapshot.cryptoPrice > 0;
      const priceDiff = hasPrice
        ? Math.abs(snapshot.cryptoPrice - threshold)
        : null;
      const favoredUp = hasPrice ? snapshot.cryptoPrice >= threshold : null;
      const favoredOutcome = hasPrice
        ? favoredUp
          ? snapshot.upOutcome
          : snapshot.downOutcome
        : null;

      const favoredTokenId =
        favoredOutcome === null
          ? null
          : favoredOutcome === snapshot.upOutcome
            ? snapshot.upTokenId
            : snapshot.downTokenId;

      const bestAsk = favoredTokenId
        ? snapshot.bestAsk.get(favoredTokenId) || null
        : null;
      const bestBid = favoredTokenId
        ? snapshot.bestBid.get(favoredTokenId) || null
        : null;
      const book = favoredTokenId
        ? snapshot.orderBooks.get(favoredTokenId)
        : undefined;

      let signalSpread: number | null = null;
      let signalImbalance: number | null = null;
      let signalDepthValue: number | null = null;
      let signalConfidence: number | null = null;
      if (this.advancedSignals && snapshot.signals && favoredTokenId) {
        const tokenSignal = snapshot.signals.tokenSignals.get(favoredTokenId);
        if (tokenSignal) {
          signalSpread = tokenSignal.spread ?? null;
          signalImbalance = tokenSignal.bookImbalance ?? null;
          signalDepthValue = tokenSignal.depthValue ?? null;
        }
        if (config && snapshot.timeLeftSec !== null) {
          const activeRule = this.selectActiveRule(config, snapshot.timeLeftSec);
          signalConfidence = this.computeConfidence(
            activeRule,
            snapshot.signals,
            tokenSignal ?? null,
            favoredUp,
            priceDiff,
          );
        }
      }

      views.push({
        coin,
        marketName: snapshot.marketName,
        marketSlug: snapshot.slug,
        timeLeftSec: snapshot.timeLeftSec,
        priceToBeat: snapshot.priceToBeat,
        referencePrice: snapshot.referencePrice,
        referenceSource: snapshot.referenceSource,
        cryptoPrice: snapshot.cryptoPrice,
        priceDiff,
        favoredOutcome,
        bestAsk,
        bestBid,
        totalAskValue: book ? book.totalAskValue : null,
        totalBidValue: book ? book.totalBidValue : null,
        dataStatus: snapshot.dataStatus,
        positionShares: state?.position ? state.position.shares : 0,
        positionCost: state?.position ? state.position.cost : 0,
        positionAvgPrice: state?.position ? state.position.avgPrice : 0,
        crossed: state?.crossed ?? false,
        realizedPnl: state?.realizedPnl ?? 0,
        marketHadTrade: state?.marketHadTrade ?? false,
        lastResult,
        signalSpread,
        signalImbalance,
        signalDepthValue,
        signalConfidence,
      });
    }

    return views;
  }

  private createCoinState(): CoinTradeState {
    return {
      position: null,
      pendingOrder: null,
      simulatedConsumption: new Map(),
      simulatedBidConsumption: new Map(),
      crossed: false,
      realizedPnl: 0,
      lossStreak: 0,
      marketHadTrade: false,
      marketTradeCounted: false,
      lastDecisionMs: 0,
      lastSignalBlockMs: 0,
      lastResolvedSlug: null,
      activeTierSec: null,
      crossDebugLastMs: 0,
      crossDebugLastKey: null,
      criteriaStatus: {
        timeOk: false,
        gapOk: false,
        priceOk: false,
        spreadOk: false,
        depthOk: false,
        imbalanceOk: false,
        velocityOk: false,
        momentumOk: false,
        volatilityOk: false,
        stalenessOk: false,
        confidenceOk: false,
        exposureOk: false,
      },
      criteriaLastLoggedMs: {
        timeOk: 0,
        gapOk: 0,
        priceOk: 0,
        spreadOk: 0,
        depthOk: 0,
        imbalanceOk: 0,
        velocityOk: 0,
        momentumOk: 0,
        volatilityOk: 0,
        stalenessOk: 0,
        confidenceOk: 0,
        exposureOk: 0,
      },
    };
  }

  private resetCoinState(coin: CoinSymbol): void {
    const state = this.coinStates.get(coin);
    const next = this.createCoinState();
    if (state) {
      next.lossStreak = state.lossStreak;
    }
    this.coinStates.set(coin, next);
  }

  private evaluateMarket(
    coin: CoinSymbol,
    snapshot: MarketSnapshot,
    config: TimedTradeConfig,
    nowMs: number,
  ): void {
    const coinState = this.coinStates.get(coin);
    if (!coinState) return;

    if (snapshot.timeLeftSec !== null && snapshot.timeLeftSec <= 0) {
      this.resolveMarket(coin, snapshot);
      return;
    }

    if (coinState.position) {
      this.evaluateCross(coin, snapshot, config, nowMs);
      return;
    }

    const threshold =
      snapshot.priceToBeat > 0
        ? snapshot.priceToBeat
        : snapshot.referencePrice;
    const hasPrice = threshold > 0 && snapshot.cryptoPrice > 0;
    const priceDiff = hasPrice
      ? Math.abs(snapshot.cryptoPrice - threshold)
      : null;
    const timeOk =
      snapshot.timeLeftSec !== null &&
      snapshot.timeLeftSec <= config.tradeAllowedTimeLeft;
    const activeRule = timeOk
      ? this.selectActiveRule(config, snapshot.timeLeftSec)
      : null;
    const lossConfig = config.lossGovernor;
    const lossEnabled = lossConfig ? lossConfig.enabled !== false : false;
    let lossDiffMultiplier = 1;
    let lossSizeMultiplier = 1;
    if (
      lossEnabled &&
      coinState.lossStreak >= (lossConfig.streakThreshold ?? 2)
    ) {
      lossDiffMultiplier = lossConfig.minDiffMultiplier ?? 1.2;
      lossSizeMultiplier = lossConfig.sizeScaleMultiplier ?? 0.7;
    }
    const effectiveRule = activeRule
      ? {
          ...activeRule,
          minimumPriceDifference:
            activeRule.minimumPriceDifference * lossDiffMultiplier,
          sizeScale: (activeRule.sizeScale ?? 1) * lossSizeMultiplier,
        }
      : null;
    if (activeRule && coinState.activeTierSec !== activeRule.tierSeconds) {
      coinState.activeTierSec = activeRule.tierSeconds;
      this.logger.log(
        `${coin.toUpperCase()} tier active (${activeRule.tierSeconds}s)`,
      );
    }
    if (!activeRule) {
      coinState.activeTierSec = null;
    }
    const gapOk =
      priceDiff !== null &&
      effectiveRule !== null &&
      priceDiff >= effectiveRule.minimumPriceDifference;

    const favoredUp = hasPrice ? snapshot.cryptoPrice >= threshold : null;
    const favoredTokenId =
      favoredUp === null
        ? null
        : favoredUp
          ? snapshot.upTokenId
          : snapshot.downTokenId;
    const favoredOutcome =
      favoredUp === null
        ? null
        : favoredUp
          ? snapshot.upOutcome
          : snapshot.downOutcome;

    const bestAsk = favoredTokenId
      ? this.getBestAsk(snapshot, coinState, favoredTokenId)
      : null;

    const priceOk =
      snapshot.dataStatus === "healthy" &&
      bestAsk !== null &&
      activeRule !== null &&
      bestAsk <= activeRule.maximumSharePrice &&
      bestAsk >= activeRule.minimumSharePrice;

    const signals = this.advancedSignals ? snapshot.signals : undefined;
    const tokenSignal =
      favoredTokenId && signals
        ? signals.tokenSignals.get(favoredTokenId) ?? null
        : null;

    let spreadOk = true;
    if (this.advancedSignals && activeRule?.maxSpread !== null && activeRule?.maxSpread !== undefined) {
      spreadOk = tokenSignal?.spread !== null && tokenSignal?.spread !== undefined
        ? tokenSignal.spread <= activeRule.maxSpread
        : false;
    }

    let depthOk = true;
    if (this.advancedSignals && activeRule?.minDepthValue !== null && activeRule?.minDepthValue !== undefined) {
      depthOk = tokenSignal?.depthValue !== null && tokenSignal?.depthValue !== undefined
        ? tokenSignal.depthValue >= activeRule.minDepthValue
        : false;
    }

    let imbalanceOk = true;
    if (
      this.advancedSignals &&
      activeRule?.minBookImbalance !== null &&
      activeRule?.minBookImbalance !== undefined
    ) {
      imbalanceOk =
        tokenSignal?.bookImbalance !== null &&
        tokenSignal?.bookImbalance !== undefined
          ? tokenSignal.bookImbalance >= activeRule.minBookImbalance
          : false;
    }

    let velocityOk = true;
    if (
      this.advancedSignals &&
      activeRule?.minTradeVelocity !== null &&
      activeRule?.minTradeVelocity !== undefined
    ) {
      velocityOk =
        signals?.tradeVelocity !== null && signals?.tradeVelocity !== undefined
          ? signals.tradeVelocity >= activeRule.minTradeVelocity
          : false;
    }

    let momentumOk = true;
    if (
      this.advancedSignals &&
      activeRule?.minMomentum !== null &&
      activeRule?.minMomentum !== undefined
    ) {
      if (signals?.priceMomentum === null || signals?.priceMomentum === undefined || favoredUp === null) {
        momentumOk = false;
      } else {
        momentumOk = favoredUp
          ? signals.priceMomentum >= activeRule.minMomentum
          : signals.priceMomentum <= -activeRule.minMomentum;
      }
    }

    let volatilityOk = true;
    if (
      this.advancedSignals &&
      activeRule?.minVolatility !== null &&
      activeRule?.minVolatility !== undefined
    ) {
      volatilityOk =
        signals?.priceVolatility !== null && signals?.priceVolatility !== undefined
          ? signals.priceVolatility >= activeRule.minVolatility
          : false;
    }

    let stalenessOk = true;
    if (
      this.advancedSignals &&
      activeRule?.maxPriceStalenessSec !== null &&
      activeRule?.maxPriceStalenessSec !== undefined
    ) {
      stalenessOk =
        signals?.priceStalenessSec !== null &&
        signals?.priceStalenessSec !== undefined
          ? signals.priceStalenessSec <= activeRule.maxPriceStalenessSec
          : false;
    }

    const confidence = this.advancedSignals
      ? this.computeConfidence(
          effectiveRule ?? activeRule,
          signals,
          tokenSignal,
          favoredUp,
          priceDiff,
        )
      : null;

    const minConfidence =
      FORCED_MIN_CONFIDENCE !== null
        ? FORCED_MIN_CONFIDENCE
        : activeRule?.minConfidence ?? null;
    let confidenceOk = true;
    if (this.advancedSignals && minConfidence !== null && minConfidence !== undefined) {
      confidenceOk = confidence !== null ? confidence >= minConfidence : false;
    }

    let exposureOk = true;
    if (
      this.advancedSignals &&
      activeRule?.maxOpenExposure !== null &&
      activeRule?.maxOpenExposure !== undefined
    ) {
      const openExposure = this.getOpenExposure();
      exposureOk = openExposure <= activeRule.maxOpenExposure;
    }

    const edgeEnabled = config.edgeModel ? config.edgeModel.enabled !== false : false;
    const gateEnabled = config.gateModel ? config.gateModel.enabled !== false : false;

    const edgeScore = this.computeEdgeScore(
      effectiveRule ?? activeRule,
      signals ?? null,
      tokenSignal ?? null,
      favoredUp,
      priceDiff,
      config.edgeModel,
    );
    const minEdgeScore = config.edgeModel?.minScore ?? null;
    let edgeOk = true;
    if (this.advancedSignals && edgeEnabled && minEdgeScore !== null && minEdgeScore !== undefined) {
      edgeOk = edgeScore !== null ? edgeScore >= minEdgeScore : false;
    }

    const gate = this.computeGateMultiplier(
      effectiveRule ?? activeRule,
      signals ?? null,
      tokenSignal ?? null,
      favoredUp,
      confidence,
      config.gateModel,
    );
    const gateMultiplier = gate.multiplier;
    const gateBlocked = gate.blocked;

    if (this.advancedSignals) {
      this.recordSignalStats(tokenSignal, confidence);
    }

    this.logCriteriaTransitions(coin, coinState, effectiveRule ?? activeRule, {
      nowMs,
      timeOk,
      gapOk,
      priceOk,
      spreadOk,
      depthOk,
      imbalanceOk,
      velocityOk,
      momentumOk,
      volatilityOk,
      stalenessOk,
      confidenceOk,
      exposureOk,
      timeLeftSec: snapshot.timeLeftSec,
      priceDiff,
      bestAsk,
      favoredOutcome,
      spread: tokenSignal?.spread ?? null,
      depthValue: tokenSignal?.depthValue ?? null,
      bookImbalance: tokenSignal?.bookImbalance ?? null,
      tradeVelocity: signals?.tradeVelocity ?? null,
      momentum: signals?.priceMomentum ?? null,
      volatility: signals?.priceVolatility ?? null,
      stalenessSec: signals?.priceStalenessSec ?? null,
      confidence,
    });

    if (
      this.signalDebug &&
      this.advancedSignals &&
      timeOk &&
      gapOk &&
      priceOk &&
      !gateEnabled &&
      (!spreadOk ||
        !depthOk ||
        !imbalanceOk ||
        !velocityOk ||
        !momentumOk ||
        !volatilityOk ||
        !stalenessOk ||
        !confidenceOk ||
        !exposureOk) &&
      nowMs - coinState.lastSignalBlockMs > CRITERIA_LOG_COOLDOWN_MS
    ) {
      coinState.lastSignalBlockMs = nowMs;
      this.logger.log(
        `${coin.toUpperCase()} blocked by signals: spread=${spreadOk} depth=${depthOk} imbalance=${imbalanceOk} velocity=${velocityOk} momentum=${momentumOk} volatility=${volatilityOk} staleness=${stalenessOk} confidence=${confidenceOk} exposure=${exposureOk}`,
        "WARN",
      );
    }

    if (snapshot.dataStatus !== "healthy") {
      return;
    }

    if (coinState.position || coinState.pendingOrder) return;
    if (snapshot.timeLeftSec === null) return;
    if (
      !timeOk ||
      !gapOk ||
      !priceOk ||
      !exposureOk
    )
      return;
    if (edgeEnabled && !edgeOk) {
      if (this.signalDebug) {
        this.logger.log(
          `${coin.toUpperCase()} blocked by edge score: ${formatMetric(
            edgeScore,
            2,
          )}`,
          "WARN",
        );
      }
      return;
    }
    if (gateEnabled) {
      if (gateBlocked) {
        if (this.signalDebug) {
          this.logger.log(
            `${coin.toUpperCase()} blocked by gate multiplier (${gateMultiplier.toFixed(
              2,
            )})`,
            "WARN",
          );
        }
        return;
      }
    } else if (
      !spreadOk ||
      !depthOk ||
      !imbalanceOk ||
      !velocityOk ||
      !momentumOk ||
      !volatilityOk ||
      !stalenessOk ||
      !confidenceOk
    ) {
      return;
    }
    if (!favoredTokenId || !favoredOutcome) return;

    if (nowMs - coinState.lastDecisionMs < 200) return;
    coinState.lastDecisionMs = nowMs;

    if (this.signalDebug) {
      const exposure = this.getOpenExposure();
      this.logger.log(
        `${coin.toUpperCase()} trade criteria met: timeOk=${timeOk} gapOk=${gapOk} priceOk=${priceOk} spreadOk=${spreadOk} depthOk=${depthOk} imbalanceOk=${imbalanceOk} velocityOk=${velocityOk} momentumOk=${momentumOk} volatilityOk=${volatilityOk} stalenessOk=${stalenessOk} confidenceOk=${confidenceOk} exposureOk=${exposureOk}`,
      );
      this.logger.log(
        `${coin.toUpperCase()} trade inputs: timeLeft=${formatMetric(snapshot.timeLeftSec, 0)}s diff=${formatMetric(priceDiff, 2)} ask=${formatMetric(bestAsk, 4)} spread=${formatMetric(tokenSignal?.spread ?? null, 4)} depth=${formatMetric(tokenSignal?.depthValue ?? null, 2)} imbalance=${formatMetric(tokenSignal?.bookImbalance ?? null, 2)} velocity=${formatMetric(signals?.tradeVelocity ?? null, 2)} momentum=${formatMetric(signals?.priceMomentum ?? null, 2)} volatility=${formatMetric(signals?.priceVolatility ?? null, 2)} staleness=${formatMetric(signals?.priceStalenessSec ?? null, 1)} conf=${formatMetric(confidence, 2)} edge=${formatMetric(edgeScore, 2)} gate=${formatMetric(gateMultiplier, 2)} exposure=${formatMetric(exposure, 2)} maxExposure=${formatMetric(activeRule?.maxOpenExposure ?? null, 2)}`,
      );
      if (effectiveRule ?? activeRule) {
        const logRule = effectiveRule ?? activeRule;
        this.logger.log(
          `${coin.toUpperCase()} trade thresholds: minDiff=${formatMetric(logRule.minimumPriceDifference, 2)} minShare=${formatMetric(logRule.minimumSharePrice, 4)} maxShare=${formatMetric(logRule.maximumSharePrice, 4)} maxSpend=${formatMetric(logRule.maximumSpend, 2)} minSpend=${formatMetric(logRule.minimumSpend, 2)} maxSpread=${formatMetric(logRule.maxSpread ?? null, 4)} minImbalance=${formatMetric(logRule.minBookImbalance ?? null, 2)} minDepth=${formatMetric(logRule.minDepthValue ?? null, 2)} minVelocity=${formatMetric(logRule.minTradeVelocity ?? null, 2)} minMomentum=${formatMetric(logRule.minMomentum ?? null, 2)} minVolatility=${formatMetric(logRule.minVolatility ?? null, 2)} maxStale=${formatMetric(logRule.maxPriceStalenessSec ?? null, 1)} minConf=${formatMetric(minConfidence ?? null, 2)} sizeStrategy=${logRule.sizeStrategy ?? "fixed"} sizeScale=${formatMetric(logRule.sizeScale ?? 1, 2)}`,
        );
      }
    }

    const latencyMs =
      this.decisionLatencyMs !== null
        ? this.decisionLatencyMs
        : 250 + Math.floor(Math.random() * 251);
    coinState.pendingOrder = {
      dueMs: nowMs + latencyMs,
      tokenId: favoredTokenId,
      outcome: favoredOutcome,
      rule: activeRule,
    };

    if (this.signalDebug && confidence !== null) {
      this.logger.log(
        `${coin.toUpperCase()} preparing fake buy (${favoredOutcome}) conf=${confidence.toFixed(
          2,
        )} spread=${tokenSignal?.spread?.toFixed(4) ?? "n/a"} depth=${tokenSignal?.depthValue?.toFixed(
          2,
        ) ?? "n/a"} latency=${latencyMs}ms`,
      );
    } else {
      this.logger.log(
        `${coin.toUpperCase()} preparing fake buy (${favoredOutcome}) with ${latencyMs}ms latency`,
      );
    }
  }

  private executeTrade(
    coin: CoinSymbol,
    snapshot: MarketSnapshot,
    tokenId: string,
    outcome: string,
    rule: TradeRule,
    config: TimedTradeConfig,
    nowMs: number,
  ): void {
    const state = this.coinStates.get(coin);
    if (!state || state.position) return;

    const lossConfig = config.lossGovernor;
    const lossEnabled = lossConfig ? lossConfig.enabled !== false : false;
    let lossDiffMultiplier = 1;
    let lossSizeMultiplier = 1;
    if (
      lossEnabled &&
      state.lossStreak >= (lossConfig.streakThreshold ?? 2)
    ) {
      lossDiffMultiplier = lossConfig.minDiffMultiplier ?? 1.2;
      lossSizeMultiplier = lossConfig.sizeScaleMultiplier ?? 0.7;
    }
    const effectiveRule: TradeRule = {
      ...rule,
      minimumPriceDifference: rule.minimumPriceDifference * lossDiffMultiplier,
      sizeScale: (rule.sizeScale ?? 1) * lossSizeMultiplier,
    };

    const asks = this.getAdjustedAsks(snapshot, state, tokenId);
    if (asks.length === 0) {
      this.logger.log(`${coin.toUpperCase()} no asks available`, "WARN");
      return;
    }

    const threshold =
      snapshot.priceToBeat > 0
        ? snapshot.priceToBeat
        : snapshot.referencePrice;
    const hasPrice = threshold > 0 && snapshot.cryptoPrice > 0;
    const priceDiff = hasPrice
      ? Math.abs(snapshot.cryptoPrice - threshold)
      : null;
    const favoredUp = hasPrice ? snapshot.cryptoPrice >= threshold : null;
    const signals = this.advancedSignals ? snapshot.signals : null;
    const tokenSignal =
      this.advancedSignals && signals
        ? signals.tokenSignals.get(tokenId) ?? null
        : null;
    const confidence = this.advancedSignals
      ? this.computeConfidence(
          effectiveRule,
          signals,
          tokenSignal,
          favoredUp,
          priceDiff,
        )
      : null;
    const edgeScore = this.computeEdgeScore(
      effectiveRule,
      signals,
      tokenSignal,
      favoredUp,
      priceDiff,
      config.edgeModel,
    );
    const gate = this.computeGateMultiplier(
      effectiveRule,
      signals,
      tokenSignal,
      favoredUp,
      confidence,
      config.gateModel,
    );
    const gateEnabled = config.gateModel ? config.gateModel.enabled !== false : false;

    const resolvedMaxSpend = this.resolveMaxSpend(effectiveRule, snapshot, tokenId, {
      sizeScale: effectiveRule.sizeScale ?? 1,
      edgeScore,
      gateMultiplier: gate.multiplier,
      applyGate: gateEnabled ? config.gateModel?.applyToSize ?? true : false,
      sizeModel: config.sizeModel,
      confidence,
      tokenSignal,
    });
    const fill = this.simulateFill(asks, effectiveRule, resolvedMaxSpend);
    if (!fill) {
      this.logger.log(
        `${coin.toUpperCase()} trade skipped (insufficient liquidity)`,
        "WARN",
      );
      return;
    }

    state.position = {
      tokenId,
      outcome,
      shares: fill.shares,
      avgPrice: fill.avgPrice,
      cost: fill.cost,
      openedAt: nowMs,
    };

    this.applyConsumption(state, tokenId, fill.fills);

    state.marketHadTrade = true;
    if (!state.marketTradeCounted) {
      this.summary.totalTrades += 1;
      state.marketTradeCounted = true;
    }
    if (this.signalDebug && resolvedMaxSpend !== effectiveRule.maximumSpend) {
      this.logger.log(
        `${coin.toUpperCase()} FAKE BUY ${fill.shares.toFixed(2)} @ ${fill.avgPrice.toFixed(
          4,
        )} (cost ${fill.cost.toFixed(2)}, max ${resolvedMaxSpend.toFixed(2)})`,
      );
    } else {
      this.logger.log(
        `${coin.toUpperCase()} FAKE BUY ${fill.shares.toFixed(2)} @ ${fill.avgPrice.toFixed(
          4,
        )} (cost ${fill.cost.toFixed(2)})`,
      );
    }
  }

  private evaluateCross(
    coin: CoinSymbol,
    snapshot: MarketSnapshot,
    config: TimedTradeConfig,
    nowMs: number,
  ): void {
    const state = this.coinStates.get(coin);
    if (!state || !state.position) return;
    if (state.crossed) return;
    const crossConfig = config.cross;
    if (!crossConfig) return;
    const edgeEnabled = config.edgeModel ? config.edgeModel.enabled !== false : false;
    const gateEnabled = config.gateModel ? config.gateModel.enabled !== false : false;
    const timeLeftSec = snapshot.timeLeftSec;
    if (timeLeftSec === null || timeLeftSec <= 0) return;
    if (timeLeftSec > crossConfig.tradeAllowedTimeLeft) return;
    if (snapshot.dataStatus !== "healthy") return;

    const threshold =
      snapshot.priceToBeat > 0
        ? snapshot.priceToBeat
        : snapshot.referencePrice;
    const hasPrice = threshold > 0 && snapshot.cryptoPrice > 0;
    if (!hasPrice) return;

    const priceDiff = Math.abs(snapshot.cryptoPrice - threshold);
    const favoredUp = snapshot.cryptoPrice >= threshold;
    const favoredOutcome = favoredUp ? snapshot.upOutcome : snapshot.downOutcome;

    const shouldFlip = favoredOutcome !== state.position.outcome;
    const targetUp = shouldFlip
      ? favoredUp
      : this.crossAllowNoFlip
        ? !favoredUp
        : favoredUp;
    const targetTokenId = targetUp ? snapshot.upTokenId : snapshot.downTokenId;
    const targetOutcome = targetUp ? snapshot.upOutcome : snapshot.downOutcome;

    if (!shouldFlip && !this.crossAllowNoFlip) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "no_flip", {
        timeLeftSec,
        priceDiff,
      });
      return;
    }

    const baseRule = this.selectCrossRule(crossConfig, timeLeftSec);
    const activeRule = baseRule
      ? this.applyCrossModeOverrides(baseRule, config, crossConfig, timeLeftSec)
      : null;
    if (!activeRule) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "no_rule", {
        timeLeftSec,
      });
      return;
    }
    if (priceDiff < activeRule.minimumPriceDifference) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "min_diff", {
        timeLeftSec,
        priceDiff,
        minDiff: activeRule.minimumPriceDifference,
      });
      return;
    }

    const bestAsk = this.getBestAsk(snapshot, state, targetTokenId);
    if (
      bestAsk === null ||
      bestAsk > activeRule.maximumSharePrice ||
      bestAsk < activeRule.minimumSharePrice
    ) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "ask_range", {
        timeLeftSec,
        priceDiff,
        bestAsk,
        minShare: activeRule.minimumSharePrice,
        maxShare: activeRule.maximumSharePrice,
      });
      return;
    }

    const signals = this.advancedSignals ? snapshot.signals : undefined;
    const tokenSignal =
      this.advancedSignals && signals
        ? signals.tokenSignals.get(targetTokenId) ?? null
        : null;

    if (this.advancedSignals) {
      let spreadOk = true;
      if (
        activeRule.maxSpread !== null &&
        activeRule.maxSpread !== undefined
      ) {
        spreadOk =
          tokenSignal?.spread !== null &&
          tokenSignal?.spread !== undefined &&
          tokenSignal.spread <= activeRule.maxSpread;
      }

      let depthOk = true;
      if (
        activeRule.minDepthValue !== null &&
        activeRule.minDepthValue !== undefined
      ) {
        depthOk =
          tokenSignal?.depthValue !== null &&
          tokenSignal?.depthValue !== undefined &&
          tokenSignal.depthValue >= activeRule.minDepthValue;
      }

      let imbalanceOk = true;
      if (
        activeRule.minBookImbalance !== null &&
        activeRule.minBookImbalance !== undefined
      ) {
        imbalanceOk =
          tokenSignal?.bookImbalance !== null &&
          tokenSignal?.bookImbalance !== undefined &&
          tokenSignal.bookImbalance >= activeRule.minBookImbalance;
      }

      let velocityOk = true;
      if (
        activeRule.minTradeVelocity !== null &&
        activeRule.minTradeVelocity !== undefined
      ) {
        velocityOk =
          signals?.tradeVelocity !== null &&
          signals?.tradeVelocity !== undefined &&
          signals.tradeVelocity >= activeRule.minTradeVelocity;
      }

      let momentumOk = true;
      if (activeRule.minMomentum !== null && activeRule.minMomentum !== undefined) {
        if (signals?.priceMomentum === null || signals?.priceMomentum === undefined) {
          momentumOk = false;
        } else if (targetUp) {
          momentumOk = signals.priceMomentum >= activeRule.minMomentum;
        } else {
          momentumOk = signals.priceMomentum <= -activeRule.minMomentum;
        }
      }

      let volatilityOk = true;
      if (
        activeRule.minVolatility !== null &&
        activeRule.minVolatility !== undefined
      ) {
        volatilityOk =
          signals?.priceVolatility !== null &&
          signals?.priceVolatility !== undefined &&
          signals.priceVolatility >= activeRule.minVolatility;
      }

      let stalenessOk = true;
      if (
        activeRule.maxPriceStalenessSec !== null &&
        activeRule.maxPriceStalenessSec !== undefined
      ) {
        stalenessOk =
          signals?.priceStalenessSec !== null &&
          signals?.priceStalenessSec !== undefined &&
          signals.priceStalenessSec <= activeRule.maxPriceStalenessSec;
      }

      const crossMinConfidence =
        FORCED_MIN_CONFIDENCE !== null
          ? FORCED_MIN_CONFIDENCE
          : activeRule.minConfidence ?? null;
      const confidence = this.computeConfidence(
        activeRule,
        signals ?? null,
        tokenSignal ?? null,
        targetUp,
        priceDiff,
      );
      let confidenceOk = true;
      if (crossMinConfidence !== null && crossMinConfidence !== undefined) {
        confidenceOk = confidence !== null ? confidence >= crossMinConfidence : false;
      }

      let exposureOk = true;
      if (
        activeRule.maxOpenExposure !== null &&
        activeRule.maxOpenExposure !== undefined
      ) {
        const openExposure = this.getOpenExposure();
        exposureOk = openExposure <= activeRule.maxOpenExposure;
      }

      const edgeScore = this.computeEdgeScore(
        activeRule,
        signals ?? null,
        tokenSignal ?? null,
        targetUp,
        priceDiff,
        config.edgeModel,
      );
      const minEdgeScore = config.edgeModel?.minScore ?? null;
      if (
        edgeEnabled &&
        minEdgeScore !== null &&
        minEdgeScore !== undefined &&
        (edgeScore === null || edgeScore < minEdgeScore)
      ) {
        this.logCrossBlock(coin, snapshot, state, nowMs, "edge", {
          timeLeftSec,
          priceDiff,
          bestAsk,
          edgeScore,
        });
        return;
      }

      const gate = this.computeGateMultiplier(
        activeRule,
        signals ?? null,
        tokenSignal ?? null,
        targetUp,
        confidence,
        config.gateModel,
      );
      if (gateEnabled) {
        if (gate.blocked) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "gate", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            gateMultiplier: gate.multiplier,
          });
          return;
        }
      } else {
        if (!spreadOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "spread", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            spread: tokenSignal?.spread ?? null,
            maxSpread: activeRule.maxSpread ?? null,
          });
          return;
        }
        if (!depthOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "depth", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            depth: tokenSignal?.depthValue ?? null,
            minDepth: activeRule.minDepthValue ?? null,
          });
          return;
        }
        if (!imbalanceOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "imbalance", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            imbalance: tokenSignal?.bookImbalance ?? null,
            minImbalance: activeRule.minBookImbalance ?? null,
          });
          return;
        }
        if (!velocityOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "velocity", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            velocity: signals?.tradeVelocity ?? null,
            minVelocity: activeRule.minTradeVelocity ?? null,
          });
          return;
        }
        if (!momentumOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "momentum", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            momentum: signals?.priceMomentum ?? null,
            minMomentum: activeRule.minMomentum ?? null,
          });
          return;
        }
        if (!volatilityOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "volatility", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            volatility: signals?.priceVolatility ?? null,
            minVolatility: activeRule.minVolatility ?? null,
          });
          return;
        }
        if (!stalenessOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "staleness", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            staleness: signals?.priceStalenessSec ?? null,
            maxStaleness: activeRule.maxPriceStalenessSec ?? null,
          });
          return;
        }
        if (!confidenceOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "confidence", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            confidence,
            minConfidence: crossMinConfidence,
          });
          return;
        }
        if (!exposureOk) {
          this.logCrossBlock(coin, snapshot, state, nowMs, "exposure", {
            timeLeftSec,
            priceDiff,
            bestAsk,
            exposure: this.getOpenExposure(),
            maxExposure: activeRule.maxOpenExposure ?? null,
          });
          return;
        }
      }
    }

    const exitTokenId = state.position.tokenId;
    const bids = this.getAdjustedBids(snapshot, state, exitTokenId);
    const exitFill = this.simulateSell(bids, state.position.shares);
    if (!exitFill) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "exit_liquidity", {
        timeLeftSec,
        priceDiff,
        bestAsk,
      });
      return;
    }

    const realized = exitFill.proceeds - state.position.cost;
    if (realized >= 0) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "not_losing", {
        timeLeftSec,
        priceDiff,
        bestAsk,
        realized,
      });
      return;
    }
    const minLoss = activeRule.minLossToTrigger ?? 0;
    if (-realized < minLoss) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "min_loss", {
        timeLeftSec,
        priceDiff,
        bestAsk,
        realized,
        minLoss,
      });
      return;
    }

    const asks = this.getAdjustedAsks(snapshot, state, targetTokenId);
    const crossConfidence = this.advancedSignals
      ? this.computeConfidence(
          activeRule,
          signals ?? null,
          tokenSignal ?? null,
          targetUp,
          priceDiff,
        )
      : null;
    const crossEdgeScore = this.computeEdgeScore(
      activeRule,
      signals ?? null,
      tokenSignal ?? null,
      targetUp,
      priceDiff,
      config.edgeModel,
    );
    const crossGate = this.computeGateMultiplier(
      activeRule,
      signals ?? null,
      tokenSignal ?? null,
      targetUp,
      crossConfidence,
      config.gateModel,
    );
    const resolvedMaxSpend = this.resolveMaxSpend(activeRule, snapshot, targetTokenId, {
      sizeScale: activeRule.sizeScale ?? 1,
      edgeScore: crossEdgeScore,
      gateMultiplier: crossGate.multiplier,
      applyGate: gateEnabled ? config.gateModel?.applyToSize ?? true : false,
      sizeModel: config.sizeModel,
      confidence: crossConfidence,
      tokenSignal,
    });
    const entryFill = this.simulateFill(asks, activeRule, resolvedMaxSpend);
    if (!entryFill) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "entry_liquidity", {
        timeLeftSec,
        priceDiff,
        bestAsk,
        realized,
      });
      return;
    }

    const potentialProfit = entryFill.shares - entryFill.cost;
    const minRecovery = activeRule.minRecoveryMultiple ?? 2;
    if (potentialProfit < -realized * minRecovery) {
      this.logCrossBlock(coin, snapshot, state, nowMs, "min_recovery", {
        timeLeftSec,
        priceDiff,
        bestAsk,
        realized,
        potentialProfit,
        minRecovery,
      });
      return;
    }

    this.applyBidConsumption(state, exitTokenId, exitFill.fills);
    state.realizedPnl += realized;

    state.position = {
      tokenId: targetTokenId,
      outcome: targetOutcome,
      shares: entryFill.shares,
      avgPrice: entryFill.avgPrice,
      cost: entryFill.cost,
      openedAt: nowMs,
    };

    this.applyConsumption(state, targetTokenId, entryFill.fills);
    state.crossed = true;
    state.marketHadTrade = true;

    this.logger.log(
      `${coin.toUpperCase()} CROSS: sold ${exitFill.shares.toFixed(
        2,
      )} @ ${exitFill.avgPrice.toFixed(4)} (realized ${realized.toFixed(
        2,
      )}), bought ${entryFill.shares.toFixed(2)} @ ${entryFill.avgPrice.toFixed(
        4,
      )} (${targetOutcome})`,
    );
  }

  private logCrossBlock(
    coin: CoinSymbol,
    snapshot: MarketSnapshot,
    state: CoinTradeState,
    nowMs: number,
    reason: string,
    details: Record<string, number | null | undefined>,
  ): void {
    if (!this.crossDebug) return;
    const key = `${snapshot.slug}:${reason}`;
    if (
      state.crossDebugLastKey === key &&
      nowMs - state.crossDebugLastMs < CROSS_LOG_COOLDOWN_MS
    ) {
      return;
    }
    state.crossDebugLastKey = key;
    state.crossDebugLastMs = nowMs;

    const parts: string[] = [];
    for (const [label, value] of Object.entries(details)) {
      if (value === undefined) continue;
      if (value === null || !Number.isFinite(value)) {
        parts.push(`${label}=n/a`);
      } else {
        const digits = label === "bestAsk" ? 4 : 2;
        parts.push(`${label}=${value.toFixed(digits)}`);
      }
    }
    const extra = parts.length > 0 ? ` ${parts.join(" ")}` : "";
    this.logger.log(
      `${coin.toUpperCase()} CROSS blocked (${reason}) slug=${snapshot.slug}${extra}`,
      "WARN",
    );
  }

  private simulateFill(
    asks: OrderBookLevel[],
    rule: TradeRule,
    maxSpend: number,
  ): { shares: number; avgPrice: number; cost: number; fills: OrderBookLevel[] } | null {
    let remaining = maxSpend;
    let cost = 0;
    let shares = 0;
    const fills: OrderBookLevel[] = [];

    for (const ask of asks) {
      if (ask.price > rule.maximumSharePrice) break;
      if (ask.price < rule.minimumSharePrice) continue;

      const availableValue = ask.price * ask.size;
      if (availableValue <= 0) continue;

      const valueToUse = Math.min(availableValue, remaining);
      const sharesToBuy = valueToUse / ask.price;
      if (sharesToBuy <= 0) continue;

      fills.push({ price: ask.price, size: sharesToBuy });
      shares += sharesToBuy;
      cost += valueToUse;
      remaining -= valueToUse;

      if (remaining <= 0) break;
    }

    if (cost < rule.minimumSpend || shares <= 0) {
      return null;
    }

    const avgPrice = cost / shares;
    return { shares, avgPrice, cost, fills };
  }

  private applyCrossModeOverrides(
    rule: CrossTradeRule,
    config: TimedTradeConfig,
    crossConfig: CrossTradeConfig,
    timeLeftSec: number,
  ): CrossTradeRule {
    const modes = config.crossModes;
    if (!modes) return rule;
    const split =
      modes.splitTimeSec !== null && modes.splitTimeSec !== undefined
        ? modes.splitTimeSec
        : Math.max(30, crossConfig.tradeAllowedTimeLeft / 2);
    const overrides = timeLeftSec > split ? modes.precision : modes.opportunistic;
    if (!overrides) return rule;

    const hasOverride = (key: keyof CrossModeOverrides): boolean =>
      Object.prototype.hasOwnProperty.call(overrides, key);

    const maxShareMultiplier = overrides.maxShareMultiplier ?? 1;
    const minShareMultiplier = overrides.minShareMultiplier ?? 1;
    const maxShare = clamp(
      rule.maximumSharePrice * maxShareMultiplier,
      rule.minimumSharePrice + 0.001,
      1,
    );
    const minShare = clamp(
      rule.minimumSharePrice * minShareMultiplier,
      0.01,
      maxShare - 0.001,
    );

    return {
      ...rule,
      minimumPriceDifference:
        rule.minimumPriceDifference * (overrides.minDiffMultiplier ?? 1),
      maximumSharePrice: maxShare,
      minimumSharePrice: minShare,
      maximumSpend: rule.maximumSpend * (overrides.maxSpendMultiplier ?? 1),
      minConfidence: hasOverride("minConfidence")
        ? overrides.minConfidence ?? null
        : rule.minConfidence ?? null,
      minDepthValue: hasOverride("minDepthValue")
        ? overrides.minDepthValue ?? null
        : rule.minDepthValue ?? null,
      minTradeVelocity: hasOverride("minTradeVelocity")
        ? overrides.minTradeVelocity ?? null
        : rule.minTradeVelocity ?? null,
      sizeScale: (rule.sizeScale ?? 1) * (overrides.sizeScaleMultiplier ?? 1),
      minRecoveryMultiple: hasOverride("minRecoveryMultiple")
        ? overrides.minRecoveryMultiple ?? null
        : rule.minRecoveryMultiple ?? null,
      minLossToTrigger: hasOverride("minLossToTrigger")
        ? overrides.minLossToTrigger ?? null
        : rule.minLossToTrigger ?? null,
    };
  }

  private applyConsumption(
    state: CoinTradeState,
    tokenId: string,
    fills: OrderBookLevel[],
  ): void {
    if (!state.simulatedConsumption.has(tokenId)) {
      state.simulatedConsumption.set(tokenId, new Map());
    }

    const map = state.simulatedConsumption.get(tokenId)!;
    for (const fill of fills) {
      const consumed = map.get(fill.price) || 0;
      map.set(fill.price, consumed + fill.size);
    }
  }

  private applyBidConsumption(
    state: CoinTradeState,
    tokenId: string,
    fills: OrderBookLevel[],
  ): void {
    if (!state.simulatedBidConsumption.has(tokenId)) {
      state.simulatedBidConsumption.set(tokenId, new Map());
    }

    const map = state.simulatedBidConsumption.get(tokenId)!;
    for (const fill of fills) {
      const consumed = map.get(fill.price) || 0;
      map.set(fill.price, consumed + fill.size);
    }
  }

  private getAdjustedAsks(
    snapshot: MarketSnapshot,
    state: CoinTradeState,
    tokenId: string,
  ): OrderBookLevel[] {
    const book = snapshot.orderBooks.get(tokenId);
    if (!book) return [];
    const consumption = state.simulatedConsumption.get(tokenId);
    if (!consumption) return book.asks;

    return book.asks.map((ask) => {
      const consumed = consumption.get(ask.price) || 0;
      const remaining = Math.max(0, ask.size - consumed);
      return { price: ask.price, size: remaining };
    });
  }

  private getAdjustedBids(
    snapshot: MarketSnapshot,
    state: CoinTradeState,
    tokenId: string,
  ): OrderBookLevel[] {
    const book = snapshot.orderBooks.get(tokenId);
    if (!book) return [];
    const consumption = state.simulatedBidConsumption.get(tokenId);
    if (!consumption) return book.bids;

    return book.bids.map((bid) => {
      const consumed = consumption.get(bid.price) || 0;
      const remaining = Math.max(0, bid.size - consumed);
      return { price: bid.price, size: remaining };
    });
  }

  private getBestAsk(
    snapshot: MarketSnapshot,
    state: CoinTradeState,
    tokenId: string,
  ): number | null {
    const asks = this.getAdjustedAsks(snapshot, state, tokenId);
    if (asks.length > 0) {
      return asks[0]?.price ?? null;
    }
    return snapshot.bestAsk.get(tokenId) || null;
  }

  private simulateSell(
    bids: OrderBookLevel[],
    sharesToSell: number,
  ): { shares: number; avgPrice: number; proceeds: number; fills: OrderBookLevel[] } | null {
    if (sharesToSell <= 0) return null;
    let remaining = sharesToSell;
    let proceeds = 0;
    let shares = 0;
    const fills: OrderBookLevel[] = [];

    for (const bid of bids) {
      if (bid.size <= 0) continue;
      const sharesToTake = Math.min(bid.size, remaining);
      if (sharesToTake <= 0) continue;
      fills.push({ price: bid.price, size: sharesToTake });
      shares += sharesToTake;
      proceeds += sharesToTake * bid.price;
      remaining -= sharesToTake;
      if (remaining <= 0) break;
    }

    if (shares < sharesToSell) {
      return null;
    }

    const avgPrice = proceeds / shares;
    return { shares, avgPrice, proceeds, fills };
  }

  private getOpenExposure(): number {
    let total = 0;
    for (const state of this.coinStates.values()) {
      if (state.position) {
        total += state.position.cost;
      }
    }
    return total;
  }

  private resolveMaxSpend(
    rule: TradeRule,
    snapshot: MarketSnapshot,
    tokenId: string,
    options?: {
      sizeScale?: number;
      edgeScore?: number | null;
      gateMultiplier?: number;
      applyGate?: boolean;
      sizeModel?: SizeModelConfig;
      confidence?: number | null;
      tokenSignal?: TokenSignal | null;
    },
  ): number {
    const base = rule.maximumSpend;
    if (!this.advancedSignals) return base;

    const sizeStrategy = rule.sizeStrategy ?? "fixed";
    const sizeScale = options?.sizeScale ?? rule.sizeScale ?? 1;
    let factor = 1;

    const sizeModel = options?.sizeModel;
    const tokenSignal =
      options?.tokenSignal ?? snapshot.signals?.tokenSignals.get(tokenId) ?? null;
    const confidence = options?.confidence ?? null;

    if (sizeStrategy === "edge") {
      const threshold =
        snapshot.priceToBeat > 0
          ? snapshot.priceToBeat
          : snapshot.referencePrice;
      const hasPrice = threshold > 0 && snapshot.cryptoPrice > 0;
      const priceDiff = hasPrice
        ? Math.abs(snapshot.cryptoPrice - threshold)
        : null;
      if (priceDiff !== null) {
        const denom = Math.max(rule.minimumPriceDifference, 1);
        factor = clamp(priceDiff / denom, 0.5, 2);
      }
    } else if (sizeStrategy === "depth") {
      if (tokenSignal?.depthValue !== null && tokenSignal?.depthValue !== undefined) {
        factor = clamp(tokenSignal.depthValue / Math.max(base, 1), 0.5, 2);
      }
    } else if (sizeStrategy === "confidence") {
      if (confidence !== null && confidence !== undefined) {
        factor = clamp(0.5 + confidence * 0.5, 0.5, 1);
      }
    }

    if (sizeModel?.mode === "edge_weighted") {
      const gamma = sizeModel.edgeGamma ?? 1.2;
      const minFloor = sizeModel.minSizeFloor ?? 0.5;
      const maxCeil = sizeModel.maxSizeCeil ?? 1.5;
      const edgeScore = options?.edgeScore;
      const edgeFactor =
        edgeScore !== null && edgeScore !== undefined
          ? clamp(Math.pow(edgeScore, gamma), minFloor, maxCeil)
          : minFloor;
      factor *= edgeFactor;
    }

    if (
      sizeModel?.confidenceWeight !== undefined &&
      sizeModel.confidenceWeight !== null &&
      confidence !== null &&
      confidence !== undefined
    ) {
      const weight = clamp(sizeModel.confidenceWeight, 0, 1);
      factor *= clamp(1 - weight + weight * confidence, 0.4, 1.6);
    }

    if (
      sizeModel?.depthWeight !== undefined &&
      sizeModel.depthWeight !== null &&
      tokenSignal?.depthValue !== null &&
      tokenSignal?.depthValue !== undefined
    ) {
      const depthRatio = tokenSignal.depthValue / Math.max(base, 1);
      const weight = sizeModel.depthWeight;
      factor *= clamp(1 + weight * (depthRatio - 1), 0.4, 2);
    }

    if (
      sizeModel?.spreadPenaltyWeight !== undefined &&
      sizeModel.spreadPenaltyWeight !== null &&
      tokenSignal?.spread !== null &&
      tokenSignal?.spread !== undefined &&
      tokenSignal?.bestAsk !== null &&
      tokenSignal?.bestAsk !== undefined &&
      tokenSignal.bestAsk > 0
    ) {
      const penalty = sizeModel.spreadPenaltyWeight * (tokenSignal.spread / tokenSignal.bestAsk);
      factor *= clamp(1 - penalty, 0.2, 1);
    }

    if ((options?.applyGate ?? true) && options?.gateMultiplier !== undefined) {
      factor *= clamp(options.gateMultiplier, 0.1, 1);
    }

    const scaled = base * sizeScale * factor;
    return Math.max(rule.minimumSpend, scaled);
  }

  private computeConfidence(
    rule: TradeRule | null,
    signals: SignalSnapshot | null | undefined,
    tokenSignal: TokenSignal | null,
    favoredUp: boolean | null,
    priceDiff: number | null,
  ): number | null {
    if (!signals) return null;
    let weightedSum = 0;
    let weightTotal = 0;
    let decay = 1;

    if (tokenSignal?.spread !== null && tokenSignal?.spread !== undefined) {
      const bestAsk = tokenSignal.bestAsk ?? null;
      if (bestAsk !== null && bestAsk > 0) {
        const spreadScore = clamp(1 - tokenSignal.spread / bestAsk, 0, 1);
        weightedSum += spreadScore * CONFIDENCE_WEIGHTS.spread;
        weightTotal += CONFIDENCE_WEIGHTS.spread;
      }
    }

    if (tokenSignal?.bookImbalance !== null && tokenSignal?.bookImbalance !== undefined) {
      weightedSum += clamp(tokenSignal.bookImbalance, 0, 1) * CONFIDENCE_WEIGHTS.imbalance;
      weightTotal += CONFIDENCE_WEIGHTS.imbalance;
    }

    if (signals.tradeFlowImbalance !== null && signals.tradeFlowImbalance !== undefined) {
      const flowScore = clamp((signals.tradeFlowImbalance + 1) / 2, 0, 1);
      weightedSum += flowScore * CONFIDENCE_WEIGHTS.tradeFlow;
      weightTotal += CONFIDENCE_WEIGHTS.tradeFlow;
    }

    if (
      signals.priceMomentum !== null &&
      signals.priceMomentum !== undefined &&
      favoredUp !== null
    ) {
      const denom = Math.max(rule?.minimumPriceDifference ?? 1, 1);
      const aligned = favoredUp ? signals.priceMomentum : -signals.priceMomentum;
      const momentumScore = clamp(aligned / denom, 0, 1);
      weightedSum += momentumScore * CONFIDENCE_WEIGHTS.momentum;
      weightTotal += CONFIDENCE_WEIGHTS.momentum;
    }

    if (signals.priceStalenessSec !== null && signals.priceStalenessSec !== undefined) {
      const stalenessScore = clamp(1 - signals.priceStalenessSec / 60, 0, 1);
      weightedSum += stalenessScore * CONFIDENCE_WEIGHTS.staleness;
      weightTotal += CONFIDENCE_WEIGHTS.staleness;
      decay = clamp(Math.exp(-signals.priceStalenessSec / 30), 0.5, 1);
    }

    if (signals.referenceQuality !== null && signals.referenceQuality !== undefined) {
      weightedSum += clamp(signals.referenceQuality, 0, 1) * CONFIDENCE_WEIGHTS.reference;
      weightTotal += CONFIDENCE_WEIGHTS.reference;
    }

    if (weightTotal === 0) return null;
    const baseConfidence = weightedSum / weightTotal;

    if (priceDiff !== null && rule?.minimumPriceDifference) {
      const diffFactor = clamp(
        priceDiff / Math.max(rule.minimumPriceDifference, 1),
        0.5,
        1.5,
      );
      return clamp(baseConfidence * diffFactor * decay, 0, 1);
    }

    return clamp(baseConfidence * decay, 0, 1);
  }

  private computeEdgeScore(
    rule: TradeRule | null,
    signals: SignalSnapshot | null | undefined,
    tokenSignal: TokenSignal | null,
    favoredUp: boolean | null,
    priceDiff: number | null,
    edgeModel: EdgeModelConfig | undefined,
  ): number | null {
    if (!edgeModel) return null;
    const enabled = edgeModel.enabled !== false;
    if (!enabled) return null;
    if (!signals || (!tokenSignal && (edgeModel.requireSignals ?? true))) return null;

    const weights = { ...DEFAULT_EDGE_WEIGHTS, ...(edgeModel.weights ?? {}) };
    const caps = { ...DEFAULT_EDGE_CAPS, ...(edgeModel.caps ?? {}) };
    let weightedSum = 0;
    let weightTotal = 0;

    if (priceDiff !== null && rule?.minimumPriceDifference) {
      const denom = Math.max(rule.minimumPriceDifference, 1);
      const cap = caps.gap ?? DEFAULT_EDGE_CAPS.gap;
      const score = clamp(priceDiff / denom, 0, cap) / cap;
      weightedSum += score * (weights.gap ?? 0);
      weightTotal += weights.gap ?? 0;
    }

    if (tokenSignal?.depthValue !== null && tokenSignal?.depthValue !== undefined) {
      const denom =
        rule?.minDepthValue !== null && rule?.minDepthValue !== undefined
          ? Math.max(rule.minDepthValue, 1)
          : Math.max(rule?.maximumSpend ?? 1, 1);
      const cap = caps.depth ?? DEFAULT_EDGE_CAPS.depth;
      const score = clamp(tokenSignal.depthValue / denom, 0, cap) / cap;
      weightedSum += score * (weights.depth ?? 0);
      weightTotal += weights.depth ?? 0;
    }

    if (tokenSignal?.bookImbalance !== null && tokenSignal?.bookImbalance !== undefined) {
      weightedSum +=
        clamp(tokenSignal.bookImbalance, 0, 1) * (weights.imbalance ?? 0);
      weightTotal += weights.imbalance ?? 0;
    }

    if (signals.tradeVelocity !== null && signals.tradeVelocity !== undefined) {
      const denom =
        rule?.minTradeVelocity !== null && rule?.minTradeVelocity !== undefined
          ? Math.max(rule.minTradeVelocity, 1)
          : 1;
      const cap = caps.velocity ?? DEFAULT_EDGE_CAPS.velocity;
      const score = clamp(signals.tradeVelocity / denom, 0, cap) / cap;
      weightedSum += score * (weights.velocity ?? 0);
      weightTotal += weights.velocity ?? 0;
    }

    if (
      signals.priceMomentum !== null &&
      signals.priceMomentum !== undefined &&
      favoredUp !== null
    ) {
      const aligned = favoredUp ? signals.priceMomentum : -signals.priceMomentum;
      const denom =
        rule?.minMomentum !== null && rule?.minMomentum !== undefined
          ? Math.max(rule.minMomentum, 1)
          : Math.max(rule?.minimumPriceDifference ?? 1, 1);
      const cap = caps.momentum ?? DEFAULT_EDGE_CAPS.momentum;
      const score = clamp(aligned / denom, 0, cap) / cap;
      weightedSum += score * (weights.momentum ?? 0);
      weightTotal += weights.momentum ?? 0;
    }

    if (signals.priceVolatility !== null && signals.priceVolatility !== undefined) {
      const denom =
        rule?.minVolatility !== null && rule?.minVolatility !== undefined
          ? Math.max(rule.minVolatility, 1)
          : 1;
      const cap = caps.volatility ?? DEFAULT_EDGE_CAPS.volatility;
      const score = clamp(signals.priceVolatility / denom, 0, cap) / cap;
      weightedSum += score * (weights.volatility ?? 0);
      weightTotal += weights.volatility ?? 0;
    }

    if (
      tokenSignal?.spread !== null &&
      tokenSignal?.spread !== undefined &&
      tokenSignal.bestAsk !== null &&
      tokenSignal.bestAsk !== undefined &&
      tokenSignal.bestAsk > 0
    ) {
      const spreadBase =
        rule?.maxSpread !== null && rule?.maxSpread !== undefined
          ? rule.maxSpread
          : tokenSignal.bestAsk * 0.02;
      const score = clamp(1 - tokenSignal.spread / spreadBase, 0, 1);
      weightedSum += score * (weights.spread ?? 0);
      weightTotal += weights.spread ?? 0;
    }

    if (signals.referenceQuality !== null && signals.referenceQuality !== undefined) {
      weightedSum += clamp(signals.referenceQuality, 0, 1) * (weights.reference ?? 0);
      weightTotal += weights.reference ?? 0;
    }

    if (weightTotal <= 0) return null;

    const tau =
      edgeModel.stalenessTauSec !== null && edgeModel.stalenessTauSec !== undefined
        ? edgeModel.stalenessTauSec
        : DEFAULT_EDGE_TAU_SEC;
    let decay = 1;
    if (signals.priceStalenessSec !== null && signals.priceStalenessSec !== undefined) {
      decay = clamp(Math.exp(-signals.priceStalenessSec / Math.max(tau, 1)), 0.3, 1);
    }

    return clamp((weightedSum / weightTotal) * decay, 0, 1);
  }

  private computeGateMultiplier(
    rule: TradeRule | null,
    signals: SignalSnapshot | null | undefined,
    tokenSignal: TokenSignal | null,
    favoredUp: boolean | null,
    confidence: number | null,
    gateModel: GateModelConfig | undefined,
  ): { multiplier: number; blocked: boolean } {
    if (!gateModel || gateModel.enabled === false || !rule || !signals) {
      return { multiplier: 1, blocked: false };
    }

    const floor = gateModel.perSignalFloor ?? DEFAULT_GATE_PER_SIGNAL_FLOOR;
    const minGate = gateModel.minGateMultiplier ?? DEFAULT_GATE_MIN_MULTIPLIER;
    let multiplier = 1;

    const applyPenalty = (ratio: number): void => {
      const penalty = clamp(ratio, floor, 1);
      multiplier *= penalty;
    };

    if (rule.maxSpread !== null && rule.maxSpread !== undefined) {
      if (tokenSignal?.spread === null || tokenSignal?.spread === undefined) {
        return { multiplier: 0, blocked: true };
      }
      applyPenalty(rule.maxSpread / Math.max(tokenSignal.spread, 1e-6));
    }

    if (rule.minDepthValue !== null && rule.minDepthValue !== undefined) {
      if (tokenSignal?.depthValue === null || tokenSignal?.depthValue === undefined) {
        return { multiplier: 0, blocked: true };
      }
      applyPenalty(tokenSignal.depthValue / Math.max(rule.minDepthValue, 1));
    }

    if (rule.minBookImbalance !== null && rule.minBookImbalance !== undefined) {
      if (
        tokenSignal?.bookImbalance === null ||
        tokenSignal?.bookImbalance === undefined
      ) {
        return { multiplier: 0, blocked: true };
      }
      applyPenalty(tokenSignal.bookImbalance / Math.max(rule.minBookImbalance, 0.01));
    }

    if (rule.minTradeVelocity !== null && rule.minTradeVelocity !== undefined) {
      if (
        signals.tradeVelocity === null ||
        signals.tradeVelocity === undefined
      ) {
        return { multiplier: 0, blocked: true };
      }
      applyPenalty(signals.tradeVelocity / Math.max(rule.minTradeVelocity, 1));
    }

    if (rule.minMomentum !== null && rule.minMomentum !== undefined) {
      if (
        signals.priceMomentum === null ||
        signals.priceMomentum === undefined ||
        favoredUp === null
      ) {
        return { multiplier: 0, blocked: true };
      }
      const aligned = favoredUp ? signals.priceMomentum : -signals.priceMomentum;
      applyPenalty(aligned / Math.max(rule.minMomentum, 1));
    }

    if (rule.minVolatility !== null && rule.minVolatility !== undefined) {
      if (
        signals.priceVolatility === null ||
        signals.priceVolatility === undefined
      ) {
        return { multiplier: 0, blocked: true };
      }
      applyPenalty(signals.priceVolatility / Math.max(rule.minVolatility, 1));
    }

    if (rule.maxPriceStalenessSec !== null && rule.maxPriceStalenessSec !== undefined) {
      if (
        signals.priceStalenessSec === null ||
        signals.priceStalenessSec === undefined
      ) {
        return { multiplier: 0, blocked: true };
      }
      applyPenalty(rule.maxPriceStalenessSec / Math.max(signals.priceStalenessSec, 1));
    }

    if (rule.minConfidence !== null && rule.minConfidence !== undefined) {
      if (confidence === null || confidence === undefined) {
        return { multiplier: 0, blocked: true };
      }
      applyPenalty(confidence / Math.max(rule.minConfidence, 0.01));
    }

    if (!Number.isFinite(multiplier)) {
      return { multiplier: 0, blocked: true };
    }

    return { multiplier, blocked: multiplier < minGate };
  }

  private recordSignalStats(
    tokenSignal: TokenSignal | null,
    confidence: number | null,
  ): void {
    this.signalStats.samples += 1;
    if (tokenSignal?.spread !== null && tokenSignal?.spread !== undefined) {
      this.signalStats.spreadSum += tokenSignal.spread;
      this.signalStats.spreadCount += 1;
    }
    if (tokenSignal?.depthValue !== null && tokenSignal?.depthValue !== undefined) {
      this.signalStats.depthSum += tokenSignal.depthValue;
      this.signalStats.depthCount += 1;
    }
    if (confidence !== null && confidence !== undefined) {
      this.signalStats.confidenceSum += confidence;
      this.signalStats.confidenceCount += 1;
    }
  }

  private resolveMarket(coin: CoinSymbol, snapshot: MarketSnapshot): void {
    const state = this.coinStates.get(coin);
    if (!state) return;
    if (state.lastResolvedSlug === snapshot.slug) return;

    const threshold =
      snapshot.priceToBeat > 0
        ? snapshot.priceToBeat
        : snapshot.referencePrice;
    if (threshold <= 0) {
      state.lastResolvedSlug = snapshot.slug;
      this.lastResultByCoin.set(coin, "No reference price available");
      this.logger.log(`${coin.toUpperCase()} resolved with no reference`, "WARN");
      state.position = null;
      state.pendingOrder = null;
      state.simulatedConsumption.clear();
      state.simulatedBidConsumption.clear();
      state.crossed = false;
      state.realizedPnl = 0;
      state.marketHadTrade = false;
      return;
    }

    const outcomeUp = snapshot.cryptoPrice >= threshold;
    const resolvedOutcome = outcomeUp ? snapshot.upOutcome : snapshot.downOutcome;

    let netPnl = state.realizedPnl;
    if (state.position) {
      const win = state.position.outcome === resolvedOutcome;
      const profit = win
        ? state.position.shares - state.position.cost
        : -state.position.cost;
      netPnl += profit;
    }

    if (state.marketHadTrade) {
      if (netPnl >= 0) {
        this.summary.wins += 1;
        state.lossStreak = 0;
      } else {
        this.summary.losses += 1;
        state.lossStreak += 1;
      }
      this.summary.totalProfit += netPnl;
      const result = `${netPnl >= 0 ? "WIN" : "LOSS"} (${resolvedOutcome}) Net PnL ${netPnl.toFixed(
        2,
      )}`;
      this.lastResultByCoin.set(coin, result);
      this.logger.log(`${coin.toUpperCase()} resolved: ${result}`);
    } else {
      const result = `No trade executed (${resolvedOutcome})`;
      this.lastResultByCoin.set(coin, result);
      this.logger.log(`${coin.toUpperCase()} resolved with no trade`);
    }

    state.position = null;
    state.pendingOrder = null;
    state.simulatedConsumption.clear();
    state.simulatedBidConsumption.clear();
    state.crossed = false;
    state.realizedPnl = 0;
    state.marketHadTrade = false;
    state.marketTradeCounted = false;
    state.lastResolvedSlug = snapshot.slug;
  }

  private logCriteriaTransitions(
    coin: CoinSymbol,
    state: CoinTradeState,
    rule: TradeRule | null,
    details: {
      nowMs: number;
      timeOk: boolean;
      gapOk: boolean;
      priceOk: boolean;
      spreadOk: boolean;
      depthOk: boolean;
      imbalanceOk: boolean;
      velocityOk: boolean;
      momentumOk: boolean;
      volatilityOk: boolean;
      stalenessOk: boolean;
      confidenceOk: boolean;
      exposureOk: boolean;
      timeLeftSec: number | null;
      priceDiff: number | null;
      bestAsk: number | null;
      favoredOutcome: string | null;
      spread: number | null;
      depthValue: number | null;
      bookImbalance: number | null;
      tradeVelocity: number | null;
      momentum: number | null;
      volatility: number | null;
      stalenessSec: number | null;
      confidence: number | null;
    },
  ): void {
    const now = details.nowMs;
    const prev = state.criteriaStatus;
    const lastLogged = state.criteriaLastLoggedMs;

    if (
      details.timeOk &&
      !prev.timeOk &&
      now - lastLogged.timeOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const timeLeft = details.timeLeftSec ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} time window open (${Math.max(
          0,
          Math.round(timeLeft),
        )}s left)`,
      );
      lastLogged.timeOk = now;
    }

    if (
      rule &&
      details.gapOk &&
      !prev.gapOk &&
      now - lastLogged.gapOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const diff = details.priceDiff ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} price gap met (${diff.toFixed(
          2,
        )} >= ${rule.minimumPriceDifference.toFixed(2)})`,
      );
      lastLogged.gapOk = now;
    }

    if (
      rule &&
      details.priceOk &&
      !prev.priceOk &&
      now - lastLogged.priceOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const ask = details.bestAsk ?? 0;
      const outcome = details.favoredOutcome
        ? ` ${details.favoredOutcome}`
        : "";
      this.logger.log(
        `${coin.toUpperCase()} share price ok${outcome} (${ask.toFixed(
          4,
        )} within ${rule.minimumSharePrice.toFixed(
          2,
        )}-${rule.maximumSharePrice.toFixed(2)})`,
      );
      lastLogged.priceOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.spreadOk &&
      !prev.spreadOk &&
      now - lastLogged.spreadOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const spread = details.spread ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} spread ok (${spread.toFixed(4)} <= ${
          rule.maxSpread ?? 0
        })`,
      );
      lastLogged.spreadOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.depthOk &&
      !prev.depthOk &&
      now - lastLogged.depthOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const depth = details.depthValue ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} depth ok (${depth.toFixed(2)} >= ${
          rule.minDepthValue ?? 0
        })`,
      );
      lastLogged.depthOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.imbalanceOk &&
      !prev.imbalanceOk &&
      now - lastLogged.imbalanceOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const imbalance = details.bookImbalance ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} imbalance ok (${imbalance.toFixed(2)} >= ${
          rule.minBookImbalance ?? 0
        })`,
      );
      lastLogged.imbalanceOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.velocityOk &&
      !prev.velocityOk &&
      now - lastLogged.velocityOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const velocity = details.tradeVelocity ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} velocity ok (${velocity.toFixed(2)} >= ${
          rule.minTradeVelocity ?? 0
        })`,
      );
      lastLogged.velocityOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.momentumOk &&
      !prev.momentumOk &&
      now - lastLogged.momentumOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const momentum = details.momentum ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} momentum ok (${momentum.toFixed(2)} >= ${
          rule.minMomentum ?? 0
        })`,
      );
      lastLogged.momentumOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.volatilityOk &&
      !prev.volatilityOk &&
      now - lastLogged.volatilityOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const volatility = details.volatility ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} volatility ok (${volatility.toFixed(2)} >= ${
          rule.minVolatility ?? 0
        })`,
      );
      lastLogged.volatilityOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.stalenessOk &&
      !prev.stalenessOk &&
      now - lastLogged.stalenessOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const staleness = details.stalenessSec ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} staleness ok (${staleness.toFixed(1)}s <= ${
          rule.maxPriceStalenessSec ?? 0
        })`,
      );
      lastLogged.stalenessOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.confidenceOk &&
      !prev.confidenceOk &&
      now - lastLogged.confidenceOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      const confidence = details.confidence ?? 0;
      this.logger.log(
        `${coin.toUpperCase()} confidence ok (${confidence.toFixed(2)} >= ${
          rule.minConfidence ?? 0
        })`,
      );
      lastLogged.confidenceOk = now;
    }

    if (
      this.signalDebug &&
      rule &&
      details.exposureOk &&
      !prev.exposureOk &&
      now - lastLogged.exposureOk > CRITERIA_LOG_COOLDOWN_MS
    ) {
      this.logger.log(
        `${coin.toUpperCase()} exposure ok (<= ${rule.maxOpenExposure ?? 0})`,
      );
      lastLogged.exposureOk = now;
    }

    state.criteriaStatus = {
      timeOk: details.timeOk,
      gapOk: details.gapOk,
      priceOk: details.priceOk,
      spreadOk: details.spreadOk,
      depthOk: details.depthOk,
      imbalanceOk: details.imbalanceOk,
      velocityOk: details.velocityOk,
      momentumOk: details.momentumOk,
      volatilityOk: details.volatilityOk,
      stalenessOk: details.stalenessOk,
      confidenceOk: details.confidenceOk,
      exposureOk: details.exposureOk,
    };
  }

  private selectActiveRule(
    config: TimedTradeConfig,
    timeLeftSec: number | null,
  ): TradeRule | null {
    return this.selectRuleByTime(config.rules, timeLeftSec);
  }

  private selectCrossRule(
    config: CrossTradeConfig,
    timeLeftSec: number | null,
  ): CrossTradeRule | null {
    return this.selectRuleByTime(config.rules, timeLeftSec);
  }

  private selectRuleByTime<T extends { tierSeconds: number }>(
    rules: T[],
    timeLeftSec: number | null,
  ): T | null {
    if (timeLeftSec === null) return null;
    for (const rule of rules) {
      if (timeLeftSec <= rule.tierSeconds) {
        return rule;
      }
    }
    return null;
  }
}
