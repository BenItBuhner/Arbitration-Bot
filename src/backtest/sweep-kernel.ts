import type { CoinSymbol } from "../services/auto-market";
import type { MarketSnapshot } from "../services/market-data-hub";
import type { SizeStrategy, TimedTradeConfig } from "../services/profile-engine";

type OptionalNumber = number | null;

function readEnvNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

const LATENCY_BASE_MS = Math.max(
  0,
  readEnvNumber("SWEEP_DECISION_LATENCY_BASE_MS", 15),
);
const LATENCY_JITTER_MS = 0;
const DECISION_COOLDOWN_MS = Math.max(
  0,
  readEnvNumber("SWEEP_DECISION_COOLDOWN_MS", 200),
);
const CROSS_ALLOW_NO_FLIP = readEnvNumber("SWEEP_CROSS_ALLOW_NO_FLIP", 1) > 0;
const FORCE_MIN_CONFIDENCE = readEnvNumber("SWEEP_FORCE_MIN_CONFIDENCE", Number.NaN);

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

const SIZE_STRATEGY_MAP: Record<SizeStrategy, number> = {
  fixed: 0,
  edge: 1,
  depth: 2,
  confidence: 3,
};

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

export interface SweepKernelSummary {
  runtimeSec: number;
  totalTrades: number;
  crossTrades: number;
  wins: number;
  losses: number;
  totalProfit: number;
  openExposure: number;
}

export class SweepKernel {
  private coin: CoinSymbol;
  private startMs: number;
  private lastNowMs: number;
  private profileCount: number;
  private ruleCount: number;
  private tierSeconds: number[];
  private tradeAllowedTimeLeft: Float64Array;
  private minDiff: Float64Array;
  private maxShare: Float64Array;
  private minShare: Float64Array;
  private maxSpend: Float64Array;
  private minSpend: Float64Array;
  private maxSpread: Float64Array;
  private minImbalance: Float64Array;
  private minDepth: Float64Array;
  private minVelocity: Float64Array;
  private minConfidence: Float64Array;
  private minMomentum: Float64Array;
  private minVolatility: Float64Array;
  private maxStaleness: Float64Array;
  private sizeScale: Float64Array;
  private sizeStrategy: Int8Array;
  private maxOpenExposure: Float64Array;
  private minDiffByTier: number[];
  private minShareByTier: number[];
  private maxShareByTier: number[];
  private maxTradeAllowed: number;
  private crossRuleCount: number;
  private crossTierSeconds: number[];
  private crossTradeAllowedTimeLeft: Float64Array;
  private crossMinDiff: Float64Array;
  private crossMaxShare: Float64Array;
  private crossMinShare: Float64Array;
  private crossMaxSpend: Float64Array;
  private crossMinSpend: Float64Array;
  private crossMaxSpread: Float64Array;
  private crossMinImbalance: Float64Array;
  private crossMinDepth: Float64Array;
  private crossMinVelocity: Float64Array;
  private crossMinConfidence: Float64Array;
  private crossMinMomentum: Float64Array;
  private crossMinVolatility: Float64Array;
  private crossMaxStaleness: Float64Array;
  private crossSizeScale: Float64Array;
  private crossSizeStrategy: Int8Array;
  private crossMaxOpenExposure: Float64Array;
  private crossMinRecoveryMultiple: Float64Array;
  private crossMinLossToTrigger: Float64Array;
  private crossMinDiffByTier: number[];
  private crossMinShareByTier: number[];
  private crossMaxShareByTier: number[];
  private crossEnabled: Int8Array;
  private edgeEnabled: Int8Array;
  private edgeWeightGap: Float64Array;
  private edgeWeightDepth: Float64Array;
  private edgeWeightImbalance: Float64Array;
  private edgeWeightVelocity: Float64Array;
  private edgeWeightMomentum: Float64Array;
  private edgeWeightVolatility: Float64Array;
  private edgeWeightSpread: Float64Array;
  private edgeWeightReference: Float64Array;
  private edgeCapGap: Float64Array;
  private edgeCapDepth: Float64Array;
  private edgeCapVelocity: Float64Array;
  private edgeCapMomentum: Float64Array;
  private edgeCapVolatility: Float64Array;
  private edgeCapSpread: Float64Array;
  private edgeTauSec: Float64Array;
  private edgeMinScore: Float64Array;
  private edgeRequireSignals: Int8Array;
  private gateEnabled: Int8Array;
  private gateMinMultiplier: Float64Array;
  private gateFloor: Float64Array;
  private gateApplyToSize: Int8Array;
  private sizeModelMode: Int8Array;
  private sizeModelEdgeGamma: Float64Array;
  private sizeModelMinFloor: Float64Array;
  private sizeModelMaxCeil: Float64Array;
  private sizeModelApplyGate: Int8Array;
  private sizeModelConfidenceWeight: Float64Array;
  private sizeModelDepthWeight: Float64Array;
  private sizeModelSpreadPenaltyWeight: Float64Array;
  private lossEnabled: Int8Array;
  private lossStreakThreshold: Int16Array;
  private lossMinDiffMultiplier: Float64Array;
  private lossSizeScaleMultiplier: Float64Array;
  private lossStreak: Int16Array;
  private crossModeEnabled: Int8Array;
  private crossModeSplitSec: Float64Array;
  private crossPrecisionMinDiffMult: Float64Array;
  private crossPrecisionMaxShareMult: Float64Array;
  private crossPrecisionMinShareMult: Float64Array;
  private crossPrecisionMaxSpendMult: Float64Array;
  private crossPrecisionMinConfidence: Float64Array;
  private crossPrecisionMinDepth: Float64Array;
  private crossPrecisionMinVelocity: Float64Array;
  private crossPrecisionSizeScaleMult: Float64Array;
  private crossPrecisionMinRecovery: Float64Array;
  private crossPrecisionMinLoss: Float64Array;
  private crossOpMinDiffMult: Float64Array;
  private crossOpMaxShareMult: Float64Array;
  private crossOpMinShareMult: Float64Array;
  private crossOpMaxSpendMult: Float64Array;
  private crossOpMinConfidence: Float64Array;
  private crossOpMinDepth: Float64Array;
  private crossOpMinVelocity: Float64Array;
  private crossOpSizeScaleMult: Float64Array;
  private crossOpMinRecovery: Float64Array;
  private crossOpMinLoss: Float64Array;

  private totalTrades: Int32Array;
  private crossTrades: Int32Array;
  private wins: Int32Array;
  private losses: Int32Array;
  private totalProfit: Float64Array;
  private positionShares: Float64Array;
  private positionCost: Float64Array;
  private positionOutcome: Int8Array; // 0 none, 1 up, 2 down
  private crossed: Int8Array;
  private realizedPnl: Float64Array;
  private marketHadTrade: Int8Array;
  private marketTradeCounted: Int8Array;
  private pendingDueMs: Float64Array;
  private pendingOutcome: Int8Array; // 0 none, 1 up, 2 down
  private pendingRuleIdx: Int16Array;
  private lastDecisionMs: Float64Array;
  private lastResolvedEpoch: Int32Array;

  private currentSlug: string | null = null;
  private currentEpoch = 0;
  private lastSnapshot: MarketSnapshot | null = null;
  private nextPendingDueMs: number | null = null;

  constructor(
    coin: CoinSymbol,
    configs: TimedTradeConfig[],
    startMs: number,
  ) {
    if (configs.length === 0) {
      throw new Error("SweepKernel requires at least one profile config.");
    }
    this.coin = coin;
    this.startMs = startMs;
    this.lastNowMs = startMs;
    this.profileCount = configs.length;
    const baseConfig = configs[0]!;
    this.ruleCount = baseConfig.rules.length;
    this.tierSeconds = baseConfig.rules.map((rule) => rule.tierSeconds);
    const firstCross = configs.find((config) => !!config.cross)?.cross ?? null;
    this.crossRuleCount = firstCross ? firstCross.rules.length : 0;
    this.crossTierSeconds = firstCross ? firstCross.rules.map((rule) => rule.tierSeconds) : [];

    for (const config of configs) {
      if (config.rules.length !== this.ruleCount) {
        throw new Error("SweepKernel requires consistent tier counts.");
      }
      for (let i = 0; i < this.ruleCount; i += 1) {
        if (config.rules[i]?.tierSeconds !== this.tierSeconds[i]!) {
          throw new Error("SweepKernel requires consistent tierSeconds.");
        }
      }
      if (this.crossRuleCount > 0 && config.cross) {
        if (config.cross.rules.length !== this.crossRuleCount) {
          throw new Error("SweepKernel requires consistent cross tier counts.");
        }
        for (let i = 0; i < this.crossRuleCount; i += 1) {
          if (config.cross.rules[i]?.tierSeconds !== this.crossTierSeconds[i]!) {
            throw new Error("SweepKernel requires consistent cross tierSeconds.");
          }
        }
      }
    }

    const totalRules = this.profileCount * this.ruleCount;
    this.tradeAllowedTimeLeft = new Float64Array(this.profileCount);
    this.minDiff = new Float64Array(totalRules);
    this.maxShare = new Float64Array(totalRules);
    this.minShare = new Float64Array(totalRules);
    this.maxSpend = new Float64Array(totalRules);
    this.minSpend = new Float64Array(totalRules);
    this.maxSpread = new Float64Array(totalRules);
    this.minImbalance = new Float64Array(totalRules);
    this.minDepth = new Float64Array(totalRules);
    this.minVelocity = new Float64Array(totalRules);
    this.minConfidence = new Float64Array(totalRules);
    this.minMomentum = new Float64Array(totalRules);
    this.minVolatility = new Float64Array(totalRules);
    this.maxStaleness = new Float64Array(totalRules);
    this.sizeScale = new Float64Array(totalRules);
    this.sizeStrategy = new Int8Array(totalRules);
    this.maxOpenExposure = new Float64Array(totalRules);

    this.minDiffByTier = new Array(this.ruleCount).fill(Number.POSITIVE_INFINITY);
    this.minShareByTier = new Array(this.ruleCount).fill(Number.POSITIVE_INFINITY);
    this.maxShareByTier = new Array(this.ruleCount).fill(0);
    this.maxTradeAllowed = 0;
    const totalCrossRules = this.profileCount * this.crossRuleCount;
    this.crossTradeAllowedTimeLeft = new Float64Array(this.profileCount);
    this.crossMinDiff = new Float64Array(totalCrossRules);
    this.crossMaxShare = new Float64Array(totalCrossRules);
    this.crossMinShare = new Float64Array(totalCrossRules);
    this.crossMaxSpend = new Float64Array(totalCrossRules);
    this.crossMinSpend = new Float64Array(totalCrossRules);
    this.crossMaxSpread = new Float64Array(totalCrossRules);
    this.crossMinImbalance = new Float64Array(totalCrossRules);
    this.crossMinDepth = new Float64Array(totalCrossRules);
    this.crossMinVelocity = new Float64Array(totalCrossRules);
    this.crossMinConfidence = new Float64Array(totalCrossRules);
    this.crossMinMomentum = new Float64Array(totalCrossRules);
    this.crossMinVolatility = new Float64Array(totalCrossRules);
    this.crossMaxStaleness = new Float64Array(totalCrossRules);
    this.crossSizeScale = new Float64Array(totalCrossRules);
    this.crossSizeStrategy = new Int8Array(totalCrossRules);
    this.crossMaxOpenExposure = new Float64Array(totalCrossRules);
    this.crossMinRecoveryMultiple = new Float64Array(totalCrossRules);
    this.crossMinLossToTrigger = new Float64Array(totalCrossRules);
    this.crossMinDiffByTier = new Array(this.crossRuleCount).fill(Number.POSITIVE_INFINITY);
    this.crossMinShareByTier = new Array(this.crossRuleCount).fill(Number.POSITIVE_INFINITY);
    this.crossMaxShareByTier = new Array(this.crossRuleCount).fill(0);
    this.crossEnabled = new Int8Array(this.profileCount);
    this.edgeEnabled = new Int8Array(this.profileCount);
    this.edgeWeightGap = new Float64Array(this.profileCount);
    this.edgeWeightDepth = new Float64Array(this.profileCount);
    this.edgeWeightImbalance = new Float64Array(this.profileCount);
    this.edgeWeightVelocity = new Float64Array(this.profileCount);
    this.edgeWeightMomentum = new Float64Array(this.profileCount);
    this.edgeWeightVolatility = new Float64Array(this.profileCount);
    this.edgeWeightSpread = new Float64Array(this.profileCount);
    this.edgeWeightReference = new Float64Array(this.profileCount);
    this.edgeCapGap = new Float64Array(this.profileCount);
    this.edgeCapDepth = new Float64Array(this.profileCount);
    this.edgeCapVelocity = new Float64Array(this.profileCount);
    this.edgeCapMomentum = new Float64Array(this.profileCount);
    this.edgeCapVolatility = new Float64Array(this.profileCount);
    this.edgeCapSpread = new Float64Array(this.profileCount);
    this.edgeTauSec = new Float64Array(this.profileCount);
    this.edgeMinScore = new Float64Array(this.profileCount);
    this.edgeRequireSignals = new Int8Array(this.profileCount);
    this.gateEnabled = new Int8Array(this.profileCount);
    this.gateMinMultiplier = new Float64Array(this.profileCount);
    this.gateFloor = new Float64Array(this.profileCount);
    this.gateApplyToSize = new Int8Array(this.profileCount);
    this.sizeModelMode = new Int8Array(this.profileCount);
    this.sizeModelEdgeGamma = new Float64Array(this.profileCount);
    this.sizeModelMinFloor = new Float64Array(this.profileCount);
    this.sizeModelMaxCeil = new Float64Array(this.profileCount);
    this.sizeModelApplyGate = new Int8Array(this.profileCount);
    this.sizeModelConfidenceWeight = new Float64Array(this.profileCount);
    this.sizeModelDepthWeight = new Float64Array(this.profileCount);
    this.sizeModelSpreadPenaltyWeight = new Float64Array(this.profileCount);
    this.lossEnabled = new Int8Array(this.profileCount);
    this.lossStreakThreshold = new Int16Array(this.profileCount);
    this.lossMinDiffMultiplier = new Float64Array(this.profileCount);
    this.lossSizeScaleMultiplier = new Float64Array(this.profileCount);
    this.lossStreak = new Int16Array(this.profileCount);
    this.crossModeEnabled = new Int8Array(this.profileCount);
    this.crossModeSplitSec = new Float64Array(this.profileCount);
    this.crossPrecisionMinDiffMult = new Float64Array(this.profileCount);
    this.crossPrecisionMaxShareMult = new Float64Array(this.profileCount);
    this.crossPrecisionMinShareMult = new Float64Array(this.profileCount);
    this.crossPrecisionMaxSpendMult = new Float64Array(this.profileCount);
    this.crossPrecisionMinConfidence = new Float64Array(this.profileCount);
    this.crossPrecisionMinDepth = new Float64Array(this.profileCount);
    this.crossPrecisionMinVelocity = new Float64Array(this.profileCount);
    this.crossPrecisionSizeScaleMult = new Float64Array(this.profileCount);
    this.crossPrecisionMinRecovery = new Float64Array(this.profileCount);
    this.crossPrecisionMinLoss = new Float64Array(this.profileCount);
    this.crossOpMinDiffMult = new Float64Array(this.profileCount);
    this.crossOpMaxShareMult = new Float64Array(this.profileCount);
    this.crossOpMinShareMult = new Float64Array(this.profileCount);
    this.crossOpMaxSpendMult = new Float64Array(this.profileCount);
    this.crossOpMinConfidence = new Float64Array(this.profileCount);
    this.crossOpMinDepth = new Float64Array(this.profileCount);
    this.crossOpMinVelocity = new Float64Array(this.profileCount);
    this.crossOpSizeScaleMult = new Float64Array(this.profileCount);
    this.crossOpMinRecovery = new Float64Array(this.profileCount);
    this.crossOpMinLoss = new Float64Array(this.profileCount);

    for (let p = 0; p < this.profileCount; p += 1) {
      const config = configs[p]!;
      this.tradeAllowedTimeLeft[p] = config.tradeAllowedTimeLeft;
      this.maxTradeAllowed = Math.max(
        this.maxTradeAllowed,
        config.tradeAllowedTimeLeft,
      );
      for (let t = 0; t < this.ruleCount; t += 1) {
        const rule = config.rules[t]!;
        const offset = p * this.ruleCount + t;
        this.minDiff[offset] = rule.minimumPriceDifference;
        this.maxShare[offset] = rule.maximumSharePrice;
        this.minShare[offset] = rule.minimumSharePrice;
        this.maxSpend[offset] = rule.maximumSpend;
        this.minSpend[offset] = rule.minimumSpend;
        this.maxSpread[offset] =
          rule.maxSpread === undefined || rule.maxSpread === null
            ? Number.NaN
            : rule.maxSpread;
        this.minImbalance[offset] =
          rule.minBookImbalance === undefined || rule.minBookImbalance === null
            ? Number.NaN
            : rule.minBookImbalance;
        this.minDepth[offset] =
          rule.minDepthValue === undefined || rule.minDepthValue === null
            ? Number.NaN
            : rule.minDepthValue;
        this.minVelocity[offset] =
          rule.minTradeVelocity === undefined || rule.minTradeVelocity === null
            ? Number.NaN
            : rule.minTradeVelocity;
        this.minConfidence[offset] =
          rule.minConfidence === undefined || rule.minConfidence === null
            ? Number.NaN
            : rule.minConfidence;
        this.minMomentum[offset] =
          rule.minMomentum === undefined || rule.minMomentum === null
            ? Number.NaN
            : rule.minMomentum;
        this.minVolatility[offset] =
          rule.minVolatility === undefined || rule.minVolatility === null
            ? Number.NaN
            : rule.minVolatility;
        this.maxStaleness[offset] =
          rule.maxPriceStalenessSec === undefined ||
          rule.maxPriceStalenessSec === null
            ? Number.NaN
            : rule.maxPriceStalenessSec;
        this.sizeScale[offset] =
          rule.sizeScale === undefined || rule.sizeScale === null
            ? 1
            : rule.sizeScale;
        const strategy = rule.sizeStrategy ?? "fixed";
        this.sizeStrategy[offset] = SIZE_STRATEGY_MAP[strategy];
        this.maxOpenExposure[offset] =
          rule.maxOpenExposure === undefined || rule.maxOpenExposure === null
            ? Number.NaN
            : rule.maxOpenExposure;

        this.minDiffByTier[t] = Math.min(
          this.minDiffByTier[t] ?? Number.POSITIVE_INFINITY,
          rule.minimumPriceDifference,
        );
        this.minShareByTier[t] = Math.min(
          this.minShareByTier[t] ?? Number.POSITIVE_INFINITY,
          rule.minimumSharePrice,
        );
        this.maxShareByTier[t] = Math.max(
          this.maxShareByTier[t] ?? 0,
          rule.maximumSharePrice,
        );
      }

      if (this.crossRuleCount > 0 && config.cross) {
        this.crossEnabled[p] = 1;
        this.crossTradeAllowedTimeLeft[p] = config.cross.tradeAllowedTimeLeft;
        for (let t = 0; t < this.crossRuleCount; t += 1) {
          const rule = config.cross.rules[t]!;
          const offset = p * this.crossRuleCount + t;
          this.crossMinDiff[offset] = rule.minimumPriceDifference;
          this.crossMaxShare[offset] = rule.maximumSharePrice;
          this.crossMinShare[offset] = rule.minimumSharePrice;
          this.crossMaxSpend[offset] = rule.maximumSpend;
          this.crossMinSpend[offset] = rule.minimumSpend;
          this.crossMaxSpread[offset] =
            rule.maxSpread === undefined || rule.maxSpread === null
              ? Number.NaN
              : rule.maxSpread;
          this.crossMinImbalance[offset] =
            rule.minBookImbalance === undefined || rule.minBookImbalance === null
              ? Number.NaN
              : rule.minBookImbalance;
          this.crossMinDepth[offset] =
            rule.minDepthValue === undefined || rule.minDepthValue === null
              ? Number.NaN
              : rule.minDepthValue;
          this.crossMinVelocity[offset] =
            rule.minTradeVelocity === undefined || rule.minTradeVelocity === null
              ? Number.NaN
              : rule.minTradeVelocity;
          this.crossMinConfidence[offset] =
            rule.minConfidence === undefined || rule.minConfidence === null
              ? Number.NaN
              : rule.minConfidence;
          this.crossMinMomentum[offset] =
            rule.minMomentum === undefined || rule.minMomentum === null
              ? Number.NaN
              : rule.minMomentum;
          this.crossMinVolatility[offset] =
            rule.minVolatility === undefined || rule.minVolatility === null
              ? Number.NaN
              : rule.minVolatility;
          this.crossMaxStaleness[offset] =
            rule.maxPriceStalenessSec === undefined ||
            rule.maxPriceStalenessSec === null
              ? Number.NaN
              : rule.maxPriceStalenessSec;
          this.crossSizeScale[offset] =
            rule.sizeScale === undefined || rule.sizeScale === null
              ? 1
              : rule.sizeScale;
          const strategy = rule.sizeStrategy ?? "fixed";
          this.crossSizeStrategy[offset] = SIZE_STRATEGY_MAP[strategy];
          this.crossMaxOpenExposure[offset] =
            rule.maxOpenExposure === undefined || rule.maxOpenExposure === null
              ? Number.NaN
              : rule.maxOpenExposure;
          this.crossMinRecoveryMultiple[offset] =
            rule.minRecoveryMultiple === undefined || rule.minRecoveryMultiple === null
              ? 2
              : rule.minRecoveryMultiple;
          this.crossMinLossToTrigger[offset] =
            rule.minLossToTrigger === undefined || rule.minLossToTrigger === null
              ? 0
              : rule.minLossToTrigger;

          this.crossMinDiffByTier[t] = Math.min(
            this.crossMinDiffByTier[t] ?? Number.POSITIVE_INFINITY,
            rule.minimumPriceDifference,
          );
          this.crossMinShareByTier[t] = Math.min(
            this.crossMinShareByTier[t] ?? Number.POSITIVE_INFINITY,
            rule.minimumSharePrice,
          );
          this.crossMaxShareByTier[t] = Math.max(
            this.crossMaxShareByTier[t] ?? 0,
            rule.maximumSharePrice,
          );
        }
      } else if (this.crossRuleCount > 0) {
        this.crossEnabled[p] = 0;
        this.crossTradeAllowedTimeLeft[p] = 0;
      }

      const edgeModel = config.edgeModel;
      this.edgeEnabled[p] = edgeModel ? (edgeModel.enabled === false ? 0 : 1) : 0;
      this.edgeWeightGap[p] = edgeModel?.weights?.gap ?? DEFAULT_EDGE_WEIGHTS.gap;
      this.edgeWeightDepth[p] = edgeModel?.weights?.depth ?? DEFAULT_EDGE_WEIGHTS.depth;
      this.edgeWeightImbalance[p] =
        edgeModel?.weights?.imbalance ?? DEFAULT_EDGE_WEIGHTS.imbalance;
      this.edgeWeightVelocity[p] =
        edgeModel?.weights?.velocity ?? DEFAULT_EDGE_WEIGHTS.velocity;
      this.edgeWeightMomentum[p] =
        edgeModel?.weights?.momentum ?? DEFAULT_EDGE_WEIGHTS.momentum;
      this.edgeWeightVolatility[p] =
        edgeModel?.weights?.volatility ?? DEFAULT_EDGE_WEIGHTS.volatility;
      this.edgeWeightSpread[p] =
        edgeModel?.weights?.spread ?? DEFAULT_EDGE_WEIGHTS.spread;
      this.edgeWeightReference[p] =
        edgeModel?.weights?.reference ?? DEFAULT_EDGE_WEIGHTS.reference;
      this.edgeCapGap[p] = edgeModel?.caps?.gap ?? DEFAULT_EDGE_CAPS.gap;
      this.edgeCapDepth[p] = edgeModel?.caps?.depth ?? DEFAULT_EDGE_CAPS.depth;
      this.edgeCapVelocity[p] =
        edgeModel?.caps?.velocity ?? DEFAULT_EDGE_CAPS.velocity;
      this.edgeCapMomentum[p] =
        edgeModel?.caps?.momentum ?? DEFAULT_EDGE_CAPS.momentum;
      this.edgeCapVolatility[p] =
        edgeModel?.caps?.volatility ?? DEFAULT_EDGE_CAPS.volatility;
      this.edgeCapSpread[p] = edgeModel?.caps?.spread ?? DEFAULT_EDGE_CAPS.spread;
      this.edgeTauSec[p] = edgeModel?.stalenessTauSec ?? DEFAULT_EDGE_TAU_SEC;
      this.edgeMinScore[p] =
        edgeModel?.minScore === undefined || edgeModel?.minScore === null
          ? Number.NaN
          : edgeModel.minScore;
      this.edgeRequireSignals[p] =
        edgeModel?.requireSignals === undefined
          ? 1
          : edgeModel.requireSignals
            ? 1
            : 0;

      const gateModel = config.gateModel;
      this.gateEnabled[p] = gateModel ? (gateModel.enabled === false ? 0 : 1) : 0;
      this.gateMinMultiplier[p] =
        gateModel?.minGateMultiplier ?? DEFAULT_GATE_MIN_MULTIPLIER;
      this.gateFloor[p] = gateModel?.perSignalFloor ?? DEFAULT_GATE_PER_SIGNAL_FLOOR;
      this.gateApplyToSize[p] =
        gateModel?.applyToSize === undefined ? 1 : gateModel.applyToSize ? 1 : 0;

      const sizeModel = config.sizeModel;
      this.sizeModelMode[p] = sizeModel?.mode === "edge_weighted" ? 1 : 0;
      this.sizeModelEdgeGamma[p] = sizeModel?.edgeGamma ?? 1.2;
      this.sizeModelMinFloor[p] = sizeModel?.minSizeFloor ?? 0.5;
      this.sizeModelMaxCeil[p] = sizeModel?.maxSizeCeil ?? 1.5;
      this.sizeModelApplyGate[p] =
        sizeModel?.applyGateMultiplier === undefined
          ? 1
          : sizeModel.applyGateMultiplier
            ? 1
            : 0;
      this.sizeModelConfidenceWeight[p] = sizeModel?.confidenceWeight ?? 0;
      this.sizeModelDepthWeight[p] = sizeModel?.depthWeight ?? 0;
      this.sizeModelSpreadPenaltyWeight[p] = sizeModel?.spreadPenaltyWeight ?? 0;

      const lossGovernor = config.lossGovernor;
      this.lossEnabled[p] = lossGovernor ? (lossGovernor.enabled === false ? 0 : 1) : 0;
      this.lossStreakThreshold[p] = lossGovernor?.streakThreshold ?? 2;
      this.lossMinDiffMultiplier[p] = lossGovernor?.minDiffMultiplier ?? 1.2;
      this.lossSizeScaleMultiplier[p] = lossGovernor?.sizeScaleMultiplier ?? 0.7;

      const encodeNullable = (value: number | null | undefined): number => {
        if (value === undefined) return Number.NaN;
        if (value === null) return -1;
        return value;
      };
      const crossModes = config.crossModes;
      this.crossModeEnabled[p] = crossModes ? 1 : 0;
      this.crossModeSplitSec[p] =
        crossModes?.splitTimeSec === undefined || crossModes?.splitTimeSec === null
          ? Number.NaN
          : crossModes.splitTimeSec;
      const precision = crossModes?.precision;
      this.crossPrecisionMinDiffMult[p] = precision?.minDiffMultiplier ?? 1;
      this.crossPrecisionMaxShareMult[p] = precision?.maxShareMultiplier ?? 1;
      this.crossPrecisionMinShareMult[p] = precision?.minShareMultiplier ?? 1;
      this.crossPrecisionMaxSpendMult[p] = precision?.maxSpendMultiplier ?? 1;
      this.crossPrecisionMinConfidence[p] = encodeNullable(
        precision?.minConfidence,
      );
      this.crossPrecisionMinDepth[p] = encodeNullable(precision?.minDepthValue);
      this.crossPrecisionMinVelocity[p] = encodeNullable(
        precision?.minTradeVelocity,
      );
      this.crossPrecisionSizeScaleMult[p] = precision?.sizeScaleMultiplier ?? 1;
      this.crossPrecisionMinRecovery[p] = encodeNullable(
        precision?.minRecoveryMultiple,
      );
      this.crossPrecisionMinLoss[p] = encodeNullable(precision?.minLossToTrigger);

      const opportunistic = crossModes?.opportunistic;
      this.crossOpMinDiffMult[p] = opportunistic?.minDiffMultiplier ?? 1;
      this.crossOpMaxShareMult[p] = opportunistic?.maxShareMultiplier ?? 1;
      this.crossOpMinShareMult[p] = opportunistic?.minShareMultiplier ?? 1;
      this.crossOpMaxSpendMult[p] = opportunistic?.maxSpendMultiplier ?? 1;
      this.crossOpMinConfidence[p] = encodeNullable(opportunistic?.minConfidence);
      this.crossOpMinDepth[p] = encodeNullable(opportunistic?.minDepthValue);
      this.crossOpMinVelocity[p] = encodeNullable(opportunistic?.minTradeVelocity);
      this.crossOpSizeScaleMult[p] = opportunistic?.sizeScaleMultiplier ?? 1;
      this.crossOpMinRecovery[p] = encodeNullable(
        opportunistic?.minRecoveryMultiple,
      );
      this.crossOpMinLoss[p] = encodeNullable(opportunistic?.minLossToTrigger);
    }

    this.totalTrades = new Int32Array(this.profileCount);
    this.crossTrades = new Int32Array(this.profileCount);
    this.wins = new Int32Array(this.profileCount);
    this.losses = new Int32Array(this.profileCount);
    this.totalProfit = new Float64Array(this.profileCount);
    this.positionShares = new Float64Array(this.profileCount);
    this.positionCost = new Float64Array(this.profileCount);
    this.positionOutcome = new Int8Array(this.profileCount);
    this.crossed = new Int8Array(this.profileCount);
    this.realizedPnl = new Float64Array(this.profileCount);
    this.marketHadTrade = new Int8Array(this.profileCount);
    this.marketTradeCounted = new Int8Array(this.profileCount);
    this.pendingDueMs = new Float64Array(this.profileCount);
    this.pendingOutcome = new Int8Array(this.profileCount);
    this.pendingRuleIdx = new Int16Array(this.profileCount);
    this.lastDecisionMs = new Float64Array(this.profileCount);
    this.lastResolvedEpoch = new Int32Array(this.profileCount);
  }

  getNextPendingTime(): number | null {
    return this.nextPendingDueMs ?? null;
  }

  evaluate(snapshot: MarketSnapshot, nowMs: number): void {
    this.lastNowMs = nowMs;

    if (this.currentSlug !== snapshot.slug) {
      if (this.lastSnapshot) {
        this.resolveAll(this.lastSnapshot);
      }
      this.currentSlug = snapshot.slug;
      this.currentEpoch += 1;
      this.resetForNewMarket();
    }

    this.lastSnapshot = snapshot;

    const timeLeftSec = snapshot.timeLeftSec;
    if (timeLeftSec === null) {
      this.nextPendingDueMs = this.findNextPending(nowMs);
      return;
    }

    if (timeLeftSec <= 0) {
      this.resolveAll(snapshot);
      this.nextPendingDueMs = this.findNextPending(nowMs);
      return;
    }

    const upTokenId = snapshot.upTokenId;
    const downTokenId = snapshot.downTokenId;
    const bookUp = snapshot.orderBooks.get(upTokenId);
    const bookDown = snapshot.orderBooks.get(downTokenId);
    const asksUp = bookUp?.asks ?? [];
    const asksDown = bookDown?.asks ?? [];
    const bidsUp = bookUp?.bids ?? [];
    const bidsDown = bookDown?.bids ?? [];

    let nextPending = Number.POSITIVE_INFINITY;
    for (let p = 0; p < this.profileCount; p += 1) {
      const due = this.pendingDueMs[p] ?? 0;
      if (!Number.isFinite(due) || due <= 0) continue;
      if (due <= nowMs) {
        const outcome = this.pendingOutcome[p] ?? 0;
        const ruleIdx = this.pendingRuleIdx[p] ?? 0;
        if (outcome === 1) {
          this.executeTrade(p, asksUp, ruleIdx, 1);
        } else if (outcome === 2) {
          this.executeTrade(p, asksDown, ruleIdx, 2);
        }
        this.pendingDueMs[p] = 0;
        this.pendingOutcome[p] = 0;
        this.pendingRuleIdx[p] = 0;
      } else {
        nextPending = Math.min(nextPending, due);
      }
    }

    const threshold =
      snapshot.priceToBeat > 0 ? snapshot.priceToBeat : snapshot.referencePrice;
    const hasPrice = threshold > 0 && snapshot.cryptoPrice > 0;
    const priceDiff = hasPrice
      ? Math.abs(snapshot.cryptoPrice - threshold)
      : null;
    const favoredUp = hasPrice ? snapshot.cryptoPrice >= threshold : null;

    if (favoredUp === null || priceDiff === null) {
      this.nextPendingDueMs = Number.isFinite(nextPending) ? nextPending : null;
      return;
    }

    const signals = snapshot.signals;
    const upSignal = signals?.tokenSignals.get(upTokenId) ?? null;
    const downSignal = signals?.tokenSignals.get(downTokenId) ?? null;
    const bestAskUp = upSignal?.bestAsk ?? snapshot.bestAsk.get(upTokenId) ?? null;
    const bestAskDown =
      downSignal?.bestAsk ?? snapshot.bestAsk.get(downTokenId) ?? null;
    const spreadUp = upSignal?.spread ?? null;
    const spreadDown = downSignal?.spread ?? null;
    const bookImbalanceUp = upSignal?.bookImbalance ?? null;
    const bookImbalanceDown = downSignal?.bookImbalance ?? null;
    const depthValueUp = upSignal?.depthValue ?? null;
    const depthValueDown = downSignal?.depthValue ?? null;
    const bestAsk = favoredUp ? bestAskUp : bestAskDown;
    const spread = favoredUp ? spreadUp : spreadDown;
    const bookImbalance = favoredUp ? bookImbalanceUp : bookImbalanceDown;
    const depthValue = favoredUp ? depthValueUp : depthValueDown;
    const tradeVelocity = signals?.tradeVelocity ?? null;
    const priceMomentum = signals?.priceMomentum ?? null;
    const priceVolatility = signals?.priceVolatility ?? null;
    const priceStalenessSec = signals?.priceStalenessSec ?? null;
    const tradeFlowImbalance = signals?.tradeFlowImbalance ?? null;
    const referenceQuality = signals?.referenceQuality ?? 0;

    if (snapshot.dataStatus !== "healthy") {
      this.nextPendingDueMs = Number.isFinite(nextPending) ? nextPending : null;
      return;
    }

    if (this.crossRuleCount > 0) {
      const crossTierIdx = this.selectCrossTierIndex(timeLeftSec);
      if (crossTierIdx !== null) {
        for (let p = 0; p < this.profileCount; p += 1) {
          if ((this.crossEnabled[p] ?? 0) !== 1) continue;
          if ((this.positionOutcome[p] ?? 0) === 0) continue;
          if ((this.crossed[p] ?? 0) === 1) continue;
          if (timeLeftSec > (this.crossTradeAllowedTimeLeft[p] ?? 0)) continue;
          if (nowMs - (this.lastDecisionMs[p] ?? 0) < DECISION_COOLDOWN_MS) continue;

          const currentOutcome = this.positionOutcome[p] ?? 0;
          const favoredOutcome = favoredUp ? 1 : 2;
          const shouldFlip = currentOutcome !== favoredOutcome;
          if (!shouldFlip && !CROSS_ALLOW_NO_FLIP) continue;
          const targetOutcome = shouldFlip ? favoredOutcome : favoredUp ? 2 : 1;
          const targetUp = targetOutcome === 1;
          const targetBestAsk = targetUp ? bestAskUp : bestAskDown;
          const targetSpread = targetUp ? spreadUp : spreadDown;
          const targetImbalance = targetUp ? bookImbalanceUp : bookImbalanceDown;
          const targetDepthValue = targetUp ? depthValueUp : depthValueDown;
          if (targetBestAsk === null) continue;

          const offset = p * this.crossRuleCount + crossTierIdx;
          let minDiff = this.crossMinDiff[offset] ?? 0;
          let minShare = this.crossMinShare[offset] ?? 0;
          let maxShare = this.crossMaxShare[offset] ?? 0;
          let sizeScale = this.crossSizeScale[offset] ?? 1;

          let minConfidence = this.normalizeOptional(this.crossMinConfidence[offset] ?? Number.NaN);
          let minDepth = this.normalizeOptional(this.crossMinDepth[offset] ?? Number.NaN);
          let minVelocity = this.normalizeOptional(this.crossMinVelocity[offset] ?? Number.NaN);
          let minMomentum = this.normalizeOptional(this.crossMinMomentum[offset] ?? Number.NaN);
          let minVolatility = this.normalizeOptional(this.crossMinVolatility[offset] ?? Number.NaN);
          let maxSpread = this.normalizeOptional(this.crossMaxSpread[offset] ?? Number.NaN);
          let minImbalance = this.normalizeOptional(this.crossMinImbalance[offset] ?? Number.NaN);
          let maxStaleness = this.normalizeOptional(this.crossMaxStaleness[offset] ?? Number.NaN);
          let minRecovery = this.crossMinRecoveryMultiple[offset] ?? 2;
          let minLoss = this.crossMinLossToTrigger[offset] ?? 0;

          if ((this.crossModeEnabled[p] ?? 0) === 1) {
            const split = Number.isNaN(this.crossModeSplitSec[p] ?? Number.NaN)
              ? (this.crossTradeAllowedTimeLeft[p] ?? 0) / 2
              : this.crossModeSplitSec[p] ?? 0;
            const usePrecision = timeLeftSec > split;
            const minDiffMult = usePrecision
              ? this.crossPrecisionMinDiffMult[p] ?? 1
              : this.crossOpMinDiffMult[p] ?? 1;
            const maxShareMult = usePrecision
              ? this.crossPrecisionMaxShareMult[p] ?? 1
              : this.crossOpMaxShareMult[p] ?? 1;
            const minShareMult = usePrecision
              ? this.crossPrecisionMinShareMult[p] ?? 1
              : this.crossOpMinShareMult[p] ?? 1;
            const maxSpendMult = usePrecision
              ? this.crossPrecisionMaxSpendMult[p] ?? 1
              : this.crossOpMaxSpendMult[p] ?? 1;
            const sizeScaleMult = usePrecision
              ? this.crossPrecisionSizeScaleMult[p] ?? 1
              : this.crossOpSizeScaleMult[p] ?? 1;

            const baseMinShare = minShare;
            const baseMaxShare = maxShare;
            minDiff *= minDiffMult;
            maxShare = clamp(baseMaxShare * maxShareMult, baseMinShare + 0.001, 1);
            minShare = clamp(baseMinShare * minShareMult, 0.01, maxShare - 0.001);
            sizeScale *= maxSpendMult * sizeScaleMult;

            minConfidence = this.resolveOverride(
              minConfidence,
              usePrecision
                ? this.crossPrecisionMinConfidence[p] ?? Number.NaN
                : this.crossOpMinConfidence[p] ?? Number.NaN,
            );
            minDepth = this.resolveOverride(
              minDepth,
              usePrecision
                ? this.crossPrecisionMinDepth[p] ?? Number.NaN
                : this.crossOpMinDepth[p] ?? Number.NaN,
            );
            minVelocity = this.resolveOverride(
              minVelocity,
              usePrecision
                ? this.crossPrecisionMinVelocity[p] ?? Number.NaN
                : this.crossOpMinVelocity[p] ?? Number.NaN,
            );
            minRecovery =
              this.resolveOverride(
                minRecovery,
                usePrecision
                  ? this.crossPrecisionMinRecovery[p] ?? Number.NaN
                  : this.crossOpMinRecovery[p] ?? Number.NaN,
              ) ?? minRecovery;
            minLoss =
              this.resolveOverride(
                minLoss,
                usePrecision
                  ? this.crossPrecisionMinLoss[p] ?? Number.NaN
                  : this.crossOpMinLoss[p] ?? Number.NaN,
              ) ?? minLoss;
          }

          if (priceDiff < minDiff) continue;
          if (targetBestAsk < minShare || targetBestAsk > maxShare) continue;

          const maxExposure = this.crossMaxOpenExposure[offset] ?? Number.NaN;
          if (!Number.isNaN(maxExposure)) {
            if ((this.positionCost[p] ?? 0) > maxExposure) continue;
          }

          const confidenceGate = Number.isNaN(FORCE_MIN_CONFIDENCE)
            ? minConfidence
            : FORCE_MIN_CONFIDENCE;
          const enforceConfidence =
            confidenceGate !== null && !Number.isNaN(confidenceGate);
          const needConfidence =
            enforceConfidence ||
            (this.crossSizeStrategy[offset] ?? 0) === 3 ||
            (this.sizeModelConfidenceWeight[p] ?? 0) > 0 ||
            (this.gateEnabled[p] ?? 0) === 1;
          let confidence: number | null = null;
          if (needConfidence) {
            confidence = this.computeConfidence(
              minDiff,
              targetUp,
              priceDiff,
              targetBestAsk,
              targetSpread,
              targetImbalance,
              tradeFlowImbalance,
              priceMomentum,
              priceStalenessSec,
              referenceQuality,
            );
          }

          const edgeScore = this.computeEdgeScore(
            p,
            minDiff,
            this.crossMaxSpend[offset] ?? 0,
            minDepth,
            minVelocity,
            minMomentum,
            minVolatility,
            maxSpread,
            targetUp,
            priceDiff,
            targetBestAsk,
            targetSpread,
            targetImbalance,
            targetDepthValue,
            tradeVelocity,
            priceMomentum,
            priceVolatility,
            priceStalenessSec,
            referenceQuality,
          );
          const minEdgeScore = this.edgeMinScore[p] ?? Number.NaN;
          if (
            (this.edgeEnabled[p] ?? 0) === 1 &&
            !Number.isNaN(minEdgeScore) &&
            (edgeScore === null || edgeScore < minEdgeScore)
          ) {
            continue;
          }

          const gate = this.computeGateMultiplier(
            p,
            {
              maxSpread,
              minImbalance,
              minDepth,
              minVelocity,
              minMomentum,
              minVolatility,
              maxStaleness,
              minConfidence: enforceConfidence ? (confidenceGate as number) : null,
            },
            {
              spread: targetSpread,
              imbalance: targetImbalance,
              depth: targetDepthValue,
              velocity: tradeVelocity,
              momentum: priceMomentum,
              volatility: priceVolatility,
              staleness: priceStalenessSec,
              confidence,
              favoredUp: targetUp,
            },
          );
          if ((this.gateEnabled[p] ?? 0) === 1) {
            if (gate.blocked) continue;
          } else {
            if (maxSpread !== null && (targetSpread === null || targetSpread > maxSpread)) continue;
            if (minImbalance !== null && (targetImbalance === null || targetImbalance < minImbalance))
              continue;
            if (minDepth !== null && (targetDepthValue === null || targetDepthValue < minDepth))
              continue;
            if (minVelocity !== null && (tradeVelocity === null || tradeVelocity < minVelocity))
              continue;
            if (minMomentum !== null) {
              if (priceMomentum === null) continue;
              if (targetUp) {
                if (priceMomentum < minMomentum) continue;
              } else if (priceMomentum > -minMomentum) {
                continue;
              }
            }
            if (minVolatility !== null && (priceVolatility === null || priceVolatility < minVolatility))
              continue;
            if (maxStaleness !== null && (priceStalenessSec === null || priceStalenessSec > maxStaleness))
              continue;
            if (enforceConfidence && (confidence === null || confidence < (confidenceGate as number)))
              continue;
          }

          const exitBids = currentOutcome === 1 ? bidsUp : bidsDown;
          const exitFill = this.simulateSell(exitBids, this.positionShares[p] ?? 0);
          if (!exitFill) continue;
          const realized = exitFill.proceeds - (this.positionCost[p] ?? 0);
          if (realized >= 0) continue;
          if (-realized < minLoss) continue;

          const entryAsks = targetUp ? asksUp : asksDown;
          const maxSpend = this.resolveCrossMaxSpend(
            p,
            offset,
            entryAsks,
            targetOutcome,
            minDiff,
            sizeScale,
          );
          const entryFill = this.simulateBuy(
            entryAsks,
            minShare,
            maxShare,
            maxSpend,
            this.crossMinSpend[offset] ?? 0,
          );
          if (!entryFill) continue;

          const potentialProfit = entryFill.shares - entryFill.cost;
          if (potentialProfit < -realized * minRecovery) continue;

          this.realizedPnl[p] = (this.realizedPnl[p] ?? 0) + realized;
          this.positionOutcome[p] = targetOutcome;
          this.positionShares[p] = entryFill.shares;
          this.positionCost[p] = entryFill.cost;
          if ((this.marketTradeCounted[p] ?? 0) === 0) {
            this.totalTrades[p] = (this.totalTrades[p] ?? 0) + 1;
            this.marketTradeCounted[p] = 1;
          }
          this.crossTrades[p] = (this.crossTrades[p] ?? 0) + 1;
          this.crossed[p] = 1;
          this.marketHadTrade[p] = 1;
          this.lastDecisionMs[p] = nowMs;
        }
      }
    }

    if (timeLeftSec > this.maxTradeAllowed) {
      this.nextPendingDueMs = Number.isFinite(nextPending) ? nextPending : null;
      return;
    }

    const tierIdx = this.selectTierIndex(timeLeftSec);
    if (tierIdx === null) {
      this.nextPendingDueMs = Number.isFinite(nextPending) ? nextPending : null;
      return;
    }

    if (priceDiff < (this.minDiffByTier[tierIdx] ?? Number.POSITIVE_INFINITY)) {
      this.nextPendingDueMs = Number.isFinite(nextPending) ? nextPending : null;
      return;
    }

    if (
      bestAsk === null ||
      bestAsk < (this.minShareByTier[tierIdx] ?? Number.POSITIVE_INFINITY) ||
      bestAsk > (this.maxShareByTier[tierIdx] ?? 0)
    ) {
      this.nextPendingDueMs = Number.isFinite(nextPending) ? nextPending : null;
      return;
    }

    for (let p = 0; p < this.profileCount; p += 1) {
      if ((this.positionOutcome[p] ?? 0) !== 0) continue;
      if ((this.pendingDueMs[p] ?? 0) > nowMs) continue;
      if (timeLeftSec > (this.tradeAllowedTimeLeft[p] ?? 0)) continue;
      if (nowMs - (this.lastDecisionMs[p] ?? 0) < DECISION_COOLDOWN_MS) continue;

      const offset = p * this.ruleCount + tierIdx;
      const lossActive =
        (this.lossEnabled[p] ?? 0) === 1 &&
        (this.lossStreak[p] ?? 0) >= (this.lossStreakThreshold[p] ?? 2);
      const baseMinDiff = this.minDiff[offset] ?? 0;
      const minDiff = lossActive
        ? baseMinDiff * (this.lossMinDiffMultiplier[p] ?? 1)
        : baseMinDiff;
      if (priceDiff < minDiff) continue;
      if (
        bestAsk < (this.minShare[offset] ?? Number.POSITIVE_INFINITY) ||
        bestAsk > (this.maxShare[offset] ?? 0)
      )
        continue;

      const maxSpread = this.normalizeOptional(this.maxSpread[offset] ?? Number.NaN);
      const minImbalance = this.normalizeOptional(this.minImbalance[offset] ?? Number.NaN);
      const minDepth = this.normalizeOptional(this.minDepth[offset] ?? Number.NaN);
      const minVelocity = this.normalizeOptional(this.minVelocity[offset] ?? Number.NaN);
      const minMomentum = this.normalizeOptional(this.minMomentum[offset] ?? Number.NaN);
      const minVol = this.normalizeOptional(this.minVolatility[offset] ?? Number.NaN);
      const maxStale = this.normalizeOptional(this.maxStaleness[offset] ?? Number.NaN);

      const maxExposure = this.maxOpenExposure[offset] ?? Number.NaN;
      if (!Number.isNaN(maxExposure)) {
        if ((this.positionCost[p] ?? 0) > maxExposure) continue;
      }

      const rawMinConfidence = this.normalizeOptional(this.minConfidence[offset] ?? Number.NaN);
      const confidenceGate = Number.isNaN(FORCE_MIN_CONFIDENCE)
        ? rawMinConfidence
        : FORCE_MIN_CONFIDENCE;
      const enforceConfidence =
        confidenceGate !== null && !Number.isNaN(confidenceGate);
      const needConfidence =
        enforceConfidence ||
        (this.sizeStrategy[offset] ?? 0) === 3 ||
        (this.sizeModelConfidenceWeight[p] ?? 0) > 0 ||
        (this.gateEnabled[p] ?? 0) === 1;
      let confidence: number | null = null;
      if (needConfidence) {
        confidence = this.computeConfidence(
          minDiff,
          favoredUp,
          priceDiff,
          bestAsk,
          spread,
          bookImbalance,
          tradeFlowImbalance,
          priceMomentum,
          priceStalenessSec,
          referenceQuality,
        );
      }

      const edgeScore = this.computeEdgeScore(
        p,
        minDiff,
        this.maxSpend[offset] ?? 0,
        minDepth,
        minVelocity,
        minMomentum,
        minVol,
        maxSpread,
        favoredUp,
        priceDiff,
        bestAsk,
        spread,
        bookImbalance,
        depthValue,
        tradeVelocity,
        priceMomentum,
        priceVolatility,
        priceStalenessSec,
        referenceQuality,
      );
      const minEdgeScore = this.edgeMinScore[p] ?? Number.NaN;
      if (
        (this.edgeEnabled[p] ?? 0) === 1 &&
        !Number.isNaN(minEdgeScore) &&
        (edgeScore === null || edgeScore < minEdgeScore)
      ) {
        continue;
      }

      const gate = this.computeGateMultiplier(
        p,
        {
          maxSpread,
          minImbalance,
          minDepth,
          minVelocity,
          minMomentum,
          minVolatility: minVol,
          maxStaleness: maxStale,
          minConfidence: enforceConfidence ? (confidenceGate as number) : null,
        },
        {
          spread,
          imbalance: bookImbalance,
          depth: depthValue,
          velocity: tradeVelocity,
          momentum: priceMomentum,
          volatility: priceVolatility,
          staleness: priceStalenessSec,
          confidence,
          favoredUp,
        },
      );
      if ((this.gateEnabled[p] ?? 0) === 1) {
        if (gate.blocked) continue;
      } else {
        if (maxSpread !== null && (spread === null || spread > maxSpread)) continue;
        if (minImbalance !== null && (bookImbalance === null || bookImbalance < minImbalance))
          continue;
        if (minDepth !== null && (depthValue === null || depthValue < minDepth)) continue;
        if (minVelocity !== null && (tradeVelocity === null || tradeVelocity < minVelocity))
          continue;
        if (minMomentum !== null) {
          if (priceMomentum === null) continue;
          if (favoredUp) {
            if (priceMomentum < minMomentum) continue;
          } else if (priceMomentum > -minMomentum) {
            continue;
          }
        }
        if (minVol !== null && (priceVolatility === null || priceVolatility < minVol)) continue;
        if (maxStale !== null && (priceStalenessSec === null || priceStalenessSec > maxStale))
          continue;
        if (enforceConfidence && (confidence === null || confidence < (confidenceGate as number)))
          continue;
      }

      const dueMs =
        nowMs + LATENCY_BASE_MS + Math.floor(Math.random() * LATENCY_JITTER_MS);
      this.pendingDueMs[p] = dueMs;
      this.pendingOutcome[p] = favoredUp ? 1 : 2;
      this.pendingRuleIdx[p] = tierIdx;
      this.lastDecisionMs[p] = nowMs;
      nextPending = Math.min(nextPending, dueMs);
    }

    this.nextPendingDueMs = Number.isFinite(nextPending) ? nextPending : null;
  }

  getSummaries(): SweepKernelSummary[] {
    const runtimeSec = (this.lastNowMs - this.startMs) / 1000;
    const summaries: SweepKernelSummary[] = [];
    for (let p = 0; p < this.profileCount; p += 1) {
      summaries.push({
        runtimeSec,
        totalTrades: this.totalTrades[p] ?? 0,
        crossTrades: this.crossTrades[p] ?? 0,
        wins: this.wins[p] ?? 0,
        losses: this.losses[p] ?? 0,
        totalProfit: this.totalProfit[p] ?? 0,
        openExposure:
          (this.positionOutcome[p] ?? 0) !== 0
            ? this.positionCost[p] ?? 0
            : 0,
      });
    }
    return summaries;
  }

  private selectTierIndex(timeLeftSec: number): number | null {
    for (let i = 0; i < this.tierSeconds.length; i += 1) {
      const tier = this.tierSeconds[i];
      if (tier !== undefined && timeLeftSec <= tier) {
        return i;
      }
    }
    return null;
  }

  private selectCrossTierIndex(timeLeftSec: number): number | null {
    for (let i = 0; i < this.crossTierSeconds.length; i += 1) {
      const tier = this.crossTierSeconds[i];
      if (tier !== undefined && timeLeftSec <= tier) {
        return i;
      }
    }
    return null;
  }

  private resolveAll(snapshot: MarketSnapshot): void {
    const threshold =
      snapshot.priceToBeat > 0 ? snapshot.priceToBeat : snapshot.referencePrice;
    if (threshold <= 0) {
      for (let p = 0; p < this.profileCount; p += 1) {
        this.positionOutcome[p] = 0;
        this.positionShares[p] = 0;
        this.positionCost[p] = 0;
        this.crossed[p] = 0;
        this.realizedPnl[p] = 0;
        this.marketHadTrade[p] = 0;
        this.marketTradeCounted[p] = 0;
        this.pendingDueMs[p] = 0;
        this.pendingOutcome[p] = 0;
        this.pendingRuleIdx[p] = 0;
        this.lastResolvedEpoch[p] = this.currentEpoch;
      }
      return;
    }

    const outcomeUp = snapshot.cryptoPrice >= threshold;
    const winningOutcome = outcomeUp ? 1 : 2;
    for (let p = 0; p < this.profileCount; p += 1) {
      if ((this.lastResolvedEpoch[p] ?? -1) === this.currentEpoch) continue;
      let netPnl = this.realizedPnl[p] ?? 0;
      const outcome = this.positionOutcome[p] ?? 0;
      if (outcome !== 0) {
        const win = outcome === winningOutcome;
        const cost = this.positionCost[p] ?? 0;
        const shares = this.positionShares[p] ?? 0;
        const profit = win ? shares - cost : -cost;
        netPnl += profit;
      }
      if ((this.marketHadTrade[p] ?? 0) === 1) {
        if (netPnl >= 0) {
          this.wins[p] = (this.wins[p] ?? 0) + 1;
          this.lossStreak[p] = 0;
        } else {
          this.losses[p] = (this.losses[p] ?? 0) + 1;
          this.lossStreak[p] = (this.lossStreak[p] ?? 0) + 1;
        }
        this.totalProfit[p] = (this.totalProfit[p] ?? 0) + netPnl;
      }
      this.positionOutcome[p] = 0;
      this.positionShares[p] = 0;
      this.positionCost[p] = 0;
      this.crossed[p] = 0;
      this.realizedPnl[p] = 0;
      this.marketHadTrade[p] = 0;
      this.pendingDueMs[p] = 0;
      this.pendingOutcome[p] = 0;
      this.pendingRuleIdx[p] = 0;
      this.lastResolvedEpoch[p] = this.currentEpoch;
    }
  }

  private resetForNewMarket(): void {
    for (let p = 0; p < this.profileCount; p += 1) {
      this.positionOutcome[p] = 0;
      this.positionShares[p] = 0;
      this.positionCost[p] = 0;
      this.crossed[p] = 0;
      this.realizedPnl[p] = 0;
      this.marketHadTrade[p] = 0;
      this.marketTradeCounted[p] = 0;
      this.pendingDueMs[p] = 0;
      this.pendingOutcome[p] = 0;
      this.pendingRuleIdx[p] = 0;
      this.lastDecisionMs[p] = 0;
      this.lastResolvedEpoch[p] = -1;
    }
  }

  private executeTrade(
    profileIdx: number,
    asks: Array<{ price: number; size: number }>,
    ruleIdx: number,
    outcome: 1 | 2,
  ): void {
    if (asks.length === 0) {
      return;
    }

    const offset = profileIdx * this.ruleCount + ruleIdx;
    const lossActive =
      (this.lossEnabled[profileIdx] ?? 0) === 1 &&
      (this.lossStreak[profileIdx] ?? 0) >= (this.lossStreakThreshold[profileIdx] ?? 2);
    const baseMinDiff = this.minDiff[offset] ?? 0;
    const effectiveMinDiff = lossActive
      ? baseMinDiff * (this.lossMinDiffMultiplier[profileIdx] ?? 1)
      : baseMinDiff;
    const sizeScale = (this.sizeScale[offset] ?? 1) *
      (lossActive ? this.lossSizeScaleMultiplier[profileIdx] ?? 1 : 1);
    const maxSpend = this.resolveMaxSpend(
      profileIdx,
      offset,
      asks,
      outcome,
      effectiveMinDiff,
      sizeScale,
    );

    const fill = this.simulateBuy(
      asks,
      this.minShare[offset] ?? 0,
      this.maxShare[offset] ?? 0,
      maxSpend,
      this.minSpend[offset] ?? 0,
    );
    if (!fill) return;

    this.positionOutcome[profileIdx] = outcome;
    this.positionShares[profileIdx] = fill.shares;
    this.positionCost[profileIdx] = fill.cost;
    if ((this.marketTradeCounted[profileIdx] ?? 0) === 0) {
      this.totalTrades[profileIdx] = (this.totalTrades[profileIdx] ?? 0) + 1;
      this.marketTradeCounted[profileIdx] = 1;
    }
    this.marketHadTrade[profileIdx] = 1;
  }

  private simulateBuy(
    asks: Array<{ price: number; size: number }>,
    minShare: number,
    maxShare: number,
    maxSpend: number,
    minSpend: number,
  ): { shares: number; cost: number } | null {
    let remaining = maxSpend;
    let cost = 0;
    let shares = 0;

    for (const ask of asks) {
      if (ask.price > maxShare) break;
      if (ask.price < minShare) continue;
      const availableValue = ask.price * ask.size;
      if (availableValue <= 0) continue;
      const useValue = Math.min(remaining, availableValue);
      const sharesToBuy = useValue / ask.price;
      if (sharesToBuy <= 0) continue;
      shares += sharesToBuy;
      cost += useValue;
      remaining -= useValue;
      if (remaining <= 0) break;
    }

    if (cost < minSpend || shares <= 0) {
      return null;
    }

    return { shares, cost };
  }

  private simulateSell(
    bids: Array<{ price: number; size: number }>,
    sharesToSell: number,
  ): { shares: number; proceeds: number } | null {
    if (sharesToSell <= 0) return null;
    let remaining = sharesToSell;
    let proceeds = 0;
    let shares = 0;

    for (const bid of bids) {
      if (bid.size <= 0) continue;
      const sharesToTake = Math.min(bid.size, remaining);
      if (sharesToTake <= 0) continue;
      shares += sharesToTake;
      proceeds += sharesToTake * bid.price;
      remaining -= sharesToTake;
      if (remaining <= 0) break;
    }

    if (shares < sharesToSell) {
      return null;
    }

    return { shares, proceeds };
  }

  private resolveMaxSpend(
    profileIdx: number,
    offset: number,
    asks: Array<{ price: number; size: number }>,
    outcome: 1 | 2,
    effectiveMinDiff: number,
    sizeScaleOverride: number,
  ): number {
    const base = this.maxSpend[offset] ?? 0;
    const minSpend = this.minSpend[offset] ?? 0;
    const strategy = this.sizeStrategy[offset] ?? 0;
    let factor = 1;

    const snapshot = this.lastSnapshot;
    const threshold = snapshot
      ? snapshot.priceToBeat > 0
        ? snapshot.priceToBeat
        : snapshot.referencePrice
      : 0;
    const hasPrice = snapshot ? threshold > 0 && snapshot.cryptoPrice > 0 : false;
    const priceDiff = hasPrice && snapshot
      ? Math.abs(snapshot.cryptoPrice - threshold)
      : null;
    const favoredUp = outcome === 1;
    const signals = snapshot?.signals;
    const tokenId = snapshot ? (favoredUp ? snapshot.upTokenId : snapshot.downTokenId) : null;
    const tokenSignal =
      tokenId && signals ? signals.tokenSignals.get(tokenId) ?? null : null;
    const bestAsk =
      tokenSignal?.bestAsk ?? (tokenId && snapshot ? snapshot.bestAsk.get(tokenId) ?? null : null);
    const spread = tokenSignal?.spread ?? null;
    const bookImbalance = tokenSignal?.bookImbalance ?? null;
    const depthValue = tokenSignal?.depthValue ?? null;
    const tradeVelocity = signals?.tradeVelocity ?? null;
    const priceMomentum = signals?.priceMomentum ?? null;
    const priceVolatility = signals?.priceVolatility ?? null;
    const priceStalenessSec = signals?.priceStalenessSec ?? null;
    const tradeFlowImbalance = signals?.tradeFlowImbalance ?? null;
    const referenceQuality = signals?.referenceQuality ?? 0;

    let confidence: number | null = null;
    const needConfidence =
      strategy === 3 ||
      (this.sizeModelConfidenceWeight[profileIdx] ?? 0) > 0 ||
      (this.gateEnabled[profileIdx] ?? 0) === 1;
    if (needConfidence) {
      confidence = this.computeConfidence(
        effectiveMinDiff,
        favoredUp,
        priceDiff,
        bestAsk,
        spread,
        bookImbalance,
        tradeFlowImbalance,
        priceMomentum,
        priceStalenessSec,
        referenceQuality,
      );
    }

    if (strategy === 1) {
      if (priceDiff !== null) {
        const denom = Math.max(effectiveMinDiff, 1);
        factor = clamp(priceDiff / denom, 0.5, 2);
      }
    } else if (strategy === 2) {
      let depthTotal = 0;
      const depthLevels = Math.min(asks.length, 3);
      for (let i = 0; i < depthLevels; i += 1) {
        depthTotal += asks[i]!.price * asks[i]!.size;
      }
      if (depthTotal > 0) {
        factor = clamp(depthTotal / Math.max(base, 1), 0.5, 2);
      }
    } else if (strategy === 3 && confidence !== null) {
      factor = clamp(0.5 + confidence * 0.5, 0.5, 1);
    }

    if ((this.sizeModelMode[profileIdx] ?? 0) === 1) {
      const edgeScore = this.computeEdgeScore(
        profileIdx,
        effectiveMinDiff,
        base,
        this.normalizeOptional(this.minDepth[offset] ?? Number.NaN),
        this.normalizeOptional(this.minVelocity[offset] ?? Number.NaN),
        this.normalizeOptional(this.minMomentum[offset] ?? Number.NaN),
        this.normalizeOptional(this.minVolatility[offset] ?? Number.NaN),
        this.normalizeOptional(this.maxSpread[offset] ?? Number.NaN),
        favoredUp,
        priceDiff,
        bestAsk,
        spread,
        bookImbalance,
        depthValue,
        tradeVelocity,
        priceMomentum,
        priceVolatility,
        priceStalenessSec,
        referenceQuality,
      );
      const gamma = this.sizeModelEdgeGamma[profileIdx] ?? 1.2;
      const minFloor = this.sizeModelMinFloor[profileIdx] ?? 0.5;
      const maxCeil = this.sizeModelMaxCeil[profileIdx] ?? 1.5;
      const edgeFactor =
        edgeScore !== null
          ? clamp(Math.pow(edgeScore, gamma), minFloor, maxCeil)
          : minFloor;
      factor *= edgeFactor;
    }

    const confidenceWeight = this.sizeModelConfidenceWeight[profileIdx] ?? 0;
    if (confidenceWeight > 0 && confidence !== null) {
      const weight = clamp(confidenceWeight, 0, 1);
      factor *= clamp(1 - weight + weight * confidence, 0.4, 1.6);
    }

    const depthWeight = this.sizeModelDepthWeight[profileIdx] ?? 0;
    if (depthWeight > 0 && depthValue !== null) {
      const depthRatio = depthValue / Math.max(base, 1);
      factor *= clamp(1 + depthWeight * (depthRatio - 1), 0.4, 2);
    }

    const spreadPenaltyWeight = this.sizeModelSpreadPenaltyWeight[profileIdx] ?? 0;
    if (spreadPenaltyWeight > 0 && spread !== null && bestAsk !== null && bestAsk > 0) {
      factor *= clamp(1 - spreadPenaltyWeight * (spread / bestAsk), 0.2, 1);
    }

    const gateEnabled = (this.gateEnabled[profileIdx] ?? 0) === 1;
    const applyGate =
      gateEnabled &&
      ((this.gateApplyToSize[profileIdx] ?? 1) === 1 ||
        (this.sizeModelApplyGate[profileIdx] ?? 1) === 1);
    if (applyGate) {
      const gate = this.computeGateMultiplier(
        profileIdx,
        {
          maxSpread: this.normalizeOptional(this.maxSpread[offset] ?? Number.NaN),
          minImbalance: this.normalizeOptional(this.minImbalance[offset] ?? Number.NaN),
          minDepth: this.normalizeOptional(this.minDepth[offset] ?? Number.NaN),
          minVelocity: this.normalizeOptional(this.minVelocity[offset] ?? Number.NaN),
          minMomentum: this.normalizeOptional(this.minMomentum[offset] ?? Number.NaN),
          minVolatility: this.normalizeOptional(this.minVolatility[offset] ?? Number.NaN),
          maxStaleness: this.normalizeOptional(this.maxStaleness[offset] ?? Number.NaN),
          minConfidence: this.normalizeOptional(this.minConfidence[offset] ?? Number.NaN),
        },
        {
          spread,
          imbalance: bookImbalance,
          depth: depthValue,
          velocity: tradeVelocity,
          momentum: priceMomentum,
          volatility: priceVolatility,
          staleness: priceStalenessSec,
          confidence,
          favoredUp,
        },
      );
      factor *= clamp(gate.multiplier, 0.1, 1);
    }

    const scaled = base * sizeScaleOverride * factor;
    return Math.max(minSpend, scaled);
  }

  private resolveCrossMaxSpend(
    profileIdx: number,
    offset: number,
    asks: Array<{ price: number; size: number }>,
    outcome: 1 | 2,
    effectiveMinDiff: number,
    sizeScaleOverride: number,
  ): number {
    const base = this.crossMaxSpend[offset] ?? 0;
    const minSpend = this.crossMinSpend[offset] ?? 0;
    const strategy = this.crossSizeStrategy[offset] ?? 0;
    let factor = 1;

    const snapshot = this.lastSnapshot;
    const threshold = snapshot
      ? snapshot.priceToBeat > 0
        ? snapshot.priceToBeat
        : snapshot.referencePrice
      : 0;
    const hasPrice = snapshot ? threshold > 0 && snapshot.cryptoPrice > 0 : false;
    const priceDiff = hasPrice && snapshot
      ? Math.abs(snapshot.cryptoPrice - threshold)
      : null;
    const favoredUp = outcome === 1;
    const signals = snapshot?.signals;
    const tokenId = snapshot ? (favoredUp ? snapshot.upTokenId : snapshot.downTokenId) : null;
    const tokenSignal =
      tokenId && signals ? signals.tokenSignals.get(tokenId) ?? null : null;
    const bestAsk =
      tokenSignal?.bestAsk ?? (tokenId && snapshot ? snapshot.bestAsk.get(tokenId) ?? null : null);
    const spread = tokenSignal?.spread ?? null;
    const bookImbalance = tokenSignal?.bookImbalance ?? null;
    const depthValue = tokenSignal?.depthValue ?? null;
    const tradeVelocity = signals?.tradeVelocity ?? null;
    const priceMomentum = signals?.priceMomentum ?? null;
    const priceVolatility = signals?.priceVolatility ?? null;
    const priceStalenessSec = signals?.priceStalenessSec ?? null;
    const tradeFlowImbalance = signals?.tradeFlowImbalance ?? null;
    const referenceQuality = signals?.referenceQuality ?? 0;

    let confidence: number | null = null;
    const needConfidence =
      strategy === 3 ||
      (this.sizeModelConfidenceWeight[profileIdx] ?? 0) > 0 ||
      (this.gateEnabled[profileIdx] ?? 0) === 1;
    if (needConfidence) {
      confidence = this.computeConfidence(
        effectiveMinDiff,
        favoredUp,
        priceDiff,
        bestAsk,
        spread,
        bookImbalance,
        tradeFlowImbalance,
        priceMomentum,
        priceStalenessSec,
        referenceQuality,
      );
    }

    if (strategy === 1) {
      if (priceDiff !== null) {
        const denom = Math.max(effectiveMinDiff, 1);
        factor = clamp(priceDiff / denom, 0.5, 2);
      }
    } else if (strategy === 2) {
      let depthTotal = 0;
      const depthLevels = Math.min(asks.length, 3);
      for (let i = 0; i < depthLevels; i += 1) {
        depthTotal += asks[i]!.price * asks[i]!.size;
      }
      if (depthTotal > 0) {
        factor = clamp(depthTotal / Math.max(base, 1), 0.5, 2);
      }
    } else if (strategy === 3 && confidence !== null) {
      factor = clamp(0.5 + confidence * 0.5, 0.5, 1);
    }

    if ((this.sizeModelMode[profileIdx] ?? 0) === 1) {
      const edgeScore = this.computeEdgeScore(
        profileIdx,
        effectiveMinDiff,
        base,
        this.normalizeOptional(this.crossMinDepth[offset] ?? Number.NaN),
        this.normalizeOptional(this.crossMinVelocity[offset] ?? Number.NaN),
        this.normalizeOptional(this.crossMinMomentum[offset] ?? Number.NaN),
        this.normalizeOptional(this.crossMinVolatility[offset] ?? Number.NaN),
        this.normalizeOptional(this.crossMaxSpread[offset] ?? Number.NaN),
        favoredUp,
        priceDiff,
        bestAsk,
        spread,
        bookImbalance,
        depthValue,
        tradeVelocity,
        priceMomentum,
        priceVolatility,
        priceStalenessSec,
        referenceQuality,
      );
      const gamma = this.sizeModelEdgeGamma[profileIdx] ?? 1.2;
      const minFloor = this.sizeModelMinFloor[profileIdx] ?? 0.5;
      const maxCeil = this.sizeModelMaxCeil[profileIdx] ?? 1.5;
      const edgeFactor =
        edgeScore !== null
          ? clamp(Math.pow(edgeScore, gamma), minFloor, maxCeil)
          : minFloor;
      factor *= edgeFactor;
    }

    const confidenceWeight = this.sizeModelConfidenceWeight[profileIdx] ?? 0;
    if (confidenceWeight > 0 && confidence !== null) {
      const weight = clamp(confidenceWeight, 0, 1);
      factor *= clamp(1 - weight + weight * confidence, 0.4, 1.6);
    }

    const depthWeight = this.sizeModelDepthWeight[profileIdx] ?? 0;
    if (depthWeight > 0 && depthValue !== null) {
      const depthRatio = depthValue / Math.max(base, 1);
      factor *= clamp(1 + depthWeight * (depthRatio - 1), 0.4, 2);
    }

    const spreadPenaltyWeight = this.sizeModelSpreadPenaltyWeight[profileIdx] ?? 0;
    if (spreadPenaltyWeight > 0 && spread !== null && bestAsk !== null && bestAsk > 0) {
      factor *= clamp(1 - spreadPenaltyWeight * (spread / bestAsk), 0.2, 1);
    }

    const gateEnabled = (this.gateEnabled[profileIdx] ?? 0) === 1;
    const applyGate =
      gateEnabled &&
      ((this.gateApplyToSize[profileIdx] ?? 1) === 1 ||
        (this.sizeModelApplyGate[profileIdx] ?? 1) === 1);
    if (applyGate) {
      const gate = this.computeGateMultiplier(
        profileIdx,
        {
          maxSpread: this.normalizeOptional(this.crossMaxSpread[offset] ?? Number.NaN),
          minImbalance: this.normalizeOptional(this.crossMinImbalance[offset] ?? Number.NaN),
          minDepth: this.normalizeOptional(this.crossMinDepth[offset] ?? Number.NaN),
          minVelocity: this.normalizeOptional(this.crossMinVelocity[offset] ?? Number.NaN),
          minMomentum: this.normalizeOptional(this.crossMinMomentum[offset] ?? Number.NaN),
          minVolatility: this.normalizeOptional(this.crossMinVolatility[offset] ?? Number.NaN),
          maxStaleness: this.normalizeOptional(this.crossMaxStaleness[offset] ?? Number.NaN),
          minConfidence: this.normalizeOptional(this.crossMinConfidence[offset] ?? Number.NaN),
        },
        {
          spread,
          imbalance: bookImbalance,
          depth: depthValue,
          velocity: tradeVelocity,
          momentum: priceMomentum,
          volatility: priceVolatility,
          staleness: priceStalenessSec,
          confidence,
          favoredUp,
        },
      );
      factor *= clamp(gate.multiplier, 0.1, 1);
    }

    const scaled = base * sizeScaleOverride * factor;
    return Math.max(minSpend, scaled);
  }

  private computeConfidence(
    minDiff: number,
    favoredUp: boolean | null,
    priceDiff: number | null,
    bestAsk: number | null,
    spread: OptionalNumber,
    bookImbalance: OptionalNumber,
    tradeFlowImbalance: OptionalNumber,
    priceMomentum: OptionalNumber,
    priceStalenessSec: OptionalNumber,
    referenceQuality: number,
  ): number | null {
    let weightedSum = 0;
    let weightTotal = 0;
    let decay = 1;

    if (spread !== null && spread !== undefined && bestAsk && bestAsk > 0) {
      const spreadScore = clamp(1 - spread / bestAsk, 0, 1);
      weightedSum += spreadScore * CONFIDENCE_WEIGHTS.spread;
      weightTotal += CONFIDENCE_WEIGHTS.spread;
    }
    if (bookImbalance !== null && bookImbalance !== undefined) {
      weightedSum += clamp(bookImbalance, 0, 1) * CONFIDENCE_WEIGHTS.imbalance;
      weightTotal += CONFIDENCE_WEIGHTS.imbalance;
    }
    if (tradeFlowImbalance !== null && tradeFlowImbalance !== undefined) {
      const flowScore = clamp((tradeFlowImbalance + 1) / 2, 0, 1);
      weightedSum += flowScore * CONFIDENCE_WEIGHTS.tradeFlow;
      weightTotal += CONFIDENCE_WEIGHTS.tradeFlow;
    }
    if (
      priceMomentum !== null &&
      priceMomentum !== undefined &&
      favoredUp !== null
    ) {
      const aligned = favoredUp ? priceMomentum : -priceMomentum;
      const momentumScore = clamp(aligned / Math.max(minDiff, 1), 0, 1);
      weightedSum += momentumScore * CONFIDENCE_WEIGHTS.momentum;
      weightTotal += CONFIDENCE_WEIGHTS.momentum;
    }
    if (priceStalenessSec !== null && priceStalenessSec !== undefined) {
      const stalenessScore = clamp(1 - priceStalenessSec / 60, 0, 1);
      weightedSum += stalenessScore * CONFIDENCE_WEIGHTS.staleness;
      weightTotal += CONFIDENCE_WEIGHTS.staleness;
      decay = clamp(Math.exp(-priceStalenessSec / 30), 0.5, 1);
    }
    weightedSum += clamp(referenceQuality, 0, 1) * CONFIDENCE_WEIGHTS.reference;
    weightTotal += CONFIDENCE_WEIGHTS.reference;

    if (weightTotal === 0) return null;
    const baseConfidence = weightedSum / weightTotal;

    if (priceDiff !== null) {
      const diffFactor = clamp(
        priceDiff / Math.max(minDiff, 1),
        0.5,
        1.5,
      );
      return clamp(baseConfidence * diffFactor * decay, 0, 1);
    }

    return clamp(baseConfidence * decay, 0, 1);
  }

  private normalizeOptional(value: number): number | null {
    return Number.isNaN(value) ? null : value;
  }

  private resolveOverride(base: number | null, override: number): number | null {
    if (Number.isNaN(override)) return base;
    if (override < 0) return null;
    return override;
  }

  private computeEdgeScore(
    profileIdx: number,
    minDiff: number,
    maxSpend: number,
    minDepth: number | null,
    minVelocity: number | null,
    minMomentum: number | null,
    minVolatility: number | null,
    maxSpread: number | null,
    favoredUp: boolean,
    priceDiff: number | null,
    bestAsk: number | null,
    spread: number | null,
    bookImbalance: number | null,
    depthValue: number | null,
    tradeVelocity: number | null,
    priceMomentum: number | null,
    priceVolatility: number | null,
    priceStalenessSec: number | null,
    referenceQuality: number,
  ): number | null {
    if ((this.edgeEnabled[profileIdx] ?? 0) !== 1) return null;
    if (
      (this.edgeRequireSignals[profileIdx] ?? 1) === 1 &&
      spread === null &&
      bookImbalance === null &&
      depthValue === null &&
      tradeVelocity === null &&
      priceMomentum === null &&
      priceVolatility === null
    ) {
      return null;
    }

    let weightedSum = 0;
    let weightTotal = 0;

    if (priceDiff !== null) {
      const denom = Math.max(minDiff, 1);
      const cap = this.edgeCapGap[profileIdx] ?? DEFAULT_EDGE_CAPS.gap;
      const score = clamp(priceDiff / denom, 0, cap) / cap;
      weightedSum += score * (this.edgeWeightGap[profileIdx] ?? 0);
      weightTotal += this.edgeWeightGap[profileIdx] ?? 0;
    }

    if (depthValue !== null) {
      const denom = minDepth !== null ? Math.max(minDepth, 1) : Math.max(maxSpend, 1);
      const cap = this.edgeCapDepth[profileIdx] ?? DEFAULT_EDGE_CAPS.depth;
      const score = clamp(depthValue / denom, 0, cap) / cap;
      weightedSum += score * (this.edgeWeightDepth[profileIdx] ?? 0);
      weightTotal += this.edgeWeightDepth[profileIdx] ?? 0;
    }

    if (bookImbalance !== null) {
      weightedSum +=
        clamp(bookImbalance, 0, 1) * (this.edgeWeightImbalance[profileIdx] ?? 0);
      weightTotal += this.edgeWeightImbalance[profileIdx] ?? 0;
    }

    if (tradeVelocity !== null) {
      const denom = minVelocity !== null ? Math.max(minVelocity, 1) : 1;
      const cap = this.edgeCapVelocity[profileIdx] ?? DEFAULT_EDGE_CAPS.velocity;
      const score = clamp(tradeVelocity / denom, 0, cap) / cap;
      weightedSum += score * (this.edgeWeightVelocity[profileIdx] ?? 0);
      weightTotal += this.edgeWeightVelocity[profileIdx] ?? 0;
    }

    if (priceMomentum !== null) {
      const aligned = favoredUp ? priceMomentum : -priceMomentum;
      const denom = minMomentum !== null ? Math.max(minMomentum, 1) : Math.max(minDiff, 1);
      const cap = this.edgeCapMomentum[profileIdx] ?? DEFAULT_EDGE_CAPS.momentum;
      const score = clamp(aligned / denom, 0, cap) / cap;
      weightedSum += score * (this.edgeWeightMomentum[profileIdx] ?? 0);
      weightTotal += this.edgeWeightMomentum[profileIdx] ?? 0;
    }

    if (priceVolatility !== null) {
      const denom = minVolatility !== null ? Math.max(minVolatility, 1) : 1;
      const cap = this.edgeCapVolatility[profileIdx] ?? DEFAULT_EDGE_CAPS.volatility;
      const score = clamp(priceVolatility / denom, 0, cap) / cap;
      weightedSum += score * (this.edgeWeightVolatility[profileIdx] ?? 0);
      weightTotal += this.edgeWeightVolatility[profileIdx] ?? 0;
    }

    if (spread !== null && bestAsk !== null && bestAsk > 0) {
      const spreadBase = maxSpread !== null ? maxSpread : bestAsk * 0.02;
      const score = clamp(1 - spread / spreadBase, 0, 1);
      weightedSum += score * (this.edgeWeightSpread[profileIdx] ?? 0);
      weightTotal += this.edgeWeightSpread[profileIdx] ?? 0;
    }

    weightedSum +=
      clamp(referenceQuality, 0, 1) * (this.edgeWeightReference[profileIdx] ?? 0);
    weightTotal += this.edgeWeightReference[profileIdx] ?? 0;

    if (weightTotal <= 0) return null;

    let decay = 1;
    if (priceStalenessSec !== null) {
      const tau = Math.max(this.edgeTauSec[profileIdx] ?? DEFAULT_EDGE_TAU_SEC, 1);
      decay = clamp(Math.exp(-priceStalenessSec / tau), 0.3, 1);
    }

    return clamp((weightedSum / weightTotal) * decay, 0, 1);
  }

  private computeGateMultiplier(
    profileIdx: number,
    thresholds: {
      maxSpread: number | null;
      minImbalance: number | null;
      minDepth: number | null;
      minVelocity: number | null;
      minMomentum: number | null;
      minVolatility: number | null;
      maxStaleness: number | null;
      minConfidence: number | null;
    },
    values: {
      spread: number | null;
      imbalance: number | null;
      depth: number | null;
      velocity: number | null;
      momentum: number | null;
      volatility: number | null;
      staleness: number | null;
      confidence: number | null;
      favoredUp: boolean;
    },
  ): { multiplier: number; blocked: boolean } {
    if ((this.gateEnabled[profileIdx] ?? 0) !== 1) {
      return { multiplier: 1, blocked: false };
    }

    const minGate = this.gateMinMultiplier[profileIdx] ?? DEFAULT_GATE_MIN_MULTIPLIER;
    const floor = this.gateFloor[profileIdx] ?? DEFAULT_GATE_PER_SIGNAL_FLOOR;
    let multiplier = 1;

    const applyPenalty = (ratio: number): void => {
      const penalty = clamp(ratio, floor, 1);
      multiplier *= penalty;
    };

    if (thresholds.maxSpread !== null) {
      if (values.spread === null) return { multiplier: 0, blocked: true };
      applyPenalty(thresholds.maxSpread / Math.max(values.spread, 1e-6));
    }
    if (thresholds.minImbalance !== null) {
      if (values.imbalance === null) return { multiplier: 0, blocked: true };
      applyPenalty(values.imbalance / Math.max(thresholds.minImbalance, 0.01));
    }
    if (thresholds.minDepth !== null) {
      if (values.depth === null) return { multiplier: 0, blocked: true };
      applyPenalty(values.depth / Math.max(thresholds.minDepth, 1));
    }
    if (thresholds.minVelocity !== null) {
      if (values.velocity === null) return { multiplier: 0, blocked: true };
      applyPenalty(values.velocity / Math.max(thresholds.minVelocity, 1));
    }
    if (thresholds.minMomentum !== null) {
      if (values.momentum === null) return { multiplier: 0, blocked: true };
      const aligned = values.favoredUp ? values.momentum : -values.momentum;
      applyPenalty(aligned / Math.max(thresholds.minMomentum, 1));
    }
    if (thresholds.minVolatility !== null) {
      if (values.volatility === null) return { multiplier: 0, blocked: true };
      applyPenalty(values.volatility / Math.max(thresholds.minVolatility, 1));
    }
    if (thresholds.maxStaleness !== null) {
      if (values.staleness === null) return { multiplier: 0, blocked: true };
      applyPenalty(thresholds.maxStaleness / Math.max(values.staleness, 1));
    }
    if (thresholds.minConfidence !== null) {
      if (values.confidence === null) return { multiplier: 0, blocked: true };
      applyPenalty(values.confidence / Math.max(thresholds.minConfidence, 0.01));
    }

    return { multiplier, blocked: multiplier < minGate };
  }

  private findNextPending(nowMs: number): number | null {
    let next = Number.POSITIVE_INFINITY;
    for (let p = 0; p < this.profileCount; p += 1) {
      const due = this.pendingDueMs[p] ?? 0;
      if (!Number.isFinite(due) || due <= nowMs) continue;
      if (due < next) next = due;
    }
    return Number.isFinite(next) ? next : null;
  }
}
