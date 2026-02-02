import { readFileSync } from "fs";
import { join } from "path";
import type { CoinSymbol } from "./auto-market";
import type {
  TimedTradeConfig,
  TradeRule,
  SizeStrategy,
  CrossTradeConfig,
  CrossTradeRule,
  EdgeModelConfig,
  SizeModelConfig,
  GateModelConfig,
  LossGovernorConfig,
  CrossModeConfig,
  CrossModeOverrides,
} from "./profile-engine";

export interface ProfileDefinition {
  name: string;
  configs: Map<CoinSymbol, TimedTradeConfig>;
}

export function stripJsonComments(raw: string): string {
  return raw.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
}

export function normalizeCoinKey(key: string): CoinSymbol | null {
  const normalized = key.toLowerCase().trim();
  if (normalized === "eth" || normalized === "ethereum") return "eth";
  if (normalized === "btc" || normalized === "bitcoin") return "btc";
  if (normalized === "sol" || normalized === "solana") return "sol";
  if (normalized === "xrp" || normalized === "ripple") return "xrp";
  return null;
}

export function sanitizeProfileName(name: string): string {
  return name.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function parseNumberField(
  value: unknown,
  context: string,
  label: string,
): number {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number(value)
        : NaN;
  if (!Number.isFinite(parsed)) {
    throw new Error(`Config error: ${context} missing ${label}`);
  }
  return parsed;
}

function parseOptionalNumberField(
  value: unknown,
  context: string,
  label: string,
): number | null {
  if (value === undefined || value === null) {
    return null;
  }
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number(value)
        : NaN;
  if (!Number.isFinite(parsed)) {
    throw new Error(`Config error: ${context} invalid ${label}`);
  }
  return parsed;
}

function parseOptionalBooleanField(
  value: unknown,
  context: string,
  label: string,
): boolean | undefined {
  if (value === undefined) return undefined;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "on"].includes(normalized)) return true;
    if (["false", "0", "no", "off"].includes(normalized)) return false;
  }
  throw new Error(`Config error: ${context} invalid ${label}`);
}

function hasOwn(
  record: Record<string, unknown>,
  key: string,
): boolean {
  return Object.prototype.hasOwnProperty.call(record, key);
}

function parseSizeStrategy(
  value: unknown,
  context: string,
): SizeStrategy {
  if (value === undefined || value === null) return "fixed";
  if (typeof value !== "string") {
    throw new Error(`Config error: ${context} invalid sizeStrategy`);
  }
  const normalized = value.trim().toLowerCase();
  if (
    normalized === "fixed" ||
    normalized === "edge" ||
    normalized === "depth" ||
    normalized === "confidence"
  ) {
    return normalized as SizeStrategy;
  }
  throw new Error(`Config error: ${context} invalid sizeStrategy`);
}

function parseEdgeWeights(
  value: unknown,
  context: string,
): EdgeModelConfig["weights"] {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object") {
    throw new Error(`Config error: ${context} weights must be an object`);
  }
  const raw = value as Record<string, unknown>;
  const weights: Record<string, number> = {};
  const allowed = [
    "gap",
    "depth",
    "imbalance",
    "velocity",
    "momentum",
    "volatility",
    "spread",
    "reference",
  ];
  for (const key of allowed) {
    if (!hasOwn(raw, key)) continue;
    weights[key] = parseNumberField(raw[key], context, key);
  }
  return weights;
}

function parseEdgeCaps(
  value: unknown,
  context: string,
): EdgeModelConfig["caps"] {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object") {
    throw new Error(`Config error: ${context} caps must be an object`);
  }
  const raw = value as Record<string, unknown>;
  const caps: Record<string, number> = {};
  const allowed = [
    "gap",
    "depth",
    "velocity",
    "momentum",
    "volatility",
    "spread",
  ];
  for (const key of allowed) {
    if (!hasOwn(raw, key)) continue;
    caps[key] = parseNumberField(raw[key], context, key);
  }
  return caps;
}

function parseEdgeModel(
  value: unknown,
  context: string,
): EdgeModelConfig | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object") {
    throw new Error(`Config error: ${context} edgeModel must be an object`);
  }
  const raw = value as Record<string, unknown>;
  return {
    enabled: parseOptionalBooleanField(raw.enabled, context, "edgeModel.enabled"),
    weights: parseEdgeWeights(raw.weights, context),
    caps: parseEdgeCaps(raw.caps, context),
    stalenessTauSec: parseOptionalNumberField(
      raw.stalenessTauSec,
      context,
      "edgeModel.stalenessTauSec",
    ) ?? undefined,
    requireSignals: parseOptionalBooleanField(
      raw.requireSignals,
      context,
      "edgeModel.requireSignals",
    ),
    minScore: parseOptionalNumberField(raw.minScore, context, "edgeModel.minScore"),
  };
}

function parseSizeModel(
  value: unknown,
  context: string,
): SizeModelConfig | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object") {
    throw new Error(`Config error: ${context} sizeModel must be an object`);
  }
  const raw = value as Record<string, unknown>;
  let mode: SizeModelConfig["mode"];
  if (raw.mode !== undefined && raw.mode !== null) {
    if (typeof raw.mode !== "string") {
      throw new Error(`Config error: ${context} sizeModel.mode invalid`);
    }
    const normalized = raw.mode.trim().toLowerCase();
    if (normalized === "legacy" || normalized === "edge_weighted") {
      mode = normalized as SizeModelConfig["mode"];
    } else {
      throw new Error(`Config error: ${context} sizeModel.mode invalid`);
    }
  }
  return {
    mode,
    edgeGamma: parseOptionalNumberField(raw.edgeGamma, context, "sizeModel.edgeGamma") ?? undefined,
    minSizeFloor: parseOptionalNumberField(raw.minSizeFloor, context, "sizeModel.minSizeFloor") ?? undefined,
    maxSizeCeil: parseOptionalNumberField(raw.maxSizeCeil, context, "sizeModel.maxSizeCeil") ?? undefined,
    applyGateMultiplier: parseOptionalBooleanField(
      raw.applyGateMultiplier,
      context,
      "sizeModel.applyGateMultiplier",
    ),
    confidenceWeight: parseOptionalNumberField(
      raw.confidenceWeight,
      context,
      "sizeModel.confidenceWeight",
    ) ?? undefined,
    depthWeight: parseOptionalNumberField(raw.depthWeight, context, "sizeModel.depthWeight") ?? undefined,
    spreadPenaltyWeight: parseOptionalNumberField(
      raw.spreadPenaltyWeight,
      context,
      "sizeModel.spreadPenaltyWeight",
    ) ?? undefined,
  };
}

function parseGateModel(
  value: unknown,
  context: string,
): GateModelConfig | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object") {
    throw new Error(`Config error: ${context} gateModel must be an object`);
  }
  const raw = value as Record<string, unknown>;
  return {
    enabled: parseOptionalBooleanField(raw.enabled, context, "gateModel.enabled"),
    minGateMultiplier: parseOptionalNumberField(
      raw.minGateMultiplier,
      context,
      "gateModel.minGateMultiplier",
    ) ?? undefined,
    perSignalFloor: parseOptionalNumberField(
      raw.perSignalFloor,
      context,
      "gateModel.perSignalFloor",
    ) ?? undefined,
    applyToSize: parseOptionalBooleanField(raw.applyToSize, context, "gateModel.applyToSize"),
  };
}

function parseLossGovernor(
  value: unknown,
  context: string,
): LossGovernorConfig | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object") {
    throw new Error(`Config error: ${context} lossGovernor must be an object`);
  }
  const raw = value as Record<string, unknown>;
  return {
    enabled: parseOptionalBooleanField(raw.enabled, context, "lossGovernor.enabled"),
    streakThreshold: parseOptionalNumberField(
      raw.streakThreshold,
      context,
      "lossGovernor.streakThreshold",
    ) ?? undefined,
    minDiffMultiplier: parseOptionalNumberField(
      raw.minDiffMultiplier,
      context,
      "lossGovernor.minDiffMultiplier",
    ) ?? undefined,
    sizeScaleMultiplier: parseOptionalNumberField(
      raw.sizeScaleMultiplier,
      context,
      "lossGovernor.sizeScaleMultiplier",
    ) ?? undefined,
  };
}

function parseCrossModeOverrides(
  value: unknown,
  context: string,
): CrossModeOverrides | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object") {
    throw new Error(`Config error: ${context} overrides must be an object`);
  }
  const raw = value as Record<string, unknown>;
  const overrides: CrossModeOverrides = {};
  if (hasOwn(raw, "minDiffMultiplier")) {
    overrides.minDiffMultiplier = parseNumberField(
      raw.minDiffMultiplier,
      context,
      "minDiffMultiplier",
    );
  }
  if (hasOwn(raw, "maxShareMultiplier")) {
    overrides.maxShareMultiplier = parseNumberField(
      raw.maxShareMultiplier,
      context,
      "maxShareMultiplier",
    );
  }
  if (hasOwn(raw, "minShareMultiplier")) {
    overrides.minShareMultiplier = parseNumberField(
      raw.minShareMultiplier,
      context,
      "minShareMultiplier",
    );
  }
  if (hasOwn(raw, "maxSpendMultiplier")) {
    overrides.maxSpendMultiplier = parseNumberField(
      raw.maxSpendMultiplier,
      context,
      "maxSpendMultiplier",
    );
  }
  if (hasOwn(raw, "minConfidence")) {
    overrides.minConfidence = parseOptionalNumberField(
      raw.minConfidence,
      context,
      "minConfidence",
    );
  }
  if (hasOwn(raw, "minDepthValue")) {
    overrides.minDepthValue = parseOptionalNumberField(
      raw.minDepthValue,
      context,
      "minDepthValue",
    );
  }
  if (hasOwn(raw, "minTradeVelocity")) {
    overrides.minTradeVelocity = parseOptionalNumberField(
      raw.minTradeVelocity,
      context,
      "minTradeVelocity",
    );
  }
  if (hasOwn(raw, "sizeScaleMultiplier")) {
    overrides.sizeScaleMultiplier = parseNumberField(
      raw.sizeScaleMultiplier,
      context,
      "sizeScaleMultiplier",
    );
  }
  if (hasOwn(raw, "minRecoveryMultiple")) {
    overrides.minRecoveryMultiple = parseOptionalNumberField(
      raw.minRecoveryMultiple,
      context,
      "minRecoveryMultiple",
    );
  }
  if (hasOwn(raw, "minLossToTrigger")) {
    overrides.minLossToTrigger = parseOptionalNumberField(
      raw.minLossToTrigger,
      context,
      "minLossToTrigger",
    );
  }
  return overrides;
}

function parseCrossModes(
  value: unknown,
  context: string,
): CrossModeConfig | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object") {
    throw new Error(`Config error: ${context} crossModes must be an object`);
  }
  const raw = value as Record<string, unknown>;
  return {
    splitTimeSec: parseOptionalNumberField(
      raw.splitTimeSec,
      context,
      "crossModes.splitTimeSec",
    ) ?? undefined,
    precision: parseCrossModeOverrides(raw.precision, `${context}.precision`),
    opportunistic: parseCrossModeOverrides(
      raw.opportunistic,
      `${context}.opportunistic`,
    ),
  };
}

function parseTradeRule(
  tierSeconds: number,
  rawRule: Record<string, unknown>,
  context: string,
): TradeRule {
  return {
    tierSeconds,
    minimumPriceDifference: parseNumberField(
      rawRule.minimumPriceDifference,
      context,
      "minimumPriceDifference",
    ),
    maximumSharePrice: parseNumberField(
      rawRule.maximumSharePrice,
      context,
      "maximumSharePrice",
    ),
    minimumSharePrice: parseNumberField(
      rawRule.minimumSharePrice,
      context,
      "minimumSharePrice",
    ),
    maximumSpend: parseNumberField(
      rawRule.maximumSpend,
      context,
      "maximumSpend",
    ),
    minimumSpend: parseNumberField(
      rawRule.minimumSpend,
      context,
      "minimumSpend",
    ),
    maxSpread: parseOptionalNumberField(rawRule.maxSpread, context, "maxSpread"),
    minBookImbalance: parseOptionalNumberField(
      rawRule.minBookImbalance,
      context,
      "minBookImbalance",
    ),
    minDepthValue: parseOptionalNumberField(
      rawRule.minDepthValue,
      context,
      "minDepthValue",
    ),
    minTradeVelocity: parseOptionalNumberField(
      rawRule.minTradeVelocity,
      context,
      "minTradeVelocity",
    ),
    minMomentum: parseOptionalNumberField(
      rawRule.minMomentum,
      context,
      "minMomentum",
    ),
    minVolatility: parseOptionalNumberField(
      rawRule.minVolatility,
      context,
      "minVolatility",
    ),
    maxPriceStalenessSec: parseOptionalNumberField(
      rawRule.maxPriceStalenessSec,
      context,
      "maxPriceStalenessSec",
    ),
    minConfidence: parseOptionalNumberField(
      rawRule.minConfidence,
      context,
      "minConfidence",
    ),
    sizeStrategy: parseSizeStrategy(rawRule.sizeStrategy, context),
    sizeScale: parseOptionalNumberField(rawRule.sizeScale, context, "sizeScale") ?? 1,
    maxOpenExposure: parseOptionalNumberField(
      rawRule.maxOpenExposure,
      context,
      "maxOpenExposure",
    ),
  };
}

function parseCrossTradeRule(
  tierSeconds: number,
  rawRule: Record<string, unknown>,
  context: string,
): CrossTradeRule {
  return {
    ...parseTradeRule(tierSeconds, rawRule, context),
    minRecoveryMultiple: parseOptionalNumberField(
      rawRule.minRecoveryMultiple,
      context,
      "minRecoveryMultiple",
    ),
    minLossToTrigger: parseOptionalNumberField(
      rawRule.minLossToTrigger,
      context,
      "minLossToTrigger",
    ),
  };
}

function parseCrossConfig(
  profileName: string,
  coinKey: string,
  value: Record<string, unknown>,
): CrossTradeConfig {
  const context = `${profileName}.${coinKey}.cross`;
  const tradeAllowedTimeLeft = parseNumberField(
    value.tradeAllowedTimeLeft,
    context,
    "tradeAllowedTimeLeft",
  );
  if (!Number.isInteger(tradeAllowedTimeLeft) || tradeAllowedTimeLeft <= 0) {
    throw new Error(
      `Config error: ${context} tradeAllowedTimeLeft must be a positive integer`,
    );
  }

  const rulesByTier = new Map<number, CrossTradeRule>();
  for (const [key, ruleValue] of Object.entries(value)) {
    if (key === "tradeAllowedTimeLeft") continue;
    const tierSeconds = Number(key);
    if (!Number.isFinite(tierSeconds)) continue;
    if (!Number.isInteger(tierSeconds) || tierSeconds <= 0) {
      throw new Error(
        `Config error: ${context} tier ${key} must be a positive integer`,
      );
    }
    if (typeof ruleValue !== "object" || ruleValue === null) {
      throw new Error(`Config error: ${context}.${key} must be an object`);
    }
    if (rulesByTier.has(tierSeconds)) {
      throw new Error(`Config error: ${context} duplicate tier ${key}`);
    }
    const ruleContext = `${context}.${key}`;
    const rule = parseCrossTradeRule(
      tierSeconds,
      ruleValue as Record<string, unknown>,
      ruleContext,
    );
    rulesByTier.set(tierSeconds, rule);
  }

  if (rulesByTier.size === 0) {
    throw new Error(`Config error: ${context} has no timed tiers`);
  }

  if (!rulesByTier.has(tradeAllowedTimeLeft)) {
    throw new Error(
      `Config error: ${context} missing tier ${tradeAllowedTimeLeft} (default)`,
    );
  }

  const rules = Array.from(rulesByTier.values()).sort(
    (a, b) => a.tierSeconds - b.tierSeconds,
  );

  return { tradeAllowedTimeLeft, rules };
}

function parseTimedConfig(
  profileName: string,
  coinKey: string,
  value: Record<string, unknown>,
): TimedTradeConfig {
  const context = `${profileName}.${coinKey}`;
  const tradeAllowedTimeLeft = parseNumberField(
    value.tradeAllowedTimeLeft,
    context,
    "tradeAllowedTimeLeft",
  );
  if (!Number.isInteger(tradeAllowedTimeLeft) || tradeAllowedTimeLeft <= 0) {
    throw new Error(
      `Config error: ${context} tradeAllowedTimeLeft must be a positive integer`,
    );
  }

  let crossRaw: Record<string, unknown> | null = null;
  let edgeModelRaw: unknown = null;
  let sizeModelRaw: unknown = null;
  let gateModelRaw: unknown = null;
  let lossGovernorRaw: unknown = null;
  let crossModesRaw: unknown = null;
  const rulesByTier = new Map<number, TradeRule>();
  for (const [key, ruleValue] of Object.entries(value)) {
    if (key === "tradeAllowedTimeLeft") continue;
    if (key === "cross") {
      if (typeof ruleValue === "object" && ruleValue !== null) {
        crossRaw = ruleValue as Record<string, unknown>;
      }
      continue;
    }
    if (key === "edgeModel") {
      edgeModelRaw = ruleValue;
      continue;
    }
    if (key === "sizeModel") {
      sizeModelRaw = ruleValue;
      continue;
    }
    if (key === "gateModel") {
      gateModelRaw = ruleValue;
      continue;
    }
    if (key === "lossGovernor") {
      lossGovernorRaw = ruleValue;
      continue;
    }
    if (key === "crossModes") {
      crossModesRaw = ruleValue;
      continue;
    }
    const tierSeconds = Number(key);
    if (!Number.isFinite(tierSeconds)) continue;
    if (!Number.isInteger(tierSeconds) || tierSeconds <= 0) {
      throw new Error(
        `Config error: ${context} tier ${key} must be a positive integer`,
      );
    }
    if (typeof ruleValue !== "object" || ruleValue === null) {
      throw new Error(`Config error: ${context}.${key} must be an object`);
    }
    if (rulesByTier.has(tierSeconds)) {
      throw new Error(`Config error: ${context} duplicate tier ${key}`);
    }
    const ruleContext = `${context}.${key}`;
    const rule = parseTradeRule(
      tierSeconds,
      ruleValue as Record<string, unknown>,
      ruleContext,
    );
    rulesByTier.set(tierSeconds, rule);
  }

  if (rulesByTier.size === 0) {
    throw new Error(`Config error: ${context} has no timed tiers`);
  }

  if (!rulesByTier.has(tradeAllowedTimeLeft)) {
    throw new Error(
      `Config error: ${context} missing tier ${tradeAllowedTimeLeft} (default)`,
    );
  }

  const rules = Array.from(rulesByTier.values()).sort(
    (a, b) => a.tierSeconds - b.tierSeconds,
  );

  const cross = crossRaw
    ? parseCrossConfig(profileName, coinKey, crossRaw)
    : undefined;
  const edgeModel = parseEdgeModel(edgeModelRaw, context);
  const sizeModel = parseSizeModel(sizeModelRaw, context);
  const gateModel = parseGateModel(gateModelRaw, context);
  const lossGovernor = parseLossGovernor(lossGovernorRaw, context);
  const crossModes = parseCrossModes(crossModesRaw, context);

  return {
    tradeAllowedTimeLeft,
    rules,
    cross,
    edgeModel,
    sizeModel,
    gateModel,
    lossGovernor,
    crossModes,
  };
}

export function loadProfilesFromConfig(): {
  profiles: ProfileDefinition[];
  coinOptions: CoinSymbol[];
} {
  const raw = readFileSync(join(process.cwd(), "config.json"), "utf8");
  const parsed = JSON.parse(stripJsonComments(raw));

  const profiles: ProfileDefinition[] = [];
  const coinSet = new Set<CoinSymbol>();

  for (const [profileName, profileValue] of Object.entries(parsed)) {
    if (!profileValue || typeof profileValue !== "object") continue;

    const configs = new Map<CoinSymbol, TimedTradeConfig>();
    for (const [coinKey, configValue] of Object.entries(profileValue as object)) {
      const coin = normalizeCoinKey(coinKey);
      if (!coin || !configValue || typeof configValue !== "object") continue;
      const config = parseTimedConfig(
        profileName,
        coinKey,
        configValue as Record<string, unknown>,
      );
      configs.set(coin, config);
      coinSet.add(coin);
    }

    if (configs.size > 0) {
      profiles.push({ name: profileName, configs });
    }
  }

  return { profiles, coinOptions: Array.from(coinSet) };
}
