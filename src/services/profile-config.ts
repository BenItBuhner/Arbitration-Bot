import { readFileSync } from "fs";
import { join } from "path";
import type { CoinSymbol } from "./auto-market";
import type { MarketProvider } from "../providers/provider";
import type { MarketGroupDefinition } from "./market-groups";
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

export interface ProviderProfileDefinition {
  name: string;
  configsByGroup: Map<string, Map<CoinSymbol, TimedTradeConfig>>;
}

export interface KalshiCoinSelection {
  tickers: string[];
  seriesTickers: string[];
  eventTickers: string[];
  marketUrls: string[];
  autoDiscover: boolean;
}

export interface ProviderConfigResult {
  provider: MarketProvider;
  profiles: ProviderProfileDefinition[];
  coinOptions: CoinSymbol[];
  marketGroups: MarketGroupDefinition[];
  kalshiSelectorsByCoin?: Map<CoinSymbol, KalshiCoinSelection>;
}

export function stripJsonComments(raw: string): string {
  let result = "";
  let inString = false;
  let escaped = false;
  let i = 0;
  while (i < raw.length) {
    const char = raw[i];
    const next = i + 1 < raw.length ? raw[i + 1] : "";

    if (inString) {
      result += char;
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === "\"") {
        inString = false;
      }
      i += 1;
      continue;
    }

    if (char === "\"") {
      inString = true;
      result += char;
      i += 1;
      continue;
    }

    if (char === "/" && next === "/") {
      // Skip line comment
      i += 2;
      while (i < raw.length && raw[i] !== "\n") {
        i += 1;
      }
      continue;
    }

    if (char === "/" && next === "*") {
      // Skip block comment
      i += 2;
      while (i < raw.length) {
        if (raw[i] === "*" && i + 1 < raw.length && raw[i + 1] === "/") {
          i += 2;
          break;
        }
        i += 1;
      }
      continue;
    }

    result += char;
    i += 1;
  }

  return result;
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

function parseConfigFile(): Record<string, unknown> {
  const raw = readFileSync(join(process.cwd(), "config.json"), "utf8");
  const parsed = JSON.parse(stripJsonComments(raw));
  return parsed as Record<string, unknown>;
}

function parseMarketGroups(raw: unknown): MarketGroupDefinition[] {
  if (!Array.isArray(raw)) {
    return [{ id: "default", match: {} }];
  }

  const groups: MarketGroupDefinition[] = [];
  const seen = new Set<string>();
  for (const entry of raw) {
    if (!entry || typeof entry !== "object") continue;
    const record = entry as Record<string, unknown>;
    const id = typeof record.id === "string" ? record.id.trim() : "";
    if (!id) continue;
    if (seen.has(id)) {
      throw new Error(`Config error: duplicate marketGroup id ${id}`);
    }
    seen.add(id);
    groups.push({ id, match: (record.match ?? {}) as object });
  }

  if (groups.length === 0) {
    groups.push({ id: "default", match: {} });
  }

  return groups;
}

function parseCoinList(raw: unknown): CoinSymbol[] {
  if (!Array.isArray(raw)) return [];
  const coins: CoinSymbol[] = [];
  for (const entry of raw) {
    if (typeof entry !== "string") continue;
    const normalized = normalizeCoinKey(entry);
    if (normalized && !coins.includes(normalized)) {
      coins.push(normalized);
    }
  }
  return coins;
}

function parseKalshiCoins(raw: unknown): {
  coinOptions: CoinSymbol[];
  selectorsByCoin: Map<CoinSymbol, KalshiCoinSelection>;
} {
  const coinOptions: CoinSymbol[] = [];
  const selectorsByCoin = new Map<CoinSymbol, KalshiCoinSelection>();

  if (Array.isArray(raw)) {
    for (const entry of raw) {
      if (typeof entry !== "string") continue;
      const coin = normalizeCoinKey(entry);
      if (!coin) continue;
      coinOptions.push(coin);
      selectorsByCoin.set(coin, {
        tickers: [],
        seriesTickers: [],
        eventTickers: [],
        marketUrls: [],
        autoDiscover: true,
      });
    }
    return { coinOptions, selectorsByCoin };
  }

  if (!raw || typeof raw !== "object") {
    return { coinOptions, selectorsByCoin };
  }

  const record = raw as Record<string, unknown>;
  for (const [coinKey, value] of Object.entries(record)) {
    const coin = normalizeCoinKey(coinKey);
    if (!coin) continue;
    const valueRecord =
      value && typeof value === "object"
        ? (value as Record<string, unknown>)
        : null;
    const tickersRaw = valueRecord?.tickers;
    const seriesRaw = valueRecord?.seriesTickers;
    const eventRaw = valueRecord?.eventTickers;
    const urlRaw = valueRecord?.marketUrls ?? valueRecord?.urls;
    const autoDiscoverRaw = valueRecord?.autoDiscover;

    const normalizeList = (input: unknown): string[] => {
      if (!Array.isArray(input)) return [];
      return input
        .filter((item) => typeof item === "string")
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
    };

    const tickers = normalizeList(tickersRaw).map((t) => t.toUpperCase());
    const seriesTickers = normalizeList(seriesRaw).map((t) => t.toUpperCase());
    const eventTickers = normalizeList(eventRaw).map((t) => t.toUpperCase());
    const marketUrls = normalizeList(urlRaw);
    const autoDiscover =
      typeof autoDiscoverRaw === "boolean" ? autoDiscoverRaw : true;

    selectorsByCoin.set(coin, {
      tickers,
      seriesTickers,
      eventTickers,
      marketUrls,
      autoDiscover,
    });
    if (!coinOptions.includes(coin)) {
      coinOptions.push(coin);
    }
  }

  return { coinOptions, selectorsByCoin };
}

function parseProviderProfiles(
  provider: MarketProvider,
  raw: unknown,
): ProviderProfileDefinition[] {
  if (!raw || typeof raw !== "object") return [];
  const record = raw as Record<string, unknown>;
  const profiles: ProviderProfileDefinition[] = [];

  for (const [profileName, profileValue] of Object.entries(record)) {
    if (!profileValue || typeof profileValue !== "object") continue;
    const profileRecord = profileValue as Record<string, unknown>;
    const marketsRaw = profileRecord.markets as Record<string, unknown> | undefined;
    if (!marketsRaw || typeof marketsRaw !== "object") {
      throw new Error(
        `Config error: ${provider} profile ${profileName} missing markets`,
      );
    }

    const configsByGroup = new Map<string, Map<CoinSymbol, TimedTradeConfig>>();
    for (const [groupId, groupValue] of Object.entries(marketsRaw)) {
      if (!groupValue || typeof groupValue !== "object") continue;
      const groupRecord = groupValue as Record<string, unknown>;
      const configs = new Map<CoinSymbol, TimedTradeConfig>();
      for (const [coinKey, configValue] of Object.entries(groupRecord)) {
        const coin = normalizeCoinKey(coinKey);
        if (!coin || !configValue || typeof configValue !== "object") continue;
        const config = parseTimedConfig(
          profileName,
          coinKey,
          configValue as Record<string, unknown>,
        );
        configs.set(coin, config);
      }
      if (configs.size > 0) {
        configsByGroup.set(groupId, configs);
      }
    }

    if (configsByGroup.size > 0) {
      profiles.push({ name: profileName, configsByGroup });
    }
  }

  return profiles;
}

function deriveCoinsFromProfiles(
  profiles: ProviderProfileDefinition[],
): CoinSymbol[] {
  const coinSet = new Set<CoinSymbol>();
  for (const profile of profiles) {
    for (const group of profile.configsByGroup.values()) {
      for (const coin of group.keys()) {
        coinSet.add(coin);
      }
    }
  }
  return Array.from(coinSet);
}

export function resolveProfileConfigForCoin(
  profile: ProviderProfileDefinition,
  coin: CoinSymbol,
  groupId: string | null,
): TimedTradeConfig | null {
  if (groupId) {
    const group = profile.configsByGroup.get(groupId);
    if (group?.has(coin)) return group.get(coin) || null;
  }
  const fallback = profile.configsByGroup.get("default");
  if (fallback?.has(coin)) return fallback.get(coin) || null;

  for (const group of profile.configsByGroup.values()) {
    if (group.has(coin)) return group.get(coin) || null;
  }
  return null;
}

export function loadProviderConfig(
  provider: MarketProvider,
): ProviderConfigResult {
  const parsed = parseConfigFile();

  const schemaVersion = parsed.schemaVersion;
  const providersRaw = parsed.providers;

  if (schemaVersion === 2 || (providersRaw && typeof providersRaw === "object")) {
    const providers = providersRaw as Record<string, unknown>;
    const providerRaw = providers?.[provider];
    if (!providerRaw || typeof providerRaw !== "object") {
      throw new Error(`Config error: missing provider ${provider}`);
    }
    const providerRecord = providerRaw as Record<string, unknown>;
    const marketGroups = parseMarketGroups(providerRecord.marketGroups);
    const profiles = parseProviderProfiles(provider, providerRecord.profiles);

    let coinOptions: CoinSymbol[] = [];
    let kalshiSelectorsByCoin: Map<CoinSymbol, KalshiCoinSelection> | undefined;

    if (provider === "kalshi") {
      const parsedKalshi = parseKalshiCoins(providerRecord.coins);
      coinOptions = parsedKalshi.coinOptions;
      kalshiSelectorsByCoin = parsedKalshi.selectorsByCoin;
      if (coinOptions.length === 0) {
        coinOptions = deriveCoinsFromProfiles(profiles);
      }
      for (const coin of coinOptions) {
        if (!kalshiSelectorsByCoin.has(coin)) {
          kalshiSelectorsByCoin.set(coin, {
            tickers: [],
            seriesTickers: [],
            eventTickers: [],
            marketUrls: [],
            autoDiscover: true,
          });
        }
      }
    } else {
      coinOptions = parseCoinList(providerRecord.coins);
      if (coinOptions.length === 0) {
        coinOptions = deriveCoinsFromProfiles(profiles);
      }
    }

    return {
      provider,
      profiles,
      coinOptions,
      marketGroups,
      kalshiSelectorsByCoin,
    };
  }

  if (provider !== "polymarket") {
    throw new Error(
      `Config error: legacy config only supports polymarket (requested ${provider}).`,
    );
  }

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

  const groupedProfiles: ProviderProfileDefinition[] = profiles.map((profile) => {
    const configsByGroup = new Map<string, Map<CoinSymbol, TimedTradeConfig>>();
    configsByGroup.set("default", new Map(profile.configs));
    return { name: profile.name, configsByGroup };
  });

  return {
    provider: "polymarket",
    profiles: groupedProfiles,
    coinOptions: Array.from(coinSet),
    marketGroups: [{ id: "default", match: {} }],
  };
}

export function loadProfilesFromConfig(): {
  profiles: ProfileDefinition[];
  coinOptions: CoinSymbol[];
} {
  const providerConfig = loadProviderConfig("polymarket");
  const profiles: ProfileDefinition[] = [];

  for (const profile of providerConfig.profiles) {
    const configs = new Map<CoinSymbol, TimedTradeConfig>();
    for (const coin of providerConfig.coinOptions) {
      const config = resolveProfileConfigForCoin(profile, coin, "default");
      if (config) {
        configs.set(coin, config);
      }
    }
    if (configs.size > 0) {
      profiles.push({ name: profile.name, configs });
    }
  }

  return { profiles, coinOptions: providerConfig.coinOptions };
}
