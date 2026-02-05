import { readFileSync } from "fs";
import { join } from "path";
import type { CoinSymbol } from "./auto-market";
import { normalizeCoinKey, stripJsonComments } from "./profile-config";

export interface ArbitrageCoinConfig {
  tradeAllowedTimeLeft: number;
  tradeStopTimeLeft: number | null;
  minGap: number;
  maxSpendTotal: number;
  minSpendTotal: number;
  maxSpread: number | null;
  minDepthValue: number | null;
  maxPriceStalenessSec: number | null;
  fillUsd: number | null;
}

export interface ArbitrageProfileConfig {
  name: string;
  coins: Map<CoinSymbol, ArbitrageCoinConfig>;
}

export interface ArbitrageConfigResult {
  profiles: ArbitrageProfileConfig[];
  coinOptions: CoinSymbol[];
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
  if (value === undefined || value === null) return null;
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

function parseCoinConfig(
  profileName: string,
  coinKey: string,
  raw: Record<string, unknown>,
): ArbitrageCoinConfig {
  const context = `${profileName}.${coinKey}`;
  const tradeAllowedTimeLeft = parseNumberField(
    raw.tradeAllowedTimeLeft,
    context,
    "tradeAllowedTimeLeft",
  );
  if (!Number.isFinite(tradeAllowedTimeLeft) || tradeAllowedTimeLeft <= 0) {
    throw new Error(
      `Config error: ${context} tradeAllowedTimeLeft must be a positive number`,
    );
  }

  const tradeStopTimeLeft = parseOptionalNumberField(
    raw.tradeStopTimeLeft,
    context,
    "tradeStopTimeLeft",
  );
  if (
    tradeStopTimeLeft !== null &&
    (tradeStopTimeLeft <= 0 || tradeStopTimeLeft >= tradeAllowedTimeLeft)
  ) {
    throw new Error(
      `Config error: ${context} tradeStopTimeLeft must be > 0 and less than tradeAllowedTimeLeft`,
    );
  }

  const minGap = parseNumberField(raw.minGap, context, "minGap");
  const maxSpendTotal = parseNumberField(
    raw.maxSpendTotal,
    context,
    "maxSpendTotal",
  );
  const minSpendTotal = parseNumberField(
    raw.minSpendTotal,
    context,
    "minSpendTotal",
  );
  if (minSpendTotal > maxSpendTotal) {
    throw new Error(
      `Config error: ${context} minSpendTotal must be <= maxSpendTotal`,
    );
  }

  return {
    tradeAllowedTimeLeft,
    tradeStopTimeLeft,
    minGap,
    maxSpendTotal,
    minSpendTotal,
    maxSpread: parseOptionalNumberField(raw.maxSpread, context, "maxSpread"),
    minDepthValue: parseOptionalNumberField(
      raw.minDepthValue,
      context,
      "minDepthValue",
    ),
    maxPriceStalenessSec: parseOptionalNumberField(
      raw.maxPriceStalenessSec,
      context,
      "maxPriceStalenessSec",
    ),
    fillUsd: parseOptionalNumberField(raw.fillUsd, context, "fillUsd"),
  };
}

function parseConfigFile(): Record<string, unknown> {
  const raw = readFileSync(join(process.cwd(), "config.json"), "utf8");
  const parsed = JSON.parse(stripJsonComments(raw));
  return parsed as Record<string, unknown>;
}

function deriveCoinOptions(profiles: ArbitrageProfileConfig[]): CoinSymbol[] {
  const coins = new Set<CoinSymbol>();
  for (const profile of profiles) {
    for (const coin of profile.coins.keys()) {
      coins.add(coin);
    }
  }
  return Array.from(coins);
}

export function loadArbitrageConfig(): ArbitrageConfigResult {
  const parsed = parseConfigFile();
  const raw = parsed.arbitrage;
  if (!raw || typeof raw !== "object") {
    throw new Error("Config error: missing arbitrage section");
  }
  const record = raw as Record<string, unknown>;
  const profilesRaw = record.profiles;
  if (!profilesRaw || typeof profilesRaw !== "object") {
    throw new Error("Config error: arbitrage profiles missing");
  }
  const profiles: ArbitrageProfileConfig[] = [];
  for (const [name, value] of Object.entries(
    profilesRaw as Record<string, unknown>,
  )) {
    if (!value || typeof value !== "object") continue;
    const profileRecord = value as Record<string, unknown>;
    const coinsRaw = profileRecord.coins;
    if (!coinsRaw || typeof coinsRaw !== "object") {
      throw new Error(`Config error: arbitrage profile ${name} missing coins`);
    }
    const coins = new Map<CoinSymbol, ArbitrageCoinConfig>();
    for (const [coinKey, configValue] of Object.entries(
      coinsRaw as Record<string, unknown>,
    )) {
      const coin = normalizeCoinKey(coinKey);
      if (!coin || !configValue || typeof configValue !== "object") continue;
      const config = parseCoinConfig(
        name,
        coinKey,
        configValue as Record<string, unknown>,
      );
      coins.set(coin, config);
    }
    if (coins.size > 0) {
      profiles.push({ name, coins });
    }
  }
  if (profiles.length === 0) {
    throw new Error("Config error: arbitrage has no valid profiles");
  }

  return { profiles, coinOptions: deriveCoinOptions(profiles) };
}
