import {
  existsSync,
  readFileSync,
  writeFileSync,
  mkdirSync,
  statSync,
  renameSync,
  unlinkSync,
  openSync,
  writeSync,
  closeSync,
} from "fs";
import { join, dirname } from "path";
import { cpus } from "os";
import {
  ProfileEngine,
  type TimedTradeConfig,
  type ProfileSummary,
} from "../services/profile-engine";
import { RunLogger } from "../services/run-logger";
import { ProfileDashboard } from "../cli/profile-dashboard";
import type { CoinSymbol } from "../services/auto-market";
import {
  loadProfilesFromConfig,
  normalizeCoinKey,
  sanitizeProfileName,
  type ProfileDefinition,
} from "../services/profile-config";
import { BacktestHub } from "../backtest/backtest-hub";
import { BacktestRunner } from "../backtest/backtest-runner";
import { readJsonlFile, writeJsonlLines } from "../backtest/jsonl";
import { JsonlSyncReader } from "../backtest/jsonl-stream";
import { mergeSortedTradeFiles, mergeSortedTickFiles } from "../backtest/jsonl-merge";
import {
  compareTrades,
  sortTradesChronologically,
} from "../backtest/trade-utils";
import type {
  BacktestMarketMeta,
  BacktestTradeEvent,
} from "../backtest/types";
import {
  fetchMarketsForRange,
  fetchTradesForMarkets,
  type TradesFetchResult,
} from "../backtest/polymarket-fetch";
import { fetchBinanceCryptoHistory } from "../backtest/binance-fetch";
import { mapWithConcurrency } from "../backtest/concurrency";
import { promptText, selectMany, selectOne } from "../cli/prompts";

export interface BacktestRouteOptions {
  profiles?: string[];
  coins?: string[];
  autoSelect?: boolean;
  dataDir?: string;
  speed?: number;
  mode?: "fast" | "visual";
  startMs?: number;
  endMs?: number;
  headless?: boolean;
}

type LogLevel = "INFO" | "WARN" | "ERROR";
type LogFn = (message: string, level?: LogLevel) => void;

// ─────────────────────────────────────────────────────────────────────────────
// Cache Index Types
// ─────────────────────────────────────────────────────────────────────────────

interface CryptoTickCacheEntry {
  minTs: number;
  maxTs: number;
  count: number;
  lastFetchedAt: number;
}

interface MarketTradeCacheEntry {
  minTs: number;
  maxTs: number;
  count: number;
  truncated: boolean;
  lastFetchedAt: number;
}

interface MarketMetaCacheEntry {
  slug: string;
  coin: CoinSymbol;
  startMs: number;
  endMs: number;
  lastFetchedAt: number;
}

interface CacheIndex {
  version: number;
  cryptoTicks: Record<string, CryptoTickCacheEntry>;
  marketTrades: Record<string, MarketTradeCacheEntry>;
  marketMeta: Record<string, MarketMetaCacheEntry>;
}

const CACHE_VERSION = 1;

function getCacheIndexPath(dataDir: string): string {
  return join(dataDir, "cache", "index.json");
}

function loadCacheIndex(dataDir: string): CacheIndex {
  const indexPath = getCacheIndexPath(dataDir);
  if (!existsSync(indexPath)) {
    return {
      version: CACHE_VERSION,
      cryptoTicks: {},
      marketTrades: {},
      marketMeta: {},
    };
  }
  try {
    const raw = readFileSync(indexPath, "utf8");
    const parsed = JSON.parse(raw) as CacheIndex;
    if (parsed.version !== CACHE_VERSION) {
      // Version mismatch, return fresh cache
      return {
        version: CACHE_VERSION,
        cryptoTicks: {},
        marketTrades: {},
        marketMeta: {},
      };
    }
    return parsed;
  } catch {
    return {
      version: CACHE_VERSION,
      cryptoTicks: {},
      marketTrades: {},
      marketMeta: {},
    };
  }
}

function saveCacheIndex(dataDir: string, index: CacheIndex): void {
  const indexPath = getCacheIndexPath(dataDir);
  const dir = dirname(indexPath);
  if (dir && dir !== "." && dir !== "\\") {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(indexPath, JSON.stringify(index, null, 2), "utf8");
}

function loadCachedMarkets(
  dataDir: string,
  selectedCoins: CoinSymbol[],
  startMs: number,
  endMs: number,
  log?: LogFn,
): BacktestMarketMeta[] {
  const marketsPath = join(dataDir, "markets.jsonl");
  if (!existsSync(marketsPath)) return [];
  try {
    const cached = readJsonlFile<BacktestMarketMeta>(marketsPath);
    return cached.filter((market) => {
      if (!selectedCoins.includes(market.coin)) return false;
      if (startMs && market.endMs < startMs) return false;
      if (endMs && market.startMs > endMs) return false;
      return true;
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to parse markets cache.";
    log?.(`[cache] Failed to read cached markets: ${message}`, "WARN");
    return [];
  }
}

function mergeMarketsBySlug(
  existing: BacktestMarketMeta[],
  incoming: BacktestMarketMeta[],
): BacktestMarketMeta[] {
  const map = new Map<string, BacktestMarketMeta>();
  for (const market of existing) {
    map.set(market.slug, market);
  }
  for (const market of incoming) {
    map.set(market.slug, market);
  }
  const merged = Array.from(map.values());
  merged.sort((a, b) => a.startMs - b.startMs);
  return merged;
}

function writeJsonlLinesStreamed<T>(filePath: string, items: T[]): void {
  const fd = openSync(filePath, "w");
  try {
    for (const item of items) {
      const line = JSON.stringify(item) + "\n";
      writeSync(fd, line, undefined, "utf8");
    }
  } finally {
    closeSync(fd);
  }
}

function ensureSortedTradeFile(filePath: string, slug: string): void {
  let prev: BacktestTradeEvent | null = null;
  let isSorted = true;
  let parsedCount = 0;

  const reader = new JsonlSyncReader<BacktestTradeEvent>(filePath, {
    bufferLines: 2000,
    parse: (line) => {
      const record = JSON.parse(line) as BacktestTradeEvent;
      if (!record || typeof record !== "object") return null;
      const timestamp = Number(record.timestamp);
      if (!Number.isFinite(timestamp)) return null;
      if (record.timestamp !== timestamp) record.timestamp = timestamp;
      return record;
    },
  });

  try {
    while (true) {
      const trade = reader.shift();
      if (!trade) break;
      parsedCount += 1;
      if (prev && compareTrades(prev, trade) > 0) {
        isSorted = false;
        break;
      }
      prev = trade;
    }
  } finally {
    reader.close();
  }

  if (parsedCount === 0) {
    console.warn(
      `[backtest] No valid trades parsed for ${slug}. Check ${filePath}.`,
    );
    return;
  }

  if (isSorted) return;

  const trades = readJsonlFile<BacktestTradeEvent>(filePath);
  const sorted = sortTradesChronologically(trades, slug);
  const tempPath = `${filePath}.sorted.tmp`;
  writeJsonlLinesStreamed(tempPath, sorted);
  if (existsSync(filePath)) {
    unlinkSync(filePath);
  }
  renameSync(tempPath, filePath);
  console.warn(`[backtest] Sorted trade file for ${slug}.`);
}

function isCryptoTicksCached(
  index: CacheIndex,
  coin: CoinSymbol,
  startMs: number,
  endMs: number,
): boolean {
  const entry = index.cryptoTicks[coin];
  if (!entry) return false;
  return entry.minTs <= startMs && entry.maxTs >= endMs;
}

function isMarketTradeCached(
  index: CacheIndex,
  slug: string,
  startMs: number,
  endMs: number,
): boolean {
  const entry = index.marketTrades[slug];
  if (!entry) return false;
  return entry.minTs <= startMs && entry.maxTs >= endMs;
}

function parseLatencyMs(): number {
  const raw = process.env.BACKTEST_LATENCY_MS;
  if (!raw) return 80;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : 80;
}

function parseTradeRetryAttempts(): number {
  const raw = process.env.BACKTEST_TRADE_RETRY_ATTEMPTS;
  if (!raw) return 2;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? Math.max(0, Math.floor(parsed)) : 2;
}

function parseTradeRetryBaseMs(): number {
  const raw = process.env.BACKTEST_TRADE_RETRY_BASE_MS;
  if (!raw) return 1000;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? Math.max(100, Math.floor(parsed)) : 1000;
}

function parseCoinWorkerEnabled(): boolean {
  const raw = process.env.BACKTEST_COIN_WORKERS;
  if (!raw) return true;
  const normalized = raw.trim().toLowerCase();
  return normalized !== "false" && normalized !== "0" && normalized !== "off";
}

function parseCoinWorkerLimit(): number {
  const raw = process.env.BACKTEST_COIN_WORKER_LIMIT;
  if (!raw) return Math.max(1, cpus().length);
  const parsed = Number(raw);
  return Number.isFinite(parsed)
    ? Math.max(1, Math.floor(parsed))
    : Math.max(1, cpus().length);
}

function parsePerfLogEnabled(): boolean {
  const raw = process.env.BACKTEST_PERF_LOG;
  if (!raw) return false;
  const normalized = raw.trim().toLowerCase();
  return !["false", "0", "off", "no"].includes(normalized);
}

function parseRegressionLogEnabled(): boolean {
  const raw = process.env.BACKTEST_REGRESSION_LOG;
  if (!raw) return false;
  const normalized = raw.trim().toLowerCase();
  return !["false", "0", "off", "no"].includes(normalized);
}

function parseHeadlessLogEveryMs(): number | undefined {
  const raw = process.env.BACKTEST_HEADLESS_LOG_EVERY_MS;
  if (!raw) return undefined;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return undefined;
  return Math.max(1000, Math.floor(parsed));
}

function createCliLogger(logger: RunLogger, prefix: string): LogFn {
  return (message, level: LogLevel = "INFO") => {
    logger.log(message, level);
    const tag = prefix ? `[${prefix}]` : "[LOG]";
    const line = `${tag}[${level}] ${message}`;
    if (level === "ERROR") {
      console.error(line);
    } else if (level === "WARN") {
      console.warn(line);
    } else {
      console.log(line);
    }
  };
}

/**
 * Parse date input with UTC support.
 * Supports:
 * - YYYY-MM-DD (treated as UTC midnight)
 * - YYYY-MM-DD HH:mm (treated as UTC)
 * - YYYY-MM-DD HH:mm:ss (treated as UTC)
 * - YYYY-MM-DDTHH:mm:ssZ (ISO format)
 * - Milliseconds timestamp
 */
function parseDateInput(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  
  // Check for numeric (milliseconds) first
  const numeric = Number(trimmed);
  if (Number.isFinite(numeric) && /^\d+$/.test(trimmed)) {
    return numeric;
  }
  
  // Check for YYYY-MM-DD HH:mm(:ss) format (space-separated, treat as UTC)
  const spaceTimeMatch = trimmed.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)$/);
  if (spaceTimeMatch) {
    // Convert "YYYY-MM-DD HH:mm:ss" to ISO format with Z suffix for UTC
    const isoString = `${spaceTimeMatch[1]}T${spaceTimeMatch[2]}${spaceTimeMatch[2].length === 5 ? ":00" : ""}Z`;
    const parsed = Date.parse(isoString);
    if (!Number.isNaN(parsed)) return parsed;
  }
  
  // Check for plain YYYY-MM-DD (treat as UTC midnight)
  const dateOnlyMatch = trimmed.match(/^(\d{4}-\d{2}-\d{2})$/);
  if (dateOnlyMatch) {
    const isoString = `${dateOnlyMatch[1]}T00:00:00Z`;
    const parsed = Date.parse(isoString);
    if (!Number.isNaN(parsed)) return parsed;
  }
  
  // Try standard Date.parse for ISO formats and other valid date strings
  const parsed = Date.parse(trimmed);
  if (!Number.isNaN(parsed)) return parsed;
  
  return null;
}

function validateDateInput(raw: string): true | string {
  if (parseDateInput(raw) === null) {
    return "Use YYYY-MM-DD, YYYY-MM-DD HH:mm (UTC), or milliseconds.";
  }
  return true;
}

async function promptDateRange(): Promise<
  { startMs: number; endMs: number } | null
> {
  const startRaw = await promptText("Start date (YYYY-MM-DD, YYYY-MM-DD HH:mm UTC, or ms): ", {
    validate: validateDateInput,
  });
  if (startRaw === null) return null;
  const endRaw = await promptText("End date (YYYY-MM-DD, YYYY-MM-DD HH:mm UTC, or ms): ", {
    validate: validateDateInput,
  });
  if (endRaw === null) return null;

  const startMs = parseDateInput(startRaw);
  const endMs = parseDateInput(endRaw);
  if (startMs === null || endMs === null) return null;
  return { startMs, endMs };
}

async function promptBacktestMode(): Promise<"fast" | "visual" | null> {
  return selectOne(
    "Backtest mode",
    [
      { title: "Visual (default)", value: "visual" },
      { title: "Fast", value: "fast" },
    ],
    0,
  );
}

function setupProfileNavigation(
  profileCount: number,
  getIndex: () => number,
  setIndex: (nextIndex: number) => void,
): () => void {
  let keyBuffer = Buffer.alloc(0);

  const handleData = (data: Buffer | string) => {
    const keyStr = typeof data === "string" ? data : data.toString();
    keyBuffer = Buffer.concat([
      keyBuffer,
      typeof data === "string" ? Buffer.from(data) : data,
    ]);

    if (keyBuffer[0] == 0x1b) {
      if (keyBuffer.length >= 3 && keyBuffer[1] == 0x5b) {
        if (keyBuffer[2] == 0x42) {
          const next = Math.min(profileCount - 1, getIndex() + 1);
          setIndex(next);
          keyBuffer = Buffer.alloc(0);
          return;
        }
        if (keyBuffer[2] == 0x41) {
          const next = Math.max(0, getIndex() - 1);
          setIndex(next);
          keyBuffer = Buffer.alloc(0);
          return;
        }
      }
      if (keyBuffer.length > 10) {
        keyBuffer = Buffer.alloc(0);
      }
      return;
    }

    keyBuffer = Buffer.alloc(0);

    if (keyStr.toLowerCase() === "w" || keyStr === "k") {
      const next = Math.max(0, getIndex() - 1);
      setIndex(next);
      return;
    }

    if (keyStr.toLowerCase() === "s" || keyStr === "j") {
      const next = Math.min(profileCount - 1, getIndex() + 1);
      setIndex(next);
      return;
    }

    if (keyStr === "\x03") {
      process.emit("SIGINT", "SIGINT");
    }
  };

  const cleanup = () => {
    process.stdin.removeListener("data", handleData);
    if (process.stdin.isRaw) {
      process.stdin.setRawMode(false);
    }
  };

  if (profileCount > 1) {
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.setEncoding("utf-8");
    process.stdin.on("data", handleData);
  }

  return cleanup;
}

function getNextRunDir(): { runDir: string; runId: string } {
  const logsDir = join(process.cwd(), "logs");
  let index = 1;

  while (true) {
    const name = index === 1 ? "backtest-run" : `backtest-run${index}`;
    const candidate = join(logsDir, name);
    if (!existsSync(candidate)) {
      return { runDir: candidate, runId: name };
    }
    index += 1;
  }
}

function resolveSelection(
  options: BacktestRouteOptions,
  profiles: ProfileDefinition[],
  coinOptions: CoinSymbol[],
): {
  selectedProfiles: string[];
  selectedCoins: CoinSymbol[];
} {
  const profileNames = profiles.map((profile) => profile.name);
  const profileLookup = new Map(
    profileNames.map((name) => [name.toLowerCase(), name]),
  );

  let selectedProfiles: string[] | null = null;
  if (options.profiles && options.profiles.length > 0) {
    const chosen = new Set<string>();
    for (const name of options.profiles) {
      const match = profileLookup.get(name.toLowerCase().trim());
      if (match) {
        chosen.add(match);
      }
    }
    if (chosen.size > 0) {
      selectedProfiles = Array.from(chosen);
    }
  }

  if (!selectedProfiles && options.autoSelect) {
    selectedProfiles = profileNames;
  }

  if (!selectedProfiles) {
    if (!process.stdin.isTTY) {
      throw new Error("No profiles selected. Use --profiles or --auto.");
    }
  }

  const coinsToSelect =
    coinOptions.length > 0 ? coinOptions : ["eth", "btc", "sol", "xrp"];
  let selectedCoins: CoinSymbol[] | null = null;

  if (options.coins && options.coins.length > 0) {
    const normalized = options.coins.map((coin) => coin.toLowerCase().trim());
    if (normalized.includes("all")) {
      selectedCoins = coinsToSelect;
    } else {
      selectedCoins = normalized
        .map((coin) => normalizeCoinKey(coin))
        .filter((coin): coin is CoinSymbol => !!coin);
    }
  }

  if (!selectedCoins && options.autoSelect) {
    selectedCoins = coinsToSelect;
  }

  if (!selectedCoins) {
    if (!process.stdin.isTTY) {
      throw new Error("No coins selected. Use --coins or --auto.");
    }
  }

  return {
    selectedProfiles: selectedProfiles ?? profileNames,
    selectedCoins: selectedCoins ?? coinsToSelect,
  };
}

interface TradeFileStats {
  count?: number;
  minTs?: number;
  maxTs?: number;
  truncated?: boolean;
}

interface TickFileStats {
  count?: number;
  minTs?: number;
  maxTs?: number;
}

export async function loadBacktestData(
  dataDir: string,
  selectedCoins: CoinSymbol[],
  startMs?: number,
  endMs?: number,
): Promise<{
  marketsByCoin: Map<CoinSymbol, BacktestMarketMeta[]>;
  tradeFilesBySlug: Map<string, string>;
  cryptoTickFilesByCoin: Map<CoinSymbol, string>;
  tradeStatsBySlug: Map<string, TradeFileStats>;
  tickStatsByCoin: Map<CoinSymbol, TickFileStats>;
  tradeRangesBySlug: Map<string, { minTs: number; maxTs: number }>;
  tickRangesByCoin: Map<CoinSymbol, { minTs: number; maxTs: number }>;
  missingTradeFiles: string[];
}> {
  const marketsPath = join(dataDir, "markets.jsonl");
  if (!existsSync(marketsPath)) {
    throw new Error(
      `Backtest data not found: ${marketsPath}. Polymarket historical data is required.`,
    );
  }

  const cacheIndex = loadCacheIndex(dataDir);
  const allMarkets = readJsonlFile<BacktestMarketMeta>(marketsPath);
  const marketsByCoin = new Map<CoinSymbol, BacktestMarketMeta[]>();

  for (const market of allMarkets) {
    if (!selectedCoins.includes(market.coin)) continue;
    if (startMs && market.endMs < startMs) continue;
    if (endMs && market.startMs > endMs) continue;
    if (!marketsByCoin.has(market.coin)) {
      marketsByCoin.set(market.coin, []);
    }
    marketsByCoin.get(market.coin)!.push(market);
  }

  for (const [coin, list] of marketsByCoin.entries()) {
    list.sort((a, b) => a.startMs - b.startMs);
    if (list.length === 0) {
      marketsByCoin.delete(coin);
    }
  }

  if (marketsByCoin.size === 0) {
    throw new Error("No backtest markets found for the selected coins.");
  }

  const tradeFilesBySlug = new Map<string, string>();
  const tradeStatsBySlug = new Map<string, TradeFileStats>();
  const tradeRangesBySlug = new Map<string, { minTs: number; maxTs: number }>();
  const missingTradeFiles: string[] = [];
  const skippedMarkets = new Set<string>();

  for (const markets of marketsByCoin.values()) {
    for (const market of markets) {
      const tradePath = join(dataDir, "trades", `${market.slug}.jsonl`);
      if (!existsSync(tradePath)) {
        missingTradeFiles.push(market.slug);
        skippedMarkets.add(market.slug);
        continue;
      }
      const size = statSync(tradePath).size;
      if (size === 0) {
        console.warn(
          `[backtest] Trades file empty for ${market.slug}. Skipping market.`,
        );
        missingTradeFiles.push(market.slug);
        skippedMarkets.add(market.slug);
        continue;
      }
      try {
        ensureSortedTradeFile(tradePath, market.slug);
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Failed to sort trades file.";
        console.warn(
          `[backtest] Failed to verify trades order for ${market.slug}: ${message}`,
        );
      }
      tradeFilesBySlug.set(market.slug, tradePath);
      const stats = cacheIndex.marketTrades[market.slug];
      if (stats) {
        tradeStatsBySlug.set(market.slug, {
          count: stats.count,
          minTs: stats.minTs,
          maxTs: stats.maxTs,
          truncated: stats.truncated,
        });
        if (Number.isFinite(stats.minTs) && Number.isFinite(stats.maxTs)) {
          tradeRangesBySlug.set(market.slug, {
            minTs: stats.minTs,
            maxTs: stats.maxTs,
          });
        }
      }
    }
  }

  if (skippedMarkets.size > 0) {
    for (const [coin, list] of marketsByCoin.entries()) {
      const filtered = list.filter((market) => !skippedMarkets.has(market.slug));
      if (filtered.length > 0) {
        marketsByCoin.set(coin, filtered);
      } else {
        marketsByCoin.delete(coin);
      }
    }
  }

  if (marketsByCoin.size === 0) {
    throw new Error("No backtest markets found with trade history available.");
  }

  const cryptoTickFilesByCoin = new Map<CoinSymbol, string>();
  const tickStatsByCoin = new Map<CoinSymbol, TickFileStats>();
  const tickRangesByCoin = new Map<CoinSymbol, { minTs: number; maxTs: number }>();
  const missingCryptoCoins = new Set<CoinSymbol>();

  for (const coin of selectedCoins) {
    const coinPath = join(dataDir, "crypto", `${coin}.jsonl`);
    if (!existsSync(coinPath)) {
      console.warn(
        `[backtest] Missing crypto history for ${coin}. Expected ${coinPath}. Skipping coin.`,
      );
      missingCryptoCoins.add(coin);
      continue;
    }
    const size = statSync(coinPath).size;
    if (size === 0) {
      console.warn(
        `[backtest] Crypto history empty for ${coin}. Skipping coin.`,
      );
      missingCryptoCoins.add(coin);
      continue;
    }
    cryptoTickFilesByCoin.set(coin, coinPath);
    const stats = cacheIndex.cryptoTicks[coin];
    if (stats) {
      tickStatsByCoin.set(coin, {
        count: stats.count,
        minTs: stats.minTs,
        maxTs: stats.maxTs,
      });
      if (Number.isFinite(stats.minTs) && Number.isFinite(stats.maxTs)) {
        tickRangesByCoin.set(coin, { minTs: stats.minTs, maxTs: stats.maxTs });
      }
    }
  }

  if (missingCryptoCoins.size > 0) {
    for (const coin of missingCryptoCoins) {
      const markets = marketsByCoin.get(coin) ?? [];
      for (const market of markets) {
        tradeFilesBySlug.delete(market.slug);
        tradeStatsBySlug.delete(market.slug);
      }
      marketsByCoin.delete(coin);
      cryptoTickFilesByCoin.delete(coin);
      tickStatsByCoin.delete(coin);
      tickRangesByCoin.delete(coin);
    }
  }

  if (marketsByCoin.size === 0) {
    throw new Error(
      "No backtest markets found with trade history and crypto ticks available.",
    );
  }

  return {
    marketsByCoin,
    tradeFilesBySlug,
    cryptoTickFilesByCoin,
    tradeStatsBySlug,
    tickStatsByCoin,
    tradeRangesBySlug,
    tickRangesByCoin,
    missingTradeFiles,
  };
}

function summarizeBacktestCoverage(
  marketsByCoin: Map<CoinSymbol, BacktestMarketMeta[]>,
  tradeStatsBySlug: Map<string, TradeFileStats>,
  tickStatsByCoin: Map<CoinSymbol, TickFileStats>,
): { info: string[]; warnings: string[] } {
  const info: string[] = [];
  const warnings: string[] = [];

  for (const [coin, markets] of marketsByCoin.entries()) {
    if (markets.length === 0) continue;
    const tickStats = tickStatsByCoin.get(coin);
    const tickCount = tickStats?.count ?? null;
    const tickStart = tickStats?.minTs ?? null;
    const tickEnd = tickStats?.maxTs ?? null;

    let marketStart = Number.POSITIVE_INFINITY;
    let marketEnd = 0;
    let tradeCount: number | null = 0;
    let tradeCountUnknown = false;
    for (const market of markets) {
      marketStart = Math.min(marketStart, market.startMs);
      marketEnd = Math.max(marketEnd, market.endMs);
      const stats = tradeStatsBySlug.get(market.slug);
      if (!stats || stats.count === undefined) {
        tradeCountUnknown = true;
        continue;
      }
      tradeCount = (tradeCount ?? 0) + stats.count;
    }

    if (tradeCountUnknown) {
      tradeCount = null;
    }

    const tradeCountLabel =
      tradeCount === null ? "unknown" : String(tradeCount);
    const tickCountLabel =
      tickCount === null ? "unknown" : String(tickCount);

    info.push(
      `${coin.toUpperCase()} coverage: markets=${markets.length} trades=${tradeCountLabel} ticks=${tickCountLabel} window=${new Date(
        marketStart,
      ).toISOString()} -> ${new Date(marketEnd).toISOString()}`,
    );

    if (tickCount === 0) {
      warnings.push(`${coin.toUpperCase()} missing crypto ticks.`);
    }

    if (tradeCount === 0) {
      warnings.push(`${coin.toUpperCase()} missing trade events.`);
    }

    if (
      tickStart !== null &&
      tickEnd !== null &&
      (tickStart > marketStart || tickEnd < marketEnd)
    ) {
      warnings.push(
        `${coin.toUpperCase()} tick window ${new Date(tickStart).toISOString()} -> ${new Date(
          tickEnd,
        ).toISOString()} does not fully cover market window.`,
      );
    }
  }

  return { info, warnings };
}

function countCachedMarkets(
  dataDir: string,
  selectedCoins: CoinSymbol[],
  startMs: number,
  endMs: number,
): number {
  const marketsPath = join(dataDir, "markets.jsonl");
  if (!existsSync(marketsPath)) return 0;
  try {
    const allMarkets = readJsonlFile<BacktestMarketMeta>(marketsPath);
    return allMarkets.filter((market) => {
      if (!selectedCoins.includes(market.coin)) return false;
      if (market.endMs < startMs) return false;
      if (market.startMs > endMs) return false;
      return true;
    }).length;
  } catch {
    return 0;
  }
}

type CoinWorkerSummary = {
  name: string;
  summary: ProfileSummary;
};

type CoinWorkerResponse =
  | { ok: true; coin: CoinSymbol; summaries: CoinWorkerSummary[] }
  | { ok: false; coin: CoinSymbol; error: string };

async function runCoinWorker(
  coin: CoinSymbol,
  payload: {
    dataDir: string;
    startMs: number;
    endMs: number;
    runDir: string;
    selectedProfiles: string[];
    latencyMs: number;
  },
): Promise<CoinWorkerResponse> {
  return new Promise((resolve) => {
    const worker = new Worker(
      new URL("../backtest/coin-worker.ts", import.meta.url),
      { type: "module" },
    );

    const cleanup = () => {
      worker.terminate();
    };

    worker.addEventListener("message", (event: MessageEvent<CoinWorkerResponse>) => {
      cleanup();
      resolve(event.data);
    });

    worker.addEventListener("error", (event: ErrorEvent) => {
      cleanup();
      resolve({
        ok: false,
        coin,
        error: event.error?.message ?? event.message ?? "Coin worker failed.",
      });
    });

    worker.postMessage({
      coin,
      dataDir: payload.dataDir,
      startMs: payload.startMs,
      endMs: payload.endMs,
      runDir: payload.runDir,
      selectedProfiles: payload.selectedProfiles,
      latencyMs: payload.latencyMs,
    });
  });
}

async function runFastCoinWorkers(
  coins: CoinSymbol[],
  payload: {
    dataDir: string;
    startMs: number;
    endMs: number;
    runDir: string;
    selectedProfiles: string[];
    latencyMs: number;
  },
): Promise<{ summariesByProfile: Map<string, ProfileSummary>; failedCoins: CoinSymbol[] }> {
  const workerLimit = Math.min(parseCoinWorkerLimit(), coins.length);
  const results = await mapWithConcurrency(
    coins,
    workerLimit,
    (coin) => runCoinWorker(coin, payload),
  );

  const summariesByProfile = new Map<string, ProfileSummary>();
  const failedCoins: CoinSymbol[] = [];

  for (const result of results) {
    if (!result.ok) {
      failedCoins.push(result.coin);
      continue;
    }
    for (const { name, summary } of result.summaries) {
      const existing = summariesByProfile.get(name);
      if (!existing) {
        summariesByProfile.set(name, { ...summary });
        continue;
      }
      summariesByProfile.set(name, {
        runtimeSec: Math.max(existing.runtimeSec, summary.runtimeSec),
        totalTrades: existing.totalTrades + summary.totalTrades,
        wins: existing.wins + summary.wins,
        losses: existing.losses + summary.losses,
        totalProfit: existing.totalProfit + summary.totalProfit,
        openExposure: existing.openExposure + summary.openExposure,
      });
    }
  }

  return { summariesByProfile, failedCoins };
}

function buildSummarySignature(
  summaries: Array<{ name: string; summary: ProfileSummary }>,
): string {
  const sorted = [...summaries].sort((a, b) => a.name.localeCompare(b.name));
  const compact = sorted.map(({ name, summary }) => ({
    name,
    trades: summary.totalTrades,
    wins: summary.wins,
    losses: summary.losses,
    pnl: Number(summary.totalProfit.toFixed(4)),
    exposure: Number(summary.openExposure.toFixed(4)),
  }));
  return JSON.stringify(compact);
}

interface FetchBacktestDataResult {
  truncatedMarkets: string[];
  missingAfterRetries: string[];
  cacheHits: { markets: number; trades: number; crypto: number };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchBacktestData(
  dataDir: string,
  selectedCoins: CoinSymbol[],
  startMs: number,
  endMs: number,
  log: LogFn,
): Promise<FetchBacktestDataResult> {
  log(
    `Fetching Polymarket historical data (${new Date(startMs).toISOString()} -> ${new Date(
      endMs,
    ).toISOString()})`,
  );

  // Load cache index
  const cacheIndex = loadCacheIndex(dataDir);
  const cacheHits = { markets: 0, trades: 0, crypto: 0 };
  const now = Date.now();
  let bootstrappedTrades = 0;
  let bootstrappedCrypto = 0;

  const cachedMarkets = loadCachedMarkets(
    dataDir,
    selectedCoins,
    startMs,
    endMs,
    log,
  );
  const cachedCoins = new Set(cachedMarkets.map((market) => market.coin));
  const coinsToFetch = selectedCoins.filter((coin) => !cachedCoins.has(coin));
  let markets: BacktestMarketMeta[] = [];

  if (cachedMarkets.length > 0 && coinsToFetch.length === 0) {
    markets = cachedMarkets;
    cacheHits.markets = cachedMarkets.length;
    log(`Cache hit: using ${cachedMarkets.length} cached markets; skipping market fetch.`);
  } else {
    const coinsForFetch =
      cachedMarkets.length > 0 && coinsToFetch.length > 0
        ? coinsToFetch
        : selectedCoins;
    if (cachedMarkets.length > 0 && coinsToFetch.length > 0) {
      log(
        `Cache hit: ${cachedMarkets.length} cached markets. Fetching markets for coins: ${coinsToFetch.join(", ")}.`,
      );
      cacheHits.markets = cachedMarkets.length;
    }

    const fetchedMarkets = await fetchMarketsForRange(
      coinsForFetch,
      startMs,
      endMs,
      dataDir,
      { log },
    );
    markets = mergeMarketsBySlug(cachedMarkets, fetchedMarkets);

    // Persist merged market list so cached markets are not lost.
    const marketsPath = join(dataDir, "markets.jsonl");
    if (markets.length > 0) {
      writeJsonlLines(marketsPath, markets, { append: false });
    }
  }
  if (markets.length === 0) {
    throw new Error("No markets returned by Polymarket for the selected range.");
  }

  let marketStart = startMs;
  let marketEnd = endMs;
  for (const market of markets) {
    marketStart = Math.min(marketStart, market.startMs);
    marketEnd = Math.max(marketEnd, market.endMs);
    // Update market meta cache
    cacheIndex.marketMeta[market.slug] = {
      slug: market.slug,
      coin: market.coin,
      startMs: market.startMs,
      endMs: market.endMs,
      lastFetchedAt: now,
    };
  }

  const marketsBySlug = new Map(markets.map((m) => [m.slug, m]));
  const retryAttempts = parseTradeRetryAttempts();
  const retryBaseMs = parseTradeRetryBaseMs();

  // Categorize markets by cache status
  const marketsWithFullCache: BacktestMarketMeta[] = [];
  const marketsWithPartialCache: BacktestMarketMeta[] = [];
  const marketsNeedingTrades: BacktestMarketMeta[] = [];
  
  const tradePrevFiles = new Map<string, string>();
  
  for (const market of markets) {
    const cacheEntry = cacheIndex.marketTrades[market.slug];
    const tradePath = join(dataDir, "trades", `${market.slug}.jsonl`);
    const hasTradeFile = existsSync(tradePath);
    const isCached =
      !!cacheEntry &&
      isMarketTradeCached(cacheIndex, market.slug, market.startMs, market.endMs);
    
    if (isCached) {
      // Fully cached
      cacheHits.trades += 1;
      marketsWithFullCache.push(market);
    } else if (!cacheEntry && hasTradeFile) {
      // Bootstrap cache from existing file (assume full coverage)
      cacheIndex.marketTrades[market.slug] = {
        minTs: market.startMs,
        maxTs: market.endMs,
        count: 0,
        truncated: false,
        lastFetchedAt: now,
      };
      cacheHits.trades += 1;
      bootstrappedTrades += 1;
      marketsWithFullCache.push(market);
    } else if (cacheEntry && hasTradeFile) {
      // Partial coverage - save existing data for merge
      marketsWithPartialCache.push(market);
      marketsNeedingTrades.push(market);
      const prevPath = `${tradePath}.prev`;
      if (existsSync(prevPath)) {
        unlinkSync(prevPath);
      }
      renameSync(tradePath, prevPath);
      tradePrevFiles.set(market.slug, prevPath);
      log(`Partial cache for ${market.slug}: moved existing trades for merge.`);
    } else {
      // No cache
      marketsNeedingTrades.push(market);
    }
  }

  // Categorize coins by cache status and save partial data for merging
  const coinsNeedingCrypto: CoinSymbol[] = [];
  const tickPrevFiles = new Map<CoinSymbol, string>();
  
  for (const coin of selectedCoins) {
    const cacheEntry = cacheIndex.cryptoTicks[coin];
    const coinPath = join(dataDir, "crypto", `${coin}.jsonl`);
    const hasCoinFile = existsSync(coinPath);
    
    if (cacheEntry && isCryptoTicksCached(cacheIndex, coin, marketStart, marketEnd)) {
      cacheHits.crypto += 1;
    } else if (!cacheEntry && hasCoinFile) {
      // Bootstrap cache from existing file (assume full coverage)
      cacheIndex.cryptoTicks[coin] = {
        minTs: marketStart,
        maxTs: marketEnd,
        count: 0,
        lastFetchedAt: now,
      };
      cacheHits.crypto += 1;
      bootstrappedCrypto += 1;
    } else if (cacheEntry && hasCoinFile) {
      // Partial coverage - save existing data for merge
      coinsNeedingCrypto.push(coin);
      const prevPath = `${coinPath}.prev`;
      if (existsSync(prevPath)) {
        unlinkSync(prevPath);
      }
      renameSync(coinPath, prevPath);
      tickPrevFiles.set(coin, prevPath);
      log(`Partial cache for ${coin.toUpperCase()} crypto: moved existing ticks for merge.`);
    } else {
      coinsNeedingCrypto.push(coin);
    }
  }

  if (cacheHits.trades > 0) {
    log(`Cache hit: ${cacheHits.trades} markets with cached trades.`);
  }
  if (bootstrappedTrades > 0) {
    log(`Cache bootstrap: ${bootstrappedTrades} market trade files reused (no index).`, "WARN");
  }
  if (cacheHits.crypto > 0) {
    log(`Cache hit: ${cacheHits.crypto} coins with cached crypto ticks.`);
  }
  if (bootstrappedCrypto > 0) {
    log(`Cache bootstrap: ${bootstrappedCrypto} crypto files reused (no index).`, "WARN");
  }

  log(`Fetched ${markets.length} markets, pulling trades for ${marketsNeedingTrades.length}...`);
  if (coinsNeedingCrypto.length > 0) {
    log(`Fetching crypto price history for ${coinsNeedingCrypto.join(", ")}...`);
  }

  let tradesResult: TradesFetchResult = {
    emptyMarkets: [],
    truncatedMarkets: [],
    statsBySlug: new Map(),
  };
  const allTruncated = new Set<string>();

  const tradesPromise = (async () => {
    if (marketsNeedingTrades.length === 0) {
      log("All trades cached, skipping fetch.");
      return;
    }

    // Initial fetch
    tradesResult = await fetchTradesForMarkets(
      marketsNeedingTrades,
      dataDir,
      startMs,
      endMs,
      { log },
    );
    
    // Track all truncated markets and update cache
    for (const slug of tradesResult.truncatedMarkets) {
      allTruncated.add(slug);
    }

    // Merge partial data and update cache for successfully fetched trades
    for (const market of marketsNeedingTrades) {
      const tradePath = join(dataDir, "trades", `${market.slug}.jsonl`);
      const prevPath = tradePrevFiles.get(market.slug);
      if (tradesResult.emptyMarkets.includes(market.slug)) {
        if (prevPath && existsSync(prevPath) && !existsSync(tradePath)) {
          renameSync(prevPath, tradePath);
        }
        continue;
      }

      if (prevPath && existsSync(prevPath) && existsSync(tradePath)) {
        const mergeResult = mergeSortedTradeFiles(prevPath, tradePath, tradePath);
        unlinkSync(prevPath);
        log(
          `Merged ${market.slug}: ${mergeResult.count} total trades (streamed).`,
        );
        cacheIndex.marketTrades[market.slug] = {
          minTs: mergeResult.minTs ?? market.startMs,
          maxTs: mergeResult.maxTs ?? market.endMs,
          count: mergeResult.count,
          truncated: allTruncated.has(market.slug),
          lastFetchedAt: now,
        };
        continue;
      }

      if (prevPath && existsSync(prevPath) && !existsSync(tradePath)) {
        renameSync(prevPath, tradePath);
        continue;
      }

      const stats = tradesResult.statsBySlug.get(market.slug);
      if (stats) {
        cacheIndex.marketTrades[market.slug] = {
          minTs: stats.minTs,
          maxTs: stats.maxTs,
          count: stats.count,
          truncated: allTruncated.has(market.slug),
          lastFetchedAt: now,
        };
      }
    }

    // Retry missing markets with exponential backoff
    let missingMarkets = tradesResult.emptyMarkets;
    for (let attempt = 0; attempt < retryAttempts && missingMarkets.length > 0; attempt++) {
      const backoffMs = retryBaseMs * Math.pow(2, attempt);
      log(
        `Retrying ${missingMarkets.length} missing markets (attempt ${attempt + 1}/${retryAttempts}) after ${backoffMs}ms...`,
        "WARN",
      );
      await sleep(backoffMs);

      const retryMarkets = missingMarkets
        .map((slug) => marketsBySlug.get(slug))
        .filter((m): m is BacktestMarketMeta => m !== undefined);

      if (retryMarkets.length === 0) break;

      const retryResult = await fetchTradesForMarkets(
        retryMarkets,
        dataDir,
        startMs,
        endMs,
        { log },
      );

      // Track newly truncated markets
      for (const slug of retryResult.truncatedMarkets) {
        allTruncated.add(slug);
      }

      for (const [slug, stats] of retryResult.statsBySlug.entries()) {
        tradesResult.statsBySlug.set(slug, stats);
      }

      // Update cache for retried markets that succeeded
      for (const market of retryMarkets) {
        const tradePath = join(dataDir, "trades", `${market.slug}.jsonl`);
        const prevPath = tradePrevFiles.get(market.slug);
        if (retryResult.emptyMarkets.includes(market.slug)) {
          if (prevPath && existsSync(prevPath) && !existsSync(tradePath)) {
            renameSync(prevPath, tradePath);
          }
          continue;
        }

        if (prevPath && existsSync(prevPath) && existsSync(tradePath)) {
          const mergeResult = mergeSortedTradeFiles(prevPath, tradePath, tradePath);
          unlinkSync(prevPath);
          log(
            `Merged ${market.slug}: ${mergeResult.count} total trades (streamed).`,
          );
          cacheIndex.marketTrades[market.slug] = {
            minTs: mergeResult.minTs ?? market.startMs,
            maxTs: mergeResult.maxTs ?? market.endMs,
            count: mergeResult.count,
            truncated: allTruncated.has(market.slug),
            lastFetchedAt: now,
          };
          continue;
        }

        if (prevPath && existsSync(prevPath) && !existsSync(tradePath)) {
          renameSync(prevPath, tradePath);
          continue;
        }

        const stats = retryResult.statsBySlug.get(market.slug);
        if (stats) {
          cacheIndex.marketTrades[market.slug] = {
            minTs: stats.minTs,
            maxTs: stats.maxTs,
            count: stats.count,
            truncated: allTruncated.has(market.slug),
            lastFetchedAt: now,
          };
        }
      }

      // Update missing markets for next iteration
      missingMarkets = retryResult.emptyMarkets;
    }

    // Update final missing markets list
    tradesResult.emptyMarkets = missingMarkets;
    tradesResult.truncatedMarkets = Array.from(allTruncated);

    log("Trade fetch complete.");
  })();

  const binancePromise = (async () => {
    if (coinsNeedingCrypto.length === 0) {
      log("All crypto ticks cached, skipping fetch.");
      return;
    }

    try {
      await fetchBinanceCryptoHistory(
        coinsNeedingCrypto,
        marketStart,
        marketEnd,
        dataDir,
        { log },
      );

      // Merge partial data and update cache for fetched crypto ticks
      for (const coin of coinsNeedingCrypto) {
        const coinPath = join(dataDir, "crypto", `${coin}.jsonl`);
        const prevPath = tickPrevFiles.get(coin);

        if (prevPath && existsSync(prevPath) && existsSync(coinPath)) {
          const mergeResult = mergeSortedTickFiles(prevPath, coinPath, coinPath);
          unlinkSync(prevPath);
          log(
            `Merged ${coin.toUpperCase()} crypto: ${mergeResult.count} total ticks (streamed).`,
          );
          cacheIndex.cryptoTicks[coin] = {
            minTs: mergeResult.minTs ?? marketStart,
            maxTs: mergeResult.maxTs ?? marketEnd,
            count: mergeResult.count,
            lastFetchedAt: now,
          };
          continue;
        }

        if (prevPath && existsSync(prevPath) && !existsSync(coinPath)) {
          renameSync(prevPath, coinPath);
          continue;
        }

        if (existsSync(coinPath)) {
          cacheIndex.cryptoTicks[coin] = {
            minTs: marketStart,
            maxTs: marketEnd,
            count: 0,
            lastFetchedAt: now,
          };
        }
      }

      log("Binance crypto fetch complete.");
    } catch (error) {
      const allowFailureRaw = process.env.BACKTEST_ALLOW_CRYPTO_FETCH_FAILURE;
      const allowFailure =
        allowFailureRaw === undefined
          ? true
          : ["true", "1", "yes", "on"].includes(
              allowFailureRaw.trim().toLowerCase(),
            );
      for (const coin of coinsNeedingCrypto) {
        const coinPath = join(dataDir, "crypto", `${coin}.jsonl`);
        const prevPath = tickPrevFiles.get(coin);
        if (prevPath && existsSync(prevPath) && !existsSync(coinPath)) {
          renameSync(prevPath, coinPath);
        }
      }
      if (allowFailure) {
        const message = error instanceof Error ? error.message : String(error);
        log(
          `Crypto history fetch failed (${message}). Proceeding with existing cache.`,
          "WARN",
        );
        return;
      }
      throw error;
    }
  })();

  await Promise.all([tradesPromise, binancePromise]);

  // Save updated cache index
  saveCacheIndex(dataDir, cacheIndex);
  log("Cache index updated.");

  return {
    truncatedMarkets: tradesResult.truncatedMarkets,
    missingAfterRetries: tradesResult.emptyMarkets,
    cacheHits,
  };
}

export async function backtestRoute(
  options: BacktestRouteOptions = {},
): Promise<void> {
  let profiles: ProfileDefinition[] = [];
  let coinOptions: CoinSymbol[] = [];

  try {
    const loaded = loadProfilesFromConfig();
    profiles = loaded.profiles;
    coinOptions = loaded.coinOptions;
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load config.json.";
    console.log(message);
    return;
  }

  if (profiles.length === 0) {
    console.log("No profiles found in config.json.");
    return;
  }

  let selectedProfiles: string[] = [];
  let selectedCoins: CoinSymbol[] = [];
  try {
    const selections = resolveSelection(options, profiles, coinOptions);
    selectedProfiles = selections.selectedProfiles;
    selectedCoins = selections.selectedCoins;
  } catch (error) {
    console.log(error instanceof Error ? error.message : "Selection failed.");
    return;
  }

  if (process.stdin.isTTY && (!options.profiles || !options.coins)) {
    const profileNames = profiles.map((profile) => profile.name);
    if (!options.profiles) {
      const pickedProfiles = await selectMany(
        "Select profiles",
        profileNames.map((name) => ({ title: name, value: name })),
      );
      if (pickedProfiles.length === 0) {
        console.log("No profiles selected. Exiting.");
        return;
      }
      selectedProfiles = pickedProfiles;
    }

    if (!options.coins) {
      const coinsToSelect =
        coinOptions.length > 0 ? coinOptions : ["eth", "btc", "sol", "xrp"];
      const selectedCoinsRaw = await selectMany(
        "Select coins",
        coinsToSelect.map((coin) => ({
          title: coin.toUpperCase(),
          value: coin.toUpperCase(),
        })),
      );
      if (selectedCoinsRaw.length === 0) {
        console.log("No coins selected. Exiting.");
        return;
      }
      selectedCoins = selectedCoinsRaw
        .map((coin) => normalizeCoinKey(coin))
        .filter((coin): coin is CoinSymbol => !!coin);
    }
  }

  if (selectedProfiles.length === 0 || selectedCoins.length === 0) {
    console.log("Profiles or coins not selected.");
    return;
  }

  const dataDir = options.dataDir ?? join(process.cwd(), "backtest-data");
  const latencyMs = parseLatencyMs();
  let runMode = options.mode;
  if (!runMode && process.stdin.isTTY) {
    const pickedMode = await promptBacktestMode();
    if (!pickedMode) {
      console.log("Backtest mode not selected. Exiting.");
      return;
    }
    runMode = pickedMode;
  }

  let rangeStartMs = options.startMs;
  let rangeEndMs = options.endMs;
  if (!rangeStartMs || !rangeEndMs) {
    if (!process.stdin.isTTY) {
      console.log("Backtest requires --start and --end when non-interactive.");
      return;
    }
    const range = await promptDateRange();
    if (!range) {
      console.log("No date range selected. Exiting.");
      return;
    }
    rangeStartMs = range.startMs;
    rangeEndMs = range.endMs;
  }

  if (rangeStartMs >= rangeEndMs) {
    console.log("Start date must be before end date.");
    return;
  }

  const { runDir, runId } = getNextRunDir();

  const resolvedMode = runMode ?? "visual";
  const speed = options.speed ?? (resolvedMode === "fast" ? 0 : 1);
  const headless = options.headless === true || resolvedMode === "fast";
  const useCoinWorkers =
    resolvedMode === "fast" &&
    speed <= 0 &&
    parseCoinWorkerEnabled() &&
    selectedCoins.length > 1;
  const perfLogEnabled = parsePerfLogEnabled();
  const regressionLogEnabled = parseRegressionLogEnabled();

  let fetchResult: FetchBacktestDataResult = { truncatedMarkets: [], missingAfterRetries: [] };
  try {
    const fetchLogger = new RunLogger(join(runDir, "fetch.log"));
    const fetchLog = createCliLogger(fetchLogger, "fetch");
    fetchResult = await fetchBacktestData(
      dataDir,
      selectedCoins,
      rangeStartMs,
      rangeEndMs,
      fetchLog,
    );
  } catch (error) {
    console.log(error instanceof Error ? error.message : "Backtest data error.");
    return;
  }

  if (useCoinWorkers) {
    const systemLogger = new RunLogger(join(runDir, "system.log"));
    systemLogger.log(
      `Backtest starting (${selectedCoins.join(", ")}), latency ${latencyMs}ms`,
    );
    systemLogger.log(`Backtest data dir: ${dataDir}`);
    if (rangeStartMs || rangeEndMs) {
      systemLogger.log(
        `Backtest window: ${rangeStartMs ?? "start"} -> ${rangeEndMs ?? "end"}`,
      );
    }

    const totalMarketsCount = countCachedMarkets(
      dataDir,
      selectedCoins,
      rangeStartMs,
      rangeEndMs,
    );
    systemLogger.log("─── Fetch Summary ───");
    systemLogger.log(`Total markets fetched: ${totalMarketsCount}`);
    systemLogger.log(
      `Cache hits - trades: ${fetchResult.cacheHits.trades}, crypto: ${fetchResult.cacheHits.crypto}`,
    );
    if (fetchResult.truncatedMarkets.length > 0) {
      systemLogger.log(
        `Truncated (offset limit): ${fetchResult.truncatedMarkets.length} markets (data may be incomplete).`,
        "WARN",
      );
    }
    if (fetchResult.missingAfterRetries.length > 0) {
      systemLogger.log(
        `Missing after retries: ${fetchResult.missingAfterRetries.length} markets dropped.`,
        "WARN",
      );
    }
    systemLogger.log("─── End Fetch Summary ───");
    systemLogger.log(
      `Fast mode coin workers enabled (limit ${parseCoinWorkerLimit()}).`,
    );

    const workerStart = perfLogEnabled ? Date.now() : 0;
    const { summariesByProfile, failedCoins } = await runFastCoinWorkers(
      selectedCoins,
      {
        dataDir,
        startMs: rangeStartMs,
        endMs: rangeEndMs,
        runDir,
        selectedProfiles,
        latencyMs,
      },
    );
    if (perfLogEnabled) {
      const workerDuration = Date.now() - workerStart;
      systemLogger.log(`Coin worker runtime ms: ${workerDuration}`);
    }

    if (failedCoins.length > 0) {
      const msg = `Coin workers failed for ${failedCoins.length} coins (${failedCoins.join(
        ", ",
      )}).`;
      systemLogger.log(msg, "WARN");
      console.warn(`[backtest] ${msg}`);
    }

    const summaryList = Array.from(summariesByProfile.entries()).map(
      ([name, summary]) => ({ name, summary }),
    );

    if (summaryList.length === 0) {
      const warn =
        "Coin workers produced no summaries; falling back to single-process run.";
      systemLogger.log(warn, "WARN");
      console.warn(`[backtest] ${warn}`);
    } else {
      const header = `Backtest complete (${selectedCoins.join(", ")}), run ${runId}`;
      systemLogger.log(header);
      console.log(`[backtest] ${header}`);

      for (const { name, summary } of summaryList) {
        const line = `${name} summary trades=${summary.totalTrades} wins=${summary.wins} losses=${summary.losses} pnl=${summary.totalProfit.toFixed(
          2,
        )} exposure=${summary.openExposure.toFixed(2)} runtime=${summary.runtimeSec.toFixed(
          1,
        )}s`;
        systemLogger.log(line);
        console.log(`[backtest] ${line}`);
      }

      if (regressionLogEnabled) {
        systemLogger.log(
          `Regression signature: ${buildSummarySignature(summaryList)}`,
        );
      }

      return;
    }
  }

  let data;
  let loadDurationMs: number | null = null;
  try {
    const loadStart = perfLogEnabled ? Date.now() : 0;
    data = await loadBacktestData(dataDir, selectedCoins, rangeStartMs, rangeEndMs);
    if (perfLogEnabled) {
      loadDurationMs = Date.now() - loadStart;
    }
  } catch (error) {
    console.log(error instanceof Error ? error.message : "Backtest data error.");
    return;
  }

  const systemLogger = new RunLogger(join(runDir, "system.log"));
  systemLogger.log(
    `Backtest starting (${selectedCoins.join(", ")}), latency ${latencyMs}ms`,
  );
  systemLogger.log(`Backtest data dir: ${dataDir}`);
  if (
    resolvedMode === "fast" &&
    speed <= 0 &&
    parseCoinWorkerEnabled() &&
    selectedCoins.length <= 1
  ) {
    systemLogger.log("Coin workers disabled for single-coin fast mode.");
  }
  if (perfLogEnabled && loadDurationMs !== null) {
    systemLogger.log(`Backtest data load ms: ${loadDurationMs}`);
  }
  if (rangeStartMs || rangeEndMs) {
    systemLogger.log(
      `Backtest window: ${rangeStartMs ?? "start"} -> ${rangeEndMs ?? "end"}`,
    );
  }

  const hub = new BacktestHub({
    marketsByCoin: data.marketsByCoin,
    tradeFilesBySlug: data.tradeFilesBySlug,
    cryptoTickFilesByCoin: data.cryptoTickFilesByCoin,
    tradeRangesBySlug: data.tradeRangesBySlug,
    tickRangesByCoin: data.tickRangesByCoin,
    latencyMs,
  });
  const coverage = summarizeBacktestCoverage(
    data.marketsByCoin,
    data.tradeStatsBySlug,
    data.tickStatsByCoin,
  );
  for (const line of coverage.info) {
    systemLogger.log(line);
  }
  for (const line of coverage.warnings) {
    systemLogger.log(line, "WARN");
  }

  const profileEngines: ProfileEngine[] = [];
  for (const profile of profiles) {
    if (!selectedProfiles.includes(profile.name)) {
      continue;
    }

    const filtered = new Map<CoinSymbol, TimedTradeConfig>();
    for (const coin of selectedCoins) {
      const cfg = profile.configs.get(coin);
      if (cfg) {
        filtered.set(coin, cfg);
      }
    }

    if (filtered.size === 0) {
      systemLogger.log(
        `Profile ${profile.name} has no configs for selected coins, skipping.`,
        "WARN",
      );
      continue;
    }

    const profileLogName = `${sanitizeProfileName(profile.name)}.log`;
    const profileLogger = new RunLogger(join(runDir, profileLogName));
    profileEngines.push(
      new ProfileEngine(
        profile.name,
        filtered,
        profileLogger,
        hub.getStartTimeMs(),
        {
          advancedSignals: true,
          decisionLatencyMs: 250,
          crossDebug: true,
          crossAllowNoFlip: true,
        },
      ),
    );
  }

  if (profileEngines.length === 0) {
    systemLogger.log("No profiles eligible for selected coins.", "WARN");
    return;
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Fetch Summary
  // ─────────────────────────────────────────────────────────────────────────────
  let totalMarketsCount = 0;
  for (const markets of data.marketsByCoin.values()) {
    totalMarketsCount += markets.length;
  }
  
  systemLogger.log("─── Fetch Summary ───");
  systemLogger.log(`Total markets fetched: ${totalMarketsCount}`);
  systemLogger.log(`Cache hits - trades: ${fetchResult.cacheHits.trades}, crypto: ${fetchResult.cacheHits.crypto}`);
  
  if (fetchResult.truncatedMarkets.length > 0) {
    systemLogger.log(
      `Truncated (offset limit): ${fetchResult.truncatedMarkets.length} markets (data may be incomplete).`,
      "WARN",
    );
  }
  
  if (fetchResult.missingAfterRetries.length > 0) {
    systemLogger.log(
      `Missing after retries: ${fetchResult.missingAfterRetries.length} markets dropped.`,
      "WARN",
    );
  }
  
  if (data.missingTradeFiles.length > 0) {
    systemLogger.log(
      `Missing trade files: ${data.missingTradeFiles.length} markets skipped during load.`,
      "WARN",
    );
  }
  
  systemLogger.log("─── End Fetch Summary ───");

  const renderDashboard = resolvedMode !== "fast" && !headless;
  const dashboard = new ProfileDashboard();
  let activeProfileIndex = 0;
  const cleanupNavigation = renderDashboard
    ? setupProfileNavigation(
        profileEngines.length,
        () => activeProfileIndex,
        (nextIndex) => {
          activeProfileIndex = nextIndex;
        },
      )
    : () => {};

  const runStart = perfLogEnabled ? Date.now() : 0;
  const logFinalSummary = () => {
    const header = `Backtest complete (${selectedCoins.join(", ")}), run ${runId}`;
    systemLogger.log(header);
    console.log(`[backtest] ${header}`);

    for (const engine of profileEngines) {
      const summary = engine.getSummary();
      const line = `${engine.getName()} summary trades=${summary.totalTrades} wins=${summary.wins} losses=${summary.losses} pnl=${summary.totalProfit.toFixed(
        2,
      )} exposure=${summary.openExposure.toFixed(2)} runtime=${summary.runtimeSec.toFixed(
        1,
      )}s`;
      systemLogger.log(line);
      console.log(`[backtest] ${line}`);
    }

    if (regressionLogEnabled) {
      const summaryList = profileEngines.map((engine) => ({
        name: engine.getName(),
        summary: engine.getSummary(),
      }));
      systemLogger.log(
        `Regression signature: ${buildSummarySignature(summaryList)}`,
      );
    }

    if (perfLogEnabled) {
      const runtimeMs = Date.now() - runStart;
      systemLogger.log(`Backtest runtime ms: ${runtimeMs}`);
    }
  };

  const runner = new BacktestRunner(hub, profileEngines, dashboard, {
    speed,
    runId,
    modeLabel:
      resolvedMode === "fast"
        ? "Backtest Fast Mode"
        : headless
          ? "Backtest Headless Mode"
          : "Backtest Visual Mode",
    activeProfileIndex: () => activeProfileIndex,
    setActiveProfileIndex: (index) => {
      activeProfileIndex = index;
    },
    selectedCoins,
    render: renderDashboard,
    headless,
    headlessLogEveryMs: parseHeadlessLogEveryMs(),
    onComplete: () => {
      hub.close();
      logFinalSummary();
      cleanupNavigation();
      process.exit(0);
    },
  });

  runner.start();

  process.on("SIGINT", () => {
    runner.stop();
    hub.close();
    cleanupNavigation();
    process.exit(0);
  });
}
