import { existsSync, renameSync, unlinkSync } from "fs";
import { join } from "path";
import { Readable } from "stream";
import * as readline from "readline";
import * as unzipper from "unzipper";
import type { CoinSymbol } from "../services/auto-market";
import type { BacktestCryptoTick } from "./types";
import { writeJsonlLines } from "./jsonl";
import { mapWithConcurrency } from "./concurrency";

const BINANCE_BASE =
  process.env.BINANCE_API_BASE ?? "https://api.binance.com";
const BINANCE_US_BASE =
  process.env.BINANCE_US_API_BASE ?? "https://api.binance.us";
const BINANCE_VISION_BASE =
  process.env.BINANCE_VISION_BASE ?? "https://data.binance.vision";
const AGGTRADES_ENDPOINT = "/api/v3/aggTrades";
const LIMIT_RAW = Number(process.env.BINANCE_AGGTRADES_LIMIT);
const BINANCE_LIMIT = Number.isFinite(LIMIT_RAW) ? LIMIT_RAW : 1000;
const DELAY_RAW = Number(process.env.BINANCE_AGGTRADES_DELAY_MS);
const BINANCE_DELAY_MS = Number.isFinite(DELAY_RAW) ? DELAY_RAW : 300;
const CHUNK_RAW = Number(process.env.BINANCE_AGGTRADES_CHUNK_MS);
const BINANCE_CHUNK_MS = Number.isFinite(CHUNK_RAW) ? CHUNK_RAW : 60000;
const TIMEOUT_RAW = Number(process.env.BINANCE_AGGTRADES_TIMEOUT_MS);
const BINANCE_TIMEOUT_MS = Number.isFinite(TIMEOUT_RAW)
  ? Math.max(1000, Math.floor(TIMEOUT_RAW))
  : 15000;
const RETRIES_RAW = Number(process.env.BINANCE_AGGTRADES_RETRIES);
const BINANCE_MAX_RETRIES = Number.isFinite(RETRIES_RAW)
  ? Math.max(0, Math.floor(RETRIES_RAW))
  : 5;
const VISION_TIMEOUT_RAW = Number(process.env.BINANCE_VISION_TIMEOUT_MS);
const BINANCE_VISION_TIMEOUT_MS = Number.isFinite(VISION_TIMEOUT_RAW)
  ? Math.max(5000, Math.floor(VISION_TIMEOUT_RAW))
  : 60000;
const VISION_BATCH_RAW = Number(process.env.BINANCE_VISION_BATCH_SIZE);
const BINANCE_VISION_BATCH_SIZE = Number.isFinite(VISION_BATCH_RAW)
  ? Math.max(100, Math.floor(VISION_BATCH_RAW))
  : 5000;
const VISION_RETRIES_RAW = Number(process.env.BINANCE_VISION_RETRIES);
const BINANCE_VISION_MAX_RETRIES = Number.isFinite(VISION_RETRIES_RAW)
  ? Math.max(0, Math.floor(VISION_RETRIES_RAW))
  : 3;
const VISION_MODE_RAW = process.env.BINANCE_VISION_MODE;
const PROVIDER_RAW = process.env.CRYPTO_HISTORY_PROVIDER;
const FALLBACK_RAW = process.env.CRYPTO_HISTORY_FALLBACK_PROVIDER;
const TAIL_PROVIDER_RAW = process.env.CRYPTO_HISTORY_TAIL_PROVIDER;
const concurrencyRaw = Number(process.env.BACKTEST_BINANCE_CONCURRENCY);
const BINANCE_CONCURRENCY = Number.isFinite(concurrencyRaw)
  ? concurrencyRaw
  : 2;
const progressRaw = Number(process.env.BACKTEST_BINANCE_PROGRESS_EVERY);
const BINANCE_PROGRESS_EVERY = Number.isFinite(progressRaw)
  ? Math.max(1, Math.floor(progressRaw))
  : 10;

const COIN_TO_SYMBOL: Record<CoinSymbol, string> = {
  eth: "ETHUSDT",
  btc: "BTCUSDT",
  sol: "SOLUSDT",
  xrp: "XRPUSDT",
};

interface BinanceAggTrade {
  a: number;
  p: string;
  q: string;
  T: number;
}

type LogLevel = "INFO" | "WARN" | "ERROR";
type LogFn = (message: string, level?: LogLevel) => void;

type CryptoHistoryProviderId = "binance" | "binance_us" | "binance_vision";
type BinanceVisionMode = "auto" | "daily" | "monthly";

interface CryptoHistoryOptions {
  log?: LogFn;
  concurrency?: number;
}

interface CryptoHistoryRequest extends CryptoHistoryOptions {
  coins: CoinSymbol[];
  startMs: number;
  endMs: number;
  dataDir: string;
  outputOverrides?: Partial<
    Record<CoinSymbol, { path: string; append: boolean }>
  >;
}

interface CryptoHistoryProvider {
  id: CryptoHistoryProviderId;
  label: string;
  fetchHistory: (request: CryptoHistoryRequest) => Promise<void>;
}

class BinanceRequestError extends Error {
  status?: number;
  retryAfterMs?: number;
  isBlocked?: boolean;
  isRateLimited?: boolean;
  isEmpty?: boolean;
  url?: string;

  constructor(
    message: string,
    options: {
      status?: number;
      retryAfterMs?: number;
      isBlocked?: boolean;
      isRateLimited?: boolean;
      isEmpty?: boolean;
      url?: string;
    } = {},
  ) {
    super(message);
    this.name = "BinanceRequestError";
    this.status = options.status;
    this.retryAfterMs = options.retryAfterMs;
    this.isBlocked = options.isBlocked;
    this.isRateLimited = options.isRateLimited;
    this.isEmpty = options.isEmpty;
    this.url = options.url;
  }
}

class BinanceVisionNotFoundError extends Error {
  url: string;
  constructor(url: string) {
    super(`Binance Vision archive not found: ${url}`);
    this.name = "BinanceVisionNotFoundError";
    this.url = url;
  }
}

const RATE_LIMIT_STATUSES = new Set([429, 418]);
const BLOCKED_STATUSES = new Set([403, 451]);
const DAY_MS = 24 * 60 * 60 * 1000;

function normalizeProviderId(
  raw: string | null | undefined,
): CryptoHistoryProviderId | null {
  if (!raw) return null;
  switch (raw.toLowerCase()) {
    case "binance":
      return "binance";
    case "binance_us":
    case "binance-us":
    case "binanceus":
      return "binance_us";
    case "binance_vision":
    case "binance-vision":
    case "binancevision":
      return "binance_vision";
    default:
      return null;
  }
}

function normalizeVisionMode(
  raw: string | null | undefined,
): BinanceVisionMode {
  if (!raw) return "auto";
  switch (raw.toLowerCase()) {
    case "daily":
      return "daily";
    case "monthly":
      return "monthly";
    case "auto":
    default:
      return "auto";
  }
}

function resolveProviderIds(): {
  primary: CryptoHistoryProviderId;
  fallback?: CryptoHistoryProviderId;
} {
  const primary = normalizeProviderId(PROVIDER_RAW) ?? "binance";
  const explicitFallback = normalizeProviderId(FALLBACK_RAW);
  const fallback =
    explicitFallback ??
    (primary === "binance" ? "binance_vision" : undefined);
  return fallback && fallback !== primary ? { primary, fallback } : { primary };
}

function joinUrl(base: string, path: string): string {
  const trimmedBase = base.replace(/\/+$/, "");
  const trimmedPath = path.replace(/^\/+/, "");
  return `${trimmedBase}/${trimmedPath}`;
}

function parseRetryAfterMs(value: string | null): number | null {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) {
    return Math.max(0, Math.floor(seconds * 1000));
  }
  const dateMs = Date.parse(value);
  if (!Number.isNaN(dateMs)) {
    return Math.max(0, dateMs - Date.now());
  }
  return null;
}

function formatWeightHeaders(headers: Headers): string | null {
  const entries: string[] = [];
  headers.forEach((value, key) => {
    if (key.toLowerCase().startsWith("x-mbx-used-weight")) {
      entries.push(`${key}=${value}`);
    }
  });
  return entries.length > 0 ? entries.join(", ") : null;
}

function compactText(value: string, max = 300): string {
  const trimmed = value.replace(/\s+/g, " ").trim();
  if (trimmed.length <= max) return trimmed;
  return `${trimmed.slice(0, max)}...`;
}

function buildErrorDetails(options: {
  status: number;
  retryAfterMs: number | null;
  weightHeaders: string | null;
  bodySnippet: string | null;
}): string {
  const parts = [`status=${options.status}`];
  if (options.retryAfterMs !== null) {
    parts.push(`retryAfterMs=${options.retryAfterMs}`);
  }
  if (options.weightHeaders) {
    parts.push(`weights=${options.weightHeaders}`);
  }
  if (options.bodySnippet) {
    parts.push(`body=${options.bodySnippet}`);
  }
  return parts.join(" ");
}

function computeBackoffMs(
  attempt: number,
  retryAfterMs: number | null,
): number {
  if (retryAfterMs !== null) {
    return retryAfterMs;
  }
  const base = Math.max(1, BINANCE_DELAY_MS) * Math.pow(2, attempt);
  const jitter = Math.floor(base * 0.2 * Math.random());
  return base + jitter;
}

function startOfUtcDay(ms: number): number {
  const date = new Date(ms);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

function startOfUtcMonth(ms: number): number {
  const date = new Date(ms);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
}

function addUtcMonths(ms: number, delta: number): number {
  const date = new Date(ms);
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + delta;
  return Date.UTC(year, month, 1);
}

function formatUtcDate(ms: number): string {
  const date = new Date(ms);
  const year = date.getUTCFullYear();
  const month = `${date.getUTCMonth() + 1}`.padStart(2, "0");
  const day = `${date.getUTCDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatUtcMonth(ms: number): string {
  const date = new Date(ms);
  const year = date.getUTCFullYear();
  const month = `${date.getUTCMonth() + 1}`.padStart(2, "0");
  return `${year}-${month}`;
}

function getLatestVisionDailyAvailableDay(nowMs: number): number {
  const now = new Date(nowMs);
  const todayStart = startOfUtcDay(nowMs);
  const cutoffHour = 10;
  const lagDays = now.getUTCHours() >= cutoffHour ? 1 : 2;
  return startOfUtcDay(todayStart - lagDays * DAY_MS);
}

function firstMondayOfMonth(year: number, monthIndex: number): number {
  const firstDay = Date.UTC(year, monthIndex, 1);
  const firstDate = new Date(firstDay);
  const firstDow = firstDate.getUTCDay();
  const delta = (8 - (firstDow === 0 ? 7 : firstDow)) % 7;
  return firstDay + delta * DAY_MS;
}

function isMonthlyArchiveAvailable(monthStartMs: number, nowMs: number): boolean {
  const monthStart = new Date(monthStartMs);
  const nextMonthStart = Date.UTC(
    monthStart.getUTCFullYear(),
    monthStart.getUTCMonth() + 1,
    1,
  );
  const nextMonthDate = new Date(nextMonthStart);
  const firstMonday = firstMondayOfMonth(
    nextMonthDate.getUTCFullYear(),
    nextMonthDate.getUTCMonth(),
  );
  return nowMs >= firstMonday;
}

function coerceTimestamp(value: string): number | null {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  if (numeric >= 1e17) return Math.floor(numeric / 1_000_000);
  if (numeric >= 1e14) return Math.floor(numeric / 1_000);
  if (numeric > 1e12) return Math.floor(numeric);
  if (numeric > 1e9) return Math.floor(numeric * 1000);
  return null;
}

function resolveAggTradeTimestamp(
  parts: string[],
  startMs: number,
  endMs: number,
): number | null {
  const searchStart = Math.max(0, startMs - DAY_MS);
  const searchEnd = endMs + DAY_MS;
  const preferred = [5, 6, 4];

  for (const idx of preferred) {
    if (idx >= parts.length) continue;
    const candidate = coerceTimestamp(parts[idx]!);
    if (candidate !== null && candidate >= searchStart && candidate <= searchEnd) {
      return candidate;
    }
  }

  for (const part of parts) {
    const candidate = coerceTimestamp(part);
    if (candidate !== null && candidate >= searchStart && candidate <= searchEnd) {
      return candidate;
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithTimeout(
  url: string,
  timeoutMs: number,
): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchAggTrades(
  baseUrl: string,
  symbol: string,
  params: Record<string, string>,
  log?: LogFn,
): Promise<BinanceAggTrade[]> {
  const url = new URL(`${baseUrl}${AGGTRADES_ENDPOINT}`);
  url.searchParams.set("symbol", symbol);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  let attempt = 0;
  while (true) {
    try {
      const response = await fetchWithTimeout(
        url.toString(),
        BINANCE_TIMEOUT_MS,
      );
      if (response.ok) {
        const data = await response.json();
        return Array.isArray(data) ? (data as BinanceAggTrade[]) : [];
      }

      const status = response.status;
      const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
      const weightHeaders = formatWeightHeaders(response.headers);
      const bodyText = await response.text().catch(() => "");
      const details = buildErrorDetails({
        status,
        retryAfterMs,
        weightHeaders,
        bodySnippet: bodyText ? compactText(bodyText) : null,
      });

      if (RATE_LIMIT_STATUSES.has(status) && attempt < BINANCE_MAX_RETRIES) {
        const delayMs = computeBackoffMs(attempt, retryAfterMs);
        log?.(
          `Binance aggTrades rate limited (${details}). Retrying in ${delayMs}ms.`,
          "WARN",
        );
        await sleep(delayMs);
        attempt += 1;
        continue;
      }

      if (BLOCKED_STATUSES.has(status)) {
        const message =
          `Binance aggTrades blocked (${details}). ` +
          "This likely indicates regional or WAF restrictions. " +
          "Set BINANCE_API_BASE to a reachable endpoint or set " +
          "CRYPTO_HISTORY_PROVIDER=binance_vision (or " +
          "CRYPTO_HISTORY_FALLBACK_PROVIDER=binance_vision) to use data archives.";
        throw new BinanceRequestError(message, {
          status,
          retryAfterMs,
          isBlocked: true,
          url: url.toString(),
        });
      }

      if (RATE_LIMIT_STATUSES.has(status)) {
        const message = `Binance aggTrades rate limit exceeded (${details}).`;
        throw new BinanceRequestError(message, {
          status,
          retryAfterMs,
          isRateLimited: true,
          url: url.toString(),
        });
      }

      const message = `Binance aggTrades failed (${details}).`;
      throw new BinanceRequestError(message, {
        status,
        retryAfterMs,
        url: url.toString(),
      });
    } catch (error) {
      if (error instanceof BinanceRequestError) {
        throw error;
      }

      const message =
        error instanceof Error ? error.message : "Binance aggTrades failed.";
      if (attempt < BINANCE_MAX_RETRIES) {
        const delayMs = computeBackoffMs(attempt, null);
        log?.(
          `Binance aggTrades error (${message}). Retrying in ${delayMs}ms.`,
          "WARN",
        );
        await sleep(delayMs);
        attempt += 1;
        continue;
      }
      const finalError = new BinanceRequestError(
        `Binance aggTrades failed: ${message}`,
        { url: url.toString() },
      );
      throw finalError;
    }
  }
}

function wrapBinanceError(coin: CoinSymbol, error: unknown): Error {
  if (error instanceof BinanceRequestError) {
    return new BinanceRequestError(
      `Binance ${coin.toUpperCase()} fetch failed: ${error.message}`,
      {
        status: error.status,
        retryAfterMs: error.retryAfterMs,
        isBlocked: error.isBlocked,
        isRateLimited: error.isRateLimited,
        isEmpty: error.isEmpty,
        url: error.url,
      },
    );
  }
  const message = error instanceof Error ? error.message : "Binance fetch failed.";
  return new Error(`Binance ${coin.toUpperCase()} fetch failed: ${message}`, {
    cause: error instanceof Error ? error : undefined,
  });
}

function shouldFallback(error: unknown): boolean {
  return (
    error instanceof BinanceRequestError &&
    (error.isBlocked === true || error.isEmpty === true)
  );
}

function createAggTradesProvider(
  id: CryptoHistoryProviderId,
  baseUrl: string,
  label: string,
): CryptoHistoryProvider {
  return {
    id,
    label,
    fetchHistory: (request) =>
      fetchBinanceAggTradesHistory(request, baseUrl, label),
  };
}

function resolveProvider(id: CryptoHistoryProviderId): CryptoHistoryProvider {
  switch (id) {
    case "binance":
      return createAggTradesProvider(
        "binance",
        BINANCE_BASE,
        "Binance aggTrades (binance.com)",
      );
    case "binance_us":
      return createAggTradesProvider(
        "binance_us",
        BINANCE_US_BASE,
        "Binance aggTrades (binance.us)",
      );
    case "binance_vision":
      return {
        id: "binance_vision",
        label: "Binance Vision aggTrades archives",
        fetchHistory: fetchBinanceVisionHistory,
      };
  }
}

async function fetchBinanceAggTradesHistory(
  request: CryptoHistoryRequest,
  baseUrl: string,
  label: string,
): Promise<void> {
  const { coins, startMs, endMs, dataDir, log } = request;
  const concurrency = request.concurrency ?? BINANCE_CONCURRENCY;
  log?.(`Crypto history provider: ${label}`, "INFO");
  log?.(`Binance REST base: ${baseUrl}`, "INFO");

  await mapWithConcurrency(coins, concurrency, async (coin) => {
    const symbol = COIN_TO_SYMBOL[coin];
    if (!symbol) {
      throw new Error(`No Binance symbol configured for coin ${coin}.`);
    }
    const outSymbol = `${coin}/usd`;
    const override = request.outputOverrides?.[coin];
    const outPath = override?.path ?? join(dataDir, "crypto", `${coin}.jsonl`);

    try {
      log?.(
        `Binance ${coin.toUpperCase()} fetch started (${new Date(
          startMs,
        ).toISOString()} -> ${new Date(endMs).toISOString()}).`,
      );

      let chunkStart = startMs;
      let append = override?.append ?? false;
      let totalTicks = 0;
      let pageCount = 0;

      while (chunkStart <= endMs) {
        const chunkEnd = Math.min(chunkStart + BINANCE_CHUNK_MS - 1, endMs);
        let fromId: number | null = null;
        let requestStart = chunkStart;
        let reachedChunkEnd = false;

        while (!reachedChunkEnd) {
          const params: Record<string, string> = {
            limit: String(BINANCE_LIMIT),
          };
          if (fromId !== null) {
            params.fromId = String(fromId);
          } else {
            params.startTime = String(requestStart);
            params.endTime = String(chunkEnd);
          }

          const trades = await fetchAggTrades(baseUrl, symbol, params, log);
          if (trades.length === 0) {
            break;
          }

          const ticks: BacktestCryptoTick[] = [];
          const windowEnd = Math.min(chunkEnd, endMs);
          let reachedEnd = false;
          for (const trade of trades) {
            if (trade.T > windowEnd) {
              reachedEnd = true;
              break;
            }
            if (trade.T < startMs) continue;
            const price = Number(trade.p);
            if (!Number.isFinite(price)) continue;
            ticks.push({
              symbol: outSymbol,
              timestamp: trade.T,
              value: price,
            });
          }

          if (ticks.length > 0) {
            writeJsonlLines(outPath, ticks, { append });
            append = true;
            totalTicks += ticks.length;
          }

          const last = trades[trades.length - 1];
          if (last && Number.isFinite(last.a)) {
            fromId = last.a + 1;
          } else {
            fromId = null;
            if (last && Number.isFinite(last.T)) {
              requestStart = last.T + 1;
            } else {
              log?.(
                `Binance ${coin.toUpperCase()} missing trade cursor; stopping chunk.`,
                "WARN",
              );
              break;
            }
          }

          if (reachedEnd || (last && Number.isFinite(last.T) && last.T >= chunkEnd)) {
            reachedChunkEnd = true;
          }

          pageCount += 1;
          if (pageCount % BINANCE_PROGRESS_EVERY === 0) {
            const lastTs = last?.T ?? requestStart;
            log?.(
              `Binance ${coin.toUpperCase()} progress: ${totalTicks} ticks (last ${new Date(
                lastTs,
              ).toISOString()}).`,
            );
          }

          await sleep(BINANCE_DELAY_MS);
          if (reachedEnd) break;
        }

        chunkStart = chunkEnd + 1;
      }

      if (totalTicks === 0) {
        throw new BinanceRequestError(
          `Binance ${coin.toUpperCase()} returned 0 ticks for the selected range.`,
          { isEmpty: true },
        );
      }

      log?.(
        `Binance ${coin.toUpperCase()} fetch complete (${totalTicks} ticks).`,
      );
    } catch (error) {
      const wrapped = wrapBinanceError(coin, error);
      const level: LogLevel =
        wrapped instanceof BinanceRequestError && wrapped.isBlocked
          ? "WARN"
          : "ERROR";
      log?.(wrapped.message, level);
      throw wrapped;
    }
  });
}

function buildBinanceVisionDailyUrl(symbol: string, dateStr: string): string {
  return joinUrl(
    BINANCE_VISION_BASE,
    `data/spot/daily/aggTrades/${symbol}/${symbol}-aggTrades-${dateStr}.zip`,
  );
}

function buildBinanceVisionMonthlyUrl(symbol: string, monthStr: string): string {
  return joinUrl(
    BINANCE_VISION_BASE,
    `data/spot/monthly/aggTrades/${symbol}/${symbol}-aggTrades-${monthStr}.zip`,
  );
}

async function streamBinanceVisionZip(
  url: string,
  outPath: string,
  outSymbol: string,
  startMs: number,
  endMs: number,
  append: boolean,
): Promise<{ append: boolean; ticks: number; lastTimestamp: number | null }> {
  const response = await fetchWithTimeout(url, BINANCE_VISION_TIMEOUT_MS);
  if (response.status === 404) {
    throw new BinanceVisionNotFoundError(url);
  }
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    const detail = text ? ` ${compactText(text)}` : "";
    throw new Error(`Binance Vision request failed (${response.status}).${detail}`);
  }
  if (!response.body) {
    throw new Error("Binance Vision response missing body.");
  }

  const zipStream = Readable.fromWeb(response.body as any);
  const parser = zipStream.pipe(unzipper.Parse({ forceStream: true }));
  let batch: BacktestCryptoTick[] = [];
  let totalTicks = 0;
  let lastTimestamp: number | null = null;
  let appendOutput = append;
  let processedEntry = false;

  for await (const entry of parser) {
    if (entry.type !== "File") {
      entry.autodrain();
      continue;
    }
    if (processedEntry) {
      entry.autodrain();
      continue;
    }

    processedEntry = true;
    const rl = readline.createInterface({ input: entry, crlfDelay: Infinity });
    for await (const line of rl) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const parts = trimmed.split(",");
      if (parts.length < 6) continue;
      const timestamp = resolveAggTradeTimestamp(parts, startMs, endMs);
      if (!timestamp) continue;
      if (timestamp < startMs || timestamp > endMs) continue;
      const price = Number(parts[1]);
      if (!Number.isFinite(price)) continue;
      batch.push({
        symbol: outSymbol,
        timestamp,
        value: price,
      });
      lastTimestamp = timestamp;

      if (batch.length >= BINANCE_VISION_BATCH_SIZE) {
        writeJsonlLines(outPath, batch, { append: appendOutput });
        appendOutput = true;
        totalTicks += batch.length;
        batch = [];
      }
    }
  }

  if (!processedEntry) {
    throw new Error("Binance Vision zip missing data entry.");
  }

  if (batch.length > 0) {
    writeJsonlLines(outPath, batch, { append: appendOutput });
    appendOutput = true;
    totalTicks += batch.length;
  }

  return { append: appendOutput, ticks: totalTicks, lastTimestamp };
}

async function fetchBinanceVisionZip(
  url: string,
  outPath: string,
  outSymbol: string,
  startMs: number,
  endMs: number,
  append: boolean,
  log?: LogFn,
): Promise<{ append: boolean; ticks: number; lastTimestamp: number | null }> {
  let attempt = 0;
  while (true) {
    try {
      return await streamBinanceVisionZip(
        url,
        outPath,
        outSymbol,
        startMs,
        endMs,
        append,
      );
    } catch (error) {
      if (error instanceof BinanceVisionNotFoundError) {
        throw error;
      }

      if (attempt < BINANCE_VISION_MAX_RETRIES) {
        const delayMs = computeBackoffMs(attempt, null);
        const message = error instanceof Error ? error.message : "Unknown error";
        log?.(
          `Binance Vision fetch error (${message}). Retrying in ${delayMs}ms.`,
          "WARN",
        );
        await sleep(delayMs);
        attempt += 1;
        continue;
      }
      throw error;
    }
  }
}

async function fetchBinanceVisionHistory(
  request: CryptoHistoryRequest,
): Promise<void> {
  const { coins, startMs, endMs, dataDir, log } = request;
  const concurrency = request.concurrency ?? BINANCE_CONCURRENCY;
  const visionMode = normalizeVisionMode(VISION_MODE_RAW);
  const nowMs = Date.now();
  const latestAvailableDay = getLatestVisionDailyAvailableDay(nowMs);
  const tailProviderId = normalizeProviderId(TAIL_PROVIDER_RAW);
  log?.("Crypto history provider: Binance Vision aggTrades archives", "INFO");
  log?.(`Binance Vision base: ${BINANCE_VISION_BASE}`, "INFO");
  log?.(`Binance Vision mode: ${visionMode}`, "INFO");

  await mapWithConcurrency(coins, concurrency, async (coin) => {
    const symbol = COIN_TO_SYMBOL[coin];
    if (!symbol) {
      throw new Error(`No Binance symbol configured for coin ${coin}.`);
    }
    const outSymbol = `${coin}/usd`;
    const outPath = join(dataDir, "crypto", `${coin}.jsonl`);
    const tempPath = join(dataDir, "crypto", `${coin}.vision.tmp`);
    let totalTicks = 0;
    let dayCount = 0;

    if (existsSync(tempPath)) {
      unlinkSync(tempPath);
    }

    log?.(
      `Binance Vision ${coin.toUpperCase()} fetch started (${new Date(
        startMs,
      ).toISOString()} -> ${new Date(endMs).toISOString()}).`,
    );

    const fetchDaily = async (): Promise<{
      totalTicks: number;
      missingDays: number[];
      unpublishedDays: number[];
      lastTickDayMs: number | null;
    }> => {
      const startDay = startOfUtcDay(startMs);
      const endDay = startOfUtcDay(endMs);
      let append = false;
      let ticks = 0;
      const missingDays: number[] = [];
      const unpublishedDays: number[] = [];
      let lastTickDayMs: number | null = null;
      dayCount = 0;

      for (let dayMs = startDay; dayMs <= endDay; dayMs += DAY_MS) {
        if (dayMs === endDay && endMs === endDay) {
          break;
        }
        const dateStr = formatUtcDate(dayMs);
        const url = buildBinanceVisionDailyUrl(symbol, dateStr);
        try {
          const result = await fetchBinanceVisionZip(
            url,
            tempPath,
            outSymbol,
            startMs,
            endMs,
            append,
            log,
          );
          append = result.append;
          if (result.ticks > 0) {
            ticks += result.ticks;
            lastTickDayMs = dayMs;
          } else if (dayMs > latestAvailableDay) {
            unpublishedDays.push(dayMs);
            log?.(
              `Binance Vision daily archive not yet published for ${dateStr} (${coin.toUpperCase()}).`,
              "WARN",
            );
          } else {
            missingDays.push(dayMs);
            log?.(
              `Binance Vision daily archive returned 0 ticks for ${dateStr} (${coin.toUpperCase()}).`,
              "WARN",
            );
          }
        } catch (error) {
          if (error instanceof BinanceVisionNotFoundError) {
            if (dayMs > latestAvailableDay) {
              unpublishedDays.push(dayMs);
              log?.(
                `Binance Vision daily archive not yet published for ${dateStr} (${coin.toUpperCase()}).`,
                "WARN",
              );
            } else {
              missingDays.push(dayMs);
              log?.(
                `Binance Vision daily archive missing for ${dateStr} (${coin.toUpperCase()}).`,
                "WARN",
              );
            }
          } else {
            const message =
              error instanceof Error
                ? error.message
                : "Binance Vision fetch failed.";
            log?.(
              `Binance Vision ${coin.toUpperCase()} ${dateStr} failed: ${message}`,
              "ERROR",
            );
            throw new Error(
              `Binance Vision ${coin.toUpperCase()} fetch failed: ${message}`,
            );
          }
        }

        dayCount += 1;
        if (dayCount % BINANCE_PROGRESS_EVERY === 0) {
          log?.(
            `Binance Vision ${coin.toUpperCase()} progress: ${ticks} ticks (${dateStr}).`,
          );
        }
      }

      return { totalTicks: ticks, missingDays, unpublishedDays, lastTickDayMs };
    };

    const fetchMonthly = async (): Promise<number> => {
      let append = false;
      let ticks = 0;
      dayCount = 0;
      const startMonth = startOfUtcMonth(startMs);
      const endMonth = startOfUtcMonth(endMs);
      for (
        let monthMs = startMonth;
        monthMs <= endMonth;
        monthMs = addUtcMonths(monthMs, 1)
      ) {
        if (monthMs === endMonth && endMs === endMonth) {
          break;
        }
        const monthStr = formatUtcMonth(monthMs);
        const url = buildBinanceVisionMonthlyUrl(symbol, monthStr);
        try {
          const result = await fetchBinanceVisionZip(
            url,
            tempPath,
            outSymbol,
            startMs,
            endMs,
            append,
            log,
          );
          append = result.append;
          ticks += result.ticks;
        } catch (error) {
          const message =
            error instanceof Error ? error.message : "Binance Vision fetch failed.";
          log?.(
            `Binance Vision ${coin.toUpperCase()} ${monthStr} failed: ${message}`,
            "ERROR",
          );
          throw new Error(
            `Binance Vision ${coin.toUpperCase()} fetch failed: ${message}`,
          );
        }

        dayCount += 1;
        if (dayCount % BINANCE_PROGRESS_EVERY === 0) {
          log?.(
            `Binance Vision ${coin.toUpperCase()} progress: ${ticks} ticks (${monthStr}).`,
          );
        }
      }
      return ticks;
    };

    try {
      let dailyResult:
        | {
            totalTicks: number;
            missingDays: number[];
            unpublishedDays: number[];
            lastTickDayMs: number | null;
          }
        | undefined;
      let usedMonthly = false;
      let tailFilled = false;

      if (visionMode === "monthly") {
        if (existsSync(tempPath)) {
          unlinkSync(tempPath);
        }
        totalTicks = await fetchMonthly();
        usedMonthly = true;
      } else {
        dailyResult = await fetchDaily();
        totalTicks = dailyResult.totalTicks;

        if (dailyResult.missingDays.length > 0) {
          const missingMonths = new Set(
            dailyResult.missingDays.map((dayMs) => startOfUtcMonth(dayMs)),
          );
          const missingMonthsUnavailable = Array.from(missingMonths).filter(
            (monthMs) => !isMonthlyArchiveAvailable(monthMs, nowMs),
          );
          if (visionMode === "daily") {
            const missingLabels = dailyResult.missingDays
              .map((dayMs) => formatUtcDate(dayMs))
              .join(", ");
            throw new Error(
              `Binance Vision daily archives missing for ${coin.toUpperCase()}: ${missingLabels}. Set BINANCE_VISION_MODE=monthly to retry.`,
            );
          }

          if (missingMonthsUnavailable.length > 0) {
            const unavailableLabels = missingMonthsUnavailable
              .map((monthMs) => formatUtcMonth(monthMs))
              .join(", ");
            throw new Error(
              `Binance Vision daily archives missing for ${coin.toUpperCase()} and monthly archives are not yet published for: ${unavailableLabels}. Set CRYPTO_HISTORY_TAIL_PROVIDER to fill the gap or adjust the end date.`,
            );
          }

          log?.(
            `Binance Vision daily coverage incomplete for ${coin.toUpperCase()} (missing ${
              dailyResult.missingDays.length
            } days). Switching to monthly archives.`,
            "WARN",
          );
          if (existsSync(tempPath)) {
            unlinkSync(tempPath);
          }
          totalTicks = await fetchMonthly();
          usedMonthly = true;
        }
      }

      if (!usedMonthly && dailyResult?.unpublishedDays.length) {
        const tailStartMs = Math.min(...dailyResult.unpublishedDays);
        const lastTickDayMs = dailyResult.lastTickDayMs;
        const tailIsContiguous =
          lastTickDayMs === null || tailStartMs >= lastTickDayMs;

        if (!tailIsContiguous) {
          const unpublishedLabels = dailyResult.unpublishedDays
            .map((dayMs) => formatUtcDate(dayMs))
            .join(", ");
          throw new Error(
            `Binance Vision daily archives missing for non-tail days (${unpublishedLabels}). Set BINANCE_VISION_MODE=monthly or adjust the date range.`,
          );
        }

        if (!tailProviderId || tailProviderId === "binance_vision") {
          const unpublishedLabels = dailyResult.unpublishedDays
            .map((dayMs) => formatUtcDate(dayMs))
            .join(", ");
          throw new Error(
            `Binance Vision daily archives not yet published for ${coin.toUpperCase()}: ${unpublishedLabels}. Set CRYPTO_HISTORY_TAIL_PROVIDER (binance or binance_us) to fill the latest days.`,
          );
        }

        const tailProvider = resolveProvider(tailProviderId);
        log?.(
          `Filling ${coin.toUpperCase()} tail data with ${tailProvider.label} (${formatUtcDate(
            tailStartMs,
          )} -> ${new Date(endMs).toISOString()}).`,
          "WARN",
        );

        await tailProvider.fetchHistory({
          coins: [coin],
          startMs: tailStartMs,
          endMs,
          dataDir,
          log,
          concurrency: 1,
          outputOverrides: {
            [coin]: { path: tempPath, append: true },
          },
        });
        tailFilled = true;
      }

      if (totalTicks === 0 && !tailFilled) {
        throw new Error(
          `Binance Vision ${coin.toUpperCase()} returned 0 ticks for the selected range.`,
        );
      }

      if (existsSync(outPath)) {
        unlinkSync(outPath);
      }
      renameSync(tempPath, outPath);

      log?.(
        `Binance Vision ${coin.toUpperCase()} fetch complete (${totalTicks} ticks).`,
      );
    } catch (error) {
      if (existsSync(tempPath)) {
        unlinkSync(tempPath);
      }
      throw error;
    }
  });
}

export async function fetchBinanceCryptoHistory(
  coins: CoinSymbol[],
  startMs: number,
  endMs: number,
  dataDir: string,
  options?: CryptoHistoryOptions,
): Promise<void> {
  const log = options?.log;
  const concurrency = options?.concurrency ?? BINANCE_CONCURRENCY;
  const { primary, fallback } = resolveProviderIds();
  const primaryProvider = resolveProvider(primary);

  const request: CryptoHistoryRequest = {
    coins,
    startMs,
    endMs,
    dataDir,
    log,
    concurrency,
  };

  try {
    await primaryProvider.fetchHistory(request);
  } catch (error) {
    if (fallback && shouldFallback(error)) {
      log?.(
        `Crypto history provider ${primaryProvider.id} blocked; switching to ${fallback}.`,
        "WARN",
      );
      const fallbackProvider = resolveProvider(fallback);
      await fallbackProvider.fetchHistory(request);
      return;
    }
    throw error;
  }
}
