import { join } from "path";
import {
  Chain,
  ClobClient,
} from "@polymarket/clob-client";
import {
  extractOutcomes,
  extractTokenIds,
  getEventBySlug,
  getMarketBySlug,
  getSeriesBySlug,
  type EventDetails,
  type MarketDetails,
} from "../services/market-service";
import { resolveMarketStartMs, parseUpDownSlugStartMs } from "../services/auto-market";
import type { CoinSymbol } from "../services/auto-market";
import type {
  BacktestMarketMeta,
  BacktestTradeEvent,
  BacktestTradeLevel,
  BacktestTradeSide,
} from "./types";
import { writeJsonlLines } from "./jsonl";
import { mapWithConcurrency } from "./concurrency";
import { sortTradesChronologically } from "./trade-utils";

function stripEnvValue(value: string | undefined): string | undefined {
  if (!value) return value;
  let trimmed = value.trim();
  if (!trimmed) return undefined;
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed[trimmed.length - 1];
    if ((first === "\"" && last === "\"") || (first === "'" && last === "'")) {
      trimmed = trimmed.slice(1, -1).trim();
    }
  }
  const lowered = trimmed.toLowerCase();
  if (lowered === "null" || lowered === "undefined") return undefined;
  return trimmed;
}

const MARKET_DURATION_MS = 15 * 60 * 1000;
const DEFAULT_TRADES_LIMIT = 500;
const DEFAULT_TRADES_DELAY_MS = 300;
const DEFAULT_TRADES_TIMEOUT_MS = 15000;
const DEFAULT_TRADES_RETRIES = 5;
const DATA_API_MAX_OFFSET = 10000;
const DATA_API_MAX_LIMIT = 10000;
const DEFAULT_TRADE_END_GRACE_MS = 60_000;
const DEFAULT_TRADE_START_GRACE_MS = 0;
const DEFAULT_MARKETS_LIMIT = 200;
const MAX_MARKETS_PAGES = 50;
const GAMMA_MARKETS_BASE = "https://gamma-api.polymarket.com/markets";

const DATA_API_BASE =
  stripEnvValue(process.env.POLYMARKET_DATA_API_BASE) ??
  "https://data-api.polymarket.com";
const TRADES_ENDPOINT =
  stripEnvValue(process.env.POLYMARKET_TRADES_ENDPOINT) ?? "/trades";
const CLOB_API_BASE =
  stripEnvValue(process.env.POLYMARKET_CLOB_API_BASE) ??
  "https://clob.polymarket.com";
const CLOB_CHAIN_RAW = Number(stripEnvValue(process.env.POLYMARKET_CLOB_CHAIN_ID));
const CLOB_CHAIN_ID = Number.isFinite(CLOB_CHAIN_RAW)
  ? Math.floor(CLOB_CHAIN_RAW)
  : Chain.POLYGON;
const CLOB_GEO_BLOCK_TOKEN = stripEnvValue(
  process.env.POLYMARKET_CLOB_GEO_BLOCK_TOKEN,
);
const CLOB_USE_SERVER_TIME =
  process.env.POLYMARKET_CLOB_USE_SERVER_TIME === "true";
const delayRaw = Number(process.env.POLYMARKET_TRADES_DELAY_MS);
const TRADES_DELAY_MS = Number.isFinite(delayRaw)
  ? delayRaw
  : DEFAULT_TRADES_DELAY_MS;
const limitRaw = Number(process.env.POLYMARKET_TRADES_LIMIT);
const TRADES_LIMIT = Number.isFinite(limitRaw)
  ? Math.max(1, Math.min(10000, Math.floor(limitRaw)))
  : DEFAULT_TRADES_LIMIT;
const timeoutRaw = Number(process.env.POLYMARKET_TRADES_TIMEOUT_MS);
const TRADES_TIMEOUT_MS = Number.isFinite(timeoutRaw)
  ? Math.max(1000, Math.floor(timeoutRaw))
  : DEFAULT_TRADES_TIMEOUT_MS;
const retriesRaw = Number(process.env.POLYMARKET_TRADES_RETRIES);
const TRADES_MAX_RETRIES = Number.isFinite(retriesRaw)
  ? Math.max(0, Math.floor(retriesRaw))
  : DEFAULT_TRADES_RETRIES;
const tradeEndGraceRaw = Number(process.env.POLYMARKET_TRADE_END_GRACE_MS);
const TRADE_END_GRACE_MS = Number.isFinite(tradeEndGraceRaw)
  ? Math.max(0, Math.floor(tradeEndGraceRaw))
  : DEFAULT_TRADE_END_GRACE_MS;
const tradeStartGraceRaw = Number(process.env.POLYMARKET_TRADE_START_GRACE_MS);
const TRADE_START_GRACE_MS = Number.isFinite(tradeStartGraceRaw)
  ? Math.max(0, Math.floor(tradeStartGraceRaw))
  : DEFAULT_TRADE_START_GRACE_MS;
const tradesProviderRaw = stripEnvValue(process.env.POLYMARKET_TRADES_PROVIDER);
const marketConcurrencyRaw = Number(process.env.BACKTEST_MARKET_CONCURRENCY);
const MARKET_CONCURRENCY = Number.isFinite(marketConcurrencyRaw)
  ? marketConcurrencyRaw
  : 2;
const eventConcurrencyRaw = Number(process.env.BACKTEST_EVENT_CONCURRENCY);
const EVENT_CONCURRENCY = Number.isFinite(eventConcurrencyRaw)
  ? eventConcurrencyRaw
  : 6;
const marketProgressRaw = Number(process.env.BACKTEST_MARKET_PROGRESS_EVERY);
const MARKET_PROGRESS_EVERY = Number.isFinite(marketProgressRaw)
  ? Math.max(1, Math.floor(marketProgressRaw))
  : 200;
const tradeConcurrencyRaw = Number(process.env.BACKTEST_TRADE_CONCURRENCY);
const TRADE_CONCURRENCY = Number.isFinite(tradeConcurrencyRaw)
  ? tradeConcurrencyRaw
  : 4;
const progressEveryRaw = Number(process.env.BACKTEST_PROGRESS_EVERY);
const PROGRESS_EVERY = Number.isFinite(progressEveryRaw)
  ? Math.max(1, Math.floor(progressEveryRaw))
  : 10;
const truncationCooldownRaw = Number(process.env.BACKTEST_TRUNCATION_COOLDOWN_MS);
const TRUNCATION_COOLDOWN_MS = Number.isFinite(truncationCooldownRaw)
  ? Math.max(500, Math.floor(truncationCooldownRaw))
  : 2000;
const truncationRetriesRaw = Number(process.env.BACKTEST_TRUNCATION_RETRIES);
const TRUNCATION_MAX_RETRIES = Number.isFinite(truncationRetriesRaw)
  ? Math.max(0, Math.floor(truncationRetriesRaw))
  : 1;

type LogLevel = "INFO" | "WARN" | "ERROR";
type LogFn = (message: string, level?: LogLevel) => void;
type TradesProvider = "auto" | "data-api" | "clob";

export interface TradesFetchResult {
  emptyMarkets: string[];
  truncatedMarkets: string[];
  statsBySlug: Map<string, { count: number; minTs: number; maxTs: number }>;
}

let clobEmptyFallbackLogged = false;
let clobMarketTradesUnavailable = false;
let clobMarketTradesUnavailableLogged = false;
let publicClobClient: ClobClient | null = null;

/**
 * Get a public (unauthenticated) CLOB client for fetching market trade events.
 * The getMarketTradesEvents endpoint is public and doesn't require authentication.
 */
function getPublicClobClient(): ClobClient {
  if (!publicClobClient) {
    const chainId = CLOB_CHAIN_ID as Chain;
    // Create client without signer/creds - only for public methods
    publicClobClient = new ClobClient(
      CLOB_API_BASE,
      chainId,
      undefined, // no signer needed for public methods
      undefined, // no creds needed for public methods
      undefined, // signature type
      undefined, // funder address
      CLOB_GEO_BLOCK_TOKEN,
      CLOB_USE_SERVER_TIME,
    );
  }
  return publicClobClient;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function computeBackoffMs(attempt: number, retryAfterMs: number | null): number {
  if (retryAfterMs !== null) {
    return retryAfterMs;
  }
  const base = Math.max(1, TRADES_DELAY_MS) * Math.pow(2, attempt);
  const jitter = Math.floor(base * 0.2 * Math.random());
  return base + jitter;
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

function parseTimestamp(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    if (value >= 1e17) return Math.floor(value / 1_000_000);
    if (value >= 1e14) return Math.floor(value / 1_000);
    return value < 1e12 ? value * 1000 : value;
  }
  if (typeof value === "string") {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      if (numeric >= 1e17) return Math.floor(numeric / 1_000_000);
      if (numeric >= 1e14) return Math.floor(numeric / 1_000);
      return numeric < 1e12 ? numeric * 1000 : numeric;
    }
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return null;
}

function normalizeSideValue(value: unknown): BacktestTradeSide | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return null;
  if (normalized === "buy" || normalized === "bid") return "BUY";
  if (normalized === "sell" || normalized === "ask") return "SELL";
  if (normalized.startsWith("buy")) return "BUY";
  if (normalized.startsWith("sell")) return "SELL";
  return null;
}

function parseNumberValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function extractTradeSide(
  raw: Record<string, unknown>,
): BacktestTradeSide | null {
  const candidates = [
    raw.side,
    raw.tradeSide,
    raw.trade_side,
    raw.takerSide,
    raw.taker_side,
    raw.makerSide,
    raw.maker_side,
    raw.action,
    raw.direction,
  ];

  for (const candidate of candidates) {
    const side = normalizeSideValue(candidate);
    if (side) return side;
  }

  return null;
}

function resolveTokenId(
  raw: Record<string, unknown>,
  validTokenIds: Set<string>,
  tokens?: { upTokenId: string; downTokenId: string },
): string | null {
  let tokenId =
    (raw.tokenId as string | undefined) ??
    (raw.token_id as string | undefined) ??
    (raw.asset as string | undefined) ??
    (raw.assetId as string | undefined) ??
    (raw.asset_id as string | undefined) ??
    (raw.outcomeTokenId as string | undefined) ??
    (raw.outcome_token_id as string | undefined);

  if ((!tokenId || !validTokenIds.has(tokenId)) && tokens) {
    const outcomeRaw =
      (raw.outcome as string | undefined) ??
      (raw.outcomeName as string | undefined) ??
      (raw.outcome_name as string | undefined);
    const outcomeIndexRaw =
      (raw.outcomeIndex as number | string | undefined) ??
      (raw.outcome_index as number | string | undefined);

    if (typeof outcomeRaw === "string") {
      const normalized = outcomeRaw.trim().toLowerCase();
      if (normalized === "up" || normalized === "yes") {
        tokenId = tokens.upTokenId;
      } else if (normalized === "down" || normalized === "no") {
        tokenId = tokens.downTokenId;
      }
    } else if (outcomeIndexRaw !== undefined) {
      const index =
        typeof outcomeIndexRaw === "string"
          ? Number(outcomeIndexRaw)
          : outcomeIndexRaw;
      if (index === 0) {
        tokenId = tokens.upTokenId;
      } else if (index === 1) {
        tokenId = tokens.downTokenId;
      }
    }
  }

  if (!tokenId || !validTokenIds.has(tokenId)) {
    return null;
  }

  return tokenId;
}

function extractMakerOrders(
  raw: Record<string, unknown>,
  validTokenIds: Set<string>,
  tokens: { upTokenId: string; downTokenId: string } | undefined,
  tradeSide: BacktestTradeSide | null,
  fallbackTokenId: string,
): BacktestTradeLevel[] {
  const makerRaw =
    (raw.maker_orders as unknown[] | undefined) ??
    (raw.makerOrders as unknown[] | undefined);

  if (!Array.isArray(makerRaw)) return [];

  const fallbackSide =
    tradeSide === "BUY" ? "SELL" : tradeSide === "SELL" ? "BUY" : null;
  const levels: BacktestTradeLevel[] = [];

  for (const entry of makerRaw) {
    if (!entry || typeof entry !== "object") continue;
    const item = entry as Record<string, unknown>;
    const price = parseNumberValue(item.price);
    const size =
      parseNumberValue(item.matched_amount) ??
      parseNumberValue(item.matchedAmount) ??
      parseNumberValue(item.size) ??
      parseNumberValue(item.amount);
    if (price === null || size === null) continue;

    const side = normalizeSideValue(item.side) ?? fallbackSide;
    if (!side) continue;

    const tokenId =
      resolveTokenId(item, validTokenIds, tokens) ?? fallbackTokenId;
    if (!tokenId || !validTokenIds.has(tokenId)) continue;

    levels.push({ price, size, side, tokenId });
  }

  return levels;
}

function extractMarkets(payload: unknown): MarketDetails[] {
  if (Array.isArray(payload)) return payload as MarketDetails[];
  if (payload && typeof payload === "object") {
    const record = payload as Record<string, unknown>;
    if (Array.isArray(record.markets)) {
      return record.markets as MarketDetails[];
    }
    if (Array.isArray(record.data)) {
      return record.data as MarketDetails[];
    }
    if (Array.isArray(record.results)) {
      return record.results as MarketDetails[];
    }
  }
  return [];
}

function resolveEventWindow(event: EventDetails): {
  startMs: number | null;
  endMs: number | null;
  isFallbackStart: boolean;
} {
  const startTime =
    parseTimestamp(event.startTime) ??
    (event.slug ? parseUpDownSlugStartMs(event.slug) : null);
  const fallbackStart = parseTimestamp(event.startDate);
  const start = startTime ?? fallbackStart;
  if (!start) {
    return { startMs: null, endMs: null, isFallbackStart: false };
  }
  const end =
    parseTimestamp(event.endDate) ??
    (start ? start + MARKET_DURATION_MS : null);
  return {
    startMs: start,
    endMs: end,
    isFallbackStart: startTime === null && fallbackStart !== null,
  };
}

function isUpDown15mSlug(slug: string | undefined, coin: CoinSymbol): boolean {
  if (!slug) return false;
  const pattern = new RegExp(
    `^${coin}-(?:updown|up-or-down)-15m-\\d{9,13}`,
    "i",
  );
  return pattern.test(slug);
}

function normalizeTrade(
  raw: Record<string, unknown>,
  validTokenIds: Set<string>,
  tokens?: { upTokenId: string; downTokenId: string },
): BacktestTradeEvent | null {
  const timestamp =
    parseTimestamp(raw.timestamp) ??
    parseTimestamp(raw.time) ??
    parseTimestamp(raw.createdAt) ??
    parseTimestamp(raw.created_at) ??
    parseTimestamp(raw.takerTimestamp) ??
    parseTimestamp(raw.blockTimestamp) ??
    parseTimestamp(raw.match_time) ??
    parseTimestamp(raw.last_update) ??
    parseTimestamp(raw.lastUpdate);

  const tokenId = resolveTokenId(raw, validTokenIds, tokens);

  const priceRaw =
    (raw.price as number | string | undefined) ??
    (raw.pricePerShare as number | string | undefined) ??
    (raw.price_per_share as number | string | undefined);

  const sizeRaw =
    (raw.size as number | string | undefined) ??
    (raw.quantity as number | string | undefined) ??
    (raw.amount as number | string | undefined) ??
    (raw.shares as number | string | undefined);

  if (!timestamp || !tokenId) return null;

  const priceValue = Number(priceRaw);
  const sizeValue = Number(sizeRaw);
  if (!Number.isFinite(priceValue) || !Number.isFinite(sizeValue)) return null;

  const side = extractTradeSide(raw);
  const tradeId =
    (raw.id as string | undefined) ??
    (raw.tradeId as string | undefined) ??
    (raw.trade_id as string | undefined);
  const takerOrderId =
    (raw.taker_order_id as string | undefined) ??
    (raw.takerOrderId as string | undefined) ??
    (raw.takerOrderID as string | undefined) ??
    (raw.taker_orderID as string | undefined);
  const bucketIndexRaw =
    (raw.bucket_index as number | string | undefined) ??
    (raw.bucketIndex as number | string | undefined);
  const bucketIndexValue = parseNumberValue(bucketIndexRaw);
  const bucketIndex =
    bucketIndexValue === null
      ? undefined
      : Math.max(0, Math.floor(bucketIndexValue));

  const makerOrders = extractMakerOrders(
    raw,
    validTokenIds,
    tokens,
    side,
    tokenId,
  );

  const trade: BacktestTradeEvent = {
    timestamp,
    tokenId,
    price: priceValue,
    size: sizeValue,
  };
  if (side) {
    trade.side = side;
  }
  if (tradeId) {
    trade.tradeId = tradeId;
  }
  if (takerOrderId) {
    trade.takerOrderId = takerOrderId;
  }
  if (bucketIndex !== undefined) {
    trade.bucketIndex = bucketIndex;
  }
  if (makerOrders.length > 0) {
    trade.makerOrders = makerOrders;
  }
  return trade;
}

function resolveUpDownTokens(market: MarketDetails): {
  upTokenId: string;
  downTokenId: string;
} | null {
  const outcomes = extractOutcomes(market);
  const tokenIds = extractTokenIds(market);
  if (outcomes.length < 2 || tokenIds.length < 2) return null;

  const lower = outcomes.map((o) => o.toLowerCase());
  const upIndex = lower.indexOf("up");
  const downIndex = lower.indexOf("down");
  if (upIndex === -1 || downIndex === -1) return null;

  const upTokenId = tokenIds[upIndex];
  const downTokenId = tokenIds[downIndex];
  if (!upTokenId || !downTokenId) return null;

  return { upTokenId, downTokenId };
}

export async function fetchMarketsForRange(
  coins: CoinSymbol[],
  startMs: number,
  endMs: number,
  dataDir: string,
  options?: {
    log?: LogFn;
    concurrency?: number;
  },
): Promise<BacktestMarketMeta[]> {
  const marketsMap = new Map<string, BacktestMarketMeta>();
  const log = options?.log;
  const concurrency = options?.concurrency ?? MARKET_CONCURRENCY;

  const coinResults = await mapWithConcurrency(
    coins,
    concurrency,
    async (coin) => {
      try {
        log?.(`Fetching ${coin.toUpperCase()} markets...`);
        const coinMarkets: BacktestMarketMeta[] = [];
        const seriesSlugs = [
          `${coin}-up-or-down-15m`,
          `${coin}-updown-15m`,
        ];
        const seenEvents = new Set<string>();

        const seriesResults = await Promise.all(
          seriesSlugs.map(async (seriesSlug) => ({
            seriesSlug,
            series: await getSeriesBySlug(seriesSlug),
          })),
        );

        const allEvents: EventDetails[] = [];
        for (const { series } of seriesResults) {
          const events = series?.events ?? [];
          for (const event of events) {
            if (!event) continue;
            const eventKey = event.slug || event.id;
            if (eventKey && seenEvents.has(eventKey)) continue;
            if (eventKey) {
              seenEvents.add(eventKey);
            }
            allEvents.push(event);
          }
        }

        log?.(
          `Processing ${allEvents.length} ${coin.toUpperCase()} events...`,
        );

        let matchedMarkets = 0;
        const eventResults = await mapWithConcurrency(
          allEvents,
          EVENT_CONCURRENCY,
          async (event) => {
            let eventDetails: EventDetails = event;
            let {
              startMs: eventStart,
              endMs: eventEnd,
              isFallbackStart,
            } = resolveEventWindow(eventDetails);

            if (
              !isFallbackStart &&
              eventStart !== null &&
              (eventStart < startMs || eventStart > endMs)
            ) {
              return [];
            }

            let eventMarkets = Array.isArray(eventDetails.markets)
              ? [...eventDetails.markets]
              : [];

            if (eventMarkets.length === 0 && eventDetails.slug) {
              const fetchedMarket = await getMarketBySlug(eventDetails.slug);
              if (fetchedMarket) {
                eventMarkets = [fetchedMarket];
              }
            }

            if (eventMarkets.length === 0 && eventDetails.slug) {
              const fetched = await getEventBySlug(eventDetails.slug);
              if (fetched) {
                eventDetails = fetched;
                const resolved = resolveEventWindow(eventDetails);
                if (resolved.startMs) {
                  eventStart = resolved.startMs;
                }
                if (resolved.endMs) {
                  eventEnd = resolved.endMs;
                }
                if (resolved.isFallbackStart) {
                  isFallbackStart = true;
                }
                eventMarkets = Array.isArray(eventDetails.markets)
                  ? [...eventDetails.markets]
                  : [];
              }
            }

            const metas: BacktestMarketMeta[] = [];

            for (const market of eventMarkets) {
              if (!market.slug) continue;
              let marketDetails: MarketDetails | null = market;
              let tokens = resolveUpDownTokens(marketDetails);
              if (!tokens) {
                marketDetails = await getMarketBySlug(market.slug);
                if (!marketDetails) continue;
                tokens = resolveUpDownTokens(marketDetails);
              }
              if (!tokens || !marketDetails) continue;

              const marketStart =
                resolveMarketStartMs(marketDetails) ?? eventStart;
              if (!marketStart) continue;
              if (marketStart < startMs || marketStart > endMs) continue;
              const marketEnd =
                parseTimestamp(marketDetails.endDate) ??
                eventEnd ??
                marketStart + MARKET_DURATION_MS;

              metas.push({
                slug: marketDetails.slug,
                coin,
                marketName:
                  marketDetails.question ||
                  eventDetails.title ||
                  marketDetails.slug,
                marketId: marketDetails.id,
                conditionId: marketDetails.conditionId,
                startMs: marketStart,
                endMs: marketEnd,
                upTokenId: tokens.upTokenId,
                downTokenId: tokens.downTokenId,
              });
            }

            matchedMarkets += metas.length;
            return metas;
          },
          (completed, total) => {
            if (
              completed % MARKET_PROGRESS_EVERY === 0 ||
              completed === total
            ) {
              log?.(
                `${coin.toUpperCase()} progress: ${completed}/${total} events processed, ${matchedMarkets} markets matched.`,
              );
            }
          },
        );

        for (const metas of eventResults) {
          coinMarkets.push(...metas);
        }

        if (coinMarkets.length === 0) {
          log?.(
            `No ${coin.toUpperCase()} markets found via series; running fallback search.`,
            "WARN",
          );
          const fallback = await fetchMarketsByQuery(coin, startMs, endMs);
          coinMarkets.push(...fallback);
        }

        log?.(
          `Found ${coinMarkets.length} ${coin.toUpperCase()} markets in range.`,
        );
        return { coin, markets: coinMarkets };
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Market fetch failed.";
        log?.(
          `${coin.toUpperCase()} market fetch failed: ${message}`,
          "ERROR",
        );
        return { coin, markets: [] };
      }
    },
  );

  for (const result of coinResults) {
    for (const market of result.markets) {
      if (!marketsMap.has(market.slug)) {
        marketsMap.set(market.slug, market);
      }
    }
  }

  const markets = Array.from(marketsMap.values());
  if (markets.length > 0) {
    markets.sort((a, b) => a.startMs - b.startMs);
    const marketsPath = join(dataDir, "markets.jsonl");
    writeJsonlLines(marketsPath, markets);
  }

  return markets;
}

async function fetchMarketsByQuery(
  coin: CoinSymbol,
  startMs: number,
  endMs: number,
): Promise<BacktestMarketMeta[]> {
  const queries = [
    `${coin}-updown-15m`,
    `${coin} updown 15m`,
    `${coin} up or down 15m`,
  ];
  const closedModes: Array<boolean | undefined> = [true, false, undefined];
  const results: BacktestMarketMeta[] = [];
  const seen = new Set<string>();

  for (const query of queries) {
    for (const closed of closedModes) {
      const markets = await fetchMarketQueryPages(query, closed);
      if (markets.length === 0) continue;

      for (const market of markets) {
        if (!market.slug || !isUpDown15mSlug(market.slug, coin)) continue;
        if (seen.has(market.slug)) continue;

        let marketDetails: MarketDetails | null = market;
        let tokens = resolveUpDownTokens(marketDetails);
        if (!tokens) {
          marketDetails = await getMarketBySlug(market.slug);
          if (!marketDetails) continue;
          tokens = resolveUpDownTokens(marketDetails);
        }
        if (!tokens || !marketDetails) continue;

        const marketStart = resolveMarketStartMs(marketDetails);
        if (!marketStart) continue;
        if (marketStart < startMs || marketStart > endMs) continue;
        const marketEnd =
          parseTimestamp(marketDetails.endDate) ??
          marketStart + MARKET_DURATION_MS;

        results.push({
          slug: marketDetails.slug,
          coin,
          marketName: marketDetails.question || marketDetails.slug,
          marketId: marketDetails.id,
          conditionId: marketDetails.conditionId,
          startMs: marketStart,
          endMs: marketEnd,
          upTokenId: tokens.upTokenId,
          downTokenId: tokens.downTokenId,
        });
        seen.add(marketDetails.slug);
      }
    }
  }

  return results;
}

async function fetchMarketQueryPages(
  query: string,
  closed: boolean | undefined,
): Promise<MarketDetails[]> {
  const results: MarketDetails[] = [];
  let offset = 0;

  for (let page = 0; page < MAX_MARKETS_PAGES; page += 1) {
    const url = new URL(GAMMA_MARKETS_BASE);
    url.searchParams.set("query", query);
    url.searchParams.set("limit", String(DEFAULT_MARKETS_LIMIT));
    url.searchParams.set("offset", String(offset));
    if (closed !== undefined) {
      url.searchParams.set("closed", String(closed));
    }

    const response = await fetch(url.toString());
    if (!response.ok) break;
    const data = await response.json();
    const markets = extractMarkets(data);
    if (markets.length === 0) break;

    results.push(...markets);
    if (markets.length < DEFAULT_MARKETS_LIMIT) break;
    offset += markets.length;
  }

  return results;
}

async function fetchDataApiTradesPage(
  marketIdValue: string,
  limit: number,
  offset: number,
  paramName: string,
  log?: LogFn,
  extraParams?: Record<string, string | number | boolean | undefined>,
): Promise<unknown[]> {
  const url = new URL(`${DATA_API_BASE}${TRADES_ENDPOINT}`);
  url.searchParams.set(paramName, marketIdValue);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  if (extraParams) {
    for (const [key, value] of Object.entries(extraParams)) {
      if (value === undefined || value === null) continue;
      url.searchParams.set(key, String(value));
    }
  }

  let attempt = 0;
  while (true) {
    const response = await fetchWithTimeout(url.toString(), TRADES_TIMEOUT_MS);
    if (response.ok) {
      const data = (await response.json()) as unknown;
      if (Array.isArray(data)) return data;
      if (data && typeof data === "object") {
        const record = data as Record<string, unknown>;
        if (Array.isArray(record.trades)) {
          return record.trades as unknown[];
        }
      }
      return [];
    }

    const status = response.status;
    const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
    if ((status === 429 || status >= 500) && attempt < TRADES_MAX_RETRIES) {
      const delayMs = computeBackoffMs(attempt, retryAfterMs);
      log?.(
        `Trade fetch retry (${status}) for ${url.toString()} in ${delayMs}ms.`,
        "WARN",
      );
      await sleep(delayMs);
      attempt += 1;
      continue;
    }

    throw new Error(`Trade fetch failed (${status}) ${url.toString()}`);
  }
}

function normalizeTrades(
  items: unknown[],
  validTokenIds: Set<string>,
  tokenPair: { upTokenId: string; downTokenId: string },
  tradeWindowStart: number,
  tradeWindowEnd: number,
): BacktestTradeEvent[] {
  const normalized: BacktestTradeEvent[] = [];
  for (const item of items) {
    if (!item || typeof item !== "object") continue;
    const trade = normalizeTrade(
      item as Record<string, unknown>,
      validTokenIds,
      tokenPair,
    );
    if (!trade) continue;
    if (trade.timestamp < tradeWindowStart || trade.timestamp > tradeWindowEnd) {
      continue;
    }
    normalized.push(trade);
  }
  return normalized;
}

async function fetchDataApiTradesAllPages(
  marketIdValue: string,
  paramName: string,
  limit: number,
  log?: LogFn,
  extraParams?: Record<string, string | number | boolean | undefined>,
): Promise<{ trades: unknown[]; hitOffsetLimit: boolean }> {
  const allTrades: unknown[] = [];
  const safeLimit = Math.max(1, Math.min(DATA_API_MAX_LIMIT, Math.floor(limit)));
  let offset = 0;
  let hitOffsetLimit = false;

  while (true) {
    let page: unknown[];
    try {
      page = await fetchDataApiTradesPage(
        marketIdValue,
        safeLimit,
        offset,
        paramName,
        log,
        extraParams,
      );
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Trade fetch failed.";
      log?.(`Trade fetch failed for ${paramName}=${marketIdValue}: ${message}`, "WARN");
      break;
    }

    if (page.length === 0) break;
    allTrades.push(...page);
    offset += page.length;

    if (offset >= DATA_API_MAX_OFFSET) {
      hitOffsetLimit = true;
      break;
    }

    await sleep(TRADES_DELAY_MS);
  }

  return { trades: allTrades, hitOffsetLimit };
}

/**
 * Fetch public market trade events using getMarketTradesEvents.
 * NOTE: This endpoint (/live-activity/events) only works for RECENT markets.
 * For historical backtesting, it often returns 404 for older markets.
 * The data-api endpoint should be preferred for historical data.
 */
async function fetchClobMarketTradesEvents(
  conditionId: string,
  afterMs: number,
  beforeMs: number,
  log?: LogFn,
): Promise<unknown[]> {
  const client = getPublicClobClient();

  let attempt = 0;
  while (true) {
    try {
      // getMarketTradesEvents is a PUBLIC method - no authentication needed!
      // However, it uses /live-activity/events which only works for recent markets
      const response = await client.getMarketTradesEvents(conditionId);
      
      // Handle non-array responses (404s return undefined or error objects)
      if (!Array.isArray(response)) {
        // The live-activity endpoint doesn't have data for this market
        // This is expected for historical/older markets
        return [];
      }
      
      // Filter trades by timestamp since the API doesn't support time params
      const filteredTrades = response.filter((trade: unknown) => {
        if (!trade || typeof trade !== "object") return false;
        const record = trade as Record<string, unknown>;
        const timestamp = parseTimestamp(record.timestamp);
        if (!timestamp) return true; // Include trades without timestamp for normalization to handle
        return timestamp >= afterMs && timestamp <= beforeMs;
      });

      return filteredTrades;
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "CLOB market trades fetch failed.";
      
      // Check if this is a 404 or client-side error (expected for old markets)
      const errorStr = String(error);
      if (errorStr.includes("404") || errorStr.includes("Not Found") || errorStr.includes("is not a function")) {
        // Live-activity endpoint doesn't have data for this market - return empty
        return [];
      }
      
      if (attempt < TRADES_MAX_RETRIES) {
        const delayMs = computeBackoffMs(attempt, null);
        log?.(`CLOB market trades retry (${message}) in ${delayMs}ms.`, "WARN");
        await sleep(delayMs);
        attempt += 1;
        continue;
      }
      // For historical data, we expect failures - return empty instead of throwing
      return [];
    }
  }
}

interface MarketTradeResult {
  written: number;
  truncated: boolean;
  minTs: number | null;
  maxTs: number | null;
}

async function fetchTradesForMarket(
  market: BacktestMarketMeta,
  dataDir: string,
  startMs: number,
  endMs: number,
  log?: LogFn,
): Promise<MarketTradeResult> {
  const tokenPair = { upTokenId: market.upTokenId, downTokenId: market.downTokenId };
  const validTokenIds = new Set([market.upTokenId, market.downTokenId]);
  const tradeWindowStart = Math.max(0, market.startMs - TRADE_START_GRACE_MS);
  const tradeWindowEnd = market.endMs + TRADE_END_GRACE_MS;
  const explicitProvider: TradesProvider =
    tradesProviderRaw?.toLowerCase() === "clob"
      ? "clob"
      : tradesProviderRaw?.toLowerCase() === "data-api"
        ? "data-api"
        : "auto";
  const preferClob = explicitProvider === "clob";
  let clobReturnedEmpty = false;

  let conditionId = market.conditionId;
  if (!conditionId && market.slug) {
    const refreshed = await getMarketBySlug(market.slug);
    if (refreshed?.conditionId) {
      conditionId = refreshed.conditionId;
    }
  }

  if (preferClob) {
    if (clobMarketTradesUnavailable) {
      clobReturnedEmpty = true;
    }
    // Note: getMarketTradesEvents is a PUBLIC endpoint - no credentials needed!
    if (!conditionId) {
      log?.(
        `CLOB trade fetch skipped for ${market.slug}: missing conditionId.`,
        "WARN",
      );
      clobReturnedEmpty = true;
    } else {
      try {
        const clobTrades = await fetchClobMarketTradesEvents(
          conditionId,
          tradeWindowStart,
          tradeWindowEnd,
          log,
        );
        const normalized: BacktestTradeEvent[] = [];
        for (const item of clobTrades) {
          if (!item || typeof item !== "object") continue;
          const trade = normalizeTrade(
            item as Record<string, unknown>,
            validTokenIds,
            tokenPair,
          );
          if (!trade) continue;
          if (
            trade.timestamp < tradeWindowStart ||
            trade.timestamp > tradeWindowEnd
          )
            continue;
          normalized.push(trade);
        }
        if (normalized.length > 0) {
          const tradePath = join(dataDir, "trades", `${market.slug}.jsonl`);
          const sorted = sortTradesChronologically(normalized, market.slug);
          writeJsonlLines(tradePath, sorted, { append: false });
          const minTs = sorted.length > 0 ? sorted[0]!.timestamp : null;
          const maxTs =
            sorted.length > 0 ? sorted[sorted.length - 1]!.timestamp : null;
          return { written: sorted.length, truncated: false, minTs, maxTs };
        }
        clobReturnedEmpty = true;
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "CLOB trade fetch failed.";
        log?.(`CLOB trade fetch failed for ${market.slug}: ${message}`, "WARN");
        clobReturnedEmpty = true;
      }
    }
  }

  if (preferClob && clobReturnedEmpty && !clobEmptyFallbackLogged) {
    log?.(
      "CLOB trade fetch returned no data; falling back to data-api trade history where available.",
      "WARN",
    );
    clobEmptyFallbackLogged = true;
  }

  const paramCandidates: Array<{ name: string; ids: string[] }> = conditionId
    ? [{ name: "market", ids: [conditionId] }]
    : market.slug
      ? [{ name: "market", ids: [market.slug] }]
      : [];

  if (!conditionId) {
    log?.(
      `Trade lookup missing conditionId for ${market.slug}; falling back to slug which may be unreliable.`,
      "WARN",
    );
  }

  let totalWritten = 0;
  let usedParam = paramCandidates[0]?.name ?? "market";
  let usedId = paramCandidates[0]?.ids[0] ?? market.slug;
  let identifierResolved = false;

  outer: for (const param of paramCandidates) {
    for (const idValue of param.ids) {
      try {
        const firstPage = await fetchDataApiTradesPage(
          idValue,
          TRADES_LIMIT,
          0,
          param.name,
          log,
        );
        if (firstPage.length === 0) {
          continue;
        }
        usedParam = param.name;
        usedId = idValue;
        identifierResolved = true;
        break outer;
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Trade lookup failed.";
        log?.(
          `Trade lookup failed for ${market.slug} (${param.name}=${idValue}): ${message}`,
          "WARN",
        );
        continue;
      }
    }
  }

  if (!identifierResolved) {
    log?.(
      `No trade identifier returned data for ${market.slug}; defaulting to ${usedParam}=${usedId}.`,
      "WARN",
    );
  }

  const { trades: rawTrades, hitOffsetLimit } =
    await fetchDataApiTradesAllPages(
      usedId,
      usedParam,
      TRADES_LIMIT,
      log,
    );
  let normalized = normalizeTrades(
    rawTrades,
    validTokenIds,
    tokenPair,
    tradeWindowStart,
    tradeWindowEnd,
  );

  if (rawTrades.length > 0 && normalized.length === 0) {
    log?.(
      `Trade page returned data but no matching tokens for ${market.slug}; check asset/outcome mapping.`,
      "WARN",
    );
  }

  let isTruncated = false;

  if (hitOffsetLimit) {
    log?.(
      `Trade pagination hit offset limit for ${market.slug}; retrying with side-split.`,
      "WARN",
    );
    const buyResult = await fetchDataApiTradesAllPages(
      usedId,
      usedParam,
      DATA_API_MAX_LIMIT,
      log,
      { side: "BUY" },
    );
    const sellResult = await fetchDataApiTradesAllPages(
      usedId,
      usedParam,
      DATA_API_MAX_LIMIT,
      log,
      { side: "SELL" },
    );

    const mergedRaw = [...buyResult.trades, ...sellResult.trades];
    normalized = normalizeTrades(
      mergedRaw,
      validTokenIds,
      tokenPair,
      tradeWindowStart,
      tradeWindowEnd,
    );

    // If still truncated after side-split, attempt retry-with-cooldown
    if (buyResult.hitOffsetLimit || sellResult.hitOffsetLimit) {
      let stillTruncated = true;

      for (let attempt = 0; attempt < TRUNCATION_MAX_RETRIES && stillTruncated; attempt++) {
        const cooldownMs = TRUNCATION_COOLDOWN_MS * Math.pow(1.5, attempt);
        log?.(
          `Truncation retry ${attempt + 1}/${TRUNCATION_MAX_RETRIES} for ${market.slug} after ${cooldownMs}ms cooldown.`,
          "WARN",
        );
        await sleep(cooldownMs);

        const retryBuy = await fetchDataApiTradesAllPages(
          usedId,
          usedParam,
          DATA_API_MAX_LIMIT,
          log,
          { side: "BUY" },
        );
        const retrySell = await fetchDataApiTradesAllPages(
          usedId,
          usedParam,
          DATA_API_MAX_LIMIT,
          log,
          { side: "SELL" },
        );

        const retryMerged = [...retryBuy.trades, ...retrySell.trades];
        const retryNormalized = normalizeTrades(
          retryMerged,
          validTokenIds,
          tokenPair,
          tradeWindowStart,
          tradeWindowEnd,
        );

        // Use retry results if they produced more trades
        if (retryNormalized.length > normalized.length) {
          normalized = retryNormalized;
        }

        stillTruncated = retryBuy.hitOffsetLimit || retrySell.hitOffsetLimit;
      }

      if (stillTruncated) {
        isTruncated = true;
        log?.(
          `Market ${market.slug} truncated after ${TRUNCATION_MAX_RETRIES} retries; ${normalized.length} trades recovered.`,
          "WARN",
        );
      }
    }
  }

  let sorted: BacktestTradeEvent[] | null = null;
  if (normalized.length > 0) {
    const tradePath = join(dataDir, "trades", `${market.slug}.jsonl`);
    sorted = sortTradesChronologically(normalized, market.slug);
    writeJsonlLines(tradePath, sorted, { append: false });
    totalWritten += sorted.length;
  }

  if (
    preferClob &&
    clobReturnedEmpty &&
    totalWritten > 0 &&
    !clobMarketTradesUnavailableLogged
  ) {
    log?.(
      "CLOB trade endpoint returned no public trades; using data-api trade history for the remaining markets.",
      "WARN",
    );
    clobMarketTradesUnavailable = true;
    clobMarketTradesUnavailableLogged = true;
  }

  // NOTE: The CLOB /live-activity/events endpoint only works for RECENT/LIVE markets.
  // For HISTORICAL backtesting, the data-api is the correct source.
  // We don't fall back to CLOB since it returns 404 for historical markets.

  const minTs =
    sorted && sorted.length > 0 ? sorted[0]!.timestamp : null;
  const maxTs =
    sorted && sorted.length > 0 ? sorted[sorted.length - 1]!.timestamp : null;
  return { written: totalWritten, truncated: isTruncated, minTs, maxTs };
}

export async function fetchTradesForMarkets(
  markets: BacktestMarketMeta[],
  dataDir: string,
  startMs: number,
  endMs: number,
  options?: {
    log?: LogFn;
    concurrency?: number;
    progressEvery?: number;
  },
): Promise<TradesFetchResult> {
  const log = options?.log;
  const concurrency = options?.concurrency ?? TRADE_CONCURRENCY;
  const progressEvery = options?.progressEvery ?? PROGRESS_EVERY;

  const results = await mapWithConcurrency(
    markets,
    concurrency,
    async (market) => {
      try {
        const result = await fetchTradesForMarket(
          market,
          dataDir,
          startMs,
          endMs,
          log,
        );
        return {
          slug: market.slug,
          written: result.written,
          truncated: result.truncated,
          minTs: result.minTs,
          maxTs: result.maxTs,
        };
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Trade fetch failed.";
        log?.(`Trade fetch failed for ${market.slug}: ${message}`, "ERROR");
        return { slug: market.slug, written: 0, truncated: false, minTs: null, maxTs: null };
      }
    },
    (completed, total) => {
      if (completed % progressEvery === 0 || completed === total) {
        log?.(`Trades fetched for ${completed}/${total} markets.`);
      }
    },
  );

  const emptyMarkets = results
    .filter((item) => item.written === 0)
    .map((item) => item.slug);

  const truncatedMarkets = results
    .filter((item) => item.truncated && item.written > 0)
    .map((item) => item.slug);

  const statsBySlug = new Map<string, { count: number; minTs: number; maxTs: number }>();
  for (const item of results) {
    if (item.written > 0 && item.minTs !== null && item.maxTs !== null) {
      statsBySlug.set(item.slug, {
        count: item.written,
        minTs: item.minTs,
        maxTs: item.maxTs,
      });
    }
  }

  if (emptyMarkets.length > 0) {
    log?.(`Trades missing for ${emptyMarkets.length} markets.`, "WARN");
  }

  if (truncatedMarkets.length > 0) {
    log?.(`Trades truncated for ${truncatedMarkets.length} markets (data may be incomplete).`, "WARN");
  }

  return { emptyMarkets, truncatedMarkets, statsBySlug };
}
