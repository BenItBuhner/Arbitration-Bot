import { MarketWS, type MarketEvent } from "../clients/market-ws";
import { CryptoWS, type CryptoPricePayload } from "../clients/crypto-ws";
import {
  extractOutcomes,
  extractPriceToBeat,
  extractTokenIds,
  getEventForMarket,
  type MarketDetails,
} from "./market-service";
import type { SignalSnapshot, TradeLike } from "./market-signals";
import {
  findLatestUpDownMarket,
  resolveMarketStartMs,
  type CoinSymbol,
} from "./auto-market";
import { fetchHistoricalCryptoPrice } from "./crypto-service";
import { fetchPolymarketHtmlReferencePrice } from "./polymarket-html";
import { RunLogger } from "./run-logger";
import { computeSignals } from "./market-signals";
import type { MarketProvider } from "../providers/provider";

const MARKET_DURATION_MS = 15 * 60 * 1000;
const BOOK_STALE_MS = parseEnvNumber("PM_BOOK_STALE_MS", 45000, 1000);
const BOOK_RESET_MS = parseEnvNumber("PM_BOOK_RESET_MS", 90000, 5000);
const WS_RESET_COOLDOWN_MS = parseEnvNumber("PM_WS_RESET_COOLDOWN_MS", 45000, 3000);
const PRICE_STALE_MS = parseEnvNumber("PM_PRICE_STALE_MS", 45000, 1000);
const PRICE_RESET_MS = parseEnvNumber("PM_PRICE_RESET_MS", 90000, 5000);
const CRYPTO_RESET_COOLDOWN_MS = parseEnvNumber("PM_CRYPTO_RESET_COOLDOWN_MS", 45000, 3000);
const DATA_STARTUP_GRACE_MS = parseEnvNumber("PM_DATA_STARTUP_GRACE_MS", 20000, 3000);
const MARKET_RESELECT_MS = parseEnvNumber("PM_MARKET_RESELECT_MS", 60000, 10000);
const MARKET_RESELECT_COOLDOWN_MS = parseEnvNumber("PM_MARKET_RESELECT_COOLDOWN_MS", 60000, 10000);
const PENDING_RETRY_BASE_MS = 5000;
const PENDING_RETRY_MAX_MS = 60000;
const PRICE_HISTORY_LIMIT = 180;
const SIGNAL_DEPTH_LEVELS = 3;
const SIGNAL_SLIPPAGE_NOTIONAL = 50;
const SIGNAL_TRADE_WINDOW_MS = 5 * 60 * 1000;
const REF_RETRY_MS = 30000;
const REF_MISSING_LOG_MS = 45000;

// WS reconnect config (shared by Polymarket market + crypto WS)
const PM_WS_RECONNECT_ATTEMPTS = parseEnvNumber("PM_WS_RECONNECT_ATTEMPTS", -1, -1);
const PM_WS_RECONNECT_DELAY_MS = parseEnvNumber("PM_WS_RECONNECT_DELAY_MS", 3000, 500);
const PM_WS_PING_INTERVAL_MS = parseEnvNumber("PM_WS_PING_INTERVAL_MS", 30000, 5000);
const CRYPTO_WS_RECONNECT_ATTEMPTS = parseEnvNumber("CRYPTO_WS_RECONNECT_ATTEMPTS", -1, -1);
const CRYPTO_WS_RECONNECT_DELAY_MS = parseEnvNumber("CRYPTO_WS_RECONNECT_DELAY_MS", 3000, 500);

function parseEnvFlag(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const normalized = raw.trim().toLowerCase();
  if (["false", "0", "off", "no"].includes(normalized)) return false;
  if (["true", "1", "on", "yes"].includes(normalized)) return true;
  return defaultValue;
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

function normalizeTimestamp(value: number): number {
  if (!Number.isFinite(value)) return Date.now();
  return value >= 1e12 ? value : value * 1000;
}

const HTML_REF_REFRESH_MS = parseEnvNumber("PM_HTML_REF_REFRESH_MS", 60000, 5000);
const HTML_REF_RETRY_BASE_MS = parseEnvNumber("PM_HTML_REF_RETRY_BASE_MS", 8000, 1000);
const HTML_REF_RETRY_MAX_MS = parseEnvNumber("PM_HTML_REF_RETRY_MAX_MS", 60000, 5000);
const HTML_REF_TIMEOUT_MS = parseEnvNumber("PM_HTML_REF_TIMEOUT_MS", 10000, 1000);
const HTML_REF_MATCH_TOLERANCE_MS = parseEnvNumber(
  "PM_HTML_REF_MATCH_TOLERANCE_MS",
  30000,
  0,
);
const HTML_REF_ENABLED = parseEnvFlag("PM_HTML_REF_ENABLED", false);

const ENABLE_LIVE_SIGNALS = parseEnvFlag("LIVE_SIGNAL_PREP", true);

const COIN_CONFIG: Record<CoinSymbol, { name: string; symbol: string }> = {
  eth: { name: "Ethereum", symbol: "eth/usd" },
  btc: { name: "Bitcoin", symbol: "btc/usd" },
  sol: { name: "Solana", symbol: "sol/usd" },
  xrp: { name: "XRP", symbol: "xrp/usd" },
};

const SYMBOL_TO_COIN: Record<string, CoinSymbol> = {
  "eth/usd": "eth",
  "btc/usd": "btc",
  "sol/usd": "sol",
  "xrp/usd": "xrp",
};

const COIN_HTML_SYMBOL: Record<CoinSymbol, string> = {
  eth: "ETH",
  btc: "BTC",
  sol: "SOL",
  xrp: "XRP",
};

export interface OrderBookLevel {
  price: number;
  size: number;
}

export interface PricePoint {
  price: number;
  ts: number;
}

export interface OrderBookSnapshot {
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
  lastTrade: number;
  totalBidValue: number;
  totalAskValue: number;
}

export interface MarketSnapshot {
  provider?: MarketProvider;
  coin: CoinSymbol;
  symbol: string;
  marketName: string;
  slug: string;
  seriesSlug?: string | null;
  marketTicker?: string | null;
  eventTicker?: string | null;
  timeLeftSec: number | null;
  marketCloseTimeMs?: number | null;
  priceToBeat: number;
  referencePrice: number;
  referenceSource:
    | "price_to_beat"
    | "historical"
    | "html"
    | "kalshi_underlying"
    | "kalshi_html"
    | "missing";
  cryptoPrice: number;
  cryptoPriceTimestamp: number;
  kalshiUnderlyingValue?: number | null;
  kalshiUnderlyingTs?: number | null;
  kalshiLastPrice?: number | null;
  kalshiLastPriceTs?: number | null;
  kalshiMarketPrice?: number | null;
  kalshiMarketPriceTs?: number | null;
  kalshiMarketPriceHistory?: number[];
  dataStatus: "unknown" | "healthy" | "stale";
  lastBookUpdateMs: number;
  upOutcome: string;
  downOutcome: string;
  upTokenId: string;
  downTokenId: string;
  orderBooks: Map<string, OrderBookSnapshot>;
  bestBid: Map<string, number>;
  bestAsk: Map<string, number>;
  priceHistory: number[];
  priceHistoryWithTs?: PricePoint[];
  signals?: SignalSnapshot;
}

interface MarketDataState extends MarketSnapshot {
  market: MarketDetails;
  tokenIds: string[];
  marketStartMs: number | null;
  marketEndMs: number | null;
  referencePriceTimestamp: number;
  lastPriceUpdateMs: number;
  lastCryptoUpdateMs: number;
  lastDataStatusMs: number;
  selectedAtMs: number;
  lastReferenceAttemptMs: number;
  referenceAttempts: number;
  lastMissingRefLogMs: number;
  htmlReferenceAttempts: number;
  lastHtmlReferenceAttemptMs: number;
  lastHtmlReferenceSuccessMs: number;
  htmlReferencePending: boolean;
  recentTrades: TradeLike[];
  lastMarketReselectMs: number;
}

interface PendingMarketState {
  attempts: number;
  lastAttemptMs: number;
}

export interface MarketDataHubOptions {
  requireCryptoPrice?: boolean;
}

export class MarketDataHub {
  private logger: RunLogger;
  private marketWs: MarketWS | null = null;
  private cryptoWs: CryptoWS | null = null;
  private states: Map<CoinSymbol, MarketDataState> = new Map();
  private tokenToCoin: Map<string, CoinSymbol> = new Map();
  private evaluationTimer: NodeJS.Timeout | null = null;
  private lastWsResetMs = 0;
  private lastCryptoWsResetMs = 0;
  private rotatingCoins: Set<CoinSymbol> = new Set();
  private pendingMarkets: Map<CoinSymbol, PendingMarketState> = new Map();
  private requireCryptoPrice: boolean;

  constructor(logger: RunLogger, options: MarketDataHubOptions = {}) {
    this.logger = logger;
    this.requireCryptoPrice = options.requireCryptoPrice !== false;
  }

  async start(coins: CoinSymbol[]): Promise<void> {
    for (const coin of coins) {
      const state = await this.initializeMarket(coin);
      if (state) {
        this.states.set(coin, state);
        this.registerTokenIds(state);
      } else {
        this.logger.log(`DATA: failed to auto-select market for ${coin}`, "WARN");
        this.pendingMarkets.set(coin, {
          attempts: 1,
          lastAttemptMs: Date.now(),
        });
      }
    }

    this.connectMarketWs();
    this.connectCryptoWs();

    this.evaluationTimer = setInterval(() => {
      this.tick();
    }, 100);
  }

  stop(): void {
    if (this.evaluationTimer) {
      clearInterval(this.evaluationTimer);
      this.evaluationTimer = null;
    }
    if (this.marketWs) {
      this.marketWs.disconnect();
      this.marketWs = null;
    }
    if (this.cryptoWs) {
      this.cryptoWs.disconnect();
      this.cryptoWs = null;
    }
    this.states.clear();
    this.tokenToCoin.clear();
    this.pendingMarkets.clear();
  }

  getSnapshots(): Map<CoinSymbol, MarketSnapshot> {
    return new Map(this.states);
  }

  private async initializeMarket(
    coin: CoinSymbol,
    minStartMs?: number,
  ): Promise<MarketDataState | null> {
    const selection = await findLatestUpDownMarket(coin, "15m", minStartMs);
    if (!selection) return null;

    const market = selection.market;
    const tokenIds = extractTokenIds(market);
    const outcomes = extractOutcomes(market);

    if (tokenIds.length < 2 || outcomes.length < 2) {
      this.logger.log(`DATA: incomplete market data for ${market.slug}`, "WARN");
      return null;
    }

    const outcomeLower = outcomes.map((o) => o.toLowerCase());
    const upIndex = outcomeLower.indexOf("up");
    const downIndex = outcomeLower.indexOf("down");
    if (upIndex === -1 || downIndex === -1) {
      this.logger.log(`DATA: missing Up/Down outcomes for ${market.slug}`, "WARN");
      return null;
    }

    const upTokenId = tokenIds[upIndex] || "";
    const downTokenId = tokenIds[downIndex] || "";
    if (!upTokenId || !downTokenId) {
      this.logger.log(`DATA: missing token IDs for ${market.slug}`, "WARN");
      return null;
    }

    let marketStartMs = selection.startMs ?? resolveMarketStartMs(market);
    let marketEndMs: number | null = null;
    if (market.endDate) {
      const parsed = Date.parse(market.endDate);
      if (!Number.isNaN(parsed)) {
        marketEndMs = parsed;
      }
    }

    if (!marketStartMs) {
      const event = await getEventForMarket(market.slug);
      if (event?.startTime) {
        const parsed = Date.parse(event.startTime);
        if (!Number.isNaN(parsed)) {
          marketStartMs = parsed;
        }
      }
      if (!marketEndMs && event?.endDate) {
        const parsed = Date.parse(event.endDate);
        if (!Number.isNaN(parsed)) {
          marketEndMs = parsed;
        }
      }
    }

    if (!marketEndMs && marketStartMs) {
      marketEndMs = marketStartMs + MARKET_DURATION_MS;
    }

    const priceToBeat = extractPriceToBeat(market);
    const hasPriceToBeat = priceToBeat > 0;
    const coinMeta = COIN_CONFIG[coin];

    const state: MarketDataState = {
      provider: "polymarket",
      coin,
      symbol: coinMeta.symbol,
      market,
      tokenIds,
      marketStartMs,
      marketEndMs,
      marketName: market.question || market.slug,
      slug: market.slug,
      seriesSlug: market.seriesSlug ?? null,
      marketTicker: null,
      eventTicker: null,
      timeLeftSec: this.getTimeLeftSec(marketEndMs),
      marketCloseTimeMs: marketEndMs,
      upOutcome: outcomes[upIndex] || "Up",
      downOutcome: outcomes[downIndex] || "Down",
      upTokenId,
      downTokenId,
      priceToBeat,
      referencePrice: hasPriceToBeat ? priceToBeat : 0,
      referenceSource: hasPriceToBeat ? "price_to_beat" : "missing",
      referencePriceTimestamp: 0,
      cryptoPrice: 0,
      cryptoPriceTimestamp: 0,
      orderBooks: new Map(),
      bestBid: new Map(),
      bestAsk: new Map(),
      lastBookUpdateMs: 0,
      lastPriceUpdateMs: 0,
      lastCryptoUpdateMs: 0,
      dataStatus: "unknown",
      lastDataStatusMs: 0,
      selectedAtMs: Date.now(),
      priceHistory: [],
      priceHistoryWithTs: [],
      lastReferenceAttemptMs: 0,
      referenceAttempts: 0,
      lastMissingRefLogMs: 0,
      htmlReferenceAttempts: 0,
      lastHtmlReferenceAttemptMs: 0,
      lastHtmlReferenceSuccessMs: 0,
      htmlReferencePending: false,
      recentTrades: [],
      lastMarketReselectMs: 0,
    };

    this.logger.log(
      `DATA: selected ${coin.toUpperCase()} market ${market.slug} (start ${
        marketStartMs ? new Date(marketStartMs).toISOString() : "unknown"
      })`,
    );
    if (!hasPriceToBeat && !marketStartMs) {
      this.logger.log(
        `DATA: ${coin.toUpperCase()} reference unavailable (missing start time)`,
        "WARN",
      );
    }

    this.tryFetchHtmlReference(state);

    return state;
  }

  private registerTokenIds(state: MarketDataState): void {
    for (const tokenId of state.tokenIds) {
      this.tokenToCoin.set(tokenId, state.coin);
    }
  }

  private connectMarketWs(): void {
    const tokenIds: string[] = [];
    for (const state of this.states.values()) {
      tokenIds.push(...state.tokenIds);
    }

    if (this.marketWs) {
      this.marketWs.disconnect();
      this.marketWs = null;
    }

    this.marketWs = new MarketWS(
      (event: MarketEvent) => this.handlePriceChange(event),
      (event: MarketEvent) => this.handleOrderBook(event),
      (event: MarketEvent) => this.handleLastTrade(event),
      () => {},
      (error: Error) => {
        this.logger.log(`DATA: market WS error ${error.message}`, "ERROR");
      },
      {
        silent: true,
        reconnectAttempts: PM_WS_RECONNECT_ATTEMPTS,
        reconnectDelay: PM_WS_RECONNECT_DELAY_MS,
        pingInterval: PM_WS_PING_INTERVAL_MS,
      },
    );

    this.marketWs.connect();
    if (tokenIds.length > 0) {
      this.marketWs.subscribe(tokenIds);
    }
  }

  /**
   * Swap subscriptions on the existing MarketWS without tearing down the connection.
   * Falls back to a full reconnect if the WS is not alive.
   */
  private refreshMarketWsSubscriptions(): void {
    const tokenIds: string[] = [];
    for (const state of this.states.values()) {
      tokenIds.push(...state.tokenIds);
    }

    if (this.marketWs && this.marketWs.isConnected()) {
      this.marketWs.replaceSubscriptions(tokenIds);
    } else {
      this.connectMarketWs();
    }
  }

  private connectCryptoWs(): void {
    const symbols = Array.from(this.states.values()).map((state) => state.symbol);

    if (this.cryptoWs) {
      this.cryptoWs.disconnect();
      this.cryptoWs = null;
    }

    this.cryptoWs = new CryptoWS(
      (payload: CryptoPricePayload) => this.handleCryptoPrice(payload),
      () => {},
      (error: Error) => {
        this.logger.log(`DATA: crypto WS error ${error.message}`, "ERROR");
      },
      {
        source: "chainlink",
        reconnectAttempts: CRYPTO_WS_RECONNECT_ATTEMPTS,
        reconnectDelay: CRYPTO_WS_RECONNECT_DELAY_MS,
      },
    );

    this.cryptoWs.connect();
    if (symbols.length > 0) {
      this.cryptoWs.subscribe(symbols);
    }
  }

  private handleCryptoPrice(payload: CryptoPricePayload): void {
    if (!Number.isFinite(payload.value) || payload.value <= 0) return;
    const symbolKey = payload.symbol.toLowerCase();
    const coin = SYMBOL_TO_COIN[symbolKey];
    if (!coin) return;

    const state = this.states.get(coin);
    if (!state) return;

    const ts = normalizeTimestamp(payload.timestamp);
    state.cryptoPrice = payload.value;
    state.cryptoPriceTimestamp = ts;
    state.lastPriceUpdateMs = Date.now();
    state.lastCryptoUpdateMs = state.lastPriceUpdateMs;
    state.priceHistory.push(payload.value);
    if (state.priceHistory.length > PRICE_HISTORY_LIMIT) {
      state.priceHistory.shift();
    }
    if (!state.priceHistoryWithTs) {
      state.priceHistoryWithTs = [];
    }
    state.priceHistoryWithTs.push({ price: payload.value, ts });
    if (state.priceHistoryWithTs.length > PRICE_HISTORY_LIMIT) {
      state.priceHistoryWithTs.shift();
    }

  }

  private handlePriceChange(event: MarketEvent): void {
    if (event.event_type !== "price_change" || !event.price_changes) return;

    for (const change of event.price_changes) {
      const coin = this.tokenToCoin.get(change.asset_id);
      if (!coin) continue;
      const state = this.states.get(coin);
      if (!state) continue;
      const now = Date.now();
      state.lastPriceUpdateMs = now;
      // price_change carries best_bid/best_ask -- proves the book is current
      state.lastBookUpdateMs = now;
      const bestBid = parseFloat(change.best_bid);
      const bestAsk = parseFloat(change.best_ask);
      if (!Number.isNaN(bestBid)) {
        state.bestBid.set(change.asset_id, bestBid);
      }
      if (!Number.isNaN(bestAsk)) {
        state.bestAsk.set(change.asset_id, bestAsk);
      }
      this.updateDataStatus(state, now);
    }
  }

  private handleOrderBook(event: MarketEvent): void {
    if (event.event_type !== "book") return;
    const coin = this.tokenToCoin.get(event.asset_id);
    if (!coin) return;
    const state = this.states.get(coin);
    if (!state) return;
    state.lastBookUpdateMs = Date.now();

    const bids = (event.bids || [])
      .map((b) => ({ price: parseFloat(b.price), size: parseFloat(b.size) }))
      .filter((b) => Number.isFinite(b.price) && Number.isFinite(b.size))
      .sort((a, b) => b.price - a.price);
    const asks = (event.asks || [])
      .map((a) => ({ price: parseFloat(a.price), size: parseFloat(a.size) }))
      .filter((a) => Number.isFinite(a.price) && Number.isFinite(a.size))
      .sort((a, b) => a.price - b.price);

    let totalBidValue = 0;
    let totalAskValue = 0;
    for (const bid of bids) totalBidValue += bid.price * bid.size;
    for (const ask of asks) totalAskValue += ask.price * ask.size;

    const snapshot: OrderBookSnapshot = {
      bids,
      asks,
      lastTrade: event.last_trade_price ? parseFloat(event.last_trade_price) : 0,
      totalBidValue,
      totalAskValue,
    };

    state.orderBooks.set(event.asset_id, snapshot);
    if (bids[0]) state.bestBid.set(event.asset_id, bids[0].price);
    if (asks[0]) state.bestAsk.set(event.asset_id, asks[0].price);

    this.updateDataStatus(state, Date.now());
  }

  private handleLastTrade(event: MarketEvent): void {
    if (event.event_type !== "last_trade_price") return;
    const coin = this.tokenToCoin.get(event.asset_id);
    if (!coin) return;
    const state = this.states.get(coin);
    if (!state) return;

    const price = Number(event.price);
    const size = Number(event.size);
    if (!Number.isFinite(price) || !Number.isFinite(size)) return;

    const parsedTs = Date.parse(event.timestamp);
    const now = Date.now();
    const timestamp = Number.isFinite(parsedTs) ? parsedTs : now;

    // Trades prove the market is actively trading -- count as book freshness
    state.lastBookUpdateMs = now;

    state.recentTrades.push({
      timestamp,
      price,
      size,
      side: event.side,
      tokenId: event.asset_id,
    });

    this.trimRecentTrades(state, now);
    this.updateDataStatus(state, now);
  }

  private tick(): void {
    const now = Date.now();
    void this.retryPendingMarkets(now);
    for (const state of this.states.values()) {
      this.maybeRefreshHtmlReference(state, now);
      this.maybeRefreshReference(state, now);
      this.updateDataStatus(state, now);
      state.timeLeftSec = this.getTimeLeftSec(state.marketEndMs);
      if (state.recentTrades.length > 0) {
        this.trimRecentTrades(state, now);
      }
      if (ENABLE_LIVE_SIGNALS) {
        state.signals = computeSignals(state, now, state.recentTrades, {
          depthLevels: SIGNAL_DEPTH_LEVELS,
          slippageNotional: SIGNAL_SLIPPAGE_NOTIONAL,
          tradeWindowMs: SIGNAL_TRADE_WINDOW_MS,
        });
      }

      if (state.timeLeftSec !== null && state.timeLeftSec <= 0) {
        if (!this.rotatingCoins.has(state.coin)) {
          this.rotatingCoins.add(state.coin);
          this.rotateMarket(state.coin, state.marketStartMs || 0).finally(() => {
            this.rotatingCoins.delete(state.coin);
          });
        }
        continue;
      }

      this.maybeRecoverData(state, now);
    }
  }

  private trimRecentTrades(state: MarketDataState, now: number): void {
    const cutoff = now - SIGNAL_TRADE_WINDOW_MS;
    while (state.recentTrades.length > 0) {
      const first = state.recentTrades[0];
      if (!first || first.timestamp >= cutoff) break;
      state.recentTrades.shift();
    }
  }

  private updateDataStatus(state: MarketDataState, now: number): void {
    const bookFresh = this.isBookFresh(state, now);
    const priceFresh = this.isCryptoFresh(state, now);
    const effectivePriceFresh = this.requireCryptoPrice ? priceFresh : true;
    const hasAnyData =
      state.lastBookUpdateMs > 0 || state.lastCryptoUpdateMs > 0;
    let nextStatus: MarketDataState["dataStatus"] = "unknown";
    if (bookFresh && effectivePriceFresh) {
      nextStatus = "healthy";
    } else if (
      !hasAnyData &&
      now - state.selectedAtMs < DATA_STARTUP_GRACE_MS
    ) {
      nextStatus = "unknown";
    } else {
      nextStatus = "stale";
    }

    if (nextStatus !== state.dataStatus) {
      state.dataStatus = nextStatus;
      state.lastDataStatusMs = now;
      if (nextStatus === "healthy") {
        const label = this.requireCryptoPrice ? "book + price" : "book";
        this.logger.log(
          `DATA: ${state.coin.toUpperCase()} data active (${label})`,
        );
      } else if (nextStatus === "stale") {
        const reasons: string[] = [];
        if (!bookFresh) {
          if (state.lastBookUpdateMs > 0) {
            const bookAge = Math.round((now - state.lastBookUpdateMs) / 1000);
            reasons.push(`book stale ${bookAge}s ago`);
          } else {
            reasons.push("book missing");
          }
        }
        if (this.requireCryptoPrice && !priceFresh) {
          if (state.lastCryptoUpdateMs > 0) {
            const priceAge = Math.round((now - state.lastCryptoUpdateMs) / 1000);
            reasons.push(`price stale ${priceAge}s ago`);
          } else {
            reasons.push("price missing");
          }
        }
        const since = Math.round((now - state.selectedAtMs) / 1000);
        this.logger.log(
          `DATA: ${state.coin.toUpperCase()} data stale (${reasons.join(
            ", ",
          )}) (${since}s since selection)`,
          "WARN",
        );
      }
    }
  }

  private maybeRefreshHtmlReference(state: MarketDataState, now: number): void {
    if (!HTML_REF_ENABLED) return;
    if (!state.marketStartMs || !state.marketEndMs) return;
    if (state.marketStartMs > now) return;
    if (state.htmlReferencePending) return;

    if (state.lastHtmlReferenceSuccessMs > 0) {
      if (now - state.lastHtmlReferenceSuccessMs < HTML_REF_REFRESH_MS) {
        return;
      }
    } else if (state.lastHtmlReferenceAttemptMs > 0) {
      const attempt = Math.max(0, state.htmlReferenceAttempts);
      const backoff = Math.min(
        HTML_REF_RETRY_MAX_MS,
        HTML_REF_RETRY_BASE_MS * 2 ** Math.max(0, attempt - 1),
      );
      if (now - state.lastHtmlReferenceAttemptMs < backoff) {
        return;
      }
    }

    this.tryFetchHtmlReference(state);
  }

  private tryFetchHtmlReference(state: MarketDataState): void {
    if (!HTML_REF_ENABLED) return;
    if (!state.marketStartMs || !state.marketEndMs) return;
    if (state.htmlReferencePending) return;

    state.htmlReferencePending = true;
    state.lastHtmlReferenceAttemptMs = Date.now();
    state.htmlReferenceAttempts += 1;

    const symbol = COIN_HTML_SYMBOL[state.coin] || state.coin.toUpperCase();

    fetchPolymarketHtmlReferencePrice({
      slug: state.slug,
      symbol,
      startMs: state.marketStartMs,
      endMs: state.marketEndMs,
      timeoutMs: HTML_REF_TIMEOUT_MS,
      matchToleranceMs: HTML_REF_MATCH_TOLERANCE_MS,
    })
      .then((result) => {
        state.htmlReferencePending = false;
        if (!result) {
          this.logger.log(
            `DATA: ${state.coin.toUpperCase()} HTML reference fetch failed (attempt ${state.htmlReferenceAttempts})`,
            "WARN",
          );
          this.tryFetchReferencePrice(state);
          return;
        }

        const previous = state.referencePrice;
        const changed =
          previous > 0 && Math.abs(previous - result.openPrice) > 0.01;

        state.referencePrice = result.openPrice;
        state.referencePriceTimestamp = result.startMs;
        state.referenceSource = "html";
        state.lastHtmlReferenceSuccessMs = Date.now();
        state.htmlReferenceAttempts = 0;

        if (changed) {
          this.logger.log(
            `DATA: ${state.coin.toUpperCase()} HTML reference updated ${result.openPrice.toFixed(
              2,
            )} (was ${previous.toFixed(2)})`,
            "WARN",
          );
        } else {
          this.logger.log(
            `DATA: ${state.coin.toUpperCase()} HTML reference price set ${result.openPrice.toFixed(
              2,
            )}`,
          );
        }
      })
      .catch(() => {
        state.htmlReferencePending = false;
        this.logger.log(
          `DATA: ${state.coin.toUpperCase()} HTML reference fetch errored (attempt ${state.htmlReferenceAttempts})`,
          "WARN",
        );
        this.tryFetchReferencePrice(state);
      });
  }

  private maybeRefreshReference(state: MarketDataState, now: number): void {
    if (state.referencePrice > 0) return;
    if (state.marketStartMs && now - state.marketStartMs < 0) return;

    if (now - state.lastReferenceAttemptMs >= REF_RETRY_MS) {
      this.tryFetchReferencePrice(state);
    }

    if (
      now - state.selectedAtMs >= REF_MISSING_LOG_MS &&
      now - state.lastMissingRefLogMs >= REF_MISSING_LOG_MS
    ) {
      state.lastMissingRefLogMs = now;
      this.logger.log(
        `DATA: ${state.coin.toUpperCase()} reference missing (HTML + fallback pending)`,
        "WARN",
      );
    }
  }

  private tryFetchReferencePrice(state: MarketDataState): void {
    if (!state.marketStartMs) return;
    if (state.referenceSource === "html") return;
    if (state.referencePrice > 0 && state.referenceSource !== "missing") {
      return;
    }
    if (Date.now() - state.lastReferenceAttemptMs < REF_RETRY_MS) {
      return;
    }

    state.lastReferenceAttemptMs = Date.now();
    state.referenceAttempts += 1;

    fetchHistoricalCryptoPrice(state.symbol, new Date(state.marketStartMs))
      .then((historical) => {
        if (historical && state.referencePrice === 0) {
          state.referencePrice = historical.price;
          state.referencePriceTimestamp = historical.timestamp;
          state.referenceSource = "historical";
          this.logger.log(
            `DATA: ${state.coin.toUpperCase()} reference price set ${historical.price.toFixed(
              2,
            )}`,
          );
        } else if (!historical) {
          this.logger.log(
            `DATA: ${state.coin.toUpperCase()} reference fetch failed (attempt ${state.referenceAttempts})`,
            "WARN",
          );
        }
      })
      .catch((err) => {
        this.logger.log(
          `DATA: ${state.coin.toUpperCase()} reference fetch errored: ${err instanceof Error ? err.message : "unknown"}`,
          "WARN",
        );
      });
  }

  private isBookFresh(state: MarketDataState, now: number): boolean {
    if (state.lastBookUpdateMs === 0) {
      return false;
    }
    return now - state.lastBookUpdateMs <= BOOK_STALE_MS;
  }

  private isCryptoFresh(state: MarketDataState, now: number): boolean {
    if (state.lastCryptoUpdateMs === 0) {
      return false;
    }
    return now - state.lastCryptoUpdateMs <= PRICE_STALE_MS;
  }

  private maybeRecoverData(state: MarketDataState, now: number): void {
    const bookFresh = this.isBookFresh(state, now);
    const priceFresh = this.isCryptoFresh(state, now);
    const effectivePriceFresh = this.requireCryptoPrice ? priceFresh : true;

    if (bookFresh && effectivePriceFresh) {
      return;
    }

    if (now - state.selectedAtMs < DATA_STARTUP_GRACE_MS) {
      return;
    }

    if (!bookFresh) {
      this.maybeResetMarketWs(state, now);
    }

    if (this.requireCryptoPrice && !priceFresh) {
      this.maybeResetCryptoWs(state, now);
    }

    const selectedMs = now - state.selectedAtMs;
    if (
      selectedMs >= MARKET_RESELECT_MS &&
      now - state.lastMarketReselectMs >= MARKET_RESELECT_COOLDOWN_MS &&
      !this.rotatingCoins.has(state.coin)
    ) {
      state.lastMarketReselectMs = now;
      this.logger.log(
        `DATA: ${state.coin.toUpperCase()} reselecting market (data timeout)`,
        "WARN",
      );
      this.rotatingCoins.add(state.coin);
      this.reselectMarket(state.coin).finally(() => {
        this.rotatingCoins.delete(state.coin);
      });
    }
  }

  private maybeResetMarketWs(state: MarketDataState, now: number): void {
    if (now - this.lastWsResetMs < WS_RESET_COOLDOWN_MS) return;
    const staleMs =
      state.lastBookUpdateMs > 0 ? now - state.lastBookUpdateMs : 0;
    const selectedMs = now - state.selectedAtMs;
    const shouldReset =
      staleMs >= BOOK_RESET_MS || selectedMs >= BOOK_RESET_MS;
    if (!shouldReset) return;

    // If the WS is still connected, try refreshing subscriptions instead of
    // a destructive full reconnect that would kill subscriptions for ALL coins.
    if (this.marketWs?.isConnected()) {
      this.logger.log(
        `DATA: ${state.coin.toUpperCase()} WS connected but book stale -- refreshing subscriptions`,
        "WARN",
      );
      this.refreshMarketWsSubscriptions();
      this.lastWsResetMs = now;
      return;
    }

    this.lastWsResetMs = now;
    this.logger.log(
      `DATA: ${state.coin.toUpperCase()} resetting market WS (connection dead)`,
      "WARN",
    );
    this.connectMarketWs();
  }

  private maybeResetCryptoWs(state: MarketDataState, now: number): void {
    if (now - this.lastCryptoWsResetMs < CRYPTO_RESET_COOLDOWN_MS) return;
    const staleMs =
      state.lastCryptoUpdateMs > 0 ? now - state.lastCryptoUpdateMs : 0;
    const selectedMs = now - state.selectedAtMs;
    const shouldReset =
      staleMs >= PRICE_RESET_MS || selectedMs >= PRICE_RESET_MS;
    if (!shouldReset) return;

    // If the crypto WS is still connected, try re-subscribing instead of full reconnect.
    if (this.cryptoWs?.isConnected()) {
      const symbols = Array.from(this.states.values()).map((s) => s.symbol);
      if (symbols.length > 0) {
        this.cryptoWs.subscribe(symbols);
      }
      this.lastCryptoWsResetMs = now;
      return;
    }

    this.lastCryptoWsResetMs = now;
    this.logger.log(
      `DATA: ${state.coin.toUpperCase()} resetting crypto WS (connection dead)`,
      "WARN",
    );
    this.connectCryptoWs();
  }

  private async rotateMarket(coin: CoinSymbol, lastStartMs: number): Promise<void> {
    const next = await this.initializeMarket(coin, lastStartMs);
    if (!next) return;

    const current = this.states.get(coin);
    const oldTokenIds = current ? [...current.tokenIds] : [];
    if (current) {
      current.tokenIds.forEach((tokenId) => this.tokenToCoin.delete(tokenId));
    }

    this.states.set(coin, next);
    this.registerTokenIds(next);
    this.logger.log(`DATA: ${coin.toUpperCase()} rotating market (sub refresh)`);

    // Subscribe new tokens directly instead of full replace (avoids thrashing
    // when multiple coins rotate simultaneously at market boundaries).
    if (this.marketWs && this.marketWs.isConnected()) {
      if (oldTokenIds.length > 0) {
        this.marketWs.unsubscribe(oldTokenIds);
      }
      this.marketWs.subscribe(next.tokenIds);
    } else {
      this.connectMarketWs();
    }
    if (this.cryptoWs) {
      this.cryptoWs.subscribe([next.symbol]);
    }
  }

  private async reselectMarket(coin: CoinSymbol): Promise<void> {
    const next = await this.initializeMarket(coin);
    if (!next) {
      this.logger.log(
        `DATA: ${coin.toUpperCase()} market reselect failed`,
        "WARN",
      );
      return;
    }

    const current = this.states.get(coin);
    const oldTokenIds = current ? [...current.tokenIds] : [];
    if (current) {
      current.tokenIds.forEach((tokenId) => this.tokenToCoin.delete(tokenId));
    }

    this.states.set(coin, next);
    this.registerTokenIds(next);
    this.logger.log(
      `DATA: ${coin.toUpperCase()} market reselected (${next.slug})`,
    );

    // Subscribe new tokens directly (same approach as rotateMarket)
    if (this.marketWs && this.marketWs.isConnected()) {
      if (oldTokenIds.length > 0) {
        this.marketWs.unsubscribe(oldTokenIds);
      }
      this.marketWs.subscribe(next.tokenIds);
    } else {
      this.connectMarketWs();
    }
    if (this.cryptoWs) {
      this.cryptoWs.subscribe([next.symbol]);
    } else {
      this.connectCryptoWs();
    }
  }

  private async retryPendingMarkets(now: number): Promise<void> {
    if (this.pendingMarkets.size === 0) return;

    for (const [coin, pending] of this.pendingMarkets.entries()) {
      if (this.rotatingCoins.has(coin)) continue;
      const delay = Math.min(
        PENDING_RETRY_BASE_MS * Math.pow(2, Math.max(0, pending.attempts - 1)),
        PENDING_RETRY_MAX_MS,
      );
      if (now - pending.lastAttemptMs < delay) continue;

      pending.lastAttemptMs = now;
      pending.attempts += 1;
      this.logger.log(
        `DATA: retrying market selection for ${coin.toUpperCase()} (attempt ${pending.attempts})`,
        "WARN",
      );

      this.rotatingCoins.add(coin);
      try {
        const state = await this.initializeMarket(coin);
        if (!state) {
          continue;
        }

        this.pendingMarkets.delete(coin);
        this.states.set(coin, state);
        this.registerTokenIds(state);
        this.logger.log(
          `DATA: ${coin.toUpperCase()} market loaded after retry (${state.slug})`,
        );
        if (this.marketWs) {
          this.marketWs.subscribe(state.tokenIds);
        } else {
          this.connectMarketWs();
        }
        if (this.cryptoWs) {
          this.cryptoWs.subscribe([state.symbol]);
        } else {
          this.connectCryptoWs();
        }
      } catch (err) {
        this.logger.log(
          `DATA: ${coin.toUpperCase()} retry market init errored: ${err instanceof Error ? err.message : "unknown"}`,
          "ERROR",
        );
      } finally {
        this.rotatingCoins.delete(coin);
      }
    }
  }

  private getTimeLeftSec(marketEndMs: number | null): number | null {
    if (!marketEndMs) return null;
    return (marketEndMs - Date.now()) / 1000;
  }
}
