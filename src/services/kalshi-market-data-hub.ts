import { KalshiClient } from "../clients/kalshi/kalshi-client";
import { KalshiMarketWS, type KalshiTickerUpdate } from "../clients/kalshi/kalshi-ws";
import { CryptoWS, type CryptoPricePayload } from "../clients/crypto-ws";
import { computeSignals, type SignalSnapshot, type TradeLike } from "./market-signals";
import { RunLogger } from "./run-logger";
import type { CoinSymbol } from "./auto-market";
import type { KalshiEnvConfig } from "../clients/kalshi/kalshi-config";
import {
  KALSHI_DEMO_BASE,
  KALSHI_DEMO_WS,
  KALSHI_PROD_BASE,
  KALSHI_PROD_WS,
} from "../clients/kalshi/kalshi-config";
import type { MarketSnapshot, OrderBookSnapshot, OrderBookLevel } from "./market-data-hub";
import type { KalshiCoinSelection } from "./profile-config";
import {
  deriveSeriesTickerFromMarket,
  normalizeKalshiTicker,
  parseKalshiMarketUrl,
} from "../clients/kalshi/kalshi-url";
import { fetchKalshiHtmlReference } from "./kalshi-html";

const BOOK_STALE_MS = 10000;
const PRICE_STALE_MS = 12000;
const DATA_STARTUP_GRACE_MS = 12000;
const MARKET_RESELECT_MS = 60000;
const MARKET_RESELECT_COOLDOWN_MS = 60000;
const SIGNAL_DEPTH_LEVELS = 3;
const SIGNAL_SLIPPAGE_NOTIONAL = 50;
const SIGNAL_TRADE_WINDOW_MS = 5 * 60 * 1000;
const KALSHI_REF_RETRY_BASE_MS = 5000;
const KALSHI_REF_RETRY_MAX_MS = 120000;
const KALSHI_REF_RETRY_WINDOW_MS = 5 * 60 * 1000;
const KALSHI_HTML_REF_RETRY_BASE_MS = 15000;
const KALSHI_HTML_REF_RETRY_MAX_MS = 120000;
const KALSHI_HTML_REF_TIMEOUT_MS = 10000;

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

interface KalshiMarketState extends MarketSnapshot {
  marketTicker: string;
  eventTicker?: string | null;
  closeTimeMs: number | null;
  lastBookUpdateMs: number;
  lastCryptoUpdateMs: number;
  lastDataStatusMs: number;
  selectedAtMs: number;
  lastReselectMs: number;
  recentTrades: TradeLike[];
  htmlReferenceUrls: string[];
  htmlReferencePending: boolean;
  htmlReferenceAttempts: number;
  lastHtmlReferenceAttemptMs: number;
  refRetryAttempts: number;
  lastRefRetryMs: number;
}

interface KalshiMarketCandidate {
  ticker: string;
  market: Record<string, unknown>;
  closeTimeMs: number | null;
}

interface KalshiSelection {
  tickers: string[];
  seriesTickers: string[];
  eventTickers: string[];
  marketUrls: string[];
  autoDiscover: boolean;
}

function parseTimestamp(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value >= 1e12 ? value : value * 1000;
  }
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

function parseNumericString(value: string): number | null {
  const cleaned = value.replace(/[$,%]/g, "").replace(/,/g, "").trim();
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") return parseNumericString(value);
  return null;
}

function extractNumericValue(value: unknown): number | null {
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const candidate = record.value ?? record.display_value;
    const parsed = toNumber(candidate);
    if (parsed !== null) return parsed;
  }
  return toNumber(value);
}

function findNumericByKeys(
  value: unknown,
  keys: Set<string>,
  depth: number = 0,
): number | null {
  if (depth > 4) return null;
  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = findNumericByKeys(entry, keys, depth + 1);
      if (found !== null) return found;
    }
    return null;
  }
  if (!value || typeof value !== "object") return null;

  const record = value as Record<string, unknown>;
  for (const [key, entry] of Object.entries(record)) {
    if (keys.has(key)) {
      const parsed = extractNumericValue(entry);
      if (parsed !== null && parsed > 0) return parsed;
    }
  }
  for (const entry of Object.values(record)) {
    const found = findNumericByKeys(entry, keys, depth + 1);
    if (found !== null) return found;
  }
  return null;
}

function normalizeMarketPrice(raw: unknown, dollars: unknown): number | null {
  const dollarsValue = extractNumericValue(dollars);
  if (dollarsValue !== null) return dollarsValue;
  const rawValue = extractNumericValue(raw);
  if (rawValue === null) return null;
  return rawValue > 1.5 ? rawValue / 100 : rawValue;
}

function resolveMarketTicker(market: Record<string, unknown>): string | null {
  const tickerRaw =
    market.ticker ?? market.market_ticker ?? market.marketTicker ?? market.marketTicker;
  if (typeof tickerRaw !== "string") return null;
  const normalized = normalizeKalshiTicker(tickerRaw);
  return normalized.length > 0 ? normalized : null;
}

function resolveEventTicker(market: Record<string, unknown>): string | null {
  const tickerRaw = market.event_ticker ?? market.eventTicker;
  if (typeof tickerRaw !== "string") return null;
  const normalized = normalizeKalshiTicker(tickerRaw);
  return normalized.length > 0 ? normalized : null;
}

function resolveSeriesTicker(market: Record<string, unknown>): string | null {
  const seriesRaw = market.series_ticker ?? market.seriesTicker;
  if (typeof seriesRaw === "string") {
    const normalized = normalizeKalshiTicker(seriesRaw);
    if (normalized.length > 0) return normalized;
  }
  const eventTicker = resolveEventTicker(market);
  if (eventTicker) {
    const derived = deriveSeriesTickerFromMarket(eventTicker);
    if (derived) return derived;
  }
  const marketTicker = resolveMarketTicker(market);
  if (marketTicker) {
    const derived = deriveSeriesTickerFromMarket(marketTicker);
    if (derived) return derived;
  }
  return null;
}

function parseStrikePrice(market: Record<string, unknown>): number {
  const strikeBlock =
    market.strike && typeof market.strike === "object" ? market.strike : null;
  const strikeTypeRaw = String(
    (strikeBlock as Record<string, unknown> | null)?.strike_type ??
      market.strike_type ??
      "",
  ).toLowerCase();
  const floorStrike = extractNumericValue(
    (strikeBlock as Record<string, unknown> | null)?.floor_strike ??
      (strikeBlock as Record<string, unknown> | null)?.lower_strike ??
      market.floor_strike ??
      market.lower_strike,
  );
  const capStrike = extractNumericValue(
    (strikeBlock as Record<string, unknown> | null)?.cap_strike ??
      (strikeBlock as Record<string, unknown> | null)?.upper_strike ??
      market.cap_strike ??
      market.upper_strike,
  );

  const preferFloor =
    strikeTypeRaw.includes("greater") ||
    strikeTypeRaw.includes("above") ||
    strikeTypeRaw.includes("up");
  const preferCap =
    strikeTypeRaw.includes("less") ||
    strikeTypeRaw.includes("below") ||
    strikeTypeRaw.includes("down");

  if (preferFloor && floorStrike !== null && floorStrike > 0) {
    return floorStrike;
  }
  if (preferCap && capStrike !== null && capStrike > 0) {
    return capStrike;
  }
  if (floorStrike !== null && floorStrike > 0 && !capStrike) {
    return floorStrike;
  }
  if (capStrike !== null && capStrike > 0 && !floorStrike) {
    return capStrike;
  }
  if (floorStrike !== null && floorStrike > 0) {
    return floorStrike;
  }

  const strikeKeys = new Set([
    "floor_strike",
    "cap_strike",
    "lower_strike",
    "upper_strike",
    "strike_price",
    "strike_price_decimal",
    "strike_price_dollars",
    "strike_price_cents",
    "strike",
    "price_to_beat",
    "priceToBeat",
    "reference_price",
    "referencePrice",
  ]);
  const deepStrike = findNumericByKeys(market, strikeKeys);
  if (deepStrike !== null && deepStrike > 0) {
    return deepStrike;
  }

  const candidates: Array<{ value: unknown; scale?: "cents" }> = [
    { value: (strikeBlock as Record<string, unknown> | null)?.strike_price_dollars },
    { value: market.strike_price_dollars },
    { value: (strikeBlock as Record<string, unknown> | null)?.strike_price_decimal },
    { value: market.strike_price_decimal },
    { value: (strikeBlock as Record<string, unknown> | null)?.strike_price },
    { value: market.strike_price },
    { value: (strikeBlock as Record<string, unknown> | null)?.strike_price_cents, scale: "cents" },
    { value: market.strike_price_cents, scale: "cents" },
    { value: (strikeBlock as Record<string, unknown> | null)?.strike },
    { value: market.strike },
    { value: market.reference_price },
    { value: market.price_to_beat },
    { value: market.priceToBeat },
  ];

  for (const candidate of candidates) {
    const numeric = extractNumericValue(candidate.value);
    if (numeric === null || !Number.isFinite(numeric) || numeric <= 0) continue;
    if (candidate.scale === "cents") {
      return numeric / 100;
    }
    return numeric;
  }

  const title = String(market.title ?? "");
  if (title.includes("$")) {
    const match = title.match(
      /\$([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)/,
    );
    if (match && match[1]) {
      const cleaned = match[1].replace(/,/g, "");
      const parsed = Number(cleaned);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }

  return 0;
}

function isMarketOpen(market: Record<string, unknown>): boolean {
  const status = String(market.status ?? "").toLowerCase();
  if (status === "open" || status === "active") return true;
  if (
    status === "closed" ||
    status === "settled" ||
    status === "determined" ||
    status === "finalized"
  ) {
    return false;
  }
  if (status === "paused" || status === "inactive" || status === "unopened") {
    return false;
  }
  if (status === "initialized") return true;
  const closeTime = parseTimestamp(market.close_time ?? market.closeTime);
  if (closeTime && closeTime > Date.now()) return true;
  return status !== "closed" && status !== "settled";
}

function buildOrderBookSnapshot(
  bids: OrderBookLevel[],
  asks: OrderBookLevel[],
  lastTrade: number,
): OrderBookSnapshot {
  let totalBidValue = 0;
  let totalAskValue = 0;
  for (const bid of bids) totalBidValue += bid.price * bid.size;
  for (const ask of asks) totalAskValue += ask.price * ask.size;
  return { bids, asks, lastTrade, totalBidValue, totalAskValue };
}

export class KalshiMarketDataHub {
  private logger: RunLogger;
  private kalshiClient: KalshiClient;
  private kalshiWs: KalshiMarketWS | null = null;
  private cryptoWs: CryptoWS | null = null;
  private states: Map<CoinSymbol, KalshiMarketState> = new Map();
  private tickerToCoin: Map<string, CoinSymbol> = new Map();
  private evaluationTimer: NodeJS.Timeout | null = null;

  constructor(
    logger: RunLogger,
    private kalshiConfig: KalshiEnvConfig,
    private selectorsByCoin: Map<CoinSymbol, KalshiCoinSelection>,
  ) {
    this.logger = logger;
    this.kalshiClient = new KalshiClient(kalshiConfig);
  }

  async start(coins: CoinSymbol[]): Promise<void> {
    for (const coin of coins) {
      const state = await this.initializeMarket(coin);
      if (state) {
        this.states.set(coin, state);
        this.tickerToCoin.set(state.marketTicker, coin);
      } else {
        this.logger.log(
          `DATA: failed to select Kalshi market for ${coin.toUpperCase()}`,
          "WARN",
        );
      }
    }

    this.connectKalshiWs();
    this.connectCryptoWs();

    this.evaluationTimer = setInterval(() => {
      this.tick();
    }, 500);
  }

  stop(): void {
    if (this.evaluationTimer) {
      clearInterval(this.evaluationTimer);
      this.evaluationTimer = null;
    }
    if (this.kalshiWs) {
      this.kalshiWs.disconnect();
      this.kalshiWs = null;
    }
    if (this.cryptoWs) {
      this.cryptoWs.disconnect();
      this.cryptoWs = null;
    }
    this.states.clear();
    this.tickerToCoin.clear();
  }

  getSnapshots(): Map<CoinSymbol, MarketSnapshot> {
    return new Map(this.states);
  }

  private buildSelection(coin: CoinSymbol): KalshiSelection | null {
    const selection = this.selectorsByCoin.get(coin);
    if (!selection) return null;

    const tickerSet = new Set<string>();
    const seriesSet = new Set<string>();
    const eventSet = new Set<string>();
    const urlSet = new Set<string>();

    const addTicker = (value: string) => {
      const normalized = normalizeKalshiTicker(value);
      if (!normalized) return;
      tickerSet.add(normalized);
      const derived = deriveSeriesTickerFromMarket(normalized);
      if (derived) seriesSet.add(derived);
    };

    const addSeries = (value: string) => {
      const normalized = normalizeKalshiTicker(value);
      if (!normalized) return;
      seriesSet.add(normalized);
    };

    const addEvent = (value: string) => {
      const normalized = normalizeKalshiTicker(value);
      if (!normalized) return;
      eventSet.add(normalized);
      const derived = deriveSeriesTickerFromMarket(normalized);
      if (derived) seriesSet.add(derived);
    };

    const addUrl = (value: string) => {
      const parsed = parseKalshiMarketUrl(value);
      if (parsed.marketTicker) addTicker(parsed.marketTicker);
      if (parsed.seriesTicker) addSeries(parsed.seriesTicker);
      const trimmed = value.trim();
      if (trimmed) {
        urlSet.add(trimmed);
      }
    };

    for (const ticker of selection.tickers) {
      addTicker(ticker);
    }
    for (const series of selection.seriesTickers) {
      addSeries(series);
    }
    for (const event of selection.eventTickers) {
      addEvent(event);
    }
    for (const url of selection.marketUrls) {
      addUrl(url);
    }

    if (tickerSet.size === 0 && seriesSet.size === 0 && eventSet.size === 0) {
      return null;
    }

    return {
      tickers: Array.from(tickerSet),
      seriesTickers: Array.from(seriesSet),
      eventTickers: Array.from(eventSet),
      marketUrls: Array.from(urlSet),
      autoDiscover: selection.autoDiscover !== false,
    };
  }

  private maybeSwitchToProd(selection: KalshiSelection): void {
    if (selection.marketUrls.length === 0) return;
    const wantsProd = selection.marketUrls.some((value) => {
      try {
        const host = new URL(value).hostname.toLowerCase();
        return host.endsWith("kalshi.com");
      } catch {
        return false;
      }
    });
    if (!wantsProd) return;

    const base = this.kalshiConfig.baseUrl.toLowerCase();
    const ws = this.kalshiConfig.wsUrl.toLowerCase();
    const usingDemo =
      base.includes("demo-api.kalshi.co") || ws.includes("demo-api.kalshi.co");
    if (!usingDemo) return;

    this.logger.log(
      "DATA: Kalshi market URLs appear to be production while KALSHI_ENV is demo. Switching to production endpoints.",
      "WARN",
    );

    this.kalshiConfig = {
      ...this.kalshiConfig,
      baseUrl: KALSHI_PROD_BASE,
      wsUrl: KALSHI_PROD_WS,
    };
    this.kalshiClient = new KalshiClient(this.kalshiConfig);
  }

  private async fetchMarketCandidatesByTickers(
    tickers: string[],
  ): Promise<KalshiMarketCandidate[]> {
    const candidates: KalshiMarketCandidate[] = [];
    for (const ticker of tickers) {
      try {
        const market = await this.kalshiClient.getMarket(ticker);
        if (!market) continue;
        const record = market as unknown as Record<string, unknown>;
        const resolvedTicker = resolveMarketTicker(record) ?? normalizeKalshiTicker(ticker);
        if (!resolvedTicker) continue;
        const closeTimeMs =
          parseTimestamp(record.close_time ?? record.closeTime) ?? null;
        candidates.push({
          ticker: resolvedTicker,
          market: record,
          closeTimeMs,
        });
      } catch {
        // Ignore missing markets
      }
    }
    return candidates;
  }

  private async fetchMarketCandidatesByFilter(params: {
    seriesTicker?: string;
    eventTicker?: string;
    status?: string;
    minCloseTs?: number;
  }): Promise<KalshiMarketCandidate[]> {
    const candidates: KalshiMarketCandidate[] = [];
    let cursor: string | undefined;

    for (let page = 0; page < 2; page += 1) {
      try {
        const response = await this.kalshiClient.getMarkets({
          limit: 200,
          cursor,
          status: params.status,
          seriesTicker: params.seriesTicker,
          eventTicker: params.eventTicker,
          minCloseTs: params.minCloseTs,
        });
        for (const market of response.markets) {
          const record = market as unknown as Record<string, unknown>;
          const ticker = resolveMarketTicker(record);
          if (!ticker) continue;
          const closeTimeMs =
            parseTimestamp(record.close_time ?? record.closeTime) ?? null;
          candidates.push({ ticker, market: record, closeTimeMs });
        }
        if (!response.cursor) break;
        cursor = response.cursor;
      } catch {
        break;
      }
    }

    return candidates;
  }

  private pickBestCandidate(
    candidates: KalshiMarketCandidate[],
    now: number,
  ): KalshiMarketCandidate | null {
    if (candidates.length === 0) return null;

    const openCandidates = candidates.filter((candidate) =>
      isMarketOpen(candidate.market),
    );
    const pool = openCandidates.length > 0 ? openCandidates : candidates;

    let best: KalshiMarketCandidate | null = null;
    let bestDelta = Number.POSITIVE_INFINITY;
    for (const candidate of pool) {
      if (candidate.closeTimeMs === null) continue;
      const delta = candidate.closeTimeMs - now;
      if (delta < 0) continue;
      if (delta < bestDelta) {
        bestDelta = delta;
        best = candidate;
      }
    }

    return best ?? pool[0] ?? null;
  }

  private async discoverMarket(
    selection: KalshiSelection,
    now: number,
  ): Promise<KalshiMarketCandidate | null> {
    const nowSec = Math.floor(now / 1000);
    const candidates = new Map<string, KalshiMarketCandidate>();

    for (const seriesTicker of selection.seriesTickers) {
      let matches = await this.fetchMarketCandidatesByFilter({
        seriesTicker,
        status: "open",
        minCloseTs: nowSec,
      });
      if (matches.length === 0) {
        matches = await this.fetchMarketCandidatesByFilter({
          seriesTicker,
          minCloseTs: nowSec,
        });
      }
      for (const candidate of matches) {
        if (!candidates.has(candidate.ticker)) {
          candidates.set(candidate.ticker, candidate);
        }
      }
    }

    for (const eventTicker of selection.eventTickers) {
      let matches = await this.fetchMarketCandidatesByFilter({
        eventTicker,
        status: "open",
        minCloseTs: nowSec,
      });
      if (matches.length === 0) {
        matches = await this.fetchMarketCandidatesByFilter({
          eventTicker,
          minCloseTs: nowSec,
        });
      }
      for (const candidate of matches) {
        if (!candidates.has(candidate.ticker)) {
          candidates.set(candidate.ticker, candidate);
        }
      }
    }

    return this.pickBestCandidate(Array.from(candidates.values()), now);
  }

  private async initializeMarket(
    coin: CoinSymbol,
  ): Promise<KalshiMarketState | null> {
    const selection = this.buildSelection(coin);
    if (!selection) {
      this.logger.log(
        `DATA: no Kalshi market selectors configured for ${coin.toUpperCase()}`,
        "WARN",
      );
      return null;
    }

    this.maybeSwitchToProd(selection);

    const now = Date.now();
    const manualCandidates = await this.fetchMarketCandidatesByTickers(
      selection.tickers,
    );
    const manualChoice = this.pickBestCandidate(manualCandidates, now);
    let selectedCandidate: KalshiMarketCandidate | null = manualChoice;

    if (!manualChoice || !isMarketOpen(manualChoice.market)) {
      if (selection.autoDiscover) {
        const discovered = await this.discoverMarket(selection, now);
        if (discovered) {
          selectedCandidate = discovered;
        }
      }
    }

    if (!selectedCandidate) {
      this.logger.log(
        `DATA: Kalshi selection failed for ${coin.toUpperCase()} (tickers=${selection.tickers.length}, series=${selection.seriesTickers.length}, events=${selection.eventTickers.length})`,
        "WARN",
      );
      return null;
    }

    const selected = selectedCandidate.market;
    const selectedTicker =
      resolveMarketTicker(selected) ?? selectedCandidate.ticker;
    if (!selectedTicker) {
      return null;
    }

    let detailedMarket: Record<string, unknown> | null = null;
    try {
      const fetched = await this.kalshiClient.getMarket(selectedTicker);
      if (fetched) {
        detailedMarket = fetched as Record<string, unknown>;
      }
    } catch {
      detailedMarket = null;
    }

    const marketData = detailedMarket
      ? { ...selected, ...detailedMarket }
      : selected;

    const strike = parseStrikePrice(marketData);
    const underlyingValue = extractNumericValue(
      marketData.underlying_value ?? marketData.underlyingValue,
    );
    const underlyingTs = parseTimestamp(
      marketData.underlying_value_ts ?? marketData.underlyingValueTs,
    );
    const lastPrice = normalizeMarketPrice(
      marketData.last_price ?? marketData.lastPrice,
      marketData.last_price_dollars ?? marketData.lastPriceDollars,
    );
    const lastPriceTs = lastPrice !== null ? Date.now() : null;
    const closeTime =
      parseTimestamp(marketData.close_time ?? marketData.closeTime) ?? null;
    const outcomes = [
      String(marketData.yes_sub_title ?? "Yes"),
      String(marketData.no_sub_title ?? "No"),
    ];

    const coinMeta = COIN_CONFIG[coin];
    const eventTicker = resolveEventTicker(selected);
    const seriesTicker = resolveSeriesTicker(selected);

    let priceToBeat = strike;
    let referencePrice = strike > 0 ? strike : 0;
    let referenceSource: MarketSnapshot["referenceSource"] =
      strike > 0 ? "price_to_beat" : "missing";
    if (strike <= 0 && underlyingValue !== null && underlyingValue > 0) {
      referencePrice = underlyingValue;
      referenceSource = "kalshi_underlying";
    }

    const state: KalshiMarketState = {
      provider: "kalshi",
      coin,
      symbol: coinMeta.symbol,
      marketName: String(
        marketData.title ?? marketData.subtitle ?? selectedTicker,
      ),
      slug: selectedTicker,
      marketTicker: selectedTicker,
      eventTicker: eventTicker,
      seriesSlug: seriesTicker ?? null,
      timeLeftSec: closeTime ? (closeTime - now) / 1000 : null,
      priceToBeat,
      referencePrice,
      referenceSource,
      cryptoPrice: 0,
      cryptoPriceTimestamp: 0,
      kalshiUnderlyingValue: underlyingValue,
      kalshiUnderlyingTs: underlyingTs,
      kalshiLastPrice: lastPrice,
      kalshiLastPriceTs: lastPriceTs,
      dataStatus: "unknown",
      lastBookUpdateMs: 0,
      upOutcome: outcomes[0] ?? "Yes",
      downOutcome: outcomes[1] ?? "No",
      upTokenId: "YES",
      downTokenId: "NO",
      orderBooks: new Map(),
      bestBid: new Map(),
      bestAsk: new Map(),
      priceHistory: [],
      signals: undefined,
      closeTimeMs: closeTime,
      lastCryptoUpdateMs: 0,
      lastDataStatusMs: 0,
      selectedAtMs: now,
      lastReselectMs: 0,
      recentTrades: [],
      htmlReferenceUrls: selection.marketUrls,
      htmlReferencePending: false,
      htmlReferenceAttempts: 0,
      lastHtmlReferenceAttemptMs: 0,
      refRetryAttempts: 0,
      lastRefRetryMs: 0,
    };

    this.logger.log(
      `DATA: selected Kalshi ${coin.toUpperCase()} market ${selectedTicker}`,
    );

    this.maybeRefreshHtmlReference(state, Date.now());

    return state;
  }

  private connectKalshiWs(): void {
    const tickers: string[] = [];
    for (const state of this.states.values()) {
      tickers.push(state.marketTicker);
    }

    if (this.kalshiWs) {
      this.kalshiWs.disconnect();
    }

    this.kalshiWs = new KalshiMarketWS(
      this.kalshiConfig,
      (update) => this.handleOrderbook(update),
      (trade) => this.handleTrade(trade),
      (ticker) => this.handleTicker(ticker),
      () => {},
      (error: Error) => {
        this.logger.log(`DATA: Kalshi WS error ${error.message}`, "ERROR");
      },
      { silent: true },
    );

    this.kalshiWs.connect();
    if (tickers.length > 0) {
      this.kalshiWs.subscribe(tickers);
    }
  }

  private connectCryptoWs(): void {
    const symbols = Array.from(this.states.values()).map((state) => state.symbol);
    if (this.cryptoWs) {
      this.cryptoWs.disconnect();
    }

    this.cryptoWs = new CryptoWS(
      (payload: CryptoPricePayload) => this.handleCryptoPrice(payload),
      () => {},
      (error: Error) => {
        this.logger.log(`DATA: crypto WS error ${error.message}`, "ERROR");
      },
      // TODO: Replace Polymarket RTDS with Kalshi-native spot pricing.
      { source: "chainlink" },
    );

    this.cryptoWs.connect();
    if (symbols.length > 0) {
      this.cryptoWs.subscribe(symbols);
    }
  }

  private handleCryptoPrice(payload: CryptoPricePayload): void {
    const symbolKey = payload.symbol.toLowerCase();
    const coin = SYMBOL_TO_COIN[symbolKey];
    if (!coin) return;

    const state = this.states.get(coin);
    if (!state) return;

    state.cryptoPrice = payload.value;
    state.cryptoPriceTimestamp = payload.timestamp;
    state.lastCryptoUpdateMs = Date.now();
    state.priceHistory.push(payload.value);
    if (state.priceHistory.length > 180) {
      state.priceHistory.shift();
    }
  }

  private handleOrderbook(update: {
    marketTicker: string;
    yesBids: OrderBookLevel[];
    yesAsks: OrderBookLevel[];
    noBids: OrderBookLevel[];
    noAsks: OrderBookLevel[];
    timestampMs?: number;
  }): void {
    const coin = this.tickerToCoin.get(update.marketTicker);
    if (!coin) return;
    const state = this.states.get(coin);
    if (!state) return;

    const yesBook = buildOrderBookSnapshot(
      update.yesBids,
      update.yesAsks,
      0,
    );
    const noBook = buildOrderBookSnapshot(
      update.noBids,
      update.noAsks,
      0,
    );

    state.orderBooks.set("YES", yesBook);
    state.orderBooks.set("NO", noBook);

    if (update.yesBids[0]) {
      state.bestBid.set("YES", update.yesBids[0].price);
    }
    if (update.yesAsks[0]) {
      state.bestAsk.set("YES", update.yesAsks[0].price);
    }
    if (update.noBids[0]) {
      state.bestBid.set("NO", update.noBids[0].price);
    }
    if (update.noAsks[0]) {
      state.bestAsk.set("NO", update.noAsks[0].price);
    }

    state.lastBookUpdateMs = Date.now();
    this.updateDataStatus(state, Date.now());
  }

  private handleTrade(trade: {
    marketTicker: string;
    yesPrice: number | null;
    noPrice: number | null;
    count: number | null;
    takerSide?: string;
    timestampMs?: number;
  }): void {
    const coin = this.tickerToCoin.get(trade.marketTicker);
    if (!coin) return;
    const state = this.states.get(coin);
    if (!state) return;

    const timestamp = trade.timestampMs ?? Date.now();
    const size = trade.count ?? 0;

    if (trade.yesPrice !== null) {
      state.recentTrades.push({
        timestamp,
        price: trade.yesPrice,
        size,
        side: trade.takerSide?.toLowerCase() === "yes" ? "BUY" : undefined,
        tokenId: "YES",
      });
    }
    if (trade.noPrice !== null) {
      state.recentTrades.push({
        timestamp,
        price: trade.noPrice,
        size,
        side: trade.takerSide?.toLowerCase() === "no" ? "BUY" : undefined,
        tokenId: "NO",
      });
    }

    this.trimRecentTrades(state, Date.now());
  }

  private handleTicker(update: KalshiTickerUpdate): void {
    const coin = this.tickerToCoin.get(update.marketTicker);
    if (!coin) return;
    const state = this.states.get(coin);
    if (!state) return;

    if (update.price !== null) {
      state.kalshiLastPrice = update.price;
      state.kalshiLastPriceTs = update.timestampMs ?? Date.now();
    }
    if (update.yesBid !== null) {
      state.bestBid.set("YES", update.yesBid);
    }
    if (update.yesAsk !== null) {
      state.bestAsk.set("YES", update.yesAsk);
    }
  }

  private maybeRefreshKalshiReference(state: KalshiMarketState, now: number): void {
    if (state.priceToBeat > 0) return;
    if (state.kalshiUnderlyingValue && state.kalshiUnderlyingValue > 0) return;
    if (now - state.selectedAtMs > KALSHI_REF_RETRY_WINDOW_MS) return;

    if (state.lastRefRetryMs > 0) {
      const attempt = Math.max(0, state.refRetryAttempts);
      const backoff = Math.min(
        KALSHI_REF_RETRY_MAX_MS,
        KALSHI_REF_RETRY_BASE_MS * 2 ** Math.max(0, attempt - 1),
      );
      if (now - state.lastRefRetryMs < backoff) {
        return;
      }
    }

    this.tryRefreshKalshiReference(state);
  }

  private tryRefreshKalshiReference(state: KalshiMarketState): void {
    state.lastRefRetryMs = Date.now();
    state.refRetryAttempts += 1;

    this.kalshiClient
      .getMarket(state.marketTicker)
      .then((market) => {
        if (!market) return;
        const record = market as Record<string, unknown>;
        const strike = parseStrikePrice(record);
        const underlyingValue = extractNumericValue(
          record.underlying_value ?? record.underlyingValue,
        );
        const underlyingTs = parseTimestamp(
          record.underlying_value_ts ?? record.underlyingValueTs,
        );

        if (strike > 0) {
          state.priceToBeat = strike;
          state.referencePrice = strike;
          state.referenceSource = "price_to_beat";
        } else if (underlyingValue && underlyingValue > 0) {
          state.referencePrice = underlyingValue;
          state.referenceSource = "kalshi_underlying";
        }

        if (underlyingValue && underlyingValue > 0) {
          state.kalshiUnderlyingValue = underlyingValue;
          state.kalshiUnderlyingTs = underlyingTs ?? Date.now();
        }
      })
      .catch(() => {});
  }

  private maybeRefreshHtmlReference(state: KalshiMarketState, now: number): void {
    if (state.priceToBeat > 0) return;
    if (state.kalshiUnderlyingValue && state.kalshiUnderlyingValue > 0) return;
    if (state.htmlReferencePending) return;
    if (state.htmlReferenceUrls.length === 0) return;

    if (state.lastHtmlReferenceAttemptMs > 0) {
      const attempt = Math.max(0, state.htmlReferenceAttempts);
      const backoff = Math.min(
        KALSHI_HTML_REF_RETRY_MAX_MS,
        KALSHI_HTML_REF_RETRY_BASE_MS * 2 ** Math.max(0, attempt - 1),
      );
      if (now - state.lastHtmlReferenceAttemptMs < backoff) {
        return;
      }
    }

    this.tryFetchHtmlReference(state);
  }

  private tryFetchHtmlReference(state: KalshiMarketState): void {
    if (state.priceToBeat > 0) return;
    if (state.kalshiUnderlyingValue && state.kalshiUnderlyingValue > 0) return;
    if (state.htmlReferencePending) return;
    if (state.htmlReferenceUrls.length === 0) return;

    state.htmlReferencePending = true;
    state.lastHtmlReferenceAttemptMs = Date.now();
    state.htmlReferenceAttempts += 1;

    fetchKalshiHtmlReference({
      urls: state.htmlReferenceUrls,
      timeoutMs: KALSHI_HTML_REF_TIMEOUT_MS,
    })
      .then((result) => {
        state.htmlReferencePending = false;
        if (!result) {
          this.logger.log(
            `DATA: Kalshi HTML reference fetch failed (attempt ${state.htmlReferenceAttempts})`,
            "WARN",
          );
          return;
        }

        let updated = false;
        if (result.strikePrice && result.strikePrice > 0) {
          state.priceToBeat = result.strikePrice;
          state.referencePrice = result.strikePrice;
          state.referenceSource = "kalshi_html";
          updated = true;
        }

        if (!state.kalshiUnderlyingValue && result.underlyingValue) {
          state.kalshiUnderlyingValue = result.underlyingValue;
          state.kalshiUnderlyingTs = Date.now();
          if (state.referenceSource === "missing") {
            state.referencePrice = result.underlyingValue;
            state.referenceSource = "kalshi_underlying";
          }
        }

        if (updated) {
          this.logger.log(
            `DATA: Kalshi HTML reference price set ${result.strikePrice?.toFixed(
              2,
            )}`,
          );
        }
      })
      .catch(() => {
        state.htmlReferencePending = false;
        this.logger.log(
          `DATA: Kalshi HTML reference fetch errored (attempt ${state.htmlReferenceAttempts})`,
          "WARN",
        );
      });
  }

  private tick(): void {
    const now = Date.now();
    for (const state of this.states.values()) {
      this.maybeRefreshKalshiReference(state, now);
      this.maybeRefreshHtmlReference(state, now);
      state.timeLeftSec = state.closeTimeMs ? (state.closeTimeMs - now) / 1000 : null;
      this.updateDataStatus(state, now);
      this.trimRecentTrades(state, now);
      state.signals = computeSignals(state, now, state.recentTrades, {
        depthLevels: SIGNAL_DEPTH_LEVELS,
        slippageNotional: SIGNAL_SLIPPAGE_NOTIONAL,
        tradeWindowMs: SIGNAL_TRADE_WINDOW_MS,
      });

      if (
        state.timeLeftSec !== null &&
        state.timeLeftSec <= 0 &&
        now - state.lastReselectMs >= MARKET_RESELECT_COOLDOWN_MS
      ) {
        state.lastReselectMs = now;
        this.rotateMarket(state.coin).catch(() => {});
        continue;
      }

      if (
        state.dataStatus === "stale" &&
        now - state.selectedAtMs >= MARKET_RESELECT_MS &&
        now - state.lastReselectMs >= MARKET_RESELECT_COOLDOWN_MS
      ) {
        state.lastReselectMs = now;
        this.logger.log(
          `DATA: Kalshi reselecting ${state.coin.toUpperCase()} (stale data)`,
          "WARN",
        );
        this.rotateMarket(state.coin).catch(() => {});
      }
    }
  }

  private trimRecentTrades(state: KalshiMarketState, now: number): void {
    const cutoff = now - SIGNAL_TRADE_WINDOW_MS;
    while (state.recentTrades.length > 0) {
      const first = state.recentTrades[0];
      if (!first || first.timestamp >= cutoff) break;
      state.recentTrades.shift();
    }
  }

  private updateDataStatus(state: KalshiMarketState, now: number): void {
    const bookFresh = now - state.lastBookUpdateMs <= BOOK_STALE_MS;
    const priceFresh = now - state.lastCryptoUpdateMs <= PRICE_STALE_MS;
    const hasAnyData = state.lastBookUpdateMs > 0 || state.lastCryptoUpdateMs > 0;

    let nextStatus: KalshiMarketState["dataStatus"] = "unknown";
    if (bookFresh && priceFresh) {
      nextStatus = "healthy";
    } else if (!hasAnyData && now - state.selectedAtMs < DATA_STARTUP_GRACE_MS) {
      nextStatus = "unknown";
    } else {
      nextStatus = "stale";
    }

    if (nextStatus !== state.dataStatus) {
      state.dataStatus = nextStatus;
      state.lastDataStatusMs = now;
    }
  }

  private async rotateMarket(coin: CoinSymbol): Promise<void> {
    const next = await this.initializeMarket(coin);
    if (!next) {
      this.logger.log(
        `DATA: Kalshi reselect failed for ${coin.toUpperCase()}`,
        "WARN",
      );
      return;
    }

    const current = this.states.get(coin);
    if (current) {
      this.tickerToCoin.delete(current.marketTicker);
    }

    this.states.set(coin, next);
    this.tickerToCoin.set(next.marketTicker, coin);
    this.logger.log(
      `DATA: Kalshi reselected ${coin.toUpperCase()} -> ${next.marketTicker}`,
    );

    if (this.kalshiWs) {
      this.kalshiWs.subscribe([next.marketTicker]);
    } else {
      this.connectKalshiWs();
    }

    if (this.cryptoWs) {
      this.cryptoWs.subscribe([next.symbol]);
    } else {
      this.connectCryptoWs();
    }
  }
}
