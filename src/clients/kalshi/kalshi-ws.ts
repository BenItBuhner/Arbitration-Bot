import type { KalshiEnvConfig } from "./kalshi-config";
import { createKalshiAuthHeaders, loadPrivateKeyPem } from "./kalshi-auth";

export interface KalshiWsConfig {
  reconnectAttempts?: number;
  reconnectDelayMs?: number;
  silent?: boolean;
  debug?: boolean;
}

export interface KalshiOrderbookLevel {
  price: number;
  size: number;
}

export interface KalshiOrderbookUpdate {
  marketTicker: string;
  yesBids: KalshiOrderbookLevel[];
  yesAsks: KalshiOrderbookLevel[];
  noBids: KalshiOrderbookLevel[];
  noAsks: KalshiOrderbookLevel[];
  timestampMs?: number;
}

export interface KalshiTradeUpdate {
  marketTicker: string;
  yesPrice: number | null;
  noPrice: number | null;
  count: number | null;
  takerSide?: string;
  timestampMs?: number;
}

export interface KalshiTickerUpdate {
  marketTicker: string;
  price: number | null;
  yesBid: number | null;
  yesAsk: number | null;
  timestampMs?: number;
}

export type KalshiOrderbookCallback = (update: KalshiOrderbookUpdate) => void;
export type KalshiTradeCallback = (update: KalshiTradeUpdate) => void;
export type KalshiTickerCallback = (update: KalshiTickerUpdate) => void;
export type KalshiConnectionCallback = (connected: boolean) => void;
export type KalshiErrorCallback = (error: Error) => void;

const DEFAULT_CONFIG: KalshiWsConfig = {
  reconnectAttempts: 5,
  reconnectDelayMs: 3000,
  silent: false,
  debug: false,
};

type SideKey = "yes" | "no";

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseTimestampMs(value: unknown): number | undefined {
  const numeric = toNumber(value);
  if (numeric === null) return undefined;
  if (numeric >= 1e14) return Math.floor(numeric);
  if (numeric >= 1e12) return Math.floor(numeric);
  return Math.floor(numeric * 1000);
}

function normalizePriceDollars(
  cents: unknown,
  dollars: unknown,
): number | null {
  const dollarsValue = toNumber(dollars);
  if (dollarsValue !== null) {
    return dollarsValue;
  }
  const centsValue = toNumber(cents);
  if (centsValue === null) return null;
  return centsValue / 100;
}

function normalizeLevels(raw: unknown, useDollars: boolean): KalshiOrderbookLevel[] {
  if (!Array.isArray(raw)) return [];
  const levels: KalshiOrderbookLevel[] = [];
  for (const entry of raw) {
    if (!Array.isArray(entry) || entry.length < 2) continue;
    const priceRaw = entry[0];
    const sizeRaw = entry[1];
    const price = useDollars ? toNumber(priceRaw) : normalizePriceDollars(priceRaw, null);
    const size = toNumber(sizeRaw);
    if (price === null || size === null) continue;
    levels.push({ price, size });
  }
  return levels;
}

function buildSyntheticAsks(
  oppositeBids: KalshiOrderbookLevel[],
): KalshiOrderbookLevel[] {
  const asks = oppositeBids
    .map((level) => ({
      price: Math.max(0, Math.min(1, 1 - level.price)),
      size: level.size,
    }))
    .sort((a, b) => a.price - b.price);
  return asks;
}

class KalshiOrderbookState {
  private yes = new Map<number, number>();
  private no = new Map<number, number>();

  applySnapshot(yesLevels: KalshiOrderbookLevel[], noLevels: KalshiOrderbookLevel[]): void {
    this.yes.clear();
    this.no.clear();
    for (const level of yesLevels) {
      this.yes.set(level.price, level.size);
    }
    for (const level of noLevels) {
      this.no.set(level.price, level.size);
    }
  }

  applyDelta(side: SideKey, price: number, delta: number): void {
    const book = side === "yes" ? this.yes : this.no;
    const current = book.get(price) ?? 0;
    const next = current + delta;
    if (next <= 0) {
      book.delete(price);
    } else {
      book.set(price, next);
    }
  }

  getLevels(side: SideKey): KalshiOrderbookLevel[] {
    const book = side === "yes" ? this.yes : this.no;
    return Array.from(book.entries())
      .map(([price, size]) => ({ price, size }))
      .sort((a, b) => b.price - a.price);
  }

  toUpdate(marketTicker: string, timestampMs?: number): KalshiOrderbookUpdate {
    const yesBids = this.getLevels("yes");
    const noBids = this.getLevels("no");
    return {
      marketTicker,
      yesBids,
      yesAsks: buildSyntheticAsks(noBids),
      noBids,
      noAsks: buildSyntheticAsks(yesBids),
      timestampMs,
    };
  }
}

export class KalshiMarketWS {
  private ws: WebSocket | null = null;
  private config: KalshiWsConfig;
  private reconnectAttempts = 0;
  private isManuallyClosed = false;
  private subscriptions: Set<string> = new Set();
  private orderbooks: Map<string, KalshiOrderbookState> = new Map();
  private privateKeyPem: string;

  private onOrderbook: KalshiOrderbookCallback;
  private onTrade: KalshiTradeCallback;
  private onTicker: KalshiTickerCallback;
  private onConnectionChange: KalshiConnectionCallback;
  private onError: KalshiErrorCallback;

  constructor(
    private kalshiConfig: KalshiEnvConfig,
    onOrderbook: KalshiOrderbookCallback = () => {},
    onTrade: KalshiTradeCallback = () => {},
    onTicker: KalshiTickerCallback = () => {},
    onConnectionChange: KalshiConnectionCallback = () => {},
    onError: KalshiErrorCallback = () => {},
    config: KalshiWsConfig = {},
  ) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.onOrderbook = onOrderbook;
    this.onTrade = onTrade;
    this.onTicker = onTicker;
    this.onConnectionChange = onConnectionChange;
    this.onError = onError;
    try {
      this.privateKeyPem =
        this.kalshiConfig.privateKeyPem ??
        (this.kalshiConfig.privateKeyPath
          ? loadPrivateKeyPem(this.kalshiConfig.privateKeyPath)
          : "");
    } catch {
      this.privateKeyPem = "";
    }
  }

  connect(): void {
    if (!this.privateKeyPem) {
      this.onError(new Error("Kalshi private key is missing."));
      return;
    }

    let wsPath = "/trade-api/ws/v2";
    try {
      wsPath = new URL(this.kalshiConfig.wsUrl).pathname || wsPath;
    } catch {
      // Keep default path
    }

    const headers = createKalshiAuthHeaders({
      apiKey: this.kalshiConfig.apiKey,
      privateKeyPem: this.privateKeyPem,
      method: "GET",
      path: wsPath,
    });

    try {
      const ws = new (WebSocket as any)(this.kalshiConfig.wsUrl, {
        headers,
      });
      this.ws = ws;

      ws.onopen = () => {
        this.reconnectAttempts = 0;
        this.onConnectionChange(true);
        if (this.subscriptions.size > 0) {
          this.sendSubscribe(Array.from(this.subscriptions));
        }
      };

      ws.onmessage = (event: any) => {
        const data = typeof event.data === "string" ? event.data : "";
        if (!data) return;
        try {
          const payload = JSON.parse(data);
          this.handleMessage(payload);
        } catch (error) {
          if (this.config.debug && !this.config.silent) {
            console.error("Kalshi WS parse error:", error);
          }
        }
      };

      ws.onclose = () => {
        this.onConnectionChange(false);
        if (!this.isManuallyClosed) {
          this.attemptReconnect();
        }
      };

      ws.onerror = () => {
        this.onError(new Error("Kalshi WebSocket error"));
      };
    } catch (error) {
      this.onError(error instanceof Error ? error : new Error(String(error)));
    }
  }

  disconnect(): void {
    this.isManuallyClosed = true;
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.subscriptions.clear();
    this.orderbooks.clear();
  }

  subscribe(
    marketTickers: string[],
    channels: string[] = ["orderbook_delta", "trade", "ticker"],
  ): void {
    for (const ticker of marketTickers) {
      if (ticker) {
        this.subscriptions.add(ticker);
      }
    }
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendSubscribe(marketTickers, channels);
    }
  }

  private sendSubscribe(
    marketTickers: string[],
    channels: string[] = ["orderbook_delta", "trade", "ticker"],
  ): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const params: Record<string, unknown> = {
      channels,
    };
    if (marketTickers.length === 1) {
      params.market_ticker = marketTickers[0];
    } else {
      params.market_tickers = marketTickers;
    }
    const payload = {
      id: Date.now(),
      cmd: "subscribe",
      params,
    };
    this.ws.send(JSON.stringify(payload));
  }

  private attemptReconnect(): void {
    const maxAttempts = this.config.reconnectAttempts ?? 5;
    if (this.reconnectAttempts >= maxAttempts) {
      this.onError(new Error("Kalshi WebSocket max reconnection attempts reached"));
      return;
    }
    const delay = (this.config.reconnectDelayMs ?? 3000) * Math.pow(2, this.reconnectAttempts);
    this.reconnectAttempts += 1;
    setTimeout(() => {
      if (!this.isManuallyClosed) {
        this.connect();
      }
    }, Math.min(delay, 30000));
  }

  private handleMessage(payload: any): void {
    if (!payload || typeof payload !== "object") return;
    const type = payload.type ?? payload.msg?.type ?? payload.data?.type;
    if (!type) return;

    if (this.config.debug && !this.config.silent) {
      console.log("Kalshi WS event:", JSON.stringify(payload));
    }

    if (type === "orderbook_snapshot" || type === "orderbook_delta" || type === "orderbook_update") {
      this.handleOrderbook(payload);
      return;
    }

    if (type === "trade" || type === "trades") {
      this.handleTrade(payload);
      return;
    }

    if (type === "ticker" || type === "ticker_v2") {
      this.handleTicker(payload);
    }
  }

  private handleOrderbook(payload: any): void {
    const data = payload.msg ?? payload.data ?? payload;
    const ticker: string | undefined = data?.market_ticker ?? data?.marketTicker;
    if (!ticker) return;

    let state = this.orderbooks.get(ticker);
    if (!state) {
      state = new KalshiOrderbookState();
      this.orderbooks.set(ticker, state);
    }

    if (payload.type === "orderbook_snapshot") {
      const yesLevels =
        data?.yes_dollars ??
        data?.yes_dollars_fp ??
        data?.yes ??
        [];
      const noLevels =
        data?.no_dollars ??
        data?.no_dollars_fp ??
        data?.no ??
        [];
      const yes = normalizeLevels(yesLevels, data?.yes_dollars != null || data?.yes_dollars_fp != null);
      const no = normalizeLevels(noLevels, data?.no_dollars != null || data?.no_dollars_fp != null);
      state.applySnapshot(yes, no);
      const ts = parseTimestampMs(data?.ts);
      this.onOrderbook(state.toUpdate(ticker, ts));
      return;
    }

    if (payload.type === "orderbook_delta" || payload.type === "orderbook_update") {
      const sideRaw = String(data?.side ?? "").toLowerCase();
      const side = sideRaw === "no" ? "no" : "yes";
      const price = normalizePriceDollars(data?.price, data?.price_dollars);
      const delta = toNumber(data?.delta);
      if (price === null || delta === null) return;
      state.applyDelta(side, price, delta);
      const ts = parseTimestampMs(data?.ts);
      this.onOrderbook(state.toUpdate(ticker, ts));
    }
  }

  private handleTrade(payload: any): void {
    const data = payload.msg ?? payload.data ?? payload;
    const ticker: string | undefined = data?.market_ticker ?? data?.marketTicker;
    if (!ticker) return;
    const yesPrice = normalizePriceDollars(data?.yes_price, data?.yes_price_dollars);
    const noPrice = normalizePriceDollars(data?.no_price, data?.no_price_dollars);
    const count = toNumber(data?.count);
    const ts = parseTimestampMs(data?.ts);
    const takerSide = typeof data?.taker_side === "string" ? data.taker_side : undefined;

    this.onTrade({
      marketTicker: ticker,
      yesPrice,
      noPrice,
      count,
      takerSide,
      timestampMs: ts,
    });
  }

  private handleTicker(payload: any): void {
    const data = payload.msg ?? payload.data ?? payload;
    const ticker: string | undefined = data?.market_ticker ?? data?.marketTicker;
    if (!ticker) return;

    const price = normalizePriceDollars(data?.price, data?.price_dollars);
    const yesBid = normalizePriceDollars(data?.yes_bid, data?.yes_bid_dollars);
    const yesAsk = normalizePriceDollars(data?.yes_ask, data?.yes_ask_dollars);
    const ts = parseTimestampMs(data?.ts);

    this.onTicker({
      marketTicker: ticker,
      price,
      yesBid,
      yesAsk,
      timestampMs: ts,
    });
  }
}

export { KalshiOrderbookState, buildSyntheticAsks, normalizeLevels, normalizePriceDollars };
