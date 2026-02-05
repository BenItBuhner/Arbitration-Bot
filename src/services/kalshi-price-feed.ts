/**
 * Kalshi first-party price feed: polls Trade API (getMarket last_price + candlesticks),
 * normalizes to numeric price (0–1 for market odds) + timestamp, with backoff/retry and cache TTL.
 */

import type { KalshiEnvConfig } from "../clients/kalshi/kalshi-config";
import { KalshiClient } from "../clients/kalshi/kalshi-client";
import { MarketApi, Configuration } from "kalshi-typescript";

const CACHE_TTL_MS = 30_000;
const POLL_INTERVAL_MS = 30_000;
const CANDLE_PERIOD_MIN = 1;
const CANDLE_HISTORY_SEC = 3600;
const MAX_RETRIES = 3;
const RETRY_BASE_MS = 2000;

export interface KalshiPricePoint {
  price: number;
  ts: number;
}

export interface KalshiPriceFeedSnapshot {
  price: number;
  ts: number;
  history: KalshiPricePoint[];
}

function extractCents(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const n = Number(value.replace(/[$,%]/g, "").trim());
    return Number.isFinite(n) ? n : null;
  }
  if (value && typeof value === "object") {
    const r = value as Record<string, unknown>;
    return extractCents(r.value ?? r.display_value ?? r.close ?? r.open);
  }
  return null;
}

/** Normalize market price: cents (1–99) → 0–1; dollars → as-is if already 0–1. */
function normalizePrice(centsOrDollars: number): number {
  if (centsOrDollars > 1.5) return centsOrDollars / 100;
  return Math.max(0, Math.min(1, centsOrDollars));
}

export class KalshiPriceFeed {
  private client: KalshiClient;
  private marketApi: MarketApi;
  private marketTicker: string;
  private seriesTicker: string;
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private lastPrice: number | null = null;
  private lastTs: number = 0;
  private history: KalshiPricePoint[] = [];
  private lastFetchMs: number = 0;
  private retries: number = 0;

  constructor(
    config: KalshiEnvConfig,
    marketTicker: string,
    seriesTicker: string,
  ) {
    this.marketTicker = marketTicker;
    this.seriesTicker = seriesTicker;
    this.client = new KalshiClient(config);
    const kalshiConfig = new Configuration({
      apiKey: config.apiKey,
      privateKeyPath: config.privateKeyPath,
      privateKeyPem: config.privateKeyPem,
      basePath: config.baseUrl,
    });
    this.marketApi = new MarketApi(kalshiConfig);
  }

  /** Latest normalized price and timestamp. */
  getLatest(): { price: number; ts: number } | null {
    if (this.lastPrice === null) return null;
    return { price: this.lastPrice, ts: this.lastTs };
  }

  /** Recent history (newest last). Caller may cap length. */
  getHistory(): KalshiPricePoint[] {
    return [...this.history];
  }

  /** Full snapshot for UI/hub. */
  getSnapshot(): KalshiPriceFeedSnapshot | null {
    const latest = this.getLatest();
    if (!latest) return null;
    return {
      price: latest.price,
      ts: latest.ts,
      history: this.getHistory(),
    };
  }

  /** Returns true if cache is still valid (within TTL). */
  isCacheFresh(): boolean {
    return Date.now() - this.lastFetchMs < CACHE_TTL_MS;
  }

  /** Fetch once: getMarket last_price + candlesticks, update cache. */
  async refresh(): Promise<void> {
    const backoffMs = RETRY_BASE_MS * Math.pow(2, Math.min(this.retries, MAX_RETRIES));
    const doFetch = async (): Promise<void> => {
      const market = await this.client.getMarket(this.marketTicker);
      const rawLast = extractCents((market as any)?.last_price)
        ?? extractCents((market as any)?.last_price_dollars);
      if (rawLast !== null && Number.isFinite(rawLast) && rawLast >= 0) {
        const normalized = normalizePrice(rawLast);
        if (Number.isFinite(normalized) && normalized >= 0 && normalized <= 1) {
          this.lastPrice = normalized;
          const ts = (market as any)?.last_price_ts ?? (market as any)?.last_trade_ts;
          this.lastTs = typeof ts === "number" && ts > 0
            ? (ts >= 1e12 ? ts : ts * 1000)
            : Date.now();
        }
      }

      const endTs = Math.floor(Date.now() / 1000);
      const startTs = endTs - CANDLE_HISTORY_SEC;
      try {
        const resp = await this.marketApi.getMarketCandlesticks(
          this.seriesTicker,
          this.marketTicker,
          startTs,
          endTs,
          CANDLE_PERIOD_MIN,
          false,
        );
        const payload = (resp as any)?.data ?? (resp as any)?.body ?? resp;
        const candles = payload?.candlesticks ?? [];
        if (Array.isArray(candles)) {
          const points: KalshiPricePoint[] = [];
          for (const c of candles) {
            const priceDist = c?.price;
            const close = priceDist?.close ?? extractCents(c?.close);
            const ts = c?.end_period_ts;
            if (close != null && Number.isFinite(ts)) {
              points.push({ price: normalizePrice(close), ts: ts >= 1e12 ? ts : ts * 1000 });
            }
          }
          this.history = points;
          if (points.length > 0 && this.lastPrice === null) {
            const last = points[points.length - 1];
            if (last) {
              this.lastPrice = last.price;
              this.lastTs = last.ts;
            }
          }
        }
      } catch {
        // Keep previous history; last_price still updated from getMarket
      }

      this.lastFetchMs = Date.now();
      this.retries = 0;
    };

    try {
      await doFetch();
    } catch (e) {
      this.retries += 1;
      await new Promise((r) => setTimeout(r, backoffMs));
      try {
        await doFetch();
      } catch {
        // Leave lastPrice/lastTs as-is if we had them
      }
    }
  }

  /** Start background polling. */
  start(): void {
    if (this.intervalId != null) return;
    this.refresh();
    this.intervalId = setInterval(() => {
      if (!this.isCacheFresh()) this.refresh();
    }, POLL_INTERVAL_MS);
  }

  /** Stop polling. */
  stop(): void {
    if (this.intervalId != null) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }
}
