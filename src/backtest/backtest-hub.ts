import type { CoinSymbol } from "../services/auto-market";
import type {
  MarketSnapshot,
  OrderBookLevel,
  OrderBookSnapshot,
} from "../services/market-data-hub";
import type {
  BacktestCryptoTick,
  BacktestMarketMeta,
  BacktestTradeEvent,
  BacktestTradeLevel,
} from "./types";
import { MinHeap } from "./min-heap";
import { JsonlSyncReader } from "./jsonl-stream";
import { computeSignals } from "../services/market-signals";

const BOOK_STALE_MS = 10000;
const PRICE_HISTORY_LIMIT = 180;
const SIGNAL_DEPTH_LEVELS = 3;
const SIGNAL_SLIPPAGE_NOTIONAL = 50;
const SIGNAL_TRADE_WINDOW_MS = 5 * 60 * 1000;

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
  return Number.isFinite(parsed)
    ? Math.max(minValue, Math.floor(parsed))
    : defaultValue;
}

interface BacktestEventEntry {
  type: "tick" | "market" | "trade";
  coin: CoinSymbol;
  slug?: string;
}

interface TradeRange {
  minTs: number;
  maxTs: number;
}

interface TickRange {
  minTs: number;
  maxTs: number;
}

interface BacktestMarketState extends MarketSnapshot {
  marketStartMs: number;
  marketEndMs: number;
  lastBookUpdateMs: number;
  selectedAtMs: number;
  lastPriceUpdateMs: number;
  recentTrades: BacktestTradeEvent[];
}

interface BacktestInputs {
  marketsByCoin: Map<CoinSymbol, BacktestMarketMeta[]>;
  tradeFilesBySlug: Map<string, string>;
  cryptoTickFilesByCoin: Map<CoinSymbol, string>;
  tradeRangesBySlug?: Map<string, TradeRange>;
  tickRangesByCoin?: Map<CoinSymbol, TickRange>;
  latencyMs: number;
}

export class BacktestHub {
  private marketsByCoin: Map<CoinSymbol, BacktestMarketMeta[]>;
  private tradeFilesBySlug: Map<string, string>;
  private cryptoTickFilesByCoin: Map<CoinSymbol, string>;
  private tradeRangesBySlug?: Map<string, TradeRange>;
  private tickRangesByCoin?: Map<CoinSymbol, TickRange>;
  private latencyMs: number;
  private states: Map<CoinSymbol, BacktestMarketState> = new Map();
  private marketIndexByCoin: Map<CoinSymbol, number> = new Map();
  private activeSlugByCoin: Map<CoinSymbol, string> = new Map();
  private tradeStreamsBySlug: Map<string, JsonlSyncReader<BacktestTradeEvent>> =
    new Map();
  private tickStreamsByCoin: Map<CoinSymbol, JsonlSyncReader<BacktestCryptoTick>> =
    new Map();
  private finishedTradeSlugs: Set<string> = new Set();
  private currentTimeMs = 0;
  private minTimeMs = 0;
  private maxTimeMs = 0;
  private eventQueue = new MinHeap<BacktestEventEntry>();
  private dirtyCoins: Set<CoinSymbol> = new Set();
  private useEventQueue = parseEnvFlag("BACKTEST_EVENT_QUEUE", true);

  constructor(inputs: BacktestInputs) {
    this.marketsByCoin = inputs.marketsByCoin;
    this.tradeFilesBySlug = inputs.tradeFilesBySlug;
    this.cryptoTickFilesByCoin = inputs.cryptoTickFilesByCoin;
    this.tradeRangesBySlug = inputs.tradeRangesBySlug;
    this.tickRangesByCoin = inputs.tickRangesByCoin;
    this.latencyMs = inputs.latencyMs;
    this.computeBounds();
    if (this.useEventQueue) {
      this.initEventQueue();
    }
  }

  getSnapshots(): Map<CoinSymbol, MarketSnapshot> {
    return new Map(this.states);
  }

  getSnapshot(coin: CoinSymbol): MarketSnapshot | null {
    return this.states.get(coin) ?? null;
  }

  getCurrentTimeMs(): number {
    return this.currentTimeMs;
  }

  getStartTimeMs(): number {
    return this.minTimeMs;
  }

  getEndTimeMs(): number {
    return this.maxTimeMs;
  }

  getNextEventTime(): number | null {
    if (this.useEventQueue) {
      const next = this.eventQueue.peek();
      return next ? next.time : null;
    }

    let next: number | null = null;

    for (const [coin] of this.cryptoTickFilesByCoin.entries()) {
      const state = this.states.get(coin);
      if (!state || this.currentTimeMs > state.marketEndMs) continue;
      const stream = this.getTickStream(coin);
      if (!stream) continue;
      const tick = stream.peek();
      if (tick) {
        const tickTime = tick.timestamp + this.latencyMs;
        next = next === null ? tickTime : Math.min(next, tickTime);
      }
    }

    for (const [coin, markets] of this.marketsByCoin.entries()) {
      const marketIdx = this.marketIndexByCoin.get(coin) ?? 0;
      if (marketIdx < markets.length) {
        const marketStart = markets[marketIdx]!.startMs;
        next = next === null ? marketStart : Math.min(next, marketStart);
      }
    }

    for (const state of this.states.values()) {
      const stream = this.getTradeStream(state.slug);
      if (!stream) continue;
      const trade = stream.peek();
      if (trade) {
        next = next === null ? trade.timestamp : Math.min(next, trade.timestamp);
      }
    }

    return next;
  }

  advanceTo(timeMs: number): void {
    this.currentTimeMs = timeMs;
    this.activateMarkets(timeMs);
    this.processCryptoTicks(timeMs);
    this.processTrades(timeMs);
    this.updateSnapshots(timeMs);
  }

  drainDirtyCoins(): Set<CoinSymbol> {
    const dirty = new Set(this.dirtyCoins);
    this.dirtyCoins.clear();
    return dirty;
  }

  close(): void {
    for (const stream of this.tradeStreamsBySlug.values()) {
      stream.close();
    }
    for (const stream of this.tickStreamsByCoin.values()) {
      stream.close();
    }
    this.tradeStreamsBySlug.clear();
    this.tickStreamsByCoin.clear();
  }

  private markDirty(coin: CoinSymbol): void {
    this.dirtyCoins.add(coin);
  }

  private getTickStream(coin: CoinSymbol): JsonlSyncReader<BacktestCryptoTick> | null {
    const existing = this.tickStreamsByCoin.get(coin);
    if (existing) return existing;
    const filePath = this.cryptoTickFilesByCoin.get(coin);
    if (!filePath) return null;

    const bufferLines = parseEnvNumber(
      "BACKTEST_STREAM_TICK_BUFFER_LINES",
      5000,
      100,
    );
    const chunkSize = parseEnvNumber(
      "BACKTEST_STREAM_CHUNK_BYTES",
      1 << 20,
      4096,
    );

    const stream = new JsonlSyncReader<BacktestCryptoTick>(filePath, {
      bufferLines,
      chunkSize,
      parse: (line) => {
        const record = JSON.parse(line) as BacktestCryptoTick;
        if (!record || typeof record !== "object") return null;
        const timestamp = Number(record.timestamp);
        const value = Number(record.value);
        if (!Number.isFinite(timestamp) || !Number.isFinite(value)) return null;
        if (record.timestamp !== timestamp) record.timestamp = timestamp;
        if (record.value !== value) record.value = value;
        return record;
      },
      onError: (error) => {
        console.warn(
          `[backtest] Failed to parse tick line for ${coin.toUpperCase()}: ${error.message}`,
        );
      },
    });

    this.tickStreamsByCoin.set(coin, stream);
    return stream;
  }

  private getTradeStream(slug: string): JsonlSyncReader<BacktestTradeEvent> | null {
    if (this.finishedTradeSlugs.has(slug)) return null;
    const existing = this.tradeStreamsBySlug.get(slug);
    if (existing) return existing;
    const filePath = this.tradeFilesBySlug.get(slug);
    if (!filePath) return null;

    const bufferLines = parseEnvNumber(
      "BACKTEST_STREAM_TRADE_BUFFER_LINES",
      2000,
      100,
    );
    const chunkSize = parseEnvNumber(
      "BACKTEST_STREAM_CHUNK_BYTES",
      1 << 20,
      4096,
    );

    const stream = new JsonlSyncReader<BacktestTradeEvent>(filePath, {
      bufferLines,
      chunkSize,
      parse: (line) => {
        const record = JSON.parse(line) as BacktestTradeEvent;
        if (!record || typeof record !== "object") return null;
        const timestamp = Number(record.timestamp);
        if (!Number.isFinite(timestamp)) return null;
        if (record.timestamp !== timestamp) record.timestamp = timestamp;
        return record;
      },
      onError: (error) => {
        console.warn(
          `[backtest] Failed to parse trade line for ${slug}: ${error.message}`,
        );
      },
    });

    this.tradeStreamsBySlug.set(slug, stream);
    return stream;
  }

  private closeTradeStream(slug: string, markFinished: boolean): void {
    const stream = this.tradeStreamsBySlug.get(slug);
    if (!stream) return;
    stream.close();
    this.tradeStreamsBySlug.delete(slug);
    if (markFinished) {
      this.finishedTradeSlugs.add(slug);
    }
  }

  private initEventQueue(): void {
    for (const [coin, markets] of this.marketsByCoin.entries()) {
      if (markets.length === 0) continue;
      this.updateMarketEvent(coin);
      this.eventQueue.remove(`tick:${coin}`);
      this.eventQueue.remove(`trade:${coin}`);
    }
  }

  private updateMarketEvent(coin: CoinSymbol): void {
    if (!this.useEventQueue) return;
    const markets = this.marketsByCoin.get(coin) ?? [];
    const idx = this.marketIndexByCoin.get(coin) ?? 0;
    if (idx < markets.length) {
      const next = markets[idx]!;
      this.eventQueue.upsert(`market:${coin}`, next.startMs, {
        type: "market",
        coin,
      });
    } else {
      this.eventQueue.remove(`market:${coin}`);
    }
  }

  private updateTickEvent(coin: CoinSymbol): void {
    if (!this.useEventQueue) return;
    const state = this.states.get(coin);
    if (!state || this.currentTimeMs > state.marketEndMs) {
      this.eventQueue.remove(`tick:${coin}`);
      return;
    }
    const stream = this.getTickStream(coin);
    if (!stream) {
      this.eventQueue.remove(`tick:${coin}`);
      return;
    }
    const nextTick = stream.peek();
    if (!nextTick) {
      this.eventQueue.remove(`tick:${coin}`);
      return;
    }
    const nextTime = nextTick.timestamp + this.latencyMs;
    this.eventQueue.upsert(`tick:${coin}`, nextTime, {
      type: "tick",
      coin,
    });
  }

  private updateTradeEvent(coin: CoinSymbol, slug: string | undefined): void {
    if (!this.useEventQueue) return;
    if (!slug) {
      this.eventQueue.remove(`trade:${coin}`);
      return;
    }
    const stream = this.getTradeStream(slug);
    if (!stream) {
      this.eventQueue.remove(`trade:${coin}`);
      return;
    }
    const nextTrade = stream.peek();
    if (!nextTrade) {
      this.eventQueue.remove(`trade:${coin}`);
      return;
    }
    this.eventQueue.upsert(`trade:${coin}`, nextTrade.timestamp, {
      type: "trade",
      coin,
      slug,
    });
  }

  isDone(): boolean {
    return this.currentTimeMs >= this.maxTimeMs;
  }

  private activateMarkets(timeMs: number): void {
    for (const [coin, markets] of this.marketsByCoin.entries()) {
      let idx = this.marketIndexByCoin.get(coin) ?? 0;
      const current = this.states.get(coin);
      if (current && timeMs >= current.marketEndMs) {
        const prevSlug = this.activeSlugByCoin.get(coin);
        if (prevSlug) {
          this.closeTradeStream(prevSlug, true);
        }
        this.activeSlugByCoin.delete(coin);
      }

      const hasCurrent =
        current &&
        timeMs < current.marketEndMs &&
        timeMs >= current.marketStartMs;
      if (hasCurrent) continue;

      while (idx < markets.length) {
        const next = markets[idx]!;
        if (timeMs < next.startMs) break;
        idx += 1;
        if (timeMs <= next.endMs + this.latencyMs) {
          this.states.set(coin, this.createMarketState(next, timeMs));
          this.activeSlugByCoin.set(coin, next.slug);
          this.getTradeStream(next.slug);
          this.markDirty(coin);
          this.updateTradeEvent(coin, next.slug);
          this.updateTickEvent(coin);
          break;
        }
      }

      this.marketIndexByCoin.set(coin, idx);
      this.updateMarketEvent(coin);
    }
  }

  private processCryptoTicks(timeMs: number): void {
    for (const [coin] of this.cryptoTickFilesByCoin.entries()) {
      const state = this.states.get(coin);
      if (!state) {
        this.updateTickEvent(coin);
        continue;
      }
      if (timeMs > state.marketEndMs) {
        this.updateTickEvent(coin);
        continue;
      }

      const stream = this.getTickStream(coin);
      if (!stream) {
        this.updateTickEvent(coin);
        continue;
      }

      let didUpdate = false;
      while (true) {
        const tick = stream.peek();
        if (!tick) break;
        const eventTime = tick.timestamp + this.latencyMs;
        if (eventTime > timeMs) break;
        stream.shift();
        if (tick.timestamp < state.marketStartMs) {
          continue;
        }

        state.cryptoPrice = tick.value;
        state.cryptoPriceTimestamp = tick.timestamp;
        state.lastPriceUpdateMs = eventTime;
        state.priceHistory.push(tick.value);
        if (state.priceHistory.length > PRICE_HISTORY_LIMIT) {
          state.priceHistory.shift();
        }

        if (state.referencePrice === 0 && tick.timestamp >= state.marketStartMs) {
          state.referencePrice = tick.value;
          state.referenceSource = "historical";
        }

        didUpdate = true;
      }

      if (didUpdate) {
        this.markDirty(coin);
      }
      this.updateTickEvent(coin);
    }
  }

  private processTrades(timeMs: number): void {
    for (const state of this.states.values()) {
      const stream = this.getTradeStream(state.slug);
      if (!stream) {
        this.updateTradeEvent(state.coin, state.slug);
        continue;
      }

      let didUpdate = false;
      while (true) {
        const trade = stream.peek();
        if (!trade) break;
        if (trade.timestamp > timeMs) break;
        stream.shift();

        const existing = state.orderBooks.get(trade.tokenId);
        let bids = existing?.bids ?? [];
        let asks = existing?.asks ?? [];
        let updatedBid = false;
        let updatedAsk = false;

        if (trade.makerOrders && trade.makerOrders.length > 0) {
          const nextBids = this.collectLevels(trade.makerOrders, "BUY", trade.tokenId);
          const nextAsks = this.collectLevels(trade.makerOrders, "SELL", trade.tokenId);
          if (nextBids.length > 0) {
            bids = nextBids;
            updatedBid = true;
          }
          if (nextAsks.length > 0) {
            asks = nextAsks;
            updatedAsk = true;
          }
        }

        if (!updatedBid && !updatedAsk && trade.side === "BUY") {
          asks = [{ price: trade.price, size: trade.size }];
          updatedAsk = true;
          bids = [];
          updatedBid = true;
        } else if (!updatedBid && !updatedAsk && trade.side === "SELL") {
          bids = [{ price: trade.price, size: trade.size }];
          updatedBid = true;
          asks = [];
          updatedAsk = true;
        }

        if (updatedBid) {
          if (bids[0]) {
            state.bestBid.set(trade.tokenId, bids[0].price);
          } else {
            state.bestBid.delete(trade.tokenId);
          }
        }

        if (updatedAsk) {
          if (asks[0]) {
            state.bestAsk.set(trade.tokenId, asks[0].price);
          } else {
            state.bestAsk.delete(trade.tokenId);
          }
        }

        let totalBidValue = 0;
        let totalAskValue = 0;
        for (const bid of bids) totalBidValue += bid.price * bid.size;
        for (const ask of asks) totalAskValue += ask.price * ask.size;

        const snapshot: OrderBookSnapshot = {
          bids,
          asks,
          lastTrade: trade.price,
          totalBidValue,
          totalAskValue,
        };

        state.orderBooks.set(trade.tokenId, snapshot);
        state.lastBookUpdateMs = trade.timestamp;
        state.recentTrades.push(trade);

        didUpdate = true;
      }

      if (state.recentTrades.length > 0) {
        this.trimRecentTrades(state, timeMs);
      }

      if (didUpdate) {
        this.markDirty(state.coin);
      }
      this.updateTradeEvent(state.coin, state.slug);
    }
  }

  private collectLevels(
    levels: BacktestTradeLevel[],
    side: "BUY" | "SELL",
    tokenId: string,
  ): OrderBookLevel[] {
    const byPrice = new Map<number, number>();
    for (const level of levels) {
      if (level.side !== side) continue;
      if (level.tokenId !== tokenId) continue;
      const existing = byPrice.get(level.price) || 0;
      byPrice.set(level.price, existing + level.size);
    }

    const entries = Array.from(byPrice.entries()).map(([price, size]) => ({
      price,
      size,
    }));

    entries.sort((a, b) => {
      if (a.price === b.price) return 0;
      return side === "BUY" ? b.price - a.price : a.price - b.price;
    });

    return entries;
  }

  private updateSnapshots(timeMs: number): void {
    for (const state of this.states.values()) {
      if (state.recentTrades.length > 0) {
        this.trimRecentTrades(state, timeMs);
      }
      state.timeLeftSec = (state.marketEndMs - timeMs) / 1000;
      if (state.lastBookUpdateMs > 0) {
        state.dataStatus = "healthy";
      } else {
        state.dataStatus = timeMs - state.selectedAtMs > BOOK_STALE_MS ? "stale" : "unknown";
      }
      state.signals = computeSignals(state, timeMs, state.recentTrades, {
        depthLevels: SIGNAL_DEPTH_LEVELS,
        slippageNotional: SIGNAL_SLIPPAGE_NOTIONAL,
        tradeWindowMs: SIGNAL_TRADE_WINDOW_MS,
      });
    }
  }

  private trimRecentTrades(state: BacktestMarketState, timeMs: number): void {
    const cutoff = timeMs - SIGNAL_TRADE_WINDOW_MS;
    while (state.recentTrades.length > 0) {
      const first = state.recentTrades[0];
      if (!first || first.timestamp >= cutoff) break;
      state.recentTrades.shift();
    }
  }

  private createMarketState(
    meta: BacktestMarketMeta,
    timeMs: number,
  ): BacktestMarketState {
    return {
      coin: meta.coin,
      symbol: `${meta.coin}/usd`,
      marketName: meta.marketName,
      slug: meta.slug,
      timeLeftSec: (meta.endMs - timeMs) / 1000,
      priceToBeat: 0,
      referencePrice: 0,
      referenceSource: "missing",
      cryptoPrice: 0,
      cryptoPriceTimestamp: 0,
      dataStatus: "unknown",
      lastBookUpdateMs: 0,
      upOutcome: "Up",
      downOutcome: "Down",
      upTokenId: meta.upTokenId,
      downTokenId: meta.downTokenId,
      orderBooks: new Map(),
      bestBid: new Map(),
      bestAsk: new Map(),
      priceHistory: [],
      marketStartMs: meta.startMs,
      marketEndMs: meta.endMs + this.latencyMs,
      lastPriceUpdateMs: 0,
      selectedAtMs: timeMs,
      recentTrades: [],
    };
  }

  private computeBounds(): void {
    let min = Number.POSITIVE_INFINITY;
    let max = 0;

    for (const markets of this.marketsByCoin.values()) {
      for (const market of markets) {
        min = Math.min(min, market.startMs);
        max = Math.max(max, market.endMs + this.latencyMs);
      }
    }

    if (this.tickRangesByCoin) {
      for (const range of this.tickRangesByCoin.values()) {
        if (!Number.isFinite(range.minTs) || !Number.isFinite(range.maxTs)) continue;
        min = Math.min(min, range.minTs + this.latencyMs);
        max = Math.max(max, range.maxTs + this.latencyMs);
      }
    }

    if (this.tradeRangesBySlug) {
      for (const range of this.tradeRangesBySlug.values()) {
        if (!Number.isFinite(range.minTs) || !Number.isFinite(range.maxTs)) continue;
        min = Math.min(min, range.minTs);
        max = Math.max(max, range.maxTs);
      }
    }

    if (!Number.isFinite(min)) {
      min = 0;
    }

    this.minTimeMs = min;
    this.maxTimeMs = max;
    this.currentTimeMs = min;
  }
}
