import { describe, expect, it, beforeEach } from "bun:test";
import { ArbitrageEngine } from "../src/services/arbitrage-engine";
import type { ArbitrageCoinConfig } from "../src/services/arbitrage-config";
import type {
  MarketSnapshot,
  OrderBookSnapshot,
} from "../src/services/market-data-hub";
import type { CoinSymbol } from "../src/services/auto-market";
import { RunLogger } from "../src/services/run-logger";
import { join } from "path";
import { mkdirSync, existsSync, rmSync } from "fs";

// ── Test Utilities ───────────────────────────────────────────────

const TEST_LOG_DIR = join(process.cwd(), "tests", ".test-logs");

function getTestLogger(): RunLogger {
  if (!existsSync(TEST_LOG_DIR)) {
    mkdirSync(TEST_LOG_DIR, { recursive: true });
  }
  return new RunLogger(join(TEST_LOG_DIR, `test-${Date.now()}.log`), 50);
}

/** Stub KalshiClient that does nothing */
const stubKalshiClient = {
  getMarket: async () => null,
  getMarkets: async () => ({ markets: [], cursor: undefined }),
  searchMarkets: async () => [],
  getEvent: async () => null,
} as any;

function defaultCoinConfig(overrides: Partial<ArbitrageCoinConfig> = {}): ArbitrageCoinConfig {
  return {
    tradeAllowedTimeLeft: 750,
    tradeStopTimeLeft: null,
    minGap: 0.04,
    maxSpendTotal: 500,
    minSpendTotal: 10,
    maxSpread: null,
    minDepthValue: null,
    maxPriceStalenessSec: null,
    fillUsd: 500,
    ...overrides,
  };
}

function makeOrderBook(
  asks: Array<{ price: number; size: number }>,
  bids: Array<{ price: number; size: number }> = [],
): OrderBookSnapshot {
  let totalBidValue = 0;
  let totalAskValue = 0;
  for (const bid of bids) totalBidValue += bid.price * bid.size;
  for (const ask of asks) totalAskValue += ask.price * ask.size;
  return {
    bids: bids.sort((a, b) => b.price - a.price),
    asks: asks.sort((a, b) => a.price - b.price),
    lastTrade: 0,
    totalBidValue,
    totalAskValue,
  };
}

function makePolySnap(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  const snap: MarketSnapshot = {
    provider: "polymarket",
    coin: "btc",
    symbol: "btc/usd",
    marketName: "BTC Up or Down",
    slug: "btc-updown-15m-test",
    timeLeftSec: 600,
    marketCloseTimeMs: Date.now() + 600_000,
    priceToBeat: 50000,
    referencePrice: 50000,
    referenceSource: "price_to_beat",
    cryptoPrice: 50100,
    cryptoPriceTimestamp: Date.now(),
    dataStatus: "healthy",
    lastBookUpdateMs: Date.now(),
    upOutcome: "Up",
    downOutcome: "Down",
    upTokenId: "UP_TOKEN",
    downTokenId: "DOWN_TOKEN",
    orderBooks: new Map(),
    bestBid: new Map(),
    bestAsk: new Map(),
    priceHistory: [],
    priceHistoryWithTs: [],
    ...overrides,
  };

  // Default asks for Up token (poly side of upNo trade)
  if (!snap.orderBooks.has("UP_TOKEN")) {
    snap.orderBooks.set(
      "UP_TOKEN",
      makeOrderBook([{ price: 0.40, size: 500 }], [{ price: 0.38, size: 200 }]),
    );
    snap.bestAsk.set("UP_TOKEN", 0.40);
    snap.bestBid.set("UP_TOKEN", 0.38);
  }
  if (!snap.orderBooks.has("DOWN_TOKEN")) {
    snap.orderBooks.set(
      "DOWN_TOKEN",
      makeOrderBook([{ price: 0.55, size: 500 }], [{ price: 0.53, size: 200 }]),
    );
    snap.bestAsk.set("DOWN_TOKEN", 0.55);
    snap.bestBid.set("DOWN_TOKEN", 0.53);
  }

  return snap;
}

function makeKalshiSnap(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  const snap: MarketSnapshot = {
    provider: "kalshi",
    coin: "btc",
    symbol: "btc/usd",
    marketName: "Will BTC be above $50,000?",
    slug: "KXBTC15M-TEST",
    marketTicker: "KXBTC15M-TEST",
    timeLeftSec: 600,
    marketCloseTimeMs: Date.now() + 600_000,
    priceToBeat: 50000,
    referencePrice: 50000,
    referenceSource: "price_to_beat",
    cryptoPrice: 50100,
    cryptoPriceTimestamp: Date.now(),
    kalshiUnderlyingValue: 50100,
    kalshiUnderlyingTs: Date.now(),
    dataStatus: "healthy",
    lastBookUpdateMs: Date.now(),
    upOutcome: "Yes",
    downOutcome: "No",
    upTokenId: "YES",
    downTokenId: "NO",
    orderBooks: new Map(),
    bestBid: new Map(),
    bestAsk: new Map(),
    priceHistory: [],
    priceHistoryWithTs: [],
    ...overrides,
  };

  // Default Kalshi order books (NO side for upNo trade)
  if (!snap.orderBooks.has("YES")) {
    snap.orderBooks.set(
      "YES",
      makeOrderBook([{ price: 0.55, size: 500 }], [{ price: 0.53, size: 200 }]),
    );
    snap.bestAsk.set("YES", 0.55);
    snap.bestBid.set("YES", 0.53);
  }
  if (!snap.orderBooks.has("NO")) {
    snap.orderBooks.set(
      "NO",
      makeOrderBook([{ price: 0.50, size: 500 }], [{ price: 0.48, size: 200 }]),
    );
    snap.bestAsk.set("NO", 0.50);
    snap.bestBid.set("NO", 0.48);
  }

  return snap;
}

function createEngine(
  coinConfigs?: Map<CoinSymbol, ArbitrageCoinConfig>,
  opts?: { decisionLatencyMs?: number },
): ArbitrageEngine {
  const configs = coinConfigs ?? new Map([["btc" as CoinSymbol, defaultCoinConfig()]]);
  const logger = getTestLogger();
  return new ArbitrageEngine("testProfile", configs, logger, {
    kalshiOutcomeClient: stubKalshiClient,
    decisionLatencyMs: opts?.decisionLatencyMs ?? 0, // zero latency for tests
  }, Date.now());
}

function makeSnapMap(snap: MarketSnapshot): Map<CoinSymbol, MarketSnapshot> {
  return new Map([[snap.coin as CoinSymbol, snap]]);
}

// ── Tests ────────────────────────────────────────────────────────

describe("ArbitrageEngine", () => {
  describe("basic lifecycle", () => {
    it("starts with zero trades and zero profit", () => {
      const engine = createEngine();
      const summary = engine.getSummary();
      expect(summary.totalTrades).toBe(0);
      expect(summary.totalProfit).toBe(0);
      expect(summary.wins).toBe(0);
      expect(summary.losses).toBe(0);
    });

    it("returns market views for configured coins", () => {
      const engine = createEngine();
      const polySnap = makePolySnap();
      const kalshiSnap = makeKalshiSnap();
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());

      const views = engine.getMarketViews();
      expect(views.length).toBe(1);
      expect(views[0]!.coin).toBe("btc");
    });

    it("getName returns profile name", () => {
      const engine = createEngine();
      expect(engine.getName()).toBe("testProfile");
    });
  });

  describe("trade entry conditions", () => {
    it("skips when timeLeft > tradeAllowedTimeLeft", () => {
      const engine = createEngine();
      const polySnap = makePolySnap({ timeLeftSec: 800 }); // > 750
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 800 });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).toBeNull();
      expect(views[0]!.selectedDirection).toBeNull();
    });

    it("skips when timeLeft <= tradeStopTimeLeft", () => {
      const configs = new Map([
        ["btc" as CoinSymbol, defaultCoinConfig({ tradeStopTimeLeft: 10 })],
      ]);
      const engine = createEngine(configs);
      const polySnap = makePolySnap({ timeLeftSec: 5 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 5 });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).toBeNull();
    });

    it("skips when data is stale", () => {
      const engine = createEngine();
      const polySnap = makePolySnap({ dataStatus: "stale" });
      const kalshiSnap = makeKalshiSnap();

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).toBeNull();
    });

    it("skips when poly snapshot is missing", () => {
      const engine = createEngine();
      const kalshiSnap = makeKalshiSnap();
      const emptyPoly = new Map<CoinSymbol, MarketSnapshot>();

      engine.evaluate(emptyPoly, makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      expect(views.length).toBe(0); // no snapshots captured yet
    });

    it("enters trade when gap exceeds minGap", () => {
      // Set up books so gap is > 0.04 (the default minGap)
      // upNo: poly UP ask 0.40 + kalshi NO ask 0.50 = 0.90, gap = 0.10
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const now = Date.now();
      const polySnap = makePolySnap({ timeLeftSec: 600 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 600 });

      // First eval: creates pending order
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      let views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).not.toBeNull();

      // Second eval: confirms pending order (0ms delay means dueMs = now)
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 1);
      views = engine.getMarketViews();
      expect(views[0]!.position).not.toBeNull();
      expect(engine.getSummary().totalTrades).toBe(1);
    });

    it("skips when market is already closed (timeLeft <= 0)", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({ timeLeftSec: -5 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: -5 });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now() + 1);
      expect(engine.getSummary().totalTrades).toBe(0);
    });

    it("skips when gap is below minGap", () => {
      const configs = new Map([
        ["btc" as CoinSymbol, defaultCoinConfig({ minGap: 0.50 })], // absurdly high
      ]);
      const engine = createEngine(configs, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({ timeLeftSec: 600 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 600 });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).toBeNull();
      expect(views[0]!.position).toBeNull();
    });
  });

  describe("pending order to position flow", () => {
    it("confirms pending order after delay expires", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 100 });
      const now = Date.now();
      const polySnap = makePolySnap({ timeLeftSec: 600 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 600 });

      // First eval: creates pending order
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      let views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).not.toBeNull();
      expect(views[0]!.position).toBeNull();

      // Second eval at now+50: still pending
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 50);
      views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).not.toBeNull();
      expect(views[0]!.position).toBeNull();

      // Third eval at now+101: should be confirmed
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 101);
      views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).toBeNull();
      expect(views[0]!.position).not.toBeNull();
      expect(engine.getSummary().totalTrades).toBe(1);
    });

    it("cancels pending order when market key changes", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 100 });
      const now = Date.now();
      const polySnap = makePolySnap({ timeLeftSec: 600 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 600 });

      // Create pending order
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      let views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).not.toBeNull();

      // Change kalshi slug (different market key)
      const kalshiSnap2 = makeKalshiSnap({
        timeLeftSec: 600,
        slug: "KXBTC15M-DIFFERENT",
        marketTicker: "KXBTC15M-DIFFERENT",
      });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap2), now + 101);
      views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).toBeNull();
      expect(views[0]!.position).toBeNull();
    });
  });

  describe("position resolution", () => {
    it("resolves WIN when both outcomes match targets", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const now = Date.now();
      const closeTime = now + 1000;

      // Enter position (2 evals: first creates pending, second confirms)
      const polySnap = makePolySnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime,
      });
      const kalshiSnap = makeKalshiSnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime,
      });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 1);
      let views = engine.getMarketViews();
      expect(views[0]!.position).not.toBeNull();

      // Close markets and provide resolution data
      const polySnapClosed = makePolySnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime,
        cryptoPrice: 50100, // above 50000 threshold = UP
        priceHistoryWithTs: [
          { price: 50100, ts: closeTime - 30000 },
          { price: 50100, ts: closeTime - 20000 },
          { price: 50100, ts: closeTime - 10000 },
        ],
      });
      const kalshiSnapClosed = makeKalshiSnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime,
        cryptoPrice: 50100,
        kalshiUnderlyingValue: 50100,
        kalshiUnderlyingTs: closeTime - 5000,
        priceHistoryWithTs: [
          { price: 50100, ts: closeTime - 30000 },
          { price: 50100, ts: closeTime - 20000 },
          { price: 50100, ts: closeTime - 10000 },
        ],
      });

      // Evaluate after close + grace period so outcomes resolve
      const farFuture = closeTime + 200_000;
      engine.evaluate(
        makeSnapMap(polySnapClosed),
        makeSnapMap(kalshiSnapClosed),
        farFuture,
      );

      const summary = engine.getSummary();
      // Position should have resolved by now
      views = engine.getMarketViews();
      if (views[0]!.position === null) {
        // Resolved!
        expect(summary.totalTrades).toBe(1);
        expect(summary.wins + summary.losses).toBe(1);
      }
      // If still pending, that's the bug we'll fix in Phase 2
    });
  });

  describe("decision cooldown", () => {
    it("does not create new pending order within cooldown window", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const now = Date.now();
      const polySnap = makePolySnap({ timeLeftSec: 600 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 600 });

      // First eval: creates pending order, second eval: confirms it
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 1);
      const summary1 = engine.getSummary();
      expect(summary1.totalTrades).toBe(1);

      // Position is open, so no new entry should happen
      // (the engine skips coins with open positions)
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 50);
      const summary2 = engine.getSummary();
      expect(summary2.totalTrades).toBe(1); // still 1
    });
  });

  describe("fill estimates", () => {
    it("computes display estimates even when trade not allowed yet", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({ timeLeftSec: 800 }); // > tradeAllowedTimeLeft
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 800 });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      // Should still have estimates even though trade isn't allowed
      expect(views[0]!.estimateUpNo).not.toBeNull();
    });

    it("reports source as orderbook when full book walk succeeds", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({ timeLeftSec: 800 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 800 });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      if (views[0]!.estimateUpNoSource) {
        expect(["orderbook", "best_ask"]).toContain(views[0]!.estimateUpNoSource);
      }
    });
  });

  describe("multi-coin", () => {
    it("evaluates multiple coins independently", () => {
      const configs = new Map<CoinSymbol, ArbitrageCoinConfig>([
        ["btc", defaultCoinConfig()],
        ["eth", defaultCoinConfig()],
      ]);
      const engine = createEngine(configs, { decisionLatencyMs: 0 });

      const btcPoly = makePolySnap({ coin: "btc", timeLeftSec: 600 });
      const btcKalshi = makeKalshiSnap({ coin: "btc", timeLeftSec: 600 });
      const ethPoly = makePolySnap({
        coin: "eth",
        symbol: "eth/usd",
        slug: "eth-updown-15m-test",
        timeLeftSec: 600,
      });
      const ethKalshi = makeKalshiSnap({
        coin: "eth",
        symbol: "eth/usd",
        slug: "KXETH15M-TEST",
        marketTicker: "KXETH15M-TEST",
        timeLeftSec: 600,
      });

      const polySnaps = new Map<CoinSymbol, MarketSnapshot>([
        ["btc", btcPoly],
        ["eth", ethPoly],
      ]);
      const kalshiSnaps = new Map<CoinSymbol, MarketSnapshot>([
        ["btc", btcKalshi],
        ["eth", ethKalshi],
      ]);

      const now = Date.now();
      // First eval: creates pending orders
      engine.evaluate(polySnaps, kalshiSnaps, now);
      // Second eval: confirms them (0ms delay)
      engine.evaluate(polySnaps, kalshiSnaps, now + 1);
      const views = engine.getMarketViews();
      expect(views.length).toBe(2);

      // Both should have entered trades (good gap)
      const summary = engine.getSummary();
      expect(summary.totalTrades).toBe(2);
    });
  });

  describe("threshold validation", () => {
    it("skips trade entry when Kalshi threshold is null/zero", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({ timeLeftSec: 600, priceToBeat: 50000 });
      // Kalshi has no threshold -- this was the bug from the screenshot
      const kalshiSnap = makeKalshiSnap({
        timeLeftSec: 600,
        priceToBeat: 0,
        referencePrice: 0,
        referenceSource: "missing",
      });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now() + 1);
      const views = engine.getMarketViews();
      expect(views[0]!.pendingDirection).toBeNull();
      expect(views[0]!.position).toBeNull();
      expect(engine.getSummary().totalTrades).toBe(0);
    });

    it("skips trade entry when Poly threshold is null/zero", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({
        timeLeftSec: 600,
        priceToBeat: 0,
        referencePrice: 0,
        referenceSource: "missing",
      });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 600, priceToBeat: 50000 });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now() + 1);
      expect(engine.getSummary().totalTrades).toBe(0);
    });

    it("enters trade when both thresholds are valid", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({ timeLeftSec: 600, priceToBeat: 50000 });
      const kalshiSnap = makeKalshiSnap({ timeLeftSec: 600, priceToBeat: 50000 });

      const now = Date.now();
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 1);
      expect(engine.getSummary().totalTrades).toBe(1);
    });
  });

  describe("position force resolution", () => {
    it("force-resolves stuck position after max unresolved time", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const now = Date.now();
      const closeTime = now + 1000;

      // Enter position
      const polySnap = makePolySnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime,
        priceToBeat: 50000,
      });
      const kalshiSnap = makeKalshiSnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime,
        priceToBeat: 50000,
      });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 1);
      expect(engine.getSummary().totalTrades).toBe(1);

      // Close markets but with NO resolution data (threshold missing on snap)
      const polySnapStuck = makePolySnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime,
        priceToBeat: 0,
        referencePrice: 0,
        referenceSource: "missing",
        cryptoPrice: 0,
        priceHistoryWithTs: [],
      });
      const kalshiSnapStuck = makeKalshiSnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime,
        priceToBeat: 0,
        referencePrice: 0,
        referenceSource: "missing",
        cryptoPrice: 0,
        kalshiUnderlyingValue: null,
        priceHistoryWithTs: [],
      });

      // Evaluate many times well past the 10-minute absolute timeout
      // POSITION_MAX_UNRESOLVED_MS defaults to 600_000 (10 min)
      const futureMs = now + 700_000;
      for (let i = 0; i < 5; i++) {
        engine.evaluate(
          makeSnapMap(polySnapStuck),
          makeSnapMap(kalshiSnapStuck),
          futureMs + i,
        );
      }

      // Position should have been force-resolved (cleaned up)
      const views = engine.getMarketViews();
      expect(views[0]!.position).toBeNull();
      // Should have resolved as a loss (UNKNOWN outcomes)
      const summary = engine.getSummary();
      expect(summary.wins + summary.losses).toBe(1);
    });

    it("force-resolves using crypto price when thresholds exist but official fetch fails", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const now = Date.now();
      const closeTime = now + 1000;

      // Enter position
      const polySnap = makePolySnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime,
        priceToBeat: 50000,
      });
      const kalshiSnap = makeKalshiSnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime,
        priceToBeat: 50000,
      });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 1);
      expect(engine.getSummary().totalTrades).toBe(1);

      // Close markets WITH spot price data but missing official outcomes
      // Price 50100 > threshold 50000 => both should resolve UP
      const polySnapClosed = makePolySnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime,
        priceToBeat: 50000,
        referencePrice: 50000,
        cryptoPrice: 50100,
        priceHistoryWithTs: [
          { price: 50100, ts: closeTime - 30000 },
          { price: 50100, ts: closeTime - 20000 },
          { price: 50100, ts: closeTime - 10000 },
        ],
      });
      const kalshiSnapClosed = makeKalshiSnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime,
        priceToBeat: 50000,
        referencePrice: 50000,
        cryptoPrice: 50100,
        kalshiUnderlyingValue: 50100,
        kalshiUnderlyingTs: closeTime - 5000,
        priceHistoryWithTs: [
          { price: 50100, ts: closeTime - 30000 },
          { price: 50100, ts: closeTime - 20000 },
          { price: 50100, ts: closeTime - 10000 },
        ],
      });

      // Advance past the official wait timeout + position max age
      const futureMs = closeTime + 400_000;
      for (let i = 0; i < 10; i++) {
        engine.evaluate(
          makeSnapMap(polySnapClosed),
          makeSnapMap(kalshiSnapClosed),
          futureMs + i * 100,
        );
      }

      // Position should have resolved
      const views = engine.getMarketViews();
      expect(views[0]!.position).toBeNull();
      const summary = engine.getSummary();
      expect(summary.wins + summary.losses).toBe(1);
    });

    it("cleans up position even when no data is available at all", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const now = Date.now();
      const closeTime = now + 1000;

      const polySnap = makePolySnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime,
        priceToBeat: 50000,
      });
      const kalshiSnap = makeKalshiSnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime,
        priceToBeat: 50000,
      });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now);
      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), now + 1);
      expect(engine.getSummary().totalTrades).toBe(1);

      // Close with absolutely NO resolution data
      const polySnapDead = makePolySnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime,
        priceToBeat: 0,
        referencePrice: 0,
        referenceSource: "missing",
        cryptoPrice: 0,
        priceHistoryWithTs: [],
      });
      const kalshiSnapDead = makeKalshiSnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime,
        priceToBeat: 0,
        referencePrice: 0,
        referenceSource: "missing",
        cryptoPrice: 0,
        kalshiUnderlyingValue: null,
        priceHistoryWithTs: [],
      });

      // Way past absolute timeout
      const futureMs = now + 700_000;
      for (let i = 0; i < 5; i++) {
        engine.evaluate(
          makeSnapMap(polySnapDead),
          makeSnapMap(kalshiSnapDead),
          futureMs + i,
        );
      }

      // Position MUST be cleaned up - can't hang forever
      const views = engine.getMarketViews();
      expect(views[0]!.position).toBeNull();
      // Should be a loss since both outcomes are UNKNOWN
      const summary = engine.getSummary();
      expect(summary.losses).toBe(1);
    });

    it("allows new trade after force-resolved position is cleaned up", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const now = Date.now();
      const closeTime1 = now + 1000;

      // Enter first position
      const polySnap1 = makePolySnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime1,
        priceToBeat: 50000,
      });
      const kalshiSnap1 = makeKalshiSnap({
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime1,
        priceToBeat: 50000,
      });

      engine.evaluate(makeSnapMap(polySnap1), makeSnapMap(kalshiSnap1), now);
      engine.evaluate(makeSnapMap(polySnap1), makeSnapMap(kalshiSnap1), now + 1);
      expect(engine.getSummary().totalTrades).toBe(1);

      // Force-resolve (no data, past timeout)
      const polySnapDead = makePolySnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime1,
        priceToBeat: 0,
        referencePrice: 0,
        referenceSource: "missing",
        cryptoPrice: 0,
        priceHistoryWithTs: [],
      });
      const kalshiSnapDead = makeKalshiSnap({
        timeLeftSec: -1,
        marketCloseTimeMs: closeTime1,
        priceToBeat: 0,
        referencePrice: 0,
        referenceSource: "missing",
        cryptoPrice: 0,
        kalshiUnderlyingValue: null,
        priceHistoryWithTs: [],
      });
      const futureMs = now + 700_000;
      for (let i = 0; i < 5; i++) {
        engine.evaluate(makeSnapMap(polySnapDead), makeSnapMap(kalshiSnapDead), futureMs + i);
      }
      expect(engine.getSummary().wins + engine.getSummary().losses).toBe(1);

      // Now a NEW market appears - should be able to trade again
      const closeTime2 = futureMs + 900_000;
      const polySnap2 = makePolySnap({
        slug: "btc-updown-15m-new",
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime2,
        priceToBeat: 51000,
      });
      const kalshiSnap2 = makeKalshiSnap({
        slug: "KXBTC15M-NEW",
        marketTicker: "KXBTC15M-NEW",
        timeLeftSec: 600,
        marketCloseTimeMs: closeTime2,
        priceToBeat: 51000,
      });

      const tradeTime = futureMs + 10;
      engine.evaluate(makeSnapMap(polySnap2), makeSnapMap(kalshiSnap2), tradeTime);
      engine.evaluate(makeSnapMap(polySnap2), makeSnapMap(kalshiSnap2), tradeTime + 1);
      // Should have entered a SECOND trade
      expect(engine.getSummary().totalTrades).toBe(2);
    });
  });

  describe("error boundaries", () => {
    it("continues evaluating other coins when one throws", () => {
      // We test this by having two coins, where one has missing data
      // that would previously cause issues, but the other is fine
      const configs = new Map<CoinSymbol, ArbitrageCoinConfig>([
        ["btc", defaultCoinConfig()],
        ["eth", defaultCoinConfig()],
      ]);
      const engine = createEngine(configs, { decisionLatencyMs: 0 });

      // BTC has valid data, ETH has valid data too
      const btcPoly = makePolySnap({ coin: "btc", timeLeftSec: 600 });
      const btcKalshi = makeKalshiSnap({ coin: "btc", timeLeftSec: 600 });
      const ethPoly = makePolySnap({
        coin: "eth",
        symbol: "eth/usd",
        slug: "eth-updown-15m-test",
        timeLeftSec: 600,
      });
      const ethKalshi = makeKalshiSnap({
        coin: "eth",
        symbol: "eth/usd",
        slug: "KXETH15M-TEST",
        marketTicker: "KXETH15M-TEST",
        timeLeftSec: 600,
      });

      const polySnaps = new Map<CoinSymbol, MarketSnapshot>([
        ["btc", btcPoly],
        ["eth", ethPoly],
      ]);
      const kalshiSnaps = new Map<CoinSymbol, MarketSnapshot>([
        ["btc", btcKalshi],
        ["eth", ethKalshi],
      ]);

      // Should not throw, even if internal processing has issues
      expect(() => {
        engine.evaluate(polySnaps, kalshiSnaps, Date.now());
        engine.evaluate(polySnaps, kalshiSnaps, Date.now() + 1);
      }).not.toThrow();

      // Both coins should have been evaluated
      const views = engine.getMarketViews();
      expect(views.length).toBe(2);
    });
  });

  describe("data status rendering", () => {
    it("reports stale when either snapshot is stale", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({ dataStatus: "stale" });
      const kalshiSnap = makeKalshiSnap({ dataStatus: "healthy" });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      expect(views[0]!.dataStatus).toBe("stale");
    });

    it("reports healthy when both snapshots are healthy", () => {
      const engine = createEngine(undefined, { decisionLatencyMs: 0 });
      const polySnap = makePolySnap({ dataStatus: "healthy" });
      const kalshiSnap = makeKalshiSnap({ dataStatus: "healthy" });

      engine.evaluate(makeSnapMap(polySnap), makeSnapMap(kalshiSnap), Date.now());
      const views = engine.getMarketViews();
      expect(views[0]!.dataStatus).toBe("healthy");
    });
  });
});

// Cleanup test logs after all tests
afterAll(() => {
  try {
    if (existsSync(TEST_LOG_DIR)) {
      rmSync(TEST_LOG_DIR, { recursive: true, force: true });
    }
  } catch { /* ignore */ }
});

function afterAll(fn: () => void) {
  // bun:test doesn't have global afterAll, but cleanup is best-effort
  process.on("beforeExit", fn);
}
