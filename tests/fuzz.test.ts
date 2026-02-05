/**
 * Randomized fuzz tests for critical code paths.
 * These test that functions never crash regardless of input shape,
 * and that outputs are always well-formed (no NaN, no Infinity, etc.)
 */

import { describe, expect, it } from "bun:test";
import {
  findMaxEqualShares,
  computeFillEstimate,
  normalizeAsks,
} from "../src/services/arbitrage-fill";
import {
  resolveThreshold,
  computeFinalPrice,
  computeOutcomeFromValues,
} from "../src/services/outcome-resolution";
import { parseStrikePrice } from "../src/services/kalshi-market-data-hub";
import { computeSignals, type SignalInput } from "../src/services/market-signals";
import type { MarketSnapshot } from "../src/services/market-data-hub";

// ── Random generators ────────────────────────────────────────────

function randomFloat(min: number, max: number): number {
  return min + Math.random() * (max - min);
}

function randomInt(min: number, max: number): number {
  return Math.floor(randomFloat(min, max + 1));
}

function randomBook(levels: number): Array<{ price: number; size: number }> {
  const book: Array<{ price: number; size: number }> = [];
  for (let i = 0; i < levels; i++) {
    book.push({
      price: randomFloat(0.01, 0.99),
      size: randomInt(1, 500),
    });
  }
  return book.sort((a, b) => a.price - b.price);
}

function randomSnapshot(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  return {
    provider: Math.random() > 0.5 ? "polymarket" : "kalshi",
    coin: (["btc", "eth", "sol", "xrp"] as const)[randomInt(0, 3)] ?? "btc",
    symbol: "btc/usd",
    marketName: "Test Market",
    slug: "test-market",
    timeLeftSec: randomFloat(-100, 1000),
    marketCloseTimeMs: Date.now() + randomFloat(-100000, 600000),
    priceToBeat: Math.random() > 0.3 ? randomFloat(100, 100000) : 0,
    referencePrice: Math.random() > 0.3 ? randomFloat(100, 100000) : 0,
    referenceSource: (["price_to_beat", "historical", "html", "missing"] as const)[randomInt(0, 3)] ?? "missing" as const,
    cryptoPrice: Math.random() > 0.2 ? randomFloat(100, 100000) : 0,
    cryptoPriceTimestamp: Date.now() - randomFloat(0, 60000),
    dataStatus: (["healthy", "stale", "unknown"] as const)[randomInt(0, 2)] ?? "unknown" as const,
    lastBookUpdateMs: Date.now() - randomFloat(0, 60000),
    upOutcome: "Up",
    downOutcome: "Down",
    upTokenId: "UP",
    downTokenId: "DOWN",
    orderBooks: new Map(),
    bestBid: new Map(),
    bestAsk: new Map(),
    priceHistory: Array.from({ length: randomInt(0, 50) }, () => randomFloat(100, 100000)),
    priceHistoryWithTs: Array.from({ length: randomInt(0, 20) }, () => ({
      price: randomFloat(100, 100000),
      ts: Date.now() - randomFloat(0, 300000),
    })),
    ...overrides,
  };
}

// ── Fuzz: Fill estimation ────────────────────────────────────────

describe("fuzz: fill estimation", () => {
  it("never crashes with random order books (1000 iterations)", () => {
    for (let i = 0; i < 1000; i++) {
      const asksA = randomBook(randomInt(0, 10));
      const asksB = randomBook(randomInt(0, 10));
      const budget = randomFloat(-100, 1000);

      // Must never throw
      const result = findMaxEqualShares(asksA, asksB, budget);

      if (result !== null) {
        expect(result.shares).toBeGreaterThan(0);
        expect(Number.isFinite(result.costA)).toBe(true);
        expect(Number.isFinite(result.costB)).toBe(true);
        expect(result.costA).toBeGreaterThanOrEqual(0);
        expect(result.costB).toBeGreaterThanOrEqual(0);
      }
    }
  });

  it("computeFillEstimate always produces valid output (500 iterations)", () => {
    for (let i = 0; i < 500; i++) {
      const asksPoly = randomBook(randomInt(0, 8));
      const asksKalshi = randomBook(randomInt(0, 8));
      const budget = randomFloat(1, 500);

      const result = computeFillEstimate(asksPoly, asksKalshi, budget);

      if (result !== null) {
        expect(Number.isFinite(result.shares)).toBe(true);
        expect(Number.isFinite(result.avgPoly)).toBe(true);
        expect(Number.isFinite(result.avgKalshi)).toBe(true);
        expect(Number.isFinite(result.gap)).toBe(true);
        expect(Number.isFinite(result.totalCost)).toBe(true);
        expect(result.shares).toBeGreaterThan(0);
        expect(result.totalCost).toBeLessThanOrEqual(budget + 0.01); // float tolerance
      }
    }
  });

  it("normalizeAsks never crashes with garbage input (500 iterations)", () => {
    for (let i = 0; i < 500; i++) {
      const levels = Array.from({ length: randomInt(0, 20) }, () => ({
        price: Math.random() > 0.1 ? randomFloat(-1, 2) : NaN,
        size: Math.random() > 0.1 ? randomFloat(-10, 100) : Infinity,
      }));

      const result = normalizeAsks(levels);
      // Must always return an array
      expect(Array.isArray(result)).toBe(true);
      // All entries must be valid
      for (const level of result) {
        expect(Number.isFinite(level.price)).toBe(true);
        expect(level.price).toBeGreaterThan(0);
        expect(Number.isFinite(level.size)).toBe(true);
        expect(level.size).toBeGreaterThan(0);
      }
    }
  });
});

// ── Fuzz: parseStrikePrice ───────────────────────────────────────

describe("fuzz: parseStrikePrice", () => {
  it("never crashes with random market objects (500 iterations)", () => {
    for (let i = 0; i < 500; i++) {
      const market: Record<string, unknown> = {};

      // Randomly populate fields
      if (Math.random() > 0.5) market.floor_strike = randomFloat(-100, 100000);
      if (Math.random() > 0.5) market.cap_strike = randomFloat(-100, 100000);
      if (Math.random() > 0.5) market.strike_price_dollars = randomFloat(-100, 100000);
      if (Math.random() > 0.5) market.strike_price_cents = randomFloat(-100, 10000000);
      if (Math.random() > 0.5) market.reference_price = randomFloat(-100, 100000);
      if (Math.random() > 0.5) market.title = `Will BTC be above $${randomInt(1000, 100000)}?`;
      if (Math.random() > 0.5) market.strike = { strike_type: "greater", floor_strike: randomFloat(100, 100000) };
      if (Math.random() > 0.3) market.status = ["open", "closed", "settled"][randomInt(0, 2)];

      const result = parseStrikePrice(market);
      expect(typeof result).toBe("number");
      expect(Number.isFinite(result)).toBe(true);
      expect(result).toBeGreaterThanOrEqual(0);
    }
  });
});

// ── Fuzz: outcome resolution ─────────────────────────────────────

describe("fuzz: outcome resolution", () => {
  it("resolveThreshold never crashes with random snapshots (500 iterations)", () => {
    for (let i = 0; i < 500; i++) {
      const snap = randomSnapshot();
      const result = resolveThreshold(snap);
      expect(result).toBeDefined();
      expect(typeof result.source).toBe("string");
      if (result.value !== null) {
        expect(Number.isFinite(result.value)).toBe(true);
      }
    }
  });

  it("computeOutcomeFromValues never crashes with random inputs (500 iterations)", () => {
    for (let i = 0; i < 500; i++) {
      const price = Math.random() > 0.2 ? randomFloat(-1000, 100000) : null;
      const threshold = Math.random() > 0.2 ? randomFloat(-1000, 100000) : null;

      const result = computeOutcomeFromValues(price, threshold);
      expect(["UP", "DOWN", "UNKNOWN"]).toContain(result);
    }
  });

  it("computeFinalPrice never crashes with random snapshots (200 iterations)", () => {
    for (let i = 0; i < 200; i++) {
      const snap = randomSnapshot();
      const now = Date.now() + randomFloat(-100000, 100000);

      const result = computeFinalPrice(snap, now, {
        windowMs: randomFloat(1000, 120000),
        minPoints: randomInt(1, 10),
        allowStaleAfterMs: randomFloat(10000, 300000),
      });

      expect(result).toBeDefined();
      expect(typeof result.source).toBe("string");
      if (result.value !== null) {
        expect(Number.isFinite(result.value)).toBe(true);
      }
    }
  });
});

// ── Fuzz: market signals ─────────────────────────────────────────

describe("fuzz: market signals", () => {
  it("computeSignals never crashes with random inputs (200 iterations)", () => {
    for (let i = 0; i < 200; i++) {
      const input: SignalInput = {
        orderBooks: new Map(),
        bestBid: new Map(),
        bestAsk: new Map(),
        priceHistory: Array.from({ length: randomInt(0, 30) }, () =>
          Math.random() > 0.9 ? NaN : randomFloat(100, 100000)
        ),
        cryptoPriceTimestamp: Date.now() - randomFloat(0, 120000),
        referenceSource: (["price_to_beat", "historical", "html", "missing"] as const)[randomInt(0, 3)] ?? "missing" as const,
        priceToBeat: Math.random() > 0.3 ? randomFloat(100, 100000) : 0,
        referencePrice: Math.random() > 0.3 ? randomFloat(100, 100000) : 0,
      };

      // Randomly add order book data
      if (Math.random() > 0.3) {
        input.orderBooks.set("YES", {
          bids: randomBook(randomInt(0, 5)),
          asks: randomBook(randomInt(0, 5)),
          totalBidValue: randomFloat(0, 1000),
          totalAskValue: randomFloat(0, 1000),
        });
        input.bestBid.set("YES", randomFloat(0.01, 0.99));
        input.bestAsk.set("YES", randomFloat(0.01, 0.99));
      }

      const result = computeSignals(input, Date.now());
      expect(result).toBeDefined();
      expect(result.tokenSignals).toBeDefined();

      // All numeric fields must be finite or null
      if (result.priceMomentum !== null) {
        expect(Number.isFinite(result.priceMomentum)).toBe(true);
      }
      if (result.priceVolatility !== null) {
        expect(Number.isFinite(result.priceVolatility)).toBe(true);
      }
      if (result.tradeVelocity !== null) {
        expect(Number.isFinite(result.tradeVelocity)).toBe(true);
      }
      expect(Number.isFinite(result.referenceQuality)).toBe(true);
      expect(result.referenceQuality).toBeGreaterThanOrEqual(0);
      expect(result.referenceQuality).toBeLessThanOrEqual(1);
    }
  });
});
