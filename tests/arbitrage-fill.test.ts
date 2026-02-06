import { describe, expect, it } from "bun:test";
import {
  findMaxEqualShares,
  computeFillEstimate,
  normalizeAsks,
  resolveAsks,
  randomDelayMs,
} from "../src/services/arbitrage-fill";
import type { MarketSnapshot, OrderBookLevel } from "../src/services/market-data-hub";

// ── normalizeAsks ────────────────────────────────────────────────

describe("normalizeAsks", () => {
  it("filters out zero/negative prices", () => {
    const result = normalizeAsks([
      { price: 0.5, size: 10 },
      { price: 0, size: 5 },
      { price: -0.1, size: 3 },
      { price: 0.3, size: 8 },
    ]);
    expect(result).toEqual([
      { price: 0.3, size: 8 },
      { price: 0.5, size: 10 },
    ]);
  });

  it("filters out zero/negative sizes", () => {
    const result = normalizeAsks([
      { price: 0.5, size: 10 },
      { price: 0.3, size: 0 },
      { price: 0.4, size: -5 },
    ]);
    expect(result).toEqual([{ price: 0.5, size: 10 }]);
  });

  it("floors fractional sizes", () => {
    const result = normalizeAsks([
      { price: 0.5, size: 10.9 },
      { price: 0.3, size: 0.5 },
    ]);
    expect(result).toEqual([{ price: 0.5, size: 10 }]);
  });

  it("sorts by price ascending", () => {
    const result = normalizeAsks([
      { price: 0.7, size: 5 },
      { price: 0.3, size: 10 },
      { price: 0.5, size: 8 },
    ]);
    expect(result[0]!.price).toBe(0.3);
    expect(result[1]!.price).toBe(0.5);
    expect(result[2]!.price).toBe(0.7);
  });

  it("returns empty for empty input", () => {
    expect(normalizeAsks([])).toEqual([]);
  });

  it("filters NaN and Infinity prices", () => {
    const result = normalizeAsks([
      { price: NaN, size: 10 },
      { price: Infinity, size: 5 },
      { price: 0.4, size: 8 },
    ]);
    expect(result).toEqual([{ price: 0.4, size: 8 }]);
  });
});

// ── resolveAsks ──────────────────────────────────────────────────

describe("resolveAsks", () => {
  it("returns empty for missing snapshot", () => {
    expect(resolveAsks(undefined, "token1")).toEqual([]);
  });

  it("returns empty for null tokenId", () => {
    const snap = createMockSnapshot();
    expect(resolveAsks(snap, null)).toEqual([]);
  });

  it("returns empty for missing order book", () => {
    const snap = createMockSnapshot();
    expect(resolveAsks(snap, "nonexistent")).toEqual([]);
  });

  it("returns normalized asks from order book", () => {
    const snap = createMockSnapshot();
    snap.orderBooks.set("YES", {
      bids: [],
      asks: [
        { price: 0.6, size: 10 },
        { price: 0.4, size: 5 },
      ],
      lastTrade: 0,
      totalBidValue: 0,
      totalAskValue: 0,
    });
    const result = resolveAsks(snap, "YES");
    expect(result).toEqual([
      { price: 0.4, size: 5 },
      { price: 0.6, size: 10 },
    ]);
  });
});

// ── findMaxEqualShares ───────────────────────────────────────────

describe("findMaxEqualShares", () => {
  it("computes correct shares with simple books", () => {
    const asksA: OrderBookLevel[] = [{ price: 0.4, size: 100 }];
    const asksB: OrderBookLevel[] = [{ price: 0.5, size: 100 }];
    const result = findMaxEqualShares(asksA, asksB, 100);
    expect(result).not.toBeNull();
    // cost per share = 0.4 + 0.5 = 0.9, so 100 / 0.9 = 111, but capped by size 100
    expect(result!.shares).toBe(100);
    expect(result!.costA).toBeCloseTo(40, 5);
    expect(result!.costB).toBeCloseTo(50, 5);
  });

  it("respects budget limits", () => {
    const asksA: OrderBookLevel[] = [{ price: 0.4, size: 1000 }];
    const asksB: OrderBookLevel[] = [{ price: 0.5, size: 1000 }];
    // budget = 9, cost per share = 0.9, so max 10 shares
    const result = findMaxEqualShares(asksA, asksB, 9);
    expect(result).not.toBeNull();
    expect(result!.shares).toBe(10);
    expect(result!.costA + result!.costB).toBeLessThanOrEqual(9);
  });

  it("walks multiple price levels", () => {
    const asksA: OrderBookLevel[] = [
      { price: 0.3, size: 5 },
      { price: 0.5, size: 10 },
    ];
    const asksB: OrderBookLevel[] = [
      { price: 0.4, size: 3 },
      { price: 0.6, size: 12 },
    ];
    const result = findMaxEqualShares(asksA, asksB, 100);
    expect(result).not.toBeNull();
    // First 3 shares: A@0.3 + B@0.4 = 0.7 each => cost 2.1
    // Next 2 shares: A@0.3 + B@0.6 = 0.9 each => cost 1.8
    // Next 10 shares: A@0.5 + B@0.6 = 1.1 each => cost 11
    expect(result!.shares).toBe(15);
  });

  it("returns null for empty books", () => {
    expect(findMaxEqualShares([], [{ price: 0.5, size: 10 }], 100)).toBeNull();
    expect(findMaxEqualShares([{ price: 0.5, size: 10 }], [], 100)).toBeNull();
    expect(findMaxEqualShares([], [], 100)).toBeNull();
  });

  it("returns null for zero budget", () => {
    const asks: OrderBookLevel[] = [{ price: 0.5, size: 10 }];
    expect(findMaxEqualShares(asks, asks, 0)).toBeNull();
  });

  it("returns null for negative budget", () => {
    const asks: OrderBookLevel[] = [{ price: 0.5, size: 10 }];
    expect(findMaxEqualShares(asks, asks, -100)).toBeNull();
  });

  it("handles size-limited scenario", () => {
    const asksA: OrderBookLevel[] = [{ price: 0.3, size: 2 }];
    const asksB: OrderBookLevel[] = [{ price: 0.3, size: 100 }];
    const result = findMaxEqualShares(asksA, asksB, 1000);
    expect(result).not.toBeNull();
    // Limited by asksA size
    expect(result!.shares).toBe(2);
  });

  it("handles single share when cost is high", () => {
    const asksA: OrderBookLevel[] = [{ price: 0.95, size: 100 }];
    const asksB: OrderBookLevel[] = [{ price: 0.95, size: 100 }];
    // cost per share = 1.90, budget = 2
    const result = findMaxEqualShares(asksA, asksB, 2);
    expect(result).not.toBeNull();
    expect(result!.shares).toBe(1);
  });

  it("returns null when first share exceeds budget", () => {
    const asksA: OrderBookLevel[] = [{ price: 0.6, size: 100 }];
    const asksB: OrderBookLevel[] = [{ price: 0.6, size: 100 }];
    // cost per share = 1.2, budget = 1.0
    const result = findMaxEqualShares(asksA, asksB, 1.0);
    expect(result).toBeNull();
  });
});

// ── computeFillEstimate ──────────────────────────────────────────

describe("computeFillEstimate", () => {
  it("computes correct gap (positive arb)", () => {
    const asksPoly: OrderBookLevel[] = [{ price: 0.4, size: 100 }];
    const asksKalshi: OrderBookLevel[] = [{ price: 0.5, size: 100 }];
    const result = computeFillEstimate(asksPoly, asksKalshi, 90);
    expect(result).not.toBeNull();
    // gap = 1 - (avgPoly + avgKalshi) = 1 - 0.9 = 0.1
    expect(result!.gap).toBeCloseTo(0.1, 5);
    expect(result!.avgPoly).toBeCloseTo(0.4, 5);
    expect(result!.avgKalshi).toBeCloseTo(0.5, 5);
  });

  it("computes negative gap (no arb)", () => {
    const asksPoly: OrderBookLevel[] = [{ price: 0.6, size: 100 }];
    const asksKalshi: OrderBookLevel[] = [{ price: 0.5, size: 100 }];
    const result = computeFillEstimate(asksPoly, asksKalshi, 100);
    expect(result).not.toBeNull();
    // gap = 1 - 1.1 = -0.1
    expect(result!.gap).toBeCloseTo(-0.1, 5);
  });

  it("returns null for empty books", () => {
    expect(computeFillEstimate([], [{ price: 0.5, size: 10 }], 100)).toBeNull();
    expect(computeFillEstimate([{ price: 0.5, size: 10 }], [], 100)).toBeNull();
  });

  it("avgPoly * shares = costPoly", () => {
    const asksPoly: OrderBookLevel[] = [
      { price: 0.3, size: 5 },
      { price: 0.5, size: 20 },
    ];
    const asksKalshi: OrderBookLevel[] = [{ price: 0.4, size: 50 }];
    const result = computeFillEstimate(asksPoly, asksKalshi, 50);
    expect(result).not.toBeNull();
    expect(result!.avgPoly * result!.shares).toBeCloseTo(result!.costPoly, 5);
    expect(result!.avgKalshi * result!.shares).toBeCloseTo(result!.costKalshi, 5);
    expect(result!.totalCost).toBeCloseTo(result!.costPoly + result!.costKalshi, 5);
  });

  it("totalCost never exceeds budget", () => {
    const asksPoly: OrderBookLevel[] = [{ price: 0.45, size: 500 }];
    const asksKalshi: OrderBookLevel[] = [{ price: 0.45, size: 500 }];
    const budget = 100;
    const result = computeFillEstimate(asksPoly, asksKalshi, budget);
    expect(result).not.toBeNull();
    expect(result!.totalCost).toBeLessThanOrEqual(budget);
  });
});

// ── Edge cases for real-world reliability ────────────────────────

describe("computeFillEstimate edge cases", () => {
  it("handles very thin liquidity (1 share each)", () => {
    const asksPoly: OrderBookLevel[] = [{ price: 0.40, size: 1 }];
    const asksKalshi: OrderBookLevel[] = [{ price: 0.50, size: 1 }];
    const result = computeFillEstimate(asksPoly, asksKalshi, 100);
    expect(result).not.toBeNull();
    expect(result!.shares).toBe(1);
    expect(result!.gap).toBeCloseTo(0.1, 5);
  });

  it("handles extremely deep books efficiently", () => {
    const asksPoly: OrderBookLevel[] = [];
    const asksKalshi: OrderBookLevel[] = [];
    for (let i = 0; i < 100; i++) {
      asksPoly.push({ price: 0.40 + i * 0.001, size: 10 });
      asksKalshi.push({ price: 0.50 + i * 0.001, size: 10 });
    }
    const result = computeFillEstimate(asksPoly, asksKalshi, 500);
    expect(result).not.toBeNull();
    expect(result!.shares).toBeGreaterThan(0);
    expect(result!.totalCost).toBeLessThanOrEqual(500);
  });

  it("gap decreases as fills walk deeper into book", () => {
    // First level is cheap, second is expensive
    const asksPoly: OrderBookLevel[] = [
      { price: 0.30, size: 5 },
      { price: 0.45, size: 100 },
    ];
    const asksKalshi: OrderBookLevel[] = [
      { price: 0.40, size: 5 },
      { price: 0.55, size: 100 },
    ];
    // Small fill: uses cheap levels
    const small = computeFillEstimate(asksPoly, asksKalshi, 5);
    // Large fill: walks into expensive levels
    const large = computeFillEstimate(asksPoly, asksKalshi, 100);
    expect(small).not.toBeNull();
    expect(large).not.toBeNull();
    // Small fill should have better gap (cheaper prices)
    expect(small!.gap).toBeGreaterThan(large!.gap);
  });
});

// ── randomDelayMs ────────────────────────────────────────────────

describe("randomDelayMs", () => {
  it("returns value within range", () => {
    for (let i = 0; i < 100; i++) {
      const delay = randomDelayMs(100, 500);
      expect(delay).toBeGreaterThanOrEqual(100);
      expect(delay).toBeLessThanOrEqual(500);
    }
  });

  it("handles equal min and max", () => {
    const delay = randomDelayMs(250, 250);
    expect(delay).toBe(250);
  });

  it("handles zero range", () => {
    const delay = randomDelayMs(0, 0);
    expect(delay).toBe(0);
  });

  it("handles inverted range (min > max)", () => {
    // Should clamp: max = max(min, max)
    const delay = randomDelayMs(500, 100);
    expect(delay).toBeGreaterThanOrEqual(100);
  });
});

// ── Helper ───────────────────────────────────────────────────────

function createMockSnapshot(): MarketSnapshot {
  return {
    provider: "polymarket",
    coin: "btc",
    symbol: "btc/usd",
    marketName: "BTC Up or Down",
    slug: "btc-updown-15m-test",
    timeLeftSec: 600,
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
  };
}
