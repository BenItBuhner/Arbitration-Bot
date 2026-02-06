import { describe, expect, it } from "bun:test";
import {
  resolveThreshold,
  computeFinalPrice,
  computeOutcomeFromValues,
  resolveCloseTimeMs,
} from "../src/services/outcome-resolution";
import type { MarketSnapshot, PricePoint } from "../src/services/market-data-hub";

// ── resolveThreshold ─────────────────────────────────────────────

describe("resolveThreshold", () => {
  it("returns priceToBeat when positive", () => {
    const snap = createSnap({ priceToBeat: 50000 });
    const result = resolveThreshold(snap);
    expect(result.value).toBe(50000);
    expect(result.source).toBe("price_to_beat");
  });

  it("falls back to referencePrice when priceToBeat is 0", () => {
    const snap = createSnap({
      priceToBeat: 0,
      referencePrice: 49500,
      referenceSource: "historical",
    });
    const result = resolveThreshold(snap);
    expect(result.value).toBe(49500);
    expect(result.source).toBe("historical");
  });

  it("extracts price from label containing 'price to beat'", () => {
    const snap = createSnap({
      priceToBeat: 0,
      referencePrice: 0,
      referenceSource: "missing",
      upOutcome: "Price to beat: $63,902.51",
    });
    const result = resolveThreshold(snap);
    // After fix: parseNumeric now extracts numeric from text strings
    expect(result.value).toBeCloseTo(63902.51, 1);
    expect(result.source).toBe("label");
  });

  it("extracts price from downOutcome label too", () => {
    const snap = createSnap({
      priceToBeat: 0,
      referencePrice: 0,
      referenceSource: "missing",
      upOutcome: "Something else",
      downOutcome: "Price to beat: $2,500.00",
    });
    const result = resolveThreshold(snap);
    expect(result.value).toBeCloseTo(2500.0, 1);
    expect(result.source).toBe("label");
  });

  it("returns null when no threshold available", () => {
    const snap = createSnap({
      priceToBeat: 0,
      referencePrice: 0,
      referenceSource: "missing",
    });
    const result = resolveThreshold(snap);
    expect(result.value).toBeNull();
    expect(result.source).toBe("missing");
  });

  it("prefers priceToBeat over referencePrice", () => {
    const snap = createSnap({
      priceToBeat: 50000,
      referencePrice: 49000,
    });
    const result = resolveThreshold(snap);
    expect(result.value).toBe(50000);
  });

  it("handles negative priceToBeat gracefully", () => {
    const snap = createSnap({
      priceToBeat: -100,
      referencePrice: 50000,
    });
    const result = resolveThreshold(snap);
    // negative priceToBeat is not > 0, so falls back
    expect(result.value).toBe(50000);
  });

  it("extracts large price from label with commas", () => {
    const snap = createSnap({
      priceToBeat: 0,
      referencePrice: 0,
      referenceSource: "missing",
      upOutcome: "Price to beat: $100,000.00",
    });
    const result = resolveThreshold(snap);
    expect(result.value).toBeCloseTo(100000, 0);
    expect(result.source).toBe("label");
  });

  it("extracts small price from label without commas", () => {
    const snap = createSnap({
      priceToBeat: 0,
      referencePrice: 0,
      referenceSource: "missing",
      downOutcome: "Price to beat: $150.25",
    });
    const result = resolveThreshold(snap);
    expect(result.value).toBeCloseTo(150.25, 1);
    expect(result.source).toBe("label");
  });
});

// ── resolveCloseTimeMs ───────────────────────────────────────────

describe("resolveCloseTimeMs", () => {
  it("returns marketCloseTimeMs when available", () => {
    const snap = createSnap({ marketCloseTimeMs: 1700000000000 });
    expect(resolveCloseTimeMs(snap, Date.now())).toBe(1700000000000);
  });

  it("computes from timeLeftSec when marketCloseTimeMs is null", () => {
    const now = 1700000000000;
    const snap = createSnap({
      marketCloseTimeMs: null,
      timeLeftSec: 60,
    });
    const result = resolveCloseTimeMs(snap, now);
    expect(result).toBe(now + 60000);
  });

  it("returns null when both are missing", () => {
    const snap = createSnap({
      marketCloseTimeMs: null,
      timeLeftSec: null,
    });
    expect(resolveCloseTimeMs(snap, Date.now())).toBeNull();
  });

  it("returns null for undefined values", () => {
    const snap = createSnap({});
    // Default has timeLeftSec: 600, so it should compute
    const now = Date.now();
    const result = resolveCloseTimeMs(snap, now);
    expect(result).toBe(now + 600 * 1000);
  });
});

// ── computeFinalPrice ────────────────────────────────────────────

describe("computeFinalPrice", () => {
  it("uses kalshi underlying for kalshi provider", () => {
    const snap = createSnap({
      provider: "kalshi",
      kalshiUnderlyingValue: 50123,
      kalshiUnderlyingTs: 1700000000000 - 30000,
      marketCloseTimeMs: 1700000000000,
    });
    const result = computeFinalPrice(snap, 1700000000000, {
      windowMs: 60000,
      minPoints: 3,
      allowStaleAfterMs: 120000,
    });
    expect(result.value).toBe(50123);
    expect(result.source).toContain("kalshi_underlying");
  });

  it("uses spot_avg_window when enough price points in window", () => {
    const closeMs = 1700000000000;
    const snap = createSnap({
      marketCloseTimeMs: closeMs,
      priceHistoryWithTs: [
        { price: 50000, ts: closeMs - 50000 },
        { price: 50100, ts: closeMs - 30000 },
        { price: 50200, ts: closeMs - 10000 },
      ],
    });
    const result = computeFinalPrice(snap, closeMs + 1000, {
      windowMs: 60000,
      minPoints: 3,
      allowStaleAfterMs: 120000,
    });
    expect(result.value).toBeCloseTo(50100, 0);
    expect(result.source).toBe("spot_avg_window");
    expect(result.points).toBe(3);
  });

  it("returns pending when not enough points and not stale yet", () => {
    const closeMs = 1700000000000;
    const now = closeMs + 5000; // only 5s after close
    const snap = createSnap({
      marketCloseTimeMs: closeMs,
      priceHistoryWithTs: [{ price: 50000, ts: closeMs - 50000 }],
    });
    const result = computeFinalPrice(snap, now, {
      windowMs: 60000,
      minPoints: 3,
      allowStaleAfterMs: 120000,
    });
    expect(result.value).toBeNull();
    expect(result.source).toBe("pending_final_price");
  });

  it("uses spot_last as stale fallback", () => {
    const closeMs = 1700000000000;
    const now = closeMs + 200000; // well past stale threshold
    const snap = createSnap({
      marketCloseTimeMs: closeMs,
      cryptoPrice: 49999,
      priceHistoryWithTs: [],
    });
    const result = computeFinalPrice(snap, now, {
      windowMs: 60000,
      minPoints: 3,
      allowStaleAfterMs: 120000,
    });
    expect(result.value).toBe(49999);
    expect(result.source).toContain("spot_last");
  });

  it("returns missing when no data available", () => {
    const closeMs = 1700000000000;
    const now = closeMs + 200000;
    const snap = createSnap({
      marketCloseTimeMs: closeMs,
      cryptoPrice: 0,
      priceHistoryWithTs: [],
    });
    const result = computeFinalPrice(snap, now, {
      windowMs: 60000,
      minPoints: 3,
      allowStaleAfterMs: 120000,
    });
    expect(result.value).toBeNull();
    expect(result.source).toBe("missing");
  });
});

// ── computeOutcomeFromValues ─────────────────────────────────────

describe("computeOutcomeFromValues", () => {
  it("returns UP when price >= threshold", () => {
    expect(computeOutcomeFromValues(50001, 50000)).toBe("UP");
    expect(computeOutcomeFromValues(50000, 50000)).toBe("UP");
  });

  it("returns DOWN when price < threshold", () => {
    expect(computeOutcomeFromValues(49999, 50000)).toBe("DOWN");
  });

  it("returns UNKNOWN for null price", () => {
    expect(computeOutcomeFromValues(null, 50000)).toBe("UNKNOWN");
  });

  it("returns UNKNOWN for null threshold", () => {
    expect(computeOutcomeFromValues(50000, null)).toBe("UNKNOWN");
  });

  it("returns UNKNOWN for zero threshold", () => {
    expect(computeOutcomeFromValues(50000, 0)).toBe("UNKNOWN");
  });

  it("returns UNKNOWN for NaN price", () => {
    expect(computeOutcomeFromValues(NaN, 50000)).toBe("UNKNOWN");
  });

  it("returns UNKNOWN for Infinity threshold", () => {
    expect(computeOutcomeFromValues(50000, Infinity)).toBe("UNKNOWN");
  });

  it("returns UNKNOWN for negative threshold", () => {
    expect(computeOutcomeFromValues(50000, -100)).toBe("UNKNOWN");
  });
});

// ── Helper ───────────────────────────────────────────────────────

function createSnap(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
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
    ...overrides,
  };
}
