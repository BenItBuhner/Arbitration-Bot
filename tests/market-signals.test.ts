import { describe, expect, it } from "bun:test";
import { computeSignals, type SignalInput, type TradeLike } from "../src/services/market-signals";

function makeInput(overrides: Partial<SignalInput> = {}): SignalInput {
  return {
    orderBooks: new Map(),
    bestBid: new Map(),
    bestAsk: new Map(),
    priceHistory: [],
    cryptoPriceTimestamp: Date.now(),
    referenceSource: "price_to_beat",
    priceToBeat: 50000,
    referencePrice: 50000,
    ...overrides,
  };
}

describe("computeSignals", () => {
  it("returns empty signals for empty input", () => {
    const input = makeInput();
    const result = computeSignals(input, Date.now());
    expect(result.tokenSignals.size).toBe(0);
    expect(result.priceMomentum).toBeNull();
    expect(result.priceVolatility).toBeNull();
    expect(result.tradeVelocity).toBeNull();
    expect(result.referenceQuality).toBe(1); // price_to_beat
  });

  it("computes token signals from order book data", () => {
    const input = makeInput();
    input.orderBooks.set("YES", {
      bids: [{ price: 0.50, size: 100 }],
      asks: [{ price: 0.55, size: 50 }],
      totalBidValue: 50,
      totalAskValue: 27.5,
    });
    input.bestBid.set("YES", 0.50);
    input.bestAsk.set("YES", 0.55);

    const result = computeSignals(input, Date.now());
    const yesSignal = result.tokenSignals.get("YES");
    expect(yesSignal).not.toBeUndefined();
    expect(yesSignal!.bestBid).toBe(0.50);
    expect(yesSignal!.bestAsk).toBe(0.55);
    expect(yesSignal!.spread).toBeCloseTo(0.05, 4);
    expect(yesSignal!.midPrice).toBeCloseTo(0.525, 4);
  });

  it("computes price momentum from price history", () => {
    const input = makeInput({
      priceHistory: [50000, 50100, 50200, 50300, 50400],
    });
    const result = computeSignals(input, Date.now());
    expect(result.priceMomentum).not.toBeNull();
    // Price is trending up, momentum should be positive
    expect(result.priceMomentum!).toBeGreaterThan(0);
  });

  it("computes price volatility from history", () => {
    const input = makeInput({
      priceHistory: [50000, 50500, 49500, 50200, 49800],
    });
    const result = computeSignals(input, Date.now());
    expect(result.priceVolatility).not.toBeNull();
    expect(result.priceVolatility!).toBeGreaterThan(0);
  });

  it("returns null momentum for < 2 price points", () => {
    const input = makeInput({ priceHistory: [50000] });
    const result = computeSignals(input, Date.now());
    expect(result.priceMomentum).toBeNull();
  });

  it("computes trade velocity from trades", () => {
    const now = Date.now();
    const trades: TradeLike[] = [];
    for (let i = 0; i < 10; i++) {
      trades.push({
        timestamp: now - i * 10_000,
        price: 0.50,
        size: 5,
        side: i % 2 === 0 ? "BUY" : "SELL",
      });
    }
    const result = computeSignals(makeInput(), now, trades);
    expect(result.tradeVelocity).not.toBeNull();
    expect(result.tradeVelocity!).toBeGreaterThan(0);
  });

  it("handles empty trades gracefully", () => {
    const result = computeSignals(makeInput(), Date.now(), []);
    expect(result.tradeVelocity).toBeNull();
    expect(result.tradeFlowImbalance).toBeNull();
  });

  it("computes reference quality correctly", () => {
    expect(computeSignals(makeInput({ referenceSource: "price_to_beat", priceToBeat: 50000 }), Date.now()).referenceQuality).toBe(1);
    expect(computeSignals(makeInput({ referenceSource: "kalshi_underlying" }), Date.now()).referenceQuality).toBe(1);
    expect(computeSignals(makeInput({ referenceSource: "html" }), Date.now()).referenceQuality).toBe(1);
    expect(computeSignals(makeInput({ referenceSource: "historical", priceToBeat: 0 }), Date.now()).referenceQuality).toBeCloseTo(0.6, 1);
    expect(computeSignals(makeInput({ referenceSource: "missing", priceToBeat: 0 }), Date.now()).referenceQuality).toBe(0);
  });

  it("computes price staleness in seconds", () => {
    const now = Date.now();
    const input = makeInput({ cryptoPriceTimestamp: now - 5000 });
    const result = computeSignals(input, now);
    expect(result.priceStalenessSec).not.toBeNull();
    expect(result.priceStalenessSec!).toBeCloseTo(5, 0);
  });

  it("handles NaN/Infinity in price history gracefully", () => {
    const input = makeInput({
      priceHistory: [50000, NaN, 50200, Infinity, 50100],
    });
    // Should not throw
    expect(() => computeSignals(input, Date.now())).not.toThrow();
  });

  it("computes book imbalance correctly", () => {
    const input = makeInput();
    input.orderBooks.set("YES", {
      bids: [{ price: 0.50, size: 200 }],
      asks: [{ price: 0.55, size: 50 }],
      totalBidValue: 100,
      totalAskValue: 27.5,
    });
    input.bestBid.set("YES", 0.50);
    input.bestAsk.set("YES", 0.55);

    const result = computeSignals(input, Date.now());
    const yesSignal = result.tokenSignals.get("YES");
    expect(yesSignal!.bookImbalance).not.toBeNull();
    // bidValue / (bidValue + askValue) = 100 / 127.5 ≈ 0.784
    expect(yesSignal!.bookImbalance!).toBeCloseTo(0.784, 2);
  });
});
