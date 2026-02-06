import { describe, expect, it } from "bun:test";
import {
  KalshiOrderbookState,
  buildSyntheticAsks,
  normalizeLevels,
} from "../../src/clients/kalshi/kalshi-ws";

describe("Kalshi orderbook normalization", () => {
  it("normalizes cent-based levels", () => {
    const levels = normalizeLevels(
      [
        [8, 300],
        [22, 333],
      ],
      false,
    );
    expect(levels[0]?.price).toBeCloseTo(0.08, 4);
    expect(levels[1]?.price).toBeCloseTo(0.22, 4);
  });

  it("builds synthetic asks from opposite bids", () => {
    const asks = buildSyntheticAsks([
      { price: 0.56, size: 10 },
      { price: 0.54, size: 5 },
    ]);
    expect(asks[0]?.price).toBeCloseTo(0.44, 4);
    expect(asks[1]?.price).toBeCloseTo(0.46, 4);
  });

  it("applies snapshot and delta updates", () => {
    const state = new KalshiOrderbookState();
    state.applySnapshot(
      [
        { price: 0.22, size: 10 },
        { price: 0.08, size: 2 },
      ],
      [{ price: 0.55, size: 5 }],
    );
    state.applyDelta("yes", 0.22, -4);
    const update = state.toUpdate("TEST");
    expect(update.yesBids[0]?.price).toBeCloseTo(0.22, 4);
    expect(update.yesBids[0]?.size).toBeCloseTo(6, 4);
    expect(update.noBids[0]?.price).toBeCloseTo(0.55, 4);
    expect(update.yesAsks[0]?.price).toBeCloseTo(0.45, 4);
  });

  it("removes level when delta makes size zero or negative", () => {
    const state = new KalshiOrderbookState();
    state.applySnapshot(
      [{ price: 0.50, size: 5 }],
      [],
    );
    state.applyDelta("yes", 0.50, -5); // Exactly zero
    const update = state.toUpdate("TEST");
    expect(update.yesBids.length).toBe(0);
  });

  it("adds new level via delta on empty book", () => {
    const state = new KalshiOrderbookState();
    state.applySnapshot([], []);
    state.applyDelta("no", 0.40, 10);
    const update = state.toUpdate("TEST");
    expect(update.noBids.length).toBe(1);
    expect(update.noBids[0]?.price).toBeCloseTo(0.40, 4);
    expect(update.noBids[0]?.size).toBe(10);
  });

  it("snapshot replaces all previous state", () => {
    const state = new KalshiOrderbookState();
    state.applySnapshot(
      [{ price: 0.30, size: 100 }],
      [{ price: 0.60, size: 50 }],
    );
    // Replace with completely different state
    state.applySnapshot(
      [{ price: 0.80, size: 1 }],
      [],
    );
    const update = state.toUpdate("TEST");
    expect(update.yesBids.length).toBe(1);
    expect(update.yesBids[0]?.price).toBeCloseTo(0.80, 4);
    expect(update.noBids.length).toBe(0);
  });

  it("normalizeLevels rejects invalid data", () => {
    // Non-array input
    expect(normalizeLevels("not an array", false)).toEqual([]);
    expect(normalizeLevels(null, false)).toEqual([]);
    expect(normalizeLevels(undefined, false)).toEqual([]);

    // Array with invalid entries
    const levels = normalizeLevels(
      [
        [5, 100],      // valid
        "bad",          // not array
        [null, 50],     // null price
        [-1, 50],       // negative price (rejected)
        [10, -5],       // negative size (rejected)
        [NaN, 50],      // NaN price
      ],
      false,
    );
    expect(levels.length).toBe(1);
    expect(levels[0]?.price).toBeCloseTo(0.05, 4);
    expect(levels[0]?.size).toBe(100);
  });

  it("synthetic asks clamp to 0-1 range", () => {
    // Price > 1 should produce ask clamped to 0
    const asks = buildSyntheticAsks([
      { price: 1.5, size: 10 },
      { price: 0.3, size: 5 },
    ]);
    // 1 - 1.5 = -0.5, clamped to 0
    expect(asks[0]?.price).toBe(0);
    // 1 - 0.3 = 0.7
    expect(asks[1]?.price).toBeCloseTo(0.7, 4);
  });
});
