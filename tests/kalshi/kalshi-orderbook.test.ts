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
});
