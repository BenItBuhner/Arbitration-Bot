import { describe, expect, it } from "bun:test";
import {
  normalizeOutcomeLabel,
  computeOddsMid,
  createAccuracyState,
  updateAccuracy,
  getAccuracyPercent,
  getWindowAccuracyPercent,
} from "../src/services/cross-platform-compare";

// ── normalizeOutcomeLabel ────────────────────────────────────────

describe("normalizeOutcomeLabel", () => {
  it("normalizes 'Up' to UP", () => {
    expect(normalizeOutcomeLabel("Up")).toBe("UP");
    expect(normalizeOutcomeLabel("up")).toBe("UP");
    expect(normalizeOutcomeLabel("UP")).toBe("UP");
  });

  it("normalizes 'Down' to DOWN", () => {
    expect(normalizeOutcomeLabel("Down")).toBe("DOWN");
    expect(normalizeOutcomeLabel("down")).toBe("DOWN");
    expect(normalizeOutcomeLabel("DOWN")).toBe("DOWN");
  });

  it("normalizes 'Yes' to UP", () => {
    expect(normalizeOutcomeLabel("Yes")).toBe("UP");
    expect(normalizeOutcomeLabel("yes")).toBe("UP");
  });

  it("normalizes 'No' to DOWN", () => {
    expect(normalizeOutcomeLabel("No")).toBe("DOWN");
    expect(normalizeOutcomeLabel("no")).toBe("DOWN");
  });

  it("normalizes 'above'/'over'/'greater' variants to UP", () => {
    expect(normalizeOutcomeLabel("above $50,000")).toBe("UP");
    expect(normalizeOutcomeLabel("over 50000")).toBe("UP");
    expect(normalizeOutcomeLabel("greater than")).toBe("UP");
  });

  it("normalizes 'below'/'under'/'less' variants to DOWN", () => {
    expect(normalizeOutcomeLabel("below $50,000")).toBe("DOWN");
    expect(normalizeOutcomeLabel("under 50000")).toBe("DOWN");
    expect(normalizeOutcomeLabel("less than")).toBe("DOWN");
  });

  it("returns UNKNOWN for unrecognizable labels", () => {
    expect(normalizeOutcomeLabel("")).toBe("UNKNOWN");
    expect(normalizeOutcomeLabel("maybe")).toBe("UNKNOWN");
    expect(normalizeOutcomeLabel("50000")).toBe("UNKNOWN");
  });

  it("handles whitespace", () => {
    expect(normalizeOutcomeLabel("  Up  ")).toBe("UP");
    expect(normalizeOutcomeLabel("  Down  ")).toBe("DOWN");
  });
});

// ── computeOddsMid ──────────────────────────────────────────────

describe("computeOddsMid", () => {
  it("computes mid of bid and ask", () => {
    expect(computeOddsMid(0.4, 0.6)).toBe(0.5);
  });

  it("clamps to 0-1 range", () => {
    // mid of -0.1 and 0.3 = 0.1
    expect(computeOddsMid(-0.1, 0.3)).toBeCloseTo(0.1, 10);
    // mid of 0.8 and 1.4 = 1.1 -> clamped to 1
    expect(computeOddsMid(0.8, 1.4)).toBe(1);
  });

  it("returns null when bid is null", () => {
    expect(computeOddsMid(null, 0.5)).toBeNull();
  });

  it("returns null when ask is null", () => {
    expect(computeOddsMid(0.5, null)).toBeNull();
  });

  it("returns null for NaN values", () => {
    expect(computeOddsMid(NaN, 0.5)).toBeNull();
    expect(computeOddsMid(0.5, NaN)).toBeNull();
  });

  it("returns null for Infinity", () => {
    expect(computeOddsMid(Infinity, 0.5)).toBeNull();
    expect(computeOddsMid(0.5, Infinity)).toBeNull();
  });
});

// ── Accuracy tracking ────────────────────────────────────────────

describe("accuracy tracking", () => {
  it("starts at zero", () => {
    const state = createAccuracyState();
    expect(state.matchCount).toBe(0);
    expect(state.totalCount).toBe(0);
    expect(getAccuracyPercent(state)).toBeNull();
  });

  it("counts matching outcomes", () => {
    const state = createAccuracyState();
    updateAccuracy(state, "UP", "UP");
    updateAccuracy(state, "DOWN", "DOWN");
    expect(state.matchCount).toBe(2);
    expect(state.totalCount).toBe(2);
    expect(getAccuracyPercent(state)).toBe(100);
  });

  it("counts mismatched outcomes", () => {
    const state = createAccuracyState();
    updateAccuracy(state, "UP", "DOWN");
    expect(state.matchCount).toBe(0);
    expect(state.totalCount).toBe(1);
    expect(getAccuracyPercent(state)).toBe(0);
  });

  it("ignores UNKNOWN outcomes", () => {
    const state = createAccuracyState();
    updateAccuracy(state, "UNKNOWN", "UP");
    updateAccuracy(state, "UP", "UNKNOWN");
    expect(state.totalCount).toBe(0);
  });

  it("computes window accuracy", () => {
    const state = createAccuracyState();
    for (let i = 0; i < 10; i++) {
      updateAccuracy(state, "UP", "UP");
    }
    updateAccuracy(state, "UP", "DOWN");
    // 10/11 matches
    const windowAcc = getWindowAccuracyPercent(state, 100);
    expect(windowAcc).toBeCloseTo(90.909, 1);
  });
});
