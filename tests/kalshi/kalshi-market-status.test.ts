import { describe, expect, it } from "bun:test";
import { isMarketOpen } from "../../src/services/kalshi-market-data-hub";

describe("isMarketOpen", () => {
  it("returns true for status 'open'", () => {
    expect(isMarketOpen({ status: "open" })).toBe(true);
  });

  it("returns true for status 'active'", () => {
    expect(isMarketOpen({ status: "active" })).toBe(true);
  });

  it("returns true for status 'initialized'", () => {
    expect(isMarketOpen({ status: "initialized" })).toBe(true);
  });

  it("returns false for status 'closed'", () => {
    expect(isMarketOpen({ status: "closed" })).toBe(false);
  });

  it("returns false for status 'settled'", () => {
    expect(isMarketOpen({ status: "settled" })).toBe(false);
  });

  it("returns false for status 'determined'", () => {
    expect(isMarketOpen({ status: "determined" })).toBe(false);
  });

  it("returns false for status 'finalized'", () => {
    expect(isMarketOpen({ status: "finalized" })).toBe(false);
  });

  it("returns false for status 'paused'", () => {
    expect(isMarketOpen({ status: "paused" })).toBe(false);
  });

  it("returns false for status 'unopened'", () => {
    expect(isMarketOpen({ status: "unopened" })).toBe(false);
  });

  it("is case-insensitive", () => {
    expect(isMarketOpen({ status: "OPEN" })).toBe(true);
    expect(isMarketOpen({ status: "Closed" })).toBe(false);
  });

  it("returns true when close_time is in the future", () => {
    const futureMs = Date.now() + 600_000;
    expect(isMarketOpen({ close_time: new Date(futureMs).toISOString() })).toBe(true);
  });

  it("falls through to default for unknown status with no close_time", () => {
    // Unknown status + no close_time: returns true (not "closed" or "settled")
    expect(isMarketOpen({ status: "unknown" })).toBe(true);
  });

  it("returns true for empty object (no status)", () => {
    // Empty string status + no close_time
    expect(isMarketOpen({})).toBe(true);
  });
});
