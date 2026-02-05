import { describe, expect, it } from "bun:test";
import { parseUpDownSlugStartMs } from "../src/services/auto-market";

describe("parseUpDownSlugStartMs", () => {
  it("parses standard 15m slug with epoch seconds", () => {
    const slug = "btc-updown-15m-1706000000";
    const result = parseUpDownSlugStartMs(slug);
    expect(result).toBe(1706000000000); // seconds → ms
  });

  it("parses standard 15m slug with epoch milliseconds", () => {
    const slug = "eth-updown-15m-1706000000000";
    const result = parseUpDownSlugStartMs(slug);
    expect(result).toBe(1706000000000);
  });

  it("parses 'up-or-down' variant slug", () => {
    const slug = "sol-up-or-down-15m-1706000000";
    const result = parseUpDownSlugStartMs(slug);
    expect(result).toBe(1706000000000);
  });

  it("parses 4h duration slugs", () => {
    const slug = "btc-updown-4h-1706000000";
    const result = parseUpDownSlugStartMs(slug);
    expect(result).toBe(1706000000000);
  });

  it("returns null for non-matching slugs", () => {
    expect(parseUpDownSlugStartMs("some-random-market")).toBeNull();
    expect(parseUpDownSlugStartMs("btc-price-prediction")).toBeNull();
    expect(parseUpDownSlugStartMs("")).toBeNull();
  });

  it("handles XRP coin symbol", () => {
    const slug = "xrp-updown-15m-1706000000";
    const result = parseUpDownSlugStartMs(slug);
    expect(result).toBe(1706000000000);
  });

  it("is case-insensitive", () => {
    const slug = "BTC-UPDOWN-15M-1706000000";
    const result = parseUpDownSlugStartMs(slug);
    expect(result).toBe(1706000000000);
  });

  it("returns null for slug with too-short timestamp", () => {
    const slug = "btc-updown-15m-12345678"; // only 8 digits
    const result = parseUpDownSlugStartMs(slug);
    expect(result).toBeNull();
  });
});
