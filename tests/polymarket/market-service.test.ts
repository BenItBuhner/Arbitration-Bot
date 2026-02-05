import { describe, expect, it } from "bun:test";
import {
  extractOutcomes,
  extractTokenIds,
  extractPriceToBeat,
} from "../../src/services/market-service";

describe("market-service", () => {
  it("extracts outcomes and token ids from JSON strings", () => {
    const market: any = {
      outcomes: JSON.stringify(["Yes", "No"]),
      clobTokenIds: JSON.stringify(["tokenA", "tokenB"]),
    };
    expect(extractOutcomes(market)).toEqual(["Yes", "No"]);
    expect(extractTokenIds(market)).toEqual(["tokenA", "tokenB"]);
  });

  it("extracts price to beat from groupItemTitle", () => {
    const market: any = {
      groupItemTitle: "$4,000",
      groupItemThreshold: "2",
    };
    expect(extractPriceToBeat(market)).toBe(4000);
  });

  it("falls back to groupItemThreshold when title missing", () => {
    const market: any = {
      groupItemThreshold: "50000",
    };
    expect(extractPriceToBeat(market)).toBe(50000);
  });

  it("extracts large price with comma from groupItemTitle", () => {
    const market: any = {
      groupItemTitle: "88,000",
    };
    expect(extractPriceToBeat(market)).toBe(88000);
  });

  it("returns 0 when groupItemTitle is a sort index (< 100)", () => {
    const market: any = {
      groupItemTitle: "5",
      groupItemThreshold: "50000",
    };
    // groupItemTitle "5" is < 100, so falls through to threshold
    expect(extractPriceToBeat(market)).toBe(50000);
  });

  it("returns 0 when no price fields exist", () => {
    const market: any = {
      slug: "some-market",
      question: "Will it rain?",
    };
    expect(extractPriceToBeat(market)).toBe(0);
  });

  it("ignores groupItemThreshold when < 100", () => {
    const market: any = {
      groupItemThreshold: "3",
    };
    expect(extractPriceToBeat(market)).toBe(0);
  });

  it("handles outcomes as array instead of JSON string", () => {
    const market: any = {
      outcomes: ["Up", "Down"],
      clobTokenIds: ["tok1", "tok2"],
    };
    expect(extractOutcomes(market)).toEqual(["Up", "Down"]);
    expect(extractTokenIds(market)).toEqual(["tok1", "tok2"]);
  });

  it("returns empty for missing outcomes", () => {
    expect(extractOutcomes({} as any)).toEqual([]);
    expect(extractTokenIds({} as any)).toEqual([]);
  });
});
