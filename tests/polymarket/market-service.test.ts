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
});
