import { describe, expect, it } from "bun:test";
import { parseStrikePrice } from "../../src/services/kalshi-market-data-hub";

describe("parseStrikePrice", () => {
  describe("strike block with strike_type", () => {
    it("extracts floor_strike when strike_type is 'greater'", () => {
      const market = {
        strike: {
          strike_type: "greater",
          floor_strike: 50000,
          cap_strike: 55000,
        },
      };
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("extracts cap_strike when strike_type is 'less'", () => {
      const market = {
        strike: {
          strike_type: "less",
          floor_strike: 45000,
          cap_strike: 50000,
        },
      };
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("extracts floor_strike when strike_type contains 'above'", () => {
      const market = {
        strike: {
          strike_type: "above_threshold",
          floor_strike: 63000,
        },
      };
      expect(parseStrikePrice(market)).toBe(63000);
    });
  });

  describe("flat market fields", () => {
    it("extracts floor_strike from top-level", () => {
      const market = { floor_strike: 50000 };
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("extracts cap_strike when floor_strike is missing", () => {
      const market = { cap_strike: 50000 };
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("extracts strike_price_dollars", () => {
      const market = { strike_price_dollars: 63902.51 };
      expect(parseStrikePrice(market)).toBeCloseTo(63902.51, 1);
    });

    it("extracts strike_price_cents and converts to dollars", () => {
      const market = { strike_price_cents: 5000000 };
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("extracts reference_price", () => {
      const market = { reference_price: 2500.50 };
      expect(parseStrikePrice(market)).toBeCloseTo(2500.50, 1);
    });

    it("extracts price_to_beat", () => {
      const market = { price_to_beat: 100000 };
      expect(parseStrikePrice(market)).toBe(100000);
    });
  });

  describe("title extraction", () => {
    it("extracts price from title with $ sign", () => {
      const market = { title: "Will BTC be above $50,000?" };
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("extracts price from title with comma formatting", () => {
      const market = { title: "Will ETH close above $3,500.00 today?" };
      expect(parseStrikePrice(market)).toBe(3500);
    });

    it("extracts large price from title", () => {
      const market = { title: "Bitcoin above $100,000?" };
      expect(parseStrikePrice(market)).toBe(100000);
    });
  });

  describe("edge cases", () => {
    it("returns 0 for empty market object", () => {
      expect(parseStrikePrice({})).toBe(0);
    });

    it("returns 0 for market with no numeric fields", () => {
      const market = { title: "Some random market", status: "open" };
      expect(parseStrikePrice(market)).toBe(0);
    });

    it("handles strike as a number (not object)", () => {
      const market = { strike: 50000 };
      // strike is a number, not an object, so strikeBlock is null
      // but it falls through to the deep search which finds "strike" key
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("handles string values that look like numbers", () => {
      const market = { strike_price_dollars: "63902.51" };
      expect(parseStrikePrice(market)).toBeCloseTo(63902.51, 1);
    });

    it("handles nested display_value objects", () => {
      const market = {
        strike_price_dollars: { value: "50000", display_value: "$50,000" },
      };
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("prefers floor_strike over deep search results", () => {
      const market = {
        floor_strike: 50000,
        strike_price: 49000,
        reference_price: 48000,
      };
      expect(parseStrikePrice(market)).toBe(50000);
    });

    it("ignores zero and negative values", () => {
      const market = {
        floor_strike: 0,
        cap_strike: -100,
        strike_price_dollars: 50000,
      };
      expect(parseStrikePrice(market)).toBe(50000);
    });
  });

  describe("real-world Kalshi market formats", () => {
    it("handles KXBTC15M market format", () => {
      const market = {
        ticker: "KXBTC15M-26FEB05-T1530-B63902",
        title: "Bitcoin above $63,902.51?",
        strike: {
          strike_type: "above",
          floor_strike: 63902.51,
        },
      };
      expect(parseStrikePrice(market)).toBeCloseTo(63902.51, 1);
    });

    it("handles market with only underlying_value-adjacent fields", () => {
      // Some markets may have reference_price but not strike
      const market = {
        title: "ETH 15-min Up or Down",
        reference_price: 2500,
      };
      expect(parseStrikePrice(market)).toBe(2500);
    });

    it("handles market where strike_price is in cents", () => {
      const market = {
        strike_price_cents: 6390251,
      };
      // 6390251 cents / 100 = 63902.51
      expect(parseStrikePrice(market)).toBeCloseTo(63902.51, 1);
    });
  });
});
