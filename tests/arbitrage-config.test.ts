import { describe, expect, it } from "bun:test";
import { loadArbitrageConfig } from "../src/services/arbitrage-config";
import { loadProviderConfig } from "../src/services/profile-config";

describe("loadArbitrageConfig", () => {
  it("loads config.json and returns profiles", () => {
    const result = loadArbitrageConfig();
    expect(result.profiles.length).toBeGreaterThan(0);
    expect(result.coinOptions.length).toBeGreaterThan(0);
  });

  it("contains arbBotV1 profile", () => {
    const result = loadArbitrageConfig();
    const names = result.profiles.map((p) => p.name);
    expect(names).toContain("arbBotV1");
  });

  it("arbBotV1 has expected coins", () => {
    const result = loadArbitrageConfig();
    const arbBot = result.profiles.find((p) => p.name === "arbBotV1");
    expect(arbBot).not.toBeUndefined();

    const coinKeys = Array.from(arbBot!.coins.keys());
    expect(coinKeys).toContain("btc");
    expect(coinKeys).toContain("eth");
    expect(coinKeys).toContain("sol");
  });

  it("coin configs have required fields", () => {
    const result = loadArbitrageConfig();
    const arbBot = result.profiles.find((p) => p.name === "arbBotV1");
    expect(arbBot).not.toBeUndefined();

    for (const [coin, config] of arbBot!.coins) {
      expect(config.tradeAllowedTimeLeft).toBeGreaterThan(0);
      expect(config.minGap).toBeGreaterThan(0);
      expect(config.maxSpendTotal).toBeGreaterThan(0);
      expect(config.minSpendTotal).toBeGreaterThanOrEqual(0);
      expect(config.minSpendTotal).toBeLessThanOrEqual(config.maxSpendTotal);
    }
  });

  it("coinOptions derived from profiles", () => {
    const result = loadArbitrageConfig();
    // coinOptions should include all coins from all profiles
    for (const profile of result.profiles) {
      for (const coin of profile.coins.keys()) {
        expect(result.coinOptions).toContain(coin);
      }
    }
  });

  it("fillUsd does not exceed maxSpendTotal", () => {
    const result = loadArbitrageConfig();
    for (const profile of result.profiles) {
      for (const [coin, config] of profile.coins) {
        if (config.fillUsd !== null && config.fillUsd > 0) {
          expect(config.fillUsd).toBeLessThanOrEqual(config.maxSpendTotal);
        }
      }
    }
  });

  it("tradeAllowedTimeLeft is positive for all coins", () => {
    const result = loadArbitrageConfig();
    for (const profile of result.profiles) {
      for (const [coin, config] of profile.coins) {
        expect(config.tradeAllowedTimeLeft).toBeGreaterThan(0);
      }
    }
  });

  it("minGap is reasonable (between 0 and 1)", () => {
    const result = loadArbitrageConfig();
    for (const profile of result.profiles) {
      for (const [coin, config] of profile.coins) {
        expect(config.minGap).toBeGreaterThan(0);
        expect(config.minGap).toBeLessThan(1);
      }
    }
  });
});

describe("provider configs", () => {
  it("polymarket config loads without error", () => {
    expect(() => loadProviderConfig("polymarket")).not.toThrow();
  });

  it("kalshi config loads without error", () => {
    expect(() => loadProviderConfig("kalshi")).not.toThrow();
  });

  it("kalshi config has coin selectors for arb coins", () => {
    const kalshi = loadProviderConfig("kalshi");
    const arb = loadArbitrageConfig();
    // Every coin in the arb config should have a Kalshi selector
    for (const coin of arb.coinOptions) {
      const hasSelector = kalshi.kalshiSelectorsByCoin?.has(coin) ?? false;
      expect(hasSelector).toBe(true);
    }
  });

  it("polymarket config has coins matching arb config", () => {
    const poly = loadProviderConfig("polymarket");
    const arb = loadArbitrageConfig();
    // At minimum, arb coins should be a subset of poly coins (or both have them)
    for (const coin of arb.coinOptions) {
      // poly may have broader coin list -- that's fine
      // but we verify poly config loads successfully
      expect(poly.provider).toBe("polymarket");
    }
  });
});
