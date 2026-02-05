import { describe, expect, it } from "bun:test";
import { loadArbitrageConfig } from "../src/services/arbitrage-config";

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
});
