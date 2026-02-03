import { describe, expect, it } from "bun:test";
import { resolveMarketProvider } from "../../src/providers/provider";

describe("resolveMarketProvider", () => {
  it("defaults to polymarket", () => {
    const provider = resolveMarketProvider({});
    expect(provider).toBe("polymarket");
  });

  it("honors kalshi env", () => {
    const provider = resolveMarketProvider({ MARKET_PROVIDER: "kalshi" });
    expect(provider).toBe("kalshi");
  });

  it("normalizes case", () => {
    const provider = resolveMarketProvider({ MARKET_PROVIDER: "KaLsHi" });
    expect(provider).toBe("kalshi");
  });
});
