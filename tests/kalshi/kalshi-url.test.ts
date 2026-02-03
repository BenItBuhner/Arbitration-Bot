import { describe, expect, it } from "bun:test";
import {
  deriveSeriesTickerFromMarket,
  looksLikeKalshiMarketTicker,
  parseKalshiMarketUrl,
} from "../../src/clients/kalshi/kalshi-url";

describe("kalshi url helpers", () => {
  it("derives series ticker from market ticker", () => {
    expect(deriveSeriesTickerFromMarket("KXETH15M-26FEB021730")).toBe(
      "KXETH15M",
    );
  });

  it("parses market url into market and series tickers", () => {
    const parsed = parseKalshiMarketUrl(
      "https://kalshi.com/markets/kxeth15m/eth-15m-price-up-down/kxeth15m-26feb021730",
    );
    expect(parsed.marketTicker).toBe("KXETH15M-26FEB021730");
    expect(parsed.seriesTicker).toBe("KXETH15M");
  });

  it("detects market ticker format", () => {
    expect(looksLikeKalshiMarketTicker("KXETH15M-26FEB021730")).toBe(true);
    expect(looksLikeKalshiMarketTicker("KXETH15M")).toBe(false);
  });
});
