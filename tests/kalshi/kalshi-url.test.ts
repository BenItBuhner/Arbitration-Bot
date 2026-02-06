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

  it("derives series for BTC and SOL tickers", () => {
    expect(deriveSeriesTickerFromMarket("KXBTC15M-26FEB051530")).toBe("KXBTC15M");
    expect(deriveSeriesTickerFromMarket("KXSOL15M-26FEB051530")).toBe("KXSOL15M");
  });

  it("returns null for tickers without dash", () => {
    expect(deriveSeriesTickerFromMarket("NODASH")).toBeNull();
    expect(deriveSeriesTickerFromMarket("")).toBeNull();
  });

  it("handles demo API URLs", () => {
    const parsed = parseKalshiMarketUrl(
      "https://demo-api.kalshi.co/markets/kxbtc15m/kxbtc15m-26feb051530",
    );
    expect(parsed.marketTicker).toBe("KXBTC15M-26FEB051530");
    expect(parsed.seriesTicker).toBe("KXBTC15M");
  });

  it("returns null for non-URL input", () => {
    const parsed = parseKalshiMarketUrl("");
    expect(parsed.marketTicker).toBeNull();
    expect(parsed.seriesTicker).toBeNull();
  });

  it("returns null for invalid URL", () => {
    const parsed = parseKalshiMarketUrl("not-a-url");
    expect(parsed.marketTicker).toBeNull();
    expect(parsed.seriesTicker).toBeNull();
  });
});
