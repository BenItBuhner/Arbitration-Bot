import { describe, expect, it } from "bun:test";
import { resolveMarketGroup } from "../src/services/market-groups";
import type { MarketSnapshot } from "../src/services/market-data-hub";

const baseSnapshot: MarketSnapshot = {
  provider: "polymarket",
  coin: "eth",
  symbol: "eth/usd",
  marketName: "ETH Up or Down 15m",
  slug: "eth-updown-15m-123",
  seriesSlug: "eth-up-or-down-15m",
  marketTicker: null,
  eventTicker: null,
  timeLeftSec: 100,
  priceToBeat: 0,
  referencePrice: 0,
  referenceSource: "missing",
  cryptoPrice: 0,
  cryptoPriceTimestamp: 0,
  dataStatus: "unknown",
  lastBookUpdateMs: 0,
  upOutcome: "Up",
  downOutcome: "Down",
  upTokenId: "YES",
  downTokenId: "NO",
  orderBooks: new Map(),
  bestBid: new Map(),
  bestAsk: new Map(),
  priceHistory: [],
};

describe("resolveMarketGroup", () => {
  it("matches polymarket slug regex", () => {
    const group = resolveMarketGroup("polymarket", baseSnapshot, [
      { id: "updown-15m", match: { slugRegex: "^eth-" } },
      { id: "default", match: {} },
    ]);
    expect(group).toBe("updown-15m");
  });

  it("matches kalshi ticker prefix", () => {
    const kalshiSnapshot: MarketSnapshot = {
      ...baseSnapshot,
      provider: "kalshi",
      marketTicker: "KXBTCUSD-2026-02-02-1800",
      slug: "KXBTCUSD-2026-02-02-1800",
      marketName: "Will BTC be above $100,000?",
      upOutcome: "Yes",
      downOutcome: "No",
    };
    const group = resolveMarketGroup("kalshi", kalshiSnapshot, [
      { id: "daily", match: { tickerPrefix: "KXBTCUSD-" } },
      { id: "default", match: {} },
    ]);
    expect(group).toBe("daily");
  });
});
