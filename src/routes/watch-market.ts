import {
  quickMarketLookup,
  extractTokenIds,
  extractOutcomes,
  extractPriceToBeat,
} from "../services/market-service";
import { resolveMarketProvider } from "../providers/provider";
import { MarketWS, type MarketEvent } from "../clients/market-ws";
import { CryptoWS, type CryptoPricePayload } from "../clients/crypto-ws";
import { fetchHistoricalCryptoPrice } from "../services/crypto-service";
import { Dashboard, colors } from "../cli/dashboard";
import { promptText } from "../cli/prompts";
import { getKalshiEnvConfig } from "../clients/kalshi/kalshi-config";
import { KalshiClient } from "../clients/kalshi/kalshi-client";
import { KalshiMarketWS } from "../clients/kalshi/kalshi-ws";
import {
  looksLikeKalshiMarketTicker,
  normalizeKalshiTicker,
  parseKalshiMarketUrl,
} from "../clients/kalshi/kalshi-url";

/**
 * Watch market route - mirrors exp/example-usage.ts monitor behavior
 */
export interface WatchMarketRouteOptions {
  searchTerm?: string;
}

export async function watchMarketRoute(
  options: WatchMarketRouteOptions = {},
): Promise<void> {
  let input = options.searchTerm;
  if (!input) {
    if (process.stdin.isTTY) {
      const response = await promptText(
        `${colors.bright}${colors.cyan}Enter market keyword or Polymarket URL:${colors.reset} `,
      );
      if (response === null) {
        console.log("No input provided. Exiting.");
        return;
      }
      input = response;
    } else {
      input = "";
    }
  }

  const searchTerm = input || "bitcoin";

  const provider = resolveMarketProvider();
  if (provider === "kalshi") {
    await watchKalshiMarket(searchTerm);
    return;
  }

  console.error(
    `${colors.yellow}🔍 Resolving market for "${searchTerm}"...${colors.reset}`,
  );

  try {
    const market = await quickMarketLookup(searchTerm);
    console.error(
      `${colors.green}✅ Monitoring:${colors.reset} ${market.question}`,
    );

    if (market.closed) {
      console.error(
        `${colors.yellow}⚠️  Warning: This market appears to be closed/resolved. No live data will be received.${colors.reset}`,
      );
    }

    const tokenIds = extractTokenIds(market);
    const outcomes = extractOutcomes(market);
    const priceToBeat = extractPriceToBeat(market);

    if (priceToBeat > 0) {
      console.error(
        `${colors.magenta}🎯 Price to Beat: $${priceToBeat.toLocaleString()}${colors.reset}`,
      );
    }

    const marketStartTime = market.startTime || undefined;

    const dashboard = new Dashboard(
      market.question,
      outcomes,
      tokenIds,
      priceToBeat,
      marketStartTime,
    );

    const marketQuestion = market.question.toLowerCase();
    let cryptoWs: CryptoWS | null = null;
    let cryptoSymbol: string | null = null;

    if (
      marketQuestion.includes("ethereum") ||
      marketQuestion.includes("eth ")
    ) {
      cryptoSymbol = "eth/usd";
    } else if (
      marketQuestion.includes("bitcoin") ||
      marketQuestion.includes("btc ")
    ) {
      cryptoSymbol = "btc/usd";
    } else if (
      marketQuestion.includes("solana") ||
      marketQuestion.includes("sol ")
    ) {
      cryptoSymbol = "sol/usd";
    }

    if (cryptoSymbol) {
      cryptoWs = new CryptoWS(
        (payload: CryptoPricePayload) => {
          dashboard.updateCryptoPrice(
            payload.symbol,
            payload.value,
            payload.timestamp,
          );
        },
        () => {},
        () => {},
        { source: "chainlink" },
      );
      cryptoWs.connect();
      cryptoWs.subscribe([cryptoSymbol]);

      const startTime = dashboard.getMarketStartTime();
      if (startTime && dashboard.needsReferencePrice()) {
        fetchHistoricalCryptoPrice(cryptoSymbol, startTime).then(
          (historicalPrice) => {
            if (historicalPrice) {
              dashboard.setReferencePrice(
                historicalPrice.price,
                historicalPrice.timestamp,
              );
              console.error(
                `${colors.cyan}📍 Fetched reference price: $${historicalPrice.price.toFixed(2)} from market start${colors.reset}`,
              );
            }
          },
        );
      }
    }

    const wsClient = new MarketWS(
      (event: MarketEvent) => {
        if (event.event_type === "price_change") {
          if (event.price_changes && event.price_changes.length > 0) {
            dashboard.updatePriceChange(event.price_changes);
          }
        }
      },
      (event: MarketEvent) => {
        if (event.event_type === "book") {
          dashboard.updateOrderBook(
            event.asset_id,
            event.bids,
            event.asks,
            event.last_trade_price,
          );
          dashboard.render();
        }
      },
      () => {},
      (connected: boolean) => {
        dashboard.setConnectionState(connected);
        if (!connected) {
          console.error(
            `\n${colors.red}❌ WebSocket Disconnected${colors.reset}`,
          );
        }
      },
      (error: Error) => {
        console.error(`${colors.red}❌ Error:${colors.reset} ${error.message}`);
      },
      { silent: true },
    );

    wsClient.connect();
    wsClient.subscribe(tokenIds);

    process.on("SIGINT", () => {
      wsClient.disconnect();
      if (cryptoWs) {
        cryptoWs.disconnect();
      }
      process.exit(0);
    });
  } catch (error: any) {
    console.log(`\n${colors.red}❌ Error:${colors.reset} ${error.message}`);
  }
}

function extractKalshiTicker(value: string): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (trimmed.startsWith("http")) {
    const parsed = parseKalshiMarketUrl(trimmed);
    return parsed.marketTicker ?? null;
  }
  return normalizeKalshiTicker(trimmed);
}

async function resolveKalshiMarket(
  client: KalshiClient,
  term: string,
): Promise<{ market: any; ticker: string }> {
  const candidate = extractKalshiTicker(term) ?? term;
  const ticker = normalizeKalshiTicker(candidate);

  if (looksLikeKalshiMarketTicker(ticker)) {
    const market = await client.getMarket(ticker);
    if (market) {
      const resolved =
        (market as any)?.ticker ?? (market as any)?.market_ticker ?? ticker;
      return { market, ticker: resolved };
    }
  }

  let matches = await client.searchMarkets(candidate, { status: "open" });
  if (matches.length === 0) {
    matches = await client.searchMarkets(candidate, { status: undefined });
  }
  if (matches.length === 0) {
    throw new Error(`Kalshi market not found for "${term}".`);
  }
  const selected = matches[0];
  const resolvedTicker =
    typeof selected?.ticker === "string"
      ? selected.ticker
      : typeof selected?.market_ticker === "string"
        ? selected.market_ticker
        : typeof (selected as any)?.marketTicker === "string"
          ? (selected as any).marketTicker
          : null;
  if (!resolvedTicker) {
    throw new Error(`Kalshi market lookup returned invalid data for "${term}".`);
  }
  return { market: selected, ticker: resolvedTicker };
}

async function watchKalshiMarket(searchTerm: string): Promise<void> {
  console.error(
    `${colors.yellow}🔍 Resolving Kalshi market for "${searchTerm}"...${colors.reset}`,
  );

  let config;
  try {
    config = getKalshiEnvConfig();
  } catch (error: any) {
    console.error(
      `${colors.red}❌ Kalshi config error:${colors.reset} ${error.message}`,
    );
    return;
  }

  const client = new KalshiClient(config);
  let market: any;
  let ticker: string;

  try {
    const resolved = await resolveKalshiMarket(client, searchTerm);
    market = resolved.market;
    ticker = resolved.ticker;
  } catch (error: any) {
    console.error(
      `${colors.red}❌ Kalshi market lookup failed:${colors.reset} ${error.message}`,
    );
    return;
  }

  const title = market?.title ?? market?.subtitle ?? ticker;
  console.error(
    `${colors.green}✅ Monitoring Kalshi:${colors.reset} ${title}`,
  );

  if (market?.status && String(market.status).toLowerCase() !== "open") {
    console.error(
      `${colors.yellow}⚠️  Warning: Market status is ${market.status}.${colors.reset}`,
    );
  }

  const outcomes = ["Yes", "No"];
  const tokenIds = ["YES", "NO"];
  const dashboard = new Dashboard(title, outcomes, tokenIds);

  const wsClient = new KalshiMarketWS(
    config,
    (update) => {
      dashboard.updateOrderBook("YES", update.yesBids, update.yesAsks);
      dashboard.updateOrderBook("NO", update.noBids, update.noAsks);
    },
    (trade) => {
      const price = trade.yesPrice ?? trade.noPrice;
      if (price !== null) {
        const rawSide = trade.takerSide?.toLowerCase() ?? "";
        const side =
          rawSide === "yes" ? "BUY" : rawSide === "no" ? "SELL" : "";
        dashboard.updateLastTrade(
          String(price),
          trade.count ? String(trade.count) : "0",
          side,
        );
      }
    },
    () => {},
    (connected: boolean) => {
      dashboard.setConnectionState(connected);
      if (!connected) {
        console.error(
          `\n${colors.red}❌ Kalshi WebSocket Disconnected${colors.reset}`,
        );
      }
    },
    (error: Error) => {
      console.error(`${colors.red}❌ Error:${colors.reset} ${error.message}`);
    },
    { silent: true },
  );

  wsClient.connect();
  wsClient.subscribe([ticker], ["orderbook_delta", "trade", "ticker"]);

  process.on("SIGINT", () => {
    wsClient.disconnect();
    process.exit(0);
  });
}
