import {
  quickMarketLookup,
  extractTokenIds,
  extractOutcomes,
  extractPriceToBeat,
} from "../services/market-service";
import { MarketWS, type MarketEvent } from "../clients/market-ws";
import { CryptoWS, type CryptoPricePayload } from "../clients/crypto-ws";
import { fetchHistoricalCryptoPrice } from "../services/crypto-service";
import { Dashboard, colors } from "../cli/dashboard";
import { promptText } from "../cli/prompts";

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
