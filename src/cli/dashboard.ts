import { PriceGraph } from "./price-graph";

export const colors = {
  reset: "\x1b[0m",
  bright: "\x1b[1m",
  green: "\x1b[32m",
  red: "\x1b[31m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m",
  dim: "\x1b[2m",
};

/**
 * Dashboard class for static inline updates
 * Creates a clean, persistent display that updates in place
 */
export class Dashboard {
  private marketName: string;
  private lastPrice: number = 0;
  private bestBid: number = 0;
  private bestAsk: number = 0;
  private spread: number = 0;
  private lastTradePrice: number = 0;
  private lastTradeSize: number = 0;
  private lastTradeSide: string = "";
  private updateCount: number = 0;
  private startTime: number = Date.now();
  private topBids: Array<{ price: number; size: number }> = [];
  private topAsks: Array<{ price: number; size: number }> = [];
  private connected: boolean = false;
  private currentAssetId: string = "";
  private lastRenderTime: number = 0;
  private readonly RENDER_THROTTLE_MS = 50; // Render at most every 50ms (20 FPS)
  private cryptoPrice: number = 0;
  private cryptoSymbol: string = "";
  private cryptoPriceTimestamp: number = 0;
  private cryptoGraph: PriceGraph;
  private renderCount: number = 0;
  private priceToBeat: number = 0; // The target threshold price for crypto markets
  private referencePrice: number = 0; // For Up/Down markets: the starting price captured at session start
  private referencePriceTimestamp: number = 0; // When the reference price was captured
  private isUpDownMarket: boolean = false; // Whether this is an Up/Down style market
  private marketStartTime: Date | null = null; // When the market's time window began (for Up/Down markets)
  private orderBooks: Map<
    string,
    {
      bids: Array<{ price: number; size: number }>;
      asks: Array<{ price: number; size: number }>;
      lastTrade: number;
    }
  > = new Map();

  // Outcome and token mapping - maps token ID to outcome name
  private outcomes: string[] = [];
  private tokenIds: string[] = [];
  private outcomeA: string = ""; // First outcome (e.g., "YES" or "Up")
  private outcomeB: string = ""; // Second outcome (e.g., "NO" or "Down")
  private tokenIdA: string = ""; // Token ID for outcome A
  private tokenIdB: string = ""; // Token ID for outcome B

  // Outcome A tracking
  private lastPriceA: number = 0;
  private bestBidA: number = 0;
  private bestAskA: number = 0;
  private spreadA: number = 0;
  private topBidsA: Array<{ price: number; size: number }> = [];
  private topAsksA: Array<{ price: number; size: number }> = [];

  // Outcome B tracking
  private lastPriceB: number = 0;
  private bestBidB: number = 0;
  private bestAskB: number = 0;
  private spreadB: number = 0;
  private topBidsB: Array<{ price: number; size: number }> = [];
  private topAsksB: Array<{ price: number; size: number }> = [];

  constructor(
    marketName: string,
    outcomes?: string[],
    tokenIds?: string[],
    priceToBeat?: number,
    marketStartTime?: string, // ISO timestamp for when the Up/Down market window began
  ) {
    this.marketName =
      marketName.length > 40 ? marketName.substring(0, 37) + "..." : marketName;

    this.cryptoGraph = new PriceGraph({
      height: 8,
      width: 50,
    });

    // Set the price to beat threshold (for crypto/stock prediction markets)
    if (priceToBeat && priceToBeat > 0) {
      this.priceToBeat = priceToBeat;
    }

    // Initialize outcome and token mapping
    if (outcomes && tokenIds && outcomes.length >= 2 && tokenIds.length >= 2) {
      this.outcomes = outcomes;
      this.tokenIds = tokenIds;
      this.outcomeA = outcomes[0] || "Outcome A";
      this.outcomeB = outcomes[1] || "Outcome B";
      this.tokenIdA = tokenIds[0] || "";
      this.tokenIdB = tokenIds[1] || "";

      // Detect Up/Down style markets (outcomes are "Up"/"Down" instead of "Yes"/"No")
      const outcomesLower = outcomes.map((o) => o.toLowerCase());
      if (outcomesLower.includes("up") && outcomesLower.includes("down")) {
        this.isUpDownMarket = true;
      }
    }

    // Store market start time for Up/Down markets
    if (marketStartTime) {
      this.marketStartTime = new Date(marketStartTime);
    }

    this.render();
  }

  /**
   * Set the reference price for Up/Down markets (the "price to beat")
   * This should be called with the actual price at the market's startTime
   */
  setReferencePrice(price: number, timestamp: number): void {
    this.referencePrice = price;
    this.referencePriceTimestamp = timestamp;
    this.render();
  }

  /**
   * Get the market start time (for fetching historical reference price)
   */
  getMarketStartTime(): Date | null {
    return this.marketStartTime;
  }

  /**
   * Check if this is an Up/Down market that needs a reference price
   */
  needsReferencePrice(): boolean {
    return (
      this.isUpDownMarket && this.priceToBeat === 0 && this.referencePrice === 0
    );
  }

  /**
   * Update connection state
   */
  setConnectionState(connected: boolean): void {
    this.connected = connected;
    this.render();
  }

  /**
   * Update crypto price from RTDS
   */
  updateCryptoPrice(symbol: string, price: number, timestamp: number): void {
    this.cryptoSymbol = symbol;
    this.cryptoPrice = price;
    this.cryptoPriceTimestamp = timestamp;
    this.cryptoGraph.addPrice(price);

    // For Up/Down markets without a preset priceToBeat:
    // Only capture first price as fallback if we don't have a startTime
    // (The actual reference price should be set via setReferencePrice if startTime is known)
    if (
      this.isUpDownMarket &&
      this.priceToBeat === 0 &&
      this.referencePrice === 0 &&
      !this.marketStartTime // Only use first price if we don't have a specific start time
    ) {
      this.referencePrice = price;
      this.referencePriceTimestamp = timestamp;
    }

    // Force render on every crypto price update to show chart immediately
    this.render(true);
  }

  /**
   * Update dashboard with price change event
   */
  updatePriceChange(priceChanges: any[]): void {
    if (priceChanges && priceChanges.length > 0) {
      // Process all price changes using token ID mapping
      for (const pc of priceChanges) {
        const assetId = pc.asset_id || "";

        // Map to outcome A by token ID
        if (assetId === this.tokenIdA) {
          this.currentAssetId = assetId;
          this.lastPriceA = parseFloat(pc.price);
          this.bestBidA = parseFloat(pc.best_bid);
          this.bestAskA = parseFloat(pc.best_ask);
          this.spreadA = this.bestAskA - this.bestBidA;

          const book = this.orderBooks.get(this.tokenIdA);
          if (book) {
            this.topBidsA = book.bids.slice(-5).reverse();
            this.topAsksA = book.asks.slice(-5).reverse();
            this.lastTradePrice = book.lastTrade;
          }
        }

        // Map to outcome B by token ID
        if (assetId === this.tokenIdB) {
          this.lastPriceB = parseFloat(pc.price);
          this.bestBidB = parseFloat(pc.best_bid);
          this.bestAskB = parseFloat(pc.best_ask);
          this.spreadB = this.bestAskB - this.bestBidB;

          const book = this.orderBooks.get(this.tokenIdB);
          if (book) {
            this.topBidsB = book.bids.slice(-5).reverse();
            this.topAsksB = book.asks.slice(-5).reverse();
          }
        }
      }

      this.updateCount++;
      this.render();
    }
  }

  /**
   * Update dashboard with order book data
   */
  updateOrderBook(
    assetId: string,
    bids: any[],
    asks: any[],
    lastTradePrice?: string,
  ): void {
    if (bids && asks) {
      // Store full order book data for this asset
      this.orderBooks.set(assetId, {
        bids: bids.map((b) => ({
          price: parseFloat(b.price),
          size: parseFloat(b.size),
        })),
        asks: asks.map((a) => ({
          price: parseFloat(a.price),
          size: parseFloat(a.size),
        })),
        lastTrade: lastTradePrice ? parseFloat(lastTradePrice) : 0,
      });

      // Update outcome A display using token ID
      if (assetId === this.tokenIdA) {
        const book = this.orderBooks.get(this.tokenIdA);
        if (book) {
          this.topBidsA = book.bids.slice(-5).reverse();
          this.topAsksA = book.asks.slice(-5).reverse();

          if (lastTradePrice) {
            this.lastTradePrice = parseFloat(lastTradePrice);
            if (this.lastPriceA === 0) {
              this.lastPriceA = this.lastTradePrice;
            }
          }

          // Set best bid/ask from order book if not set from price changes
          if (this.bestBidA === 0 && this.topBidsA.length > 0) {
            const bestBid = this.topBidsA[0];
            if (bestBid) {
              this.bestBidA = bestBid.price;
            }
          }
          if (this.bestAskA === 0 && this.topAsksA.length > 0) {
            const bestAsk = this.topAsksA[0];
            if (bestAsk) {
              this.bestAskA = bestAsk.price;
            }
          }
          if (this.bestBidA > 0 && this.bestAskA > 0) {
            this.spreadA = this.bestAskA - this.bestBidA;
          }
          if (this.lastPriceA === 0 && this.bestBidA > 0 && this.bestAskA > 0) {
            this.lastPriceA = (this.bestBidA + this.bestAskA) / 2;
          }
        }
      }

      // Update outcome B display using token ID
      if (assetId === this.tokenIdB) {
        const book = this.orderBooks.get(this.tokenIdB);
        if (book) {
          this.topBidsB = book.bids.slice(-5).reverse();
          this.topAsksB = book.asks.slice(-5).reverse();

          // Set best bid/ask from order book if not set from price changes
          if (this.bestBidB === 0 && this.topBidsB.length > 0) {
            const bestBid = this.topBidsB[0];
            if (bestBid) {
              this.bestBidB = bestBid.price;
            }
          }
          if (this.bestAskB === 0 && this.topAsksB.length > 0) {
            const bestAsk = this.topAsksB[0];
            if (bestAsk) {
              this.bestAskB = bestAsk.price;
            }
          }
          if (this.bestBidB > 0 && this.bestAskB > 0) {
            this.spreadB = this.bestAskB - this.bestBidB;
          }
          if (this.lastPriceB === 0 && this.bestBidB > 0 && this.bestAskB > 0) {
            this.lastPriceB = (this.bestBidB + this.bestAskB) / 2;
          }
        }
      }

      this.render();
    }
  }

  /**
   * Update dashboard with last trade
   */
  updateLastTrade(price: string, size: string, side: string): void {
    if (price) {
      this.lastTradePrice = parseFloat(price);
    }
    if (size) {
      this.lastTradeSize = parseFloat(size);
    }
    if (side) {
      this.lastTradeSide = side;
    }
    this.render();
  }

  /**
   * Render the static dashboard
   */
  public render(force: boolean = false): void {
    // Throttle renders to improve performance (unless forced)
    const now = Date.now();
    // Skip throttling for first render to ensure fast first paint
    if (
      !force &&
      this.renderCount > 0 &&
      now - this.lastRenderTime < this.RENDER_THROTTLE_MS
    ) {
      return; // Skip this render, too soon since last one
    }

    this.lastRenderTime = now;
    this.renderCount++;
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(1);
    const priceColor = this.lastPrice > 0.5 ? colors.green : colors.red;
    const bidColor = colors.green;
    const askColor = colors.red;
    const spreadPct =
      this.spread > 0
        ? ((this.spread / this.bestAsk) * 100).toFixed(2)
        : "0.00";

    let output = "";

    // Always clear screen completely to prevent duplicate text
    output += `\x1b[2J\x1b[H`;

    // Header
    output += `${colors.bright}${colors.cyan}═══════════════════════════════════════════════════════════════${colors.reset}\n`;
    output += `${colors.bright}${colors.cyan}  📊 ${this.marketName}${colors.reset}\n`;
    output += `${colors.bright}${colors.cyan}═══════════════════════════════════════════════════════════════${colors.reset}\n`;

    // Show real crypto price if available
    if (this.cryptoPrice > 0) {
      const priceStr = this.cryptoPrice.toLocaleString("en-US", {
        style: "currency",
        currency: "USD",
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
      const age = Math.floor((Date.now() - this.cryptoPriceTimestamp) / 1000);
      output += `  ${colors.yellow}💲 ${this.cryptoSymbol}: ${priceStr}${colors.reset} ${colors.dim}(${age}s ago)${colors.reset}\n`;

      // Show price to beat comparison if threshold is set
      if (this.priceToBeat > 0) {
        const thresholdStr = this.priceToBeat.toLocaleString("en-US", {
          style: "currency",
          currency: "USD",
          minimumFractionDigits: 0,
          maximumFractionDigits: 0,
        });
        const diff = this.cryptoPrice - this.priceToBeat;
        const diffPct = ((diff / this.priceToBeat) * 100).toFixed(2);
        const isAbove = diff > 0;
        const diffStr = Math.abs(diff).toLocaleString("en-US", {
          style: "currency",
          currency: "USD",
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });

        const statusColor = isAbove ? colors.green : colors.red;
        const statusIcon = isAbove ? "✅" : "❌";
        const statusText = isAbove ? "ABOVE" : "BELOW";

        output += `  ${colors.magenta}🎯 Price to Beat: ${thresholdStr}${colors.reset}\n`;
        output += `  ${statusColor}${statusIcon} Currently ${statusText} by ${diffStr} (${isAbove ? "+" : ""}${diffPct}%)${colors.reset}\n`;
      }
      // For Up/Down markets, show reference price comparison
      else if (this.isUpDownMarket && this.referencePrice > 0) {
        const refPriceStr = this.referencePrice.toLocaleString("en-US", {
          style: "currency",
          currency: "USD",
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });
        const diff = this.cryptoPrice - this.referencePrice;
        const diffPct = ((diff / this.referencePrice) * 100).toFixed(2);
        const isUp = diff >= 0;
        const diffStr = Math.abs(diff).toLocaleString("en-US", {
          style: "currency",
          currency: "USD",
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });

        const statusColor = isUp ? colors.green : colors.red;
        const statusIcon = isUp ? "📈" : "📉";
        const statusText = isUp ? "UP" : "DOWN";
        const refAge = Math.floor(
          (Date.now() - this.referencePriceTimestamp) / 1000,
        );
        const refAgeStr =
          refAge < 60
            ? `${refAge}s`
            : refAge < 3600
              ? `${Math.floor(refAge / 60)}m`
              : `${Math.floor(refAge / 3600)}h`;

        output += `  ${colors.cyan}📍 Reference Price: ${refPriceStr}${colors.reset} ${colors.dim}(captured ${refAgeStr} ago)${colors.reset}\n`;
        output += `  ${statusColor}${statusIcon} Currently ${statusText} by ${diffStr} (${isUp ? "+" : ""}${diffPct}%)${colors.reset}\n`;
      }

      output += "\n";
      // Always render graph when crypto price is available
      output += this.cryptoGraph.render() + "\n";
    }

    // Show waiting message if no market data has been received yet
    const hasMarketData =
      this.lastPriceA > 0 ||
      this.bestBidA > 0 ||
      this.lastPriceB > 0 ||
      this.bestBidB > 0 ||
      this.topBidsA.length > 0 ||
      this.topBidsB.length > 0;

    if (!hasMarketData && this.updateCount === 0) {
      const runtimeSec = (Date.now() - this.startTime) / 1000;
      if (runtimeSec > 5 && this.connected) {
        // Market is likely closed/resolved if connected but no data after 5 seconds
        output += `${colors.yellow}  ⚠️  No market data received - market may be closed or resolved${colors.reset}\n`;
        output += `${colors.dim}  Outcomes: ${this.outcomeA || "?"} / ${this.outcomeB || "?"}${colors.reset}\n`;
      } else {
        output += `${colors.dim}  ⏳ Waiting for market data...${colors.reset}\n`;
        if (this.outcomeA || this.outcomeB) {
          output += `${colors.dim}  Outcomes: ${this.outcomeA || "?"} / ${this.outcomeB || "?"}${colors.reset}\n`;
        }
      }
      output += "\n";
    }

    // Output A section - show as percentage for prediction markets
    if (this.lastPriceA > 0 || this.bestBidA > 0) {
      const pricePctA = (this.lastPriceA * 100).toFixed(1);
      const bidPctA = (this.bestBidA * 100).toFixed(1);
      const askPctA = (this.bestAskA * 100).toFixed(1);
      const spreadPctA =
        this.spreadA > 0
          ? ((this.spreadA / this.bestAskA) * 100).toFixed(2)
          : "0.00";

      output += `${colors.bright}💰 ${this.outcomeA || "Outcome A"} Price:${colors.reset}\n`;
      output +=
        `  ${priceColor}${colors.bright}${pricePctA}%${colors.reset} ` +
        `${colors.dim}|${colors.reset} ` +
        `${bidColor}Bid: ${bidPctA}%${colors.reset} | ` +
        `${askColor}Ask: ${askPctA}%${colors.reset} ` +
        `${colors.dim}(${spreadPctA}% spread)${colors.reset}\n`;
    }

    // Output B section - show as percentage for prediction markets
    if (this.lastPriceB > 0 || this.bestBidB > 0) {
      const pricePctB = (this.lastPriceB * 100).toFixed(1);
      const bidPctB = (this.bestBidB * 100).toFixed(1);
      const askPctB = (this.bestAskB * 100).toFixed(1);
      const spreadPctB =
        this.spreadB > 0
          ? ((this.spreadB / this.bestAskB) * 100).toFixed(2)
          : "0.00";

      output += `\n${colors.bright}💰 ${this.outcomeB || "Outcome B"} Price:${colors.reset}\n`;
      output +=
        `  ${priceColor}${colors.bright}${pricePctB}%${colors.reset} ` +
        `${colors.dim}|${colors.reset} ` +
        `${bidColor}Bid: ${bidPctB}%${colors.reset} | ` +
        `${askColor}Ask: ${askPctB}%${colors.reset} ` +
        `${colors.dim}(${spreadPctB}% spread)${colors.reset}\n`;
    }

    output += `\n`;

    // Last trade section
    if (this.lastTradePrice > 0) {
      const lastTradePct = (this.lastTradePrice * 100).toFixed(1);
      output += `${colors.bright}📈 Last Trade:${colors.reset}\n`;
      if (this.lastTradeSide) {
        const tradeColor =
          this.lastTradeSide === "BUY" ? colors.green : colors.red;
        output +=
          `  ${tradeColor}${this.lastTradeSide}${colors.reset} ${lastTradePct}% ` +
          `${colors.dim}(${this.lastTradeSize.toFixed(2)} shares)${colors.reset}\n\n`;
      } else {
        output += `  ${lastTradePct}%\n\n`;
      }
    }

    // Output A order book section - show prices as percentages with improved header
    if (this.topBidsA.length > 0 || this.topAsksA.length > 0) {
      output += `${colors.bright}📚 ${this.outcomeA || "Outcome A"} Order Book (Top Bids/Asks):${colors.reset}\n`;
      output += `${colors.dim}       ${colors.bright}BID${colors.reset}                ${colors.bright}ASK${colors.reset}\n`;
      output += `${colors.dim}  ${"Price".padStart(10)}  ${"Size".padStart(10)}  |  ${"Price".padStart(10)}  ${"Size".padStart(10)}${colors.reset}\n`;

      // Calculate totals from COMPLETE order books, not just displayed entries
      let totalBidValue = 0;
      let totalAskValue = 0;

      const fullBook = this.orderBooks.get(this.tokenIdA);
      if (fullBook) {
        // Sum all bids in the complete order book
        for (const bid of fullBook.bids) {
          totalBidValue += bid.price * bid.size;
        }
        // Sum all asks in the complete order book
        for (const ask of fullBook.asks) {
          totalAskValue += ask.price * ask.size;
        }
      }

      for (let i = 0; i < 5; i++) {
        const bid = this.topBidsA[i];
        const ask = this.topAsksA[i];
        const bidStr = bid ? `${(bid.price * 100).toFixed(1)}%` : "";
        const askStr = ask ? `${(ask.price * 100).toFixed(1)}%` : "";
        const bidSize = bid ? bid.size.toFixed(0) : "";
        const askSize = ask ? ask.size.toFixed(0) : "";

        output +=
          `  ${colors.green}${bidStr.padStart(10)}${colors.reset}  ${colors.dim}${bidSize.padStart(10)}${colors.reset}  |  ` +
          `${colors.red}${askStr.padStart(10)}${colors.reset}  ${colors.dim}${askSize.padStart(10)}${colors.reset}\n`;
      }

      // Display totals row - align with Price columns
      output += `${colors.dim}  ${"".padStart(12)}${colors.reset}${colors.dim}${"".padStart(10)}${colors.reset}  ${colors.bright}│${colors.reset}${colors.dim}  ${"".padStart(10)}${colors.reset}${colors.dim}${"".padStart(12)}${colors.reset}\n`;
      output += `${colors.bright}  Total: $${totalBidValue.toFixed(2).padStart(12)}${colors.reset}  ${colors.dim}${"".padStart(10)}${colors.reset}  ${colors.bright}|${colors.reset}  ${colors.bright}Total: $${totalAskValue.toFixed(2).padStart(12)}${colors.reset}\n`;
    }

    // Output B order book section - show prices as percentages with improved header
    if (this.topBidsB.length > 0 || this.topAsksB.length > 0) {
      output += `\n${colors.bright}📚 ${this.outcomeB || "Outcome B"} Order Book (Top Bids/Asks):${colors.reset}\n`;
      output += `${colors.dim}       ${colors.bright}BID${colors.reset}                ${colors.bright}ASK${colors.reset}\n`;
      output += `${colors.dim}  ${"Price".padStart(10)}  ${"Size".padStart(10)}  |  ${"Price".padStart(10)}  ${"Size".padStart(10)}${colors.reset}\n`;

      // Calculate totals from COMPLETE order books, not just displayed entries
      let totalBidValue = 0;
      let totalAskValue = 0;

      const fullBook = this.orderBooks.get(this.tokenIdB);
      if (fullBook) {
        // Sum all bids in the complete order book
        for (const bid of fullBook.bids) {
          totalBidValue += bid.price * bid.size;
        }
        // Sum all asks in the complete order book
        for (const ask of fullBook.asks) {
          totalAskValue += ask.price * ask.size;
        }
      }

      for (let i = 0; i < 5; i++) {
        const bid = this.topBidsB[i];
        const ask = this.topAsksB[i];
        const bidStr = bid ? `${(bid.price * 100).toFixed(1)}%` : "";
        const askStr = ask ? `${(ask.price * 100).toFixed(1)}%` : "";
        const bidSize = bid ? bid.size.toFixed(0) : "";
        const askSize = ask ? ask.size.toFixed(0) : "";

        output +=
          `  ${colors.green}${bidStr.padStart(10)}${colors.reset}  ${colors.dim}${bidSize.padStart(10)}${colors.reset}  |  ` +
          `${colors.red}${askStr.padStart(10)}${colors.reset}  ${colors.dim}${askSize.padStart(10)}${colors.reset}\n`;
      }

      // Display totals row - align with Price columns
      output += `${colors.dim}  ${"".padStart(12)}${colors.reset}${colors.dim}${"".padStart(10)}${colors.reset}  ${colors.bright}│${colors.reset}${colors.dim}  ${"".padStart(10)}${colors.reset}${colors.dim}${"".padStart(12)}${colors.reset}\n`;
      output += `${colors.bright}  Total: $${totalBidValue.toFixed(2).padStart(12)}${colors.reset}  ${colors.dim}${"".padStart(10)}${colors.reset}  ${colors.bright}|${colors.reset}  ${colors.bright}Total: $${totalAskValue.toFixed(2).padStart(12)}${colors.reset}\n`;
    }

    // Footer
    const statusColor = this.connected ? colors.green : colors.dim;
    const statusText = this.connected ? "Connected" : "Connecting...";
    output += `\n${colors.dim}   Updates: ${this.updateCount} | Runtime: ${elapsed}s${colors.reset}\n`;
    output += `   ${statusColor}${statusText}${colors.reset} ${colors.dim}| Press Ctrl+C to exit${colors.reset}`;

    // Write the output
    process.stdout.write(output);
  }
}
