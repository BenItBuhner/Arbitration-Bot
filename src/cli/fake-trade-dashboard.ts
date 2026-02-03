import { colors } from "./dashboard";

export interface FakeTradeMarketView {
  coin: string;
  marketName: string;
  marketSlug: string;
  timeLeftSec: number | null;
  priceToBeat: number | null;
  currentPrice: number | null;
  priceDiff: number | null;
  favoredOutcome: string | null;
  bestAsk: number | null;
  bestBid: number | null;
  totalAskValue: number | null;
  totalBidValue: number | null;
  positionShares: number;
  positionCost: number;
  positionAvgPrice: number;
  resolved: boolean;
  lastResult: string | null;
}

export interface FakeTradeSummary {
  runtimeSec: number;
  totalTrades: number;
  wins: number;
  losses: number;
  totalProfit: number;
}

export interface FakeTradeDashboardState {
  markets: FakeTradeMarketView[];
  summary: FakeTradeSummary;
  logs: string[];
}

export class FakeTradeDashboard {
  private state: FakeTradeDashboardState;
  private lastRenderTime = 0;
  private renderCount = 0;
  private readonly RENDER_THROTTLE_MS = 100;
  private readonly RECENT_LOG_LIMIT = 15;

  constructor() {
    this.state = {
      markets: [],
      summary: {
        runtimeSec: 0,
        totalTrades: 0,
        wins: 0,
        losses: 0,
        totalProfit: 0,
      },
      logs: [],
    };
  }

  update(state: FakeTradeDashboardState, force: boolean = false): void {
    this.state = state;
    this.render(force);
  }

  private render(force: boolean = false): void {
    const now = Date.now();
    if (
      !force &&
      this.renderCount > 0 &&
      now - this.lastRenderTime < this.RENDER_THROTTLE_MS
    ) {
      return;
    }

    this.lastRenderTime = now;
    this.renderCount++;

    const { markets, summary, logs } = this.state;
    let output = "";

    output += `\x1b[2J\x1b[H`;
    output += `${colors.bright}${colors.cyan}Fake Trade Mode${colors.reset}\n`;
    const winRate =
      summary.totalTrades > 0
        ? (summary.wins / summary.totalTrades) * 100
        : 0;
    output += `${colors.dim}Runtime: ${summary.runtimeSec.toFixed(
      1,
    )}s | Trades: ${summary.totalTrades} | Wins: ${summary.wins} | Losses: ${
      summary.losses
    } | Win Rate: ${winRate.toFixed(1)}% | PnL: ${formatCurrency(
      summary.totalProfit,
    )}${colors.reset}\n`;
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    for (const market of markets) {
      const timeLeft =
        market.timeLeftSec === null
          ? "n/a"
          : formatDuration(market.timeLeftSec);
      const priceToBeat =
        market.priceToBeat === null
          ? "n/a"
          : formatNumber(market.priceToBeat, 2);
      const currentPrice =
        market.currentPrice === null
          ? "n/a"
          : formatNumber(market.currentPrice, 2);
      const diff =
        market.priceDiff === null
          ? "n/a"
          : formatNumber(market.priceDiff, 2);
      const favored = market.favoredOutcome || "n/a";
      const bid =
        market.bestBid === null
          ? "n/a"
          : formatNumber(market.bestBid, 4);
      const ask =
        market.bestAsk === null
          ? "n/a"
          : formatNumber(market.bestAsk, 4);

      output += `${colors.bright}${market.coin.toUpperCase()}${colors.reset} ${market.marketName}\n`;
      output += `${colors.dim}Slug: ${market.marketSlug}${colors.reset}\n`;
      output += `Time Left: ${timeLeft} | Ref: ${priceToBeat} | Spot: ${currentPrice} | Diff: ${diff}\n`;
      output += `Favored: ${favored} | Best Bid: ${bid} | Best Ask: ${ask}\n`;

      const bidTotal =
        market.totalBidValue === null
          ? "n/a"
          : formatCurrency(market.totalBidValue);
      const askTotal =
        market.totalAskValue === null
          ? "n/a"
          : formatCurrency(market.totalAskValue);
      output += `Book Totals: Bid ${bidTotal} | Ask ${askTotal}\n`;

      if (market.positionShares > 0) {
        output += `Position: ${market.positionShares.toFixed(2)} shares @ ${formatNumber(
          market.positionAvgPrice,
          4,
        )} | Cost ${formatCurrency(market.positionCost)}\n`;
      } else {
        output += `Position: none\n`;
      }

      if (market.resolved && market.lastResult) {
        output += `Result: ${market.lastResult}\n`;
      }

      output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    }

    output += `${colors.bright}Recent Logs${colors.reset}\n`;
    const recentLogs = logs.slice(-this.RECENT_LOG_LIMIT);
    for (const line of recentLogs) {
      output += `${line}\n`;
    }

    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    output += `${colors.dim}Press Ctrl+C to exit${colors.reset}\n`;

    process.stdout.write(output);
  }
}

function formatDuration(totalSeconds: number): string {
  const clamped = Math.max(0, Math.floor(totalSeconds));
  const minutes = Math.floor(clamped / 60);
  const seconds = clamped % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(
    2,
    "0",
  )}`;
}

function formatNumber(value: number, digits: number): string {
  return value.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatCurrency(value: number): string {
  return value.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}
