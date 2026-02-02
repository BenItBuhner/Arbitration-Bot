import { PriceGraph } from "./price-graph";
import { colors } from "./dashboard";
import type { CoinSymbol } from "../services/auto-market";
import type {
  ProfileMarketView,
  ProfileSummary,
} from "../services/profile-engine";

export interface ProfileViewState {
  name: string;
  summary: ProfileSummary;
  markets: ProfileMarketView[];
  logs: string[];
  pnlHistory: number[];
}

export interface ProfileDashboardState {
  runId: string;
  modeLabel?: string;
  activeProfileIndex: number;
  profiles: ProfileViewState[];
  activeCoin: CoinSymbol | null;
  activeCoinPriceHistory: number[];
  useCandleGraph?: boolean;
}

export class ProfileDashboard {
  private state: ProfileDashboardState;
  private lastRenderTime = 0;
  private renderCount = 0;
  private readonly RENDER_THROTTLE_MS = 100;
  private readonly RECENT_LOG_LIMIT = 15;

  constructor() {
    this.state = {
      runId: "run",
      activeProfileIndex: 0,
      profiles: [],
      activeCoin: null,
      activeCoinPriceHistory: [],
    };
  }

  update(state: ProfileDashboardState, force: boolean = false): void {
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

    const { profiles, activeProfileIndex, runId } = this.state;
    const activeProfile =
      profiles.length > 0
        ? profiles[Math.min(activeProfileIndex, profiles.length - 1)]
        : null;

    let output = "";
    output += `\x1b[2J\x1b[H`;
    const modeLabel = this.state.modeLabel || "Fake Trade Mode";
    output += `${colors.bright}${colors.cyan}${modeLabel}${colors.reset} ${colors.dim}(${runId})${colors.reset}\n`;

    output += this.renderProfileTabs();
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    if (!activeProfile) {
      output += `${colors.dim}No profiles selected.${colors.reset}\n`;
      process.stdout.write(output);
      return;
    }

    output += this.renderSummary(activeProfile);
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    output += this.renderGraphs(activeProfile);
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    for (const market of activeProfile.markets) {
      output += this.renderMarket(market);
    }

    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    output += `${colors.bright}Recent Logs${colors.reset}\n`;
    const recentLogs = activeProfile.logs.slice(-this.RECENT_LOG_LIMIT);
    for (const line of recentLogs) {
      output += `${line}\n`;
    }
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    output += `${colors.dim}Up/Down or Left/Right: switch profile | Ctrl+C to exit${colors.reset}\n`;

    process.stdout.write(output);
  }

  private renderProfileTabs(): string {
    const { profiles, activeProfileIndex } = this.state;
    if (profiles.length === 0) {
      return `${colors.dim}Profiles: none${colors.reset}\n`;
    }

    const tabs = profiles
      .map((profile, index) => {
        const isActive = index === activeProfileIndex;
        const color = isActive ? colors.bright + colors.cyan : colors.dim;
        const pnl = profile.summary.totalProfit;
        return `${color}[${profile.name}: ${formatCurrency(pnl)}]${colors.reset}`;
      })
      .join(" ");

    return `${tabs}\n`;
  }

  private renderSummary(profile: ProfileViewState): string {
    const summary = profile.summary;
    const totalMarkets = summary.wins + summary.losses;
    const winRate =
      totalMarkets > 0
        ? (summary.wins / totalMarkets) * 100
        : 0;
    const exposure = summary.openExposure;

    let line = `${colors.bright}${profile.name}${colors.reset} `;
    line += `${colors.dim}Runtime: ${summary.runtimeSec.toFixed(
      1,
    )}s | Trades: ${summary.totalTrades} | Wins: ${summary.wins} | Losses: ${
      summary.losses
    } | Win Rate: ${winRate.toFixed(1)}% | PnL: ${formatCurrency(
      summary.totalProfit,
    )} | Exposure: ${formatCurrency(exposure)}${colors.reset}\n`;

    return line;
  }

  private renderGraphs(profile: ProfileViewState): string {
    const pnlGraph = new PriceGraph({ height: 6, width: 60 });
    if (profile.pnlHistory.length > 0) {
      pnlGraph.addPrices(profile.pnlHistory);
    }

    const priceGraph = new PriceGraph({
      height: 6,
      width: 60,
      mode: this.state.useCandleGraph ? "candles" : "line",
    });
    if (this.state.activeCoinPriceHistory.length > 0) {
      priceGraph.addPrices(this.state.activeCoinPriceHistory);
    }

    const coinLabel = this.state.activeCoin
      ? this.state.activeCoin.toUpperCase()
      : "N/A";

    let output = `${colors.bright}PnL History${colors.reset}\n`;
    output += pnlGraph.render();
    output += `\n${colors.bright}Price History (${coinLabel})${colors.reset}\n`;
    output += priceGraph.render();
    output += "\n";
    return output;
  }

  private renderMarket(market: ProfileMarketView): string {
    const timeLeft =
      market.timeLeftSec === null
        ? "n/a"
        : formatDuration(market.timeLeftSec);
    const threshold =
      market.priceToBeat > 0 ? market.priceToBeat : market.referencePrice;
    const priceToBeat = threshold > 0 ? formatNumber(threshold, 2) : "n/a";
    const refPrice =
      market.referencePrice > 0
        ? formatNumber(market.referencePrice, 2)
        : "n/a";
    const refSource =
      market.referenceSource === "missing"
        ? "pending"
        : market.referenceSource.replace(/_/g, " ");
    const priceToBeatSource =
      market.priceToBeat > 0
        ? "price to beat"
        : market.referenceSource === "missing"
          ? "pending"
          : market.referenceSource.replace(/_/g, " ");
    const currentPrice =
      market.cryptoPrice > 0
        ? formatNumber(market.cryptoPrice, 2)
        : "n/a";
    const diff =
      market.priceDiff === null
        ? "n/a"
        : formatNumber(market.priceDiff, 2);
    const bid =
      market.bestBid === null
        ? "n/a"
        : formatNumber(market.bestBid, 4);
    const ask =
      market.bestAsk === null
        ? "n/a"
        : formatNumber(market.bestAsk, 4);
    const favored = market.favoredOutcome || "n/a";
    const statusColor =
      market.dataStatus === "healthy"
        ? colors.green
        : market.dataStatus === "stale"
          ? colors.red
          : colors.yellow;

    let output = `${colors.bright}${market.coin.toUpperCase()}${colors.reset} ${market.marketName}\n`;
    output += `${colors.dim}Slug: ${market.marketSlug}${colors.reset}\n`;
    output += `Time Left: ${timeLeft} | Ref: ${refPrice} (${refSource}) | Price: ${currentPrice} | Diff: ${diff}\n`;
    output += `Favored: ${favored} | Best Bid: ${bid} | Best Ask: ${ask}\n`;
    output += `Data: ${statusColor}${market.dataStatus}${colors.reset} | Price To Beat: ${priceToBeat} (${priceToBeatSource})\n`;

    const bidTotal =
      market.totalBidValue === null
        ? "n/a"
        : formatCurrency(market.totalBidValue);
    const askTotal =
      market.totalAskValue === null
        ? "n/a"
        : formatCurrency(market.totalAskValue);
    output += `Book Totals: Bid ${bidTotal} | Ask ${askTotal}\n`;

    if (parseEnvFlag("BACKTEST_SIGNAL_DEBUG", false)) {
      const spread =
        market.signalSpread === null || market.signalSpread === undefined
          ? "n/a"
          : formatNumber(market.signalSpread, 4);
      const imbalance =
        market.signalImbalance === null || market.signalImbalance === undefined
          ? "n/a"
          : formatNumber(market.signalImbalance, 2);
      const depth =
        market.signalDepthValue === null || market.signalDepthValue === undefined
          ? "n/a"
          : formatCurrency(market.signalDepthValue);
      const confidence =
        market.signalConfidence === null || market.signalConfidence === undefined
          ? "n/a"
          : formatNumber(market.signalConfidence, 2);
      output += `Signals: Spread ${spread} | Imbalance ${imbalance} | Depth ${depth} | Conf ${confidence}\n`;
    }

    if (market.positionShares > 0) {
      output += `Position: ${market.positionShares.toFixed(
        2,
      )} shares @ ${formatNumber(market.positionAvgPrice, 4)} | Cost ${formatCurrency(
        market.positionCost,
      )}\n`;
    } else {
      output += `Position: none\n`;
    }

    const realized = formatCurrency(market.realizedPnl);
    const crossed = market.crossed ? "yes" : "no";
    output += `Crossed: ${crossed} | Realized PnL: ${realized}\n`;

    if (market.lastResult) {
      output += `Result: ${market.lastResult}\n`;
    }

    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    return output;
  }
}

function parseEnvFlag(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const normalized = raw.trim().toLowerCase();
  if (["false", "0", "off", "no"].includes(normalized)) return false;
  if (["true", "1", "on", "yes"].includes(normalized)) return true;
  return defaultValue;
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
