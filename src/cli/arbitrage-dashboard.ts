import { PriceGraph } from "./price-graph";
import { colors } from "./dashboard";
import { enableTui } from "./tui";
import type { CoinSymbol } from "../services/auto-market";
import type { MarketSnapshot } from "../services/market-data-hub";
import type {
  ArbitrageMarketView,
  ArbitrageSummary,
} from "../services/arbitrage-engine";

export interface ArbitrageViewState {
  name: string;
  summary: ArbitrageSummary;
  markets: ArbitrageMarketView[];
  logs: string[];
  pnlHistory: number[];
}

export interface ArbitrageDashboardState {
  runId: string;
  modeLabel?: string;
  activeProfileIndex: number;
  profiles: ArbitrageViewState[];
  coins: CoinSymbol[];
  activeCoinIndex: number;
  activeCoin: CoinSymbol | null;
  polySnapshots: Map<CoinSymbol, MarketSnapshot>;
  kalshiSnapshots: Map<CoinSymbol, MarketSnapshot>;
  polyOddsHistoryByCoin: Map<CoinSymbol, number[]>;
  kalshiOddsHistoryByCoin: Map<CoinSymbol, number[]>;
}

const ANSI_RE = /\x1b\[[0-9;]*m/g;

function stripAnsi(value: string): string {
  return value.replace(ANSI_RE, "");
}

function visibleLength(value: string): number {
  return stripAnsi(value).length;
}

function padLine(value: string, width: number): string {
  const len = visibleLength(value);
  if (len >= width) return value;
  return value + " ".repeat(width - len);
}

function splitLines(value: string): string[] {
  const trimmed = value.replace(/\n$/, "");
  return trimmed.length ? trimmed.split("\n") : [];
}

function renderSideBySide(
  leftLines: string[],
  rightLines: string[],
  gap: number,
): string {
  const leftWidth = Math.max(0, ...leftLines.map(visibleLength));
  const rightWidth = Math.max(0, ...rightLines.map(visibleLength));
  const height = Math.max(leftLines.length, rightLines.length);
  const gapStr = " ".repeat(gap);
  const lines: string[] = [];
  for (let i = 0; i < height; i += 1) {
    const left = leftLines[i] ?? "";
    const right = rightLines[i] ?? "";
    lines.push(`${padLine(left, leftWidth)}${gapStr}${padLine(right, rightWidth)}`);
  }
  return lines.join("\n");
}

function renderCoinTabs(coins: CoinSymbol[], activeIndex: number): string {
  if (coins.length === 0) {
    return `${colors.dim}Coins: none${colors.reset}\n`;
  }
  const safeIndex = Math.max(0, Math.min(activeIndex, coins.length - 1));
  const tabs = coins
    .map((coin, index) => {
      const isActive = index === safeIndex;
      const color = isActive ? colors.bright + colors.cyan : colors.dim;
      return `${color}[${coin.toUpperCase()}]${colors.reset}`;
    })
    .join(" ");
  return `${tabs}\n`;
}

export class ArbitrageDashboard {
  private state: ArbitrageDashboardState;
  private lastRenderTime = 0;
  private renderCount = 0;
  private readonly RENDER_THROTTLE_MS = 100;
  private readonly RECENT_LOG_LIMIT = 15;

  constructor() {
    this.state = {
      runId: "run",
      activeProfileIndex: 0,
      profiles: [],
      coins: [],
      activeCoinIndex: 0,
      activeCoin: null,
      polySnapshots: new Map(),
      kalshiSnapshots: new Map(),
      polyOddsHistoryByCoin: new Map(),
      kalshiOddsHistoryByCoin: new Map(),
    };
  }

  update(state: ArbitrageDashboardState, force: boolean = false): void {
    this.state = state;
    if (this.renderCount === 0) {
      enableTui();
    }
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

    const { profiles, activeProfileIndex, runId, coins, activeCoinIndex } = this.state;
    const activeProfile =
      profiles.length > 0
        ? profiles[Math.min(activeProfileIndex, profiles.length - 1)]
        : null;
    const safeCoinIndex =
      coins.length > 0
        ? Math.max(0, Math.min(activeCoinIndex, coins.length - 1))
        : 0;
    const activeCoin = this.state.activeCoin ?? coins[safeCoinIndex] ?? null;

    let output = "";
    output += `\x1b[2J\x1b[H`;
    const modeLabel = this.state.modeLabel || "Arbitrage Bot";
    output += `${colors.bright}${colors.cyan}${modeLabel}${colors.reset} ${colors.dim}(${runId})${colors.reset}\n`;

    output += this.renderProfileTabs();
    if (activeProfile) {
      output += renderCoinTabs(coins, safeCoinIndex);
      if (coins.length > 1 && activeCoin) {
        output += `${colors.dim}Coin: ${activeCoin.toUpperCase()} (${safeCoinIndex + 1}/${coins.length}) | Left/Right to switch coins${colors.reset}\n`;
      }
    }
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    if (!activeProfile) {
      output += `${colors.dim}No profiles selected.${colors.reset}\n`;
      process.stdout.write(output);
      return;
    }

    output += this.renderSummary(activeProfile);
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    output += this.renderGraphs(activeProfile, activeCoin);
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    const marketsToRender = activeCoin
      ? activeProfile.markets.filter((market) => market.coin === activeCoin)
      : activeProfile.markets;
    if (marketsToRender.length === 0) {
      output += `${colors.dim}No markets available for selected coin.${colors.reset}\n\n`;
    } else {
      for (const market of marketsToRender) {
        output += this.renderMarket(market);
      }
    }

    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    const coinLabel = activeCoin ? activeCoin.toUpperCase() : null;
    output += `${colors.bright}Recent Logs${coinLabel ? ` (${coinLabel})` : ""}${colors.reset}\n`;
    const filteredLogs = coinLabel
      ? activeProfile.logs.filter((line) => line.includes(`] ${coinLabel} `))
      : activeProfile.logs;
    const recentLogs = filteredLogs.slice(-this.RECENT_LOG_LIMIT);
    for (const line of recentLogs) {
      output += `${line}\n`;
    }
    output += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    output += `${colors.dim}Up/Down: switch profile | Left/Right: switch coin | Ctrl+C to exit${colors.reset}\n`;

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

  private renderSummary(profile: ArbitrageViewState): string {
    const summary = profile.summary;
    const totalMarkets = summary.wins + summary.losses;
    const winRate =
      totalMarkets > 0 ? (summary.wins / totalMarkets) * 100 : 0;
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

  private renderGraphs(
    profile: ArbitrageViewState,
    activeCoin: CoinSymbol | null,
  ): string {
    const pnlGraph = new PriceGraph({ height: 6, width: 60 });
    if (profile.pnlHistory.length > 0) {
      pnlGraph.addPrices(profile.pnlHistory);
    }

    const polySnap = activeCoin
      ? this.state.polySnapshots.get(activeCoin)
      : undefined;
    const kalshiSnap = activeCoin
      ? this.state.kalshiSnapshots.get(activeCoin)
      : undefined;
    const polySpotHistory = polySnap?.priceHistory ?? [];
    const kalshiSpotHistory = kalshiSnap?.priceHistory ?? [];
    const polyOddsHistory = activeCoin
      ? this.state.polyOddsHistoryByCoin.get(activeCoin) ?? []
      : [];
    const kalshiOddsHistory = activeCoin
      ? this.state.kalshiOddsHistoryByCoin.get(activeCoin) ?? []
      : [];

    const gap = 4;
    const graphWidth = 60;

    const polySpotTitle = `${colors.bright}Polymarket Spot${colors.reset}`;
    const polyOddsTitle = `${colors.bright}Polymarket Odds${colors.reset}`;
    const kalshiSpotTitle = `${colors.bright}Kalshi Spot${colors.reset}`;
    const kalshiOddsTitle = `${colors.bright}Kalshi Odds${colors.reset}`;

    const buildLines = (title: string, prices: number[]): string[] => {
      const graph = new PriceGraph({ height: 6, width: graphWidth, mode: "candles" });
      if (prices.length > 0) graph.addPrices(prices);
      return [title, ...splitLines(graph.render())];
    };

    let output = `${colors.bright}PnL History${colors.reset}\n`;
    output += pnlGraph.render();
    output += "\n";

    const gridPolySpotLines = buildLines(polySpotTitle, polySpotHistory);
    const gridPolyOddsLines = buildLines(polyOddsTitle, polyOddsHistory);
    const gridKalshiSpotLines = buildLines(kalshiSpotTitle, kalshiSpotHistory);
    const gridKalshiOddsLines = buildLines(kalshiOddsTitle, kalshiOddsHistory);

    output += renderSideBySide(gridPolySpotLines, gridPolyOddsLines, gap);
    output += "\n\n";
    output += renderSideBySide(gridKalshiSpotLines, gridKalshiOddsLines, gap);
    output += "\n";

    return output;
  }

  private renderMarket(market: ArbitrageMarketView): string {
    const timeLeft =
      market.timeLeftSec === null
        ? "n/a"
        : formatDuration(market.timeLeftSec);
    const spotPrice =
      market.spotPrice && market.spotPrice > 0
        ? formatNumber(market.spotPrice, 2)
        : "n/a";
    const polyThreshold =
      market.polyThreshold && market.polyThreshold > 0
        ? formatNumber(market.polyThreshold, 2)
        : "n/a";
    const kalshiThreshold =
      market.kalshiThreshold && market.kalshiThreshold > 0
        ? formatNumber(market.kalshiThreshold, 2)
        : "n/a";

    const polyUpAsk =
      market.polyUpAsk && market.polyUpAsk > 0
        ? formatNumber(market.polyUpAsk, 4)
        : "n/a";
    const polyDownAsk =
      market.polyDownAsk && market.polyDownAsk > 0
        ? formatNumber(market.polyDownAsk, 4)
        : "n/a";
    const kalshiYesAsk =
      market.kalshiYesAsk && market.kalshiYesAsk > 0
        ? formatNumber(market.kalshiYesAsk, 4)
        : "n/a";
    const kalshiNoAsk =
      market.kalshiNoAsk && market.kalshiNoAsk > 0
        ? formatNumber(market.kalshiNoAsk, 4)
        : "n/a";

    const upNoEstimate = formatEstimate(
      market.estimateUpNo,
      market.estimateUpNoSource,
    );
    const downYesEstimate = formatEstimate(
      market.estimateDownYes,
      market.estimateDownYesSource,
    );
    const selected =
      market.selectedDirection != null ? market.selectedDirection : "none";
    const pending =
      market.pendingDirection != null
        ? `${market.pendingDirection} (${market.pendingDelayMs ?? 0}ms)`
        : "none";
    const position = market.position
      ? `${market.position.direction} shares=${market.position.shares} cost=${formatCurrency(
          market.position.costTotal,
        )}`
      : "none";
    const lastResult = market.lastResult ?? "none";

    let output = `${colors.bright}${market.marketName}${colors.reset} ${colors.dim}(${market.coin.toUpperCase()})${colors.reset}\n`;
    output += `${colors.dim}Time Left: ${timeLeft} | Spot: ${spotPrice} | Poly Threshold: ${polyThreshold} | Kalshi Threshold: ${kalshiThreshold} | Status: ${market.dataStatus}${colors.reset}\n`;
    output += `${colors.dim}Asks: Poly U/D ${polyUpAsk}/${polyDownAsk} | Kalshi Y/N ${kalshiYesAsk}/${kalshiNoAsk}${colors.reset}\n`;
    output += `${colors.dim}Fill (est) Up+No: ${upNoEstimate}${colors.reset}\n`;
    output += `${colors.dim}Fill (est) Down+Yes: ${downYesEstimate}${colors.reset}\n`;
    output += `${colors.dim}Selected: ${selected} | Pending: ${pending} | Position: ${position}${colors.reset}\n`;
    output += `${colors.dim}Last Result: ${lastResult}${colors.reset}\n\n`;

    return output;
  }
}

function formatEstimate(
  estimate: ArbitrageMarketView["estimateUpNo"],
  source: ArbitrageMarketView["estimateUpNoSource"],
): string {
  if (!estimate) return "n/a";
  const sourceLabel = source
    ? source === "orderbook"
      ? " src=book"
      : " src=ask"
    : "";
  return `gap=${estimate.gap.toFixed(4)} shares=${estimate.shares} avg=${estimate.avgPoly.toFixed(
    4,
  )}/${estimate.avgKalshi.toFixed(4)} cost=${estimate.costPoly.toFixed(
    2,
  )}/${estimate.costKalshi.toFixed(2)} total=${estimate.totalCost.toFixed(
    2,
  )}${sourceLabel}`;
}

function formatNumber(value: number, decimals: number): string {
  if (!Number.isFinite(value)) return "n/a";
  return value.toFixed(decimals);
}

function formatCurrency(value: number): string {
  if (!Number.isFinite(value)) return "$0.00";
  const sign = value < 0 ? "-" : "";
  const absolute = Math.abs(value);
  return `${sign}$${absolute.toFixed(2)}`;
}

function formatDuration(seconds: number): string {
  const rounded = Math.max(0, Math.floor(seconds));
  const mins = Math.floor(rounded / 60);
  const secs = rounded % 60;
  if (mins <= 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
}
