/**
 * CLI dashboard for cross-platform outcome analysis: two graphs (Polymarket + Kalshi odds)
 * and a comparison panel with spot, price-to-beat, bid/ask, outcome labels, and resolution match %.
 */

import { PriceGraph } from "./price-graph";
import { colors } from "./dashboard";
import { enableTui } from "./tui";
import type { CoinSymbol } from "../services/auto-market";
import type { MarketSnapshot } from "../services/market-data-hub";
import type { AccuracyState, NormalizedOutcome } from "../services/cross-platform-compare";
import { getAccuracyPercent, getWindowAccuracyPercent } from "../services/cross-platform-compare";

const RENDER_THROTTLE_MS = 150;

function formatNumber(value: number, decimals: number): string {
  return value.toFixed(decimals);
}

function formatDuration(sec: number | null): string {
  if (sec === null || !Number.isFinite(sec)) return "n/a";
  const s = Math.max(0, Math.floor(sec));
  const m = Math.floor(s / 60);
  const remainder = s % 60;
  return `${String(m).padStart(2, "0")}:${String(remainder).padStart(2, "0")}`;
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

export interface CrossPlatformDashboardState {
  polySnapshots: Map<CoinSymbol, MarketSnapshot>;
  kalshiSnapshots: Map<CoinSymbol, MarketSnapshot>;
  coins: CoinSymbol[];
  accuracyByCoin: Map<CoinSymbol, AccuracyState>;
  polyOddsHistoryByCoin: Map<CoinSymbol, number[]>;
  kalshiOddsHistoryByCoin: Map<CoinSymbol, number[]>;
  lastComparisonByCoin: Map<
    CoinSymbol,
    {
      polyOutcome: NormalizedOutcome;
      kalshiOutcome: NormalizedOutcome;
      matched: boolean;
      comparedAtMs: number;
      polySlug: string;
      kalshiSlug: string;
    }
  >;
}

export class CrossPlatformDashboard {
  private lastRenderTime = 0;
  private renderCount = 0;
  private startTime = Date.now();

  update(state: CrossPlatformDashboardState, force: boolean = false): void {
    if (this.renderCount === 0) {
      enableTui();
    }
    const now = Date.now();
    if (
      !force &&
      this.renderCount > 0 &&
      now - this.lastRenderTime < RENDER_THROTTLE_MS
    ) {
      return;
    }
    this.lastRenderTime = now;
    this.renderCount++;
    this.render(state);
  }

  private render(state: CrossPlatformDashboardState): void {
    const { coins } = state;
    const activeCoin = coins[0] ?? null;
    if (!activeCoin) {
      process.stdout.write(
        `${colors.dim}No coins selected for cross-platform analysis.${colors.reset}\n`,
      );
      return;
    }

    const polySnap = state.polySnapshots.get(activeCoin);
    const kalshiSnap = state.kalshiSnapshots.get(activeCoin);
    const polyOdds = state.polyOddsHistoryByCoin.get(activeCoin) ?? [];
    const kalshiOdds = state.kalshiOddsHistoryByCoin.get(activeCoin) ?? [];
    const accuracy = state.accuracyByCoin.get(activeCoin);
    const accuracyPct = accuracy ? getAccuracyPercent(accuracy) : null;
    const windowPct = accuracy ? getWindowAccuracyPercent(accuracy, 100) : null;
    const lastComparison = state.lastComparisonByCoin.get(activeCoin);

    let out = "\x1b[2J\x1b[H";
    const runtimeSec = (Date.now() - this.startTime) / 1000;
    out += `${colors.bright}${colors.cyan}Cross-Platform Outcome Analysis${colors.reset} ${colors.dim}(${activeCoin.toUpperCase()}) | Runtime: ${runtimeSec.toFixed(1)}s${colors.reset}\n`;
    out += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    const polySpotHistory = polySnap?.priceHistory ?? [];
    const kalshiSpotHistory = kalshiSnap?.priceHistory ?? [];

    const gap = 4;
    const graphWidth = 60;

    const polySpotTitle = `${colors.bright}Polymarket Spot${colors.reset}`;
    const polyOddsTitle = `${colors.bright}Polymarket Odds${colors.reset}`;
    const kalshiSpotTitle = `${colors.bright}Kalshi Spot${colors.reset}`;
    const kalshiOddsTitle = `${colors.bright}Kalshi Odds${colors.reset}`;

    const buildLines = (
      title: string,
      prices: number[],
      width: number | "auto",
    ): string[] => {
      const graph = new PriceGraph({ height: 6, width, mode: "candles" });
      if (prices.length > 0) graph.addPrices(prices);
      return [title, ...splitLines(graph.render())];
    };

    const gridPolySpotLines = buildLines(polySpotTitle, polySpotHistory, graphWidth);
    const gridPolyOddsLines = buildLines(polyOddsTitle, polyOdds, graphWidth);
    const gridKalshiSpotLines = buildLines(kalshiSpotTitle, kalshiSpotHistory, graphWidth);
    const gridKalshiOddsLines = buildLines(kalshiOddsTitle, kalshiOdds, graphWidth);

    out += renderSideBySide(gridPolySpotLines, gridPolyOddsLines, gap);
    out += "\n\n";
    out += renderSideBySide(gridKalshiSpotLines, gridKalshiOddsLines, gap);
    out += "\n";

    out += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    out += this.renderComparison(
      activeCoin,
      polySnap,
      kalshiSnap,
      accuracyPct,
      windowPct,
      accuracy?.totalCount ?? 0,
      lastComparison,
    );
    out += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    out += `${colors.dim}Ctrl+C to exit${colors.reset}\n`;

    process.stdout.write(out);
  }

  private renderComparison(
    coin: CoinSymbol,
    polySnap: MarketSnapshot | undefined,
    kalshiSnap: MarketSnapshot | undefined,
    accuracyPct: number | null,
    windowPct: number | null,
    totalComparisons: number,
    lastComparison:
      | {
          polyOutcome: NormalizedOutcome;
          kalshiOutcome: NormalizedOutcome;
          matched: boolean;
          comparedAtMs: number;
          polySlug: string;
          kalshiSlug: string;
        }
      | undefined,
  ): string {
    let out = `${colors.bright}Comparison${colors.reset}\n`;

    const spotPoly = polySnap?.cryptoPrice ?? 0;
    const spotKalshi =
      (kalshiSnap?.kalshiUnderlyingValue ?? kalshiSnap?.cryptoPrice) ?? 0;
    const polyPtbValue =
      polySnap && polySnap.priceToBeat > 0
        ? polySnap.priceToBeat
        : polySnap?.referencePrice ?? 0;
    const polyPtbSource =
      polySnap && polySnap.priceToBeat > 0
        ? "ptb"
        : polySnap && polySnap.referencePrice > 0
          ? "ref"
          : "n/a";
    const kalshiPtbValue =
      kalshiSnap && kalshiSnap.priceToBeat > 0
        ? kalshiSnap.priceToBeat
        : kalshiSnap?.referencePrice ?? 0;
    const kalshiPtbSource =
      kalshiSnap && kalshiSnap.priceToBeat > 0
        ? "ptb"
        : kalshiSnap && kalshiSnap.referencePrice > 0
          ? "ref"
          : "n/a";

    out += `Spot: Poly ${spotPoly > 0 ? formatNumber(spotPoly, 2) : "n/a"} | Kalshi ${spotKalshi > 0 ? formatNumber(spotKalshi, 2) : "n/a"}\n`;
    out += `Threshold (price to beat): Poly ${polyPtbValue > 0 ? formatNumber(polyPtbValue, 2) : "n/a"}${polyPtbSource !== "n/a" ? ` (${polyPtbSource})` : ""} | Kalshi ${kalshiPtbValue > 0 ? formatNumber(kalshiPtbValue, 2) : "n/a"}${kalshiPtbSource !== "n/a" ? ` (${kalshiPtbSource})` : ""}\n`;

    const polyUpId = polySnap?.upTokenId;
    const polyBid = polyUpId ? polySnap?.bestBid.get(polyUpId) ?? null : null;
    const polyAsk = polyUpId ? polySnap?.bestAsk.get(polyUpId) ?? null : null;
    const kalshiBid = kalshiSnap?.bestBid.get("YES") ?? null;
    const kalshiAsk = kalshiSnap?.bestAsk.get("YES") ?? null;

    out += `Bid/Ask (up/yes): Poly ${polyBid != null ? formatNumber(polyBid, 4) : "n/a"} / ${polyAsk != null ? formatNumber(polyAsk, 4) : "n/a"} | Kalshi ${kalshiBid != null ? formatNumber(kalshiBid, 4) : "n/a"} / ${kalshiAsk != null ? formatNumber(kalshiAsk, 4) : "n/a"}\n`;

    out += `Outcome labels: Poly ${polySnap?.upOutcome ?? "n/a"} / ${polySnap?.downOutcome ?? "n/a"} | Kalshi YES=\"${kalshiSnap?.upOutcome ?? "n/a"}\" NO=\"${kalshiSnap?.downOutcome ?? "n/a"}\"\n`;

    const timeLeftPoly = polySnap?.timeLeftSec ?? null;
    const timeLeftKalshi = kalshiSnap?.timeLeftSec ?? null;
    out += `Time left: Poly ${formatDuration(timeLeftPoly)} | Kalshi ${formatDuration(timeLeftKalshi)}\n`;

    if (accuracyPct != null && totalComparisons > 0) {
      out += `${colors.bright}Final outcome match rate:${colors.reset} ${formatNumber(accuracyPct, 1)}% (${totalComparisons} markets)`;
      if (windowPct != null) out += ` | ${formatNumber(windowPct, 1)}% (last 100)`;
      out += "\n";
    } else {
      out += `${colors.dim}Final outcome match rate: (waiting for completed markets)${colors.reset}\n`;
    }

    if (lastComparison) {
      const at = new Date(lastComparison.comparedAtMs).toLocaleTimeString();
      const status = lastComparison.matched ? "matched" : "mismatched";
      out += `Last final outcome: Poly ${lastComparison.polyOutcome} vs Kalshi ${lastComparison.kalshiOutcome} (${status}) @ ${at}\n`;
    } else {
      out += `${colors.dim}Last final outcome: pending${colors.reset}\n`;
    }

    const polySlug = polySnap?.slug ?? "n/a";
    const kalshiSlug = kalshiSnap?.slug ?? "n/a";
    out += `${colors.dim}Poly slug: ${polySlug} | Kalshi slug: ${kalshiSlug}${colors.reset}\n`;

    const matchStatus = !polySnap
      ? "no match (Poly market missing)"
      : !kalshiSnap
        ? "no match (Kalshi market missing)"
        : "matched (coin + timeframe)";
    out += `${colors.dim}Match: ${matchStatus}${colors.reset}\n`;

    return out;
  }
}
