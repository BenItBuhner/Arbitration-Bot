/**
 * CLI dashboard for price diff detection: show Polymarket/Kalshi graphs
 * and live diff status for opposing outcomes.
 */

import { PriceGraph } from "./price-graph";
import { colors } from "./dashboard";
import { enableTui } from "./tui";
import type { CoinSymbol } from "../services/auto-market";
import type { MarketSnapshot } from "../services/market-data-hub";

const RENDER_THROTTLE_MS = 150;

function formatNumber(value: number, decimals: number): string {
  return value.toFixed(decimals);
}

function formatMaybe(value: number | null | undefined, decimals: number): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "n/a";
  return formatNumber(value, decimals);
}

function formatUsd(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "n/a";
  return `$${value.toFixed(2)}`;
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

function renderSideBySide(leftLines: string[], rightLines: string[], gap: number): string {
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

function renderCoinTabs(
  coins: CoinSymbol[],
  activeIndex: number,
  marketCountsByCoin: Map<
    CoinSymbol,
    {
      totalMarkets: number;
      matchedMarkets: number;
      upNoMarkets: number;
      downYesMarkets: number;
    }
  >,
): string {
  if (coins.length === 0) {
    return `${colors.dim}Coins: none${colors.reset}\n`;
  }
  const tabs = coins
    .map((coin, index) => {
      const isActive = index === activeIndex;
      const color = isActive ? colors.bright + colors.cyan : colors.dim;
      const counts = marketCountsByCoin.get(coin);
      const total = counts?.totalMarkets ?? 0;
      const matched = counts?.matchedMarkets ?? 0;
      const rate = total > 0 ? Math.round((100 * matched) / total) : 0;
      const suffix = ` ${matched}/${total} ${rate}%`;
      return `${color}[${coin.toUpperCase()}${suffix}]${colors.reset}`;
    })
    .join(" ");
  return `${tabs}\n`;
}

export interface PriceDiffDashboardState {
  coins: CoinSymbol[];
  activeCoin: CoinSymbol;
  activeCoinIndex: number;
  coinCount: number;
  realisticFillEnabled: boolean;
  fillBudgetUsd: number | null;
  polySnapshots: Map<CoinSymbol, MarketSnapshot>;
  kalshiSnapshots: Map<CoinSymbol, MarketSnapshot>;
  polyOddsHistoryByCoin: Map<CoinSymbol, number[]>;
  kalshiOddsHistoryByCoin: Map<CoinSymbol, number[]>;
  diffByCoin: Map<
    CoinSymbol,
    {
      polyUpAsk: number | null;
      polyDownAsk: number | null;
      kalshiYesAsk: number | null;
      kalshiNoAsk: number | null;
      costPolyUpKalshiNo: number | null;
      costPolyDownKalshiYes: number | null;
      gapPolyUpKalshiNo: number | null;
      gapPolyDownKalshiYes: number | null;
      abovePolyUpKalshiNo: boolean;
      abovePolyDownKalshiYes: boolean;
      realisticUpNo?: {
        shares: number;
        avgPoly: number;
        avgKalshi: number;
        costPoly: number;
        costKalshi: number;
        totalCost: number;
        gap: number;
      } | null;
      realisticDownYes?: {
        shares: number;
        avgPoly: number;
        avgKalshi: number;
        costPoly: number;
        costKalshi: number;
        totalCost: number;
        gap: number;
      } | null;
      confirmUpNo?: {
        shares: number;
        avgPoly: number;
        avgKalshi: number;
        costPoly: number;
        costKalshi: number;
        totalCost: number;
        gap: number;
        confirmedAtMs: number;
        delayMs: number;
        gapDelta: number;
      } | null;
      confirmDownYes?: {
        shares: number;
        avgPoly: number;
        avgKalshi: number;
        costPoly: number;
        costKalshi: number;
        totalCost: number;
        gap: number;
        confirmedAtMs: number;
        delayMs: number;
        gapDelta: number;
      } | null;
      updatedAtMs: number;
    }
  >;
  threshold: number;
  recentLogs: string[];
  marketCountsByCoin: Map<
    CoinSymbol,
    {
      totalMarkets: number;
      matchedMarkets: number;
      upNoMarkets: number;
      downYesMarkets: number;
    }
  >;
}

export class PriceDiffDashboard {
  private lastRenderTime = 0;
  private renderCount = 0;
  private startTime = Date.now();

  update(state: PriceDiffDashboardState, force: boolean = false): void {
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

  private render(state: PriceDiffDashboardState): void {
    const { coins } = state;
    const activeCoin = state.activeCoin ?? coins[0] ?? null;
    if (!activeCoin) {
      process.stdout.write(
        `${colors.dim}No coins selected for price diff detection.${colors.reset}\n`,
      );
      return;
    }

    const polySnap = state.polySnapshots.get(activeCoin);
    const kalshiSnap = state.kalshiSnapshots.get(activeCoin);
    const polyOdds = state.polyOddsHistoryByCoin.get(activeCoin) ?? [];
    const kalshiOdds = state.kalshiOddsHistoryByCoin.get(activeCoin) ?? [];
    const diff = state.diffByCoin.get(activeCoin);

    let out = "\x1b[2J\x1b[H";
    const runtimeSec = (Date.now() - this.startTime) / 1000;
    out += `${colors.bright}${colors.cyan}Price Diff Detection${colors.reset} ${colors.dim}(${activeCoin.toUpperCase()}) | Runtime: ${runtimeSec.toFixed(1)}s | Threshold: ${state.threshold.toFixed(4)}${colors.reset}\n`;
    out += renderCoinTabs(coins, state.activeCoinIndex, state.marketCountsByCoin);
    if (state.realisticFillEnabled) {
      out += `${colors.dim}Realistic fill: ${formatUsd(state.fillBudgetUsd)} | Confirm delay: 250-300ms${colors.reset}\n`;
    }
    if (state.coinCount > 1) {
      out += `${colors.dim}Coin: ${activeCoin.toUpperCase()} (${state.activeCoinIndex + 1}/${state.coinCount}) | Left/Right to switch coins${colors.reset}\n`;
    }
    out += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;

    const polySpotHistory = polySnap?.priceHistory ?? [];
    const kalshiSpotHistory = kalshiSnap?.priceHistory ?? [];

    const gap = 4;
    const graphWidth = 60;
    const buildLines = (title: string, prices: number[], width: number): string[] => {
      const graph = new PriceGraph({ height: 6, width, mode: "candles" });
      if (prices.length > 0) graph.addPrices(prices);
      return [title, ...splitLines(graph.render())];
    };

    const gridPolySpotLines = buildLines(
      `${colors.bright}Polymarket Spot${colors.reset}`,
      polySpotHistory,
      graphWidth,
    );
    const gridPolyOddsLines = buildLines(
      `${colors.bright}Polymarket Odds${colors.reset}`,
      polyOdds,
      graphWidth,
    );
    const gridKalshiSpotLines = buildLines(
      `${colors.bright}Kalshi Spot${colors.reset}`,
      kalshiSpotHistory,
      graphWidth,
    );
    const gridKalshiOddsLines = buildLines(
      `${colors.bright}Kalshi Odds${colors.reset}`,
      kalshiOdds,
      graphWidth,
    );

    out += renderSideBySide(gridPolySpotLines, gridPolyOddsLines, gap);
    out += "\n\n";
    out += renderSideBySide(gridKalshiSpotLines, gridKalshiOddsLines, gap);
    out += "\n";

    out += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    out += this.renderComparison(activeCoin, polySnap, kalshiSnap, diff, state);
    out += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    out += this.renderRecentLogs(state.recentLogs);
    out += `${colors.dim}${"-".repeat(80)}${colors.reset}\n`;
    out += `${colors.dim}Ctrl+C to exit${colors.reset}\n`;

    process.stdout.write(out);
  }

  private renderComparison(
    coin: CoinSymbol,
    polySnap: MarketSnapshot | undefined,
    kalshiSnap: MarketSnapshot | undefined,
    diff:
      | {
          polyUpAsk: number | null;
          polyDownAsk: number | null;
          kalshiYesAsk: number | null;
          kalshiNoAsk: number | null;
          costPolyUpKalshiNo: number | null;
          costPolyDownKalshiYes: number | null;
          gapPolyUpKalshiNo: number | null;
          gapPolyDownKalshiYes: number | null;
          abovePolyUpKalshiNo: boolean;
          abovePolyDownKalshiYes: boolean;
          realisticUpNo?: {
            shares: number;
            avgPoly: number;
            avgKalshi: number;
            costPoly: number;
            costKalshi: number;
            totalCost: number;
            gap: number;
          } | null;
          realisticDownYes?: {
            shares: number;
            avgPoly: number;
            avgKalshi: number;
            costPoly: number;
            costKalshi: number;
            totalCost: number;
            gap: number;
          } | null;
          confirmUpNo?: {
            shares: number;
            avgPoly: number;
            avgKalshi: number;
            costPoly: number;
            costKalshi: number;
            totalCost: number;
            gap: number;
            confirmedAtMs: number;
            delayMs: number;
            gapDelta: number;
          } | null;
          confirmDownYes?: {
            shares: number;
            avgPoly: number;
            avgKalshi: number;
            costPoly: number;
            costKalshi: number;
            totalCost: number;
            gap: number;
            confirmedAtMs: number;
            delayMs: number;
            gapDelta: number;
          } | null;
        }
      | undefined,
    state: PriceDiffDashboardState,
  ): string {
    let out = `${colors.bright}Comparison${colors.reset}\n`;

    const spotPoly = polySnap?.cryptoPrice ?? 0;
    const spotKalshi =
      (kalshiSnap?.kalshiUnderlyingValue ?? kalshiSnap?.cryptoPrice) ?? 0;
    out += `Spot: Poly ${spotPoly > 0 ? formatNumber(spotPoly, 2) : "n/a"} | Kalshi ${spotKalshi > 0 ? formatNumber(spotKalshi, 2) : "n/a"}\n`;

    const polyUpAsk = diff?.polyUpAsk ?? null;
    const polyDownAsk = diff?.polyDownAsk ?? null;
    const kalshiYesAsk = diff?.kalshiYesAsk ?? null;
    const kalshiNoAsk = diff?.kalshiNoAsk ?? null;

    out += `Asks (Up/Yes): Poly ${formatMaybe(polyUpAsk, 4)} | Kalshi ${formatMaybe(kalshiYesAsk, 4)}\n`;
    out += `Asks (Down/No): Poly ${formatMaybe(polyDownAsk, 4)} | Kalshi ${formatMaybe(kalshiNoAsk, 4)}\n`;

    const flagLabel = (value: boolean): string =>
      value ? `${colors.bright}${colors.green}FLAG${colors.reset}` : "ok";

    const costUpNo = diff?.costPolyUpKalshiNo ?? null;
    const costDownYes = diff?.costPolyDownKalshiYes ?? null;
    const gapUpNo = diff?.gapPolyUpKalshiNo ?? null;
    const gapDownYes = diff?.gapPolyDownKalshiYes ?? null;

    out += `Gap (profit) PolyUp+KalshiNo: ${formatMaybe(gapUpNo, 4)} (cost ${formatMaybe(
      costUpNo,
      4,
    )}) | ${flagLabel(diff?.abovePolyUpKalshiNo ?? false)} | PolyUp ${formatMaybe(
      polyUpAsk,
      4,
    )} + KalshiNo ${formatMaybe(kalshiNoAsk, 4)}\n`;
    out += `Gap (profit) PolyDown+KalshiYes: ${formatMaybe(
      gapDownYes,
      4,
    )} (cost ${formatMaybe(costDownYes, 4)}) | ${flagLabel(
      diff?.abovePolyDownKalshiYes ?? false,
    )} | PolyDown ${formatMaybe(polyDownAsk, 4)} + KalshiYes ${formatMaybe(
      kalshiYesAsk,
      4,
    )}\n`;

    if (state.realisticFillEnabled) {
      const estUpNo = diff?.realisticUpNo ?? null;
      const estDownYes = diff?.realisticDownYes ?? null;
      const confUpNo = diff?.confirmUpNo ?? null;
      const confDownYes = diff?.confirmDownYes ?? null;

      out += `Fill (est) PolyUp+KalshiNo: ${
        estUpNo
          ? `shares ${estUpNo.shares} | avg ${formatMaybe(
              estUpNo.avgPoly,
              4,
            )}/${formatMaybe(estUpNo.avgKalshi, 4)} | cost ${formatUsd(
              estUpNo.totalCost,
            )} | gap ${formatMaybe(estUpNo.gap, 4)}`
          : "n/a"
      }\n`;
      out += `Fill (est) PolyDown+KalshiYes: ${
        estDownYes
          ? `shares ${estDownYes.shares} | avg ${formatMaybe(
              estDownYes.avgPoly,
              4,
            )}/${formatMaybe(estDownYes.avgKalshi, 4)} | cost ${formatUsd(
              estDownYes.totalCost,
            )} | gap ${formatMaybe(estDownYes.gap, 4)}`
          : "n/a"
      }\n`;
      out += `Fill (confirm) PolyUp+KalshiNo: ${
        confUpNo
          ? `gap ${formatMaybe(confUpNo.gap, 4)} (Δ ${formatMaybe(
              confUpNo.gapDelta,
              4,
            )}) | cost ${formatUsd(confUpNo.totalCost)}`
          : "n/a"
      }\n`;
      out += `Fill (confirm) PolyDown+KalshiYes: ${
        confDownYes
          ? `gap ${formatMaybe(confDownYes.gap, 4)} (Δ ${formatMaybe(
              confDownYes.gapDelta,
              4,
            )}) | cost ${formatUsd(confDownYes.totalCost)}`
          : "n/a"
      }\n`;
    }

    const counts = state.marketCountsByCoin.get(coin);
    const totalMarkets = counts?.totalMarkets ?? 0;
    const upNoRate =
      totalMarkets > 0
        ? ((100 * (counts?.upNoMarkets ?? 0)) / totalMarkets).toFixed(1)
        : "n/a";
    const downYesRate =
      totalMarkets > 0
        ? ((100 * (counts?.downYesMarkets ?? 0)) / totalMarkets).toFixed(1)
        : "n/a";
    out += `Markets with diff: PolyUp/KalshiNo ${counts?.upNoMarkets ?? 0}/${totalMarkets} (${upNoRate}%) | PolyDown/KalshiYes ${counts?.downYesMarkets ?? 0}/${totalMarkets} (${downYesRate}%)\n`;

    const timeLeftPoly = polySnap?.timeLeftSec ?? null;
    const timeLeftKalshi = kalshiSnap?.timeLeftSec ?? null;
    out += `Time left: Poly ${formatDuration(timeLeftPoly)} | Kalshi ${formatDuration(timeLeftKalshi)}\n`;

    const polySlug = polySnap?.slug ?? "n/a";
    const kalshiSlug =
      kalshiSnap?.slug ?? kalshiSnap?.marketTicker ?? "n/a";
    out += `Poly slug: ${polySlug} | Kalshi slug: ${kalshiSlug}\n`;
    out += `${colors.dim}Threshold (TEST_PRICE_DIFF_REQ): ${state.threshold.toFixed(4)}${colors.reset}\n`;

    return out;
  }

  private renderRecentLogs(lines: string[]): string {
    const maxLines = 5;
    const entries = lines.slice(-maxLines);
    let out = `${colors.bright}Recent Logs${colors.reset}\n`;
    if (entries.length === 0) {
      out += `${colors.dim}(no recent logs)${colors.reset}\n`;
      for (let i = 1; i < maxLines; i += 1) {
        out += "\n";
      }
      return out;
    }
    for (const entry of entries) {
      out += `${entry}\n`;
    }
    if (entries.length < maxLines) {
      for (let i = entries.length; i < maxLines; i += 1) {
        out += "\n";
      }
    }
    return out;
  }
}
