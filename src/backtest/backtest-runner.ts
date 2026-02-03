import type { ProfileEngine } from "../services/profile-engine";
import type { ProfileDashboard } from "../cli/profile-dashboard";
import type { ProfileViewState } from "../cli/profile-dashboard";
import type { BacktestHub } from "./backtest-hub";
import type { CoinSymbol } from "../services/auto-market";
import type { MarketSnapshot } from "../services/market-data-hub";

function parseEnvFlag(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const normalized = raw.trim().toLowerCase();
  if (["false", "0", "off", "no"].includes(normalized)) return false;
  if (["true", "1", "on", "yes"].includes(normalized)) return true;
  return defaultValue;
}

export interface BacktestRunnerOptions {
  speed: number;
  tickIntervalMs?: number;
  runId: string;
  modeLabel: string;
  activeProfileIndex: () => number;
  setActiveProfileIndex: (index: number) => void;
  selectedCoins: CoinSymbol[];
  onComplete?: () => void;
  render?: boolean;
  headless?: boolean;
  headlessLogEveryMs?: number;
}

export class BacktestRunner {
  private hub: BacktestHub;
  private engines: ProfileEngine[];
  private dashboard: ProfileDashboard;
  private options: BacktestRunnerOptions;
  private timer: NodeJS.Timeout | null = null;
  private currentTime = 0;
  private tickIntervalMs = 250;
  private nextEvalTimeMs = 0;
  private headlessNextLogMs = 0;
  private headlessLogEveryMs = 15000;
  private headlessLogIndex: Map<string, number> = new Map();
  private useDirtyEval = parseEnvFlag("BACKTEST_DIRTY_EVAL", true);

  constructor(
    hub: BacktestHub,
    engines: ProfileEngine[],
    dashboard: ProfileDashboard,
    options: BacktestRunnerOptions,
  ) {
    this.hub = hub;
    this.engines = engines;
    this.dashboard = dashboard;
    this.options = options;
  }

  start(): void {
    const speed = this.options.speed;
    const isMaxSpeed = !Number.isFinite(speed) || speed <= 0;
    this.tickIntervalMs = this.options.tickIntervalMs ?? 250;

    this.currentTime = this.hub.getCurrentTimeMs();
    this.nextEvalTimeMs = this.currentTime + this.tickIntervalMs;
    if (this.options.headless) {
      this.headlessLogEveryMs = this.options.headlessLogEveryMs ?? 15000;
      this.headlessNextLogMs = this.currentTime;
    }

    if (isMaxSpeed) {
      if (this.options.headless) {
        this.headlessLogEveryMs = Number.POSITIVE_INFINITY;
        this.headlessNextLogMs = Number.POSITIVE_INFINITY;
      }
      this.runMaxSpeed();
      return;
    }

    this.scheduleNextTick();
  }

  stop(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private runMaxSpeed(): void {
    const endTime = this.hub.getEndTimeMs();
    this.processUntil(endTime, false);
    this.options.onComplete?.();
  }

  private scheduleNextTick(): void {
    const endTime = this.hub.getEndTimeMs();
    if (this.currentTime >= endTime) {
      this.options.onComplete?.();
      return;
    }

    const speed = this.options.speed;
    const frameMs = this.tickIntervalMs;
    const delayMs = Math.max(0, Math.floor(frameMs / speed));
    const targetTime = Math.min(endTime, this.currentTime + frameMs);

    this.timer = setTimeout(() => {
      this.processUntil(targetTime, true);
      this.scheduleNextTick();
    }, delayMs);
  }

  private processUntil(targetTime: number, render: boolean): void {
    const endTime = this.hub.getEndTimeMs();
    const cappedTarget = Math.min(targetTime, endTime);

    while (this.currentTime < cappedTarget) {
      const nextTime = this.getNextStepTime(cappedTarget);
      if (nextTime === null) {
        break;
      }

      const shouldRender = render && nextTime >= cappedTarget;
      this.currentTime = nextTime;
      this.hub.advanceTo(this.currentTime);
      this.evaluateAndRender(this.currentTime, shouldRender);

      while (this.nextEvalTimeMs <= this.currentTime) {
        this.nextEvalTimeMs += this.tickIntervalMs;
      }
    }
  }

  private getNextStepTime(targetTime: number): number | null {
    const endTime = this.hub.getEndTimeMs();
    if (this.currentTime >= endTime) {
      return null;
    }

    const hubNext = this.hub.getNextEventTime();
    const pendingNext = this.getNextPendingTime();

    let next = Math.min(targetTime, endTime);
    if (hubNext !== null) {
      next = Math.min(next, hubNext);
    }
    if (pendingNext !== null) {
      next = Math.min(next, pendingNext);
    }
    if (this.nextEvalTimeMs > this.currentTime) {
      next = Math.min(next, this.nextEvalTimeMs);
    }

    if (next <= this.currentTime) {
      const bumped = Math.min(endTime, this.currentTime + 1);
      return bumped > this.currentTime ? bumped : null;
    }

    return next;
  }

  private getNextPendingTime(): number | null {
    let next: number | null = null;
    for (const engine of this.engines) {
      const due = engine.getNextPendingTime();
      if (due === null || due <= this.currentTime) {
        continue;
      }
      if (next === null || due < next) {
        next = due;
      }
    }
    return next;
  }

  private evaluateAndRender(nowMs: number, render: boolean): void {
    if (!this.useDirtyEval) {
      const snapshots = this.hub.getSnapshots();
      for (const engine of this.engines) {
        engine.evaluate(snapshots, nowMs);
      }

      if (this.options.headless) {
        if (nowMs >= this.headlessNextLogMs) {
          this.logHeadlessSnapshot(snapshots, nowMs);
          this.headlessNextLogMs = nowMs + this.headlessLogEveryMs;
        }
        return;
      }

      if (render === false || this.options.render === false) {
        return;
      }

      const profileViews: ProfileViewState[] = this.engines.map((engine) => ({
        name: engine.getName(),
        summary: engine.getSummary(),
        markets: engine.getMarketViews(),
        logs: engine.getLogs(),
        pnlHistory: engine.getPnlHistory(),
      }));

      const activeIndex = Math.min(
        this.options.activeProfileIndex(),
        profileViews.length - 1,
      );
      const activeProfile = profileViews[activeIndex];
      const activeCoin =
        activeProfile && activeProfile.markets.length > 0
          ? activeProfile.markets[0]?.coin ?? null
          : this.options.selectedCoins[0] || null;
      const snap = activeCoin ? snapshots.get(activeCoin) : undefined;
      const isKalshi = snap?.provider === "kalshi";
      const usingMarketHistory = Boolean(
        isKalshi && snap?.kalshiMarketPriceHistory?.length,
      );
      const activeCoinHistory =
        activeCoin && snap
          ? usingMarketHistory
            ? snap.kalshiMarketPriceHistory
            : snap.priceHistory || []
          : [];
      const activeCoinPriceLabel = isKalshi
        ? usingMarketHistory
          ? "Market Price (odds)"
          : "Spot Price (fallback)"
        : activeCoin
          ? `Spot Price (${activeCoin.toUpperCase()})`
          : "Spot Price";

      this.dashboard.update({
        runId: this.options.runId,
        modeLabel: this.options.modeLabel,
        activeProfileIndex: activeIndex,
        profiles: profileViews,
        activeCoin,
        activeCoinPriceHistory: activeCoinHistory ?? [],
        activeCoinPriceLabel,
      });
      return;
    }

    const scheduledEval = nowMs >= this.nextEvalTimeMs;
    const shouldLogHeadless =
      this.options.headless && nowMs >= this.headlessNextLogMs;
    const dirtyCoins = this.hub.drainDirtyCoins();
    const pendingCoins = new Set<CoinSymbol>();
    for (const engine of this.engines) {
      for (const coin of engine.getPendingCoins()) {
        pendingCoins.add(coin);
      }
    }
    const shouldEvalAll = scheduledEval || render || shouldLogHeadless;
    const coinFilter =
      shouldEvalAll || (dirtyCoins.size === 0 && pendingCoins.size === 0)
        ? undefined
        : new Set([...dirtyCoins, ...pendingCoins]);

    if (!shouldEvalAll && (!coinFilter || coinFilter.size === 0)) {
      return;
    }

    const snapshots = this.hub.getSnapshots();
    for (const engine of this.engines) {
      engine.evaluate(snapshots, nowMs, coinFilter);
    }

    if (this.options.headless) {
      if (nowMs >= this.headlessNextLogMs) {
        this.logHeadlessSnapshot(snapshots, nowMs);
        this.headlessNextLogMs = nowMs + this.headlessLogEveryMs;
      }
      return;
    }

    if (render === false || this.options.render === false) {
      return;
    }

    const profileViews: ProfileViewState[] = this.engines.map((engine) => ({
      name: engine.getName(),
      summary: engine.getSummary(),
      markets: engine.getMarketViews(),
      logs: engine.getLogs(),
      pnlHistory: engine.getPnlHistory(),
    }));

    const activeIndex = Math.min(
      this.options.activeProfileIndex(),
      profileViews.length - 1,
    );
    const activeProfile = profileViews[activeIndex];
    const activeCoin =
      activeProfile && activeProfile.markets.length > 0
        ? activeProfile.markets[0]?.coin ?? null
        : this.options.selectedCoins[0] || null;
    const snap = activeCoin ? snapshots.get(activeCoin) : undefined;
    const isKalshi = snap?.provider === "kalshi";
    const usingMarketHistory = Boolean(
      isKalshi && snap?.kalshiMarketPriceHistory?.length,
    );
    const activeCoinHistory =
      activeCoin && snap
        ? usingMarketHistory
          ? snap.kalshiMarketPriceHistory
          : snap.priceHistory || []
        : [];
    const activeCoinPriceLabel = isKalshi
      ? usingMarketHistory
        ? "Market Price (odds)"
        : "Spot Price (fallback)"
      : activeCoin
        ? `Spot Price (${activeCoin.toUpperCase()})`
        : "Spot Price";

    this.dashboard.update({
      runId: this.options.runId,
      modeLabel: this.options.modeLabel,
      activeProfileIndex: activeIndex,
      profiles: profileViews,
      activeCoin,
      activeCoinPriceHistory: activeCoinHistory ?? [],
      activeCoinPriceLabel,
    });
  }

  private logHeadlessSnapshot(
    snapshots: Map<CoinSymbol, MarketSnapshot>,
    nowMs: number,
  ): void {
    console.log(`[headless] t=${new Date(nowMs).toISOString()}`);
    const logSignalStats = parseEnvFlag("BACKTEST_SIGNAL_STATS", false);

    for (const [coin, snapshot] of snapshots.entries()) {
      const upBook = snapshot.orderBooks.get(snapshot.upTokenId);
      const downBook = snapshot.orderBooks.get(snapshot.downTokenId);
      const threshold =
        snapshot.priceToBeat > 0 ? snapshot.priceToBeat : snapshot.referencePrice;
      const timeLeft =
        snapshot.timeLeftSec === null
          ? "n/a"
          : this.formatDuration(snapshot.timeLeftSec);

      const upBid = snapshot.bestBid.get(snapshot.upTokenId) ?? null;
      const upAsk = snapshot.bestAsk.get(snapshot.upTokenId) ?? null;
      const downBid = snapshot.bestBid.get(snapshot.downTokenId) ?? null;
      const downAsk = snapshot.bestAsk.get(snapshot.downTokenId) ?? null;
      const upLast = upBook?.lastTrade || null;
      const downLast = downBook?.lastTrade || null;

      console.log(
        `[headless] ${coin.toUpperCase()} ${snapshot.slug} left=${timeLeft} price=${this.formatMaybe(
          snapshot.cryptoPrice,
          2,
        )} ref=${this.formatMaybe(threshold, 2)} up(bid/ask/last)=${this.formatMaybe(
          upBid,
          4,
        )}/${this.formatMaybe(upAsk, 4)}/${this.formatMaybe(
          upLast,
          4,
        )} down(bid/ask/last)=${this.formatMaybe(
          downBid,
          4,
        )}/${this.formatMaybe(downAsk, 4)}/${this.formatMaybe(
          downLast,
          4,
        )} status=${snapshot.dataStatus}`,
      );
    }

    for (const engine of this.engines) {
      const summary = engine.getSummary();
      console.log(
        `[headless] ${engine.getName()} trades=${summary.totalTrades} pnl=${summary.totalProfit.toFixed(
          2,
        )} exposure=${summary.openExposure.toFixed(2)}`,
      );
      if (logSignalStats) {
        const stats = engine.getSignalStats();
        const spread =
          stats.avgSpread !== null ? stats.avgSpread.toFixed(4) : "n/a";
        const depth =
          stats.avgDepthValue !== null ? stats.avgDepthValue.toFixed(2) : "n/a";
        const conf =
          stats.avgConfidence !== null
            ? stats.avgConfidence.toFixed(2)
            : "n/a";
        console.log(
          `[headless] ${engine.getName()} signal avg spread=${spread} depth=${depth} conf=${conf}`,
        );
      }
      const newLines = this.drainHeadlessLogs(engine);
      for (const line of newLines) {
        console.log(`[headless][${engine.getName()}] ${line}`);
      }
    }
  }

  private drainHeadlessLogs(engine: ProfileEngine): string[] {
    const name = engine.getName();
    const lines = engine.getLogs();
    let lastIndex = this.headlessLogIndex.get(name);
    if (lastIndex === undefined || lastIndex > lines.length) {
      lastIndex = Math.max(0, lines.length - 5);
    }
    const nextLines = lines.slice(lastIndex);
    this.headlessLogIndex.set(name, lines.length);
    return nextLines;
  }

  private formatMaybe(value: number | null, digits: number): string {
    if (value === null || value === undefined) return "n/a";
    if (!Number.isFinite(value) || value === 0) return "n/a";
    return value.toFixed(digits);
  }

  private formatDuration(totalSeconds: number): string {
    const clamped = Math.max(0, Math.floor(totalSeconds));
    const minutes = Math.floor(clamped / 60);
    const seconds = clamped % 60;
    return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(
      2,
      "0",
    )}`;
  }
}
