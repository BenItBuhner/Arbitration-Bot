import { existsSync } from "fs";
import { join } from "path";
import { MarketDataHub } from "../services/market-data-hub";
import { KalshiMarketDataHub } from "../services/kalshi-market-data-hub";
import { ArbitrageEngine } from "../services/arbitrage-engine";
import { RunLogger } from "../services/run-logger";
import { ArbitrageDashboard } from "../cli/arbitrage-dashboard";
import type { CoinSymbol } from "../services/auto-market";
import { promptText, selectMany } from "../cli/prompts";
import {
  loadProviderConfig,
  normalizeCoinKey,
  sanitizeProfileName,
  type KalshiCoinSelection,
} from "../services/profile-config";
import {
  loadArbitrageConfig,
  type ArbitrageCoinConfig,
} from "../services/arbitrage-config";
import { getKalshiEnvConfig } from "../clients/kalshi/kalshi-config";
import { KalshiClient } from "../clients/kalshi/kalshi-client";
import { computeOddsMid } from "../services/cross-platform-compare";
import {
  deriveSeriesTickerFromMarket,
  looksLikeKalshiMarketTicker,
  normalizeKalshiTicker,
  parseKalshiMarketUrl,
} from "../clients/kalshi/kalshi-url";

const ODDS_HISTORY_LIMIT = 180;

export interface FakeTradeRouteOptions {
  profiles?: string[];
  coins?: string[];
  autoSelect?: boolean;
  provider?: string;
  headless?: boolean;
}

function getNextRunDir(): { runDir: string; runId: string } {
  const logsDir = join(process.cwd(), "logs");
  let index = 1;

  while (true) {
    const name = index === 1 ? "run" : `run${index}`;
    const candidate = join(logsDir, name);
    if (!existsSync(candidate)) {
      return { runDir: candidate, runId: name };
    }
    index += 1;
  }
}

function createEmptyKalshiSelection(): KalshiCoinSelection {
  return {
    tickers: [],
    seriesTickers: [],
    eventTickers: [],
    marketUrls: [],
    autoDiscover: true,
  };
}

function ensureKalshiSelection(
  selections: Map<CoinSymbol, KalshiCoinSelection>,
  coin: CoinSymbol,
): KalshiCoinSelection {
  const existing = selections.get(coin);
  if (existing) return existing;
  const next = createEmptyKalshiSelection();
  selections.set(coin, next);
  return next;
}

function hasKalshiSelection(selection: KalshiCoinSelection | undefined): boolean {
  if (!selection) return false;
  return (
    selection.tickers.length > 0 ||
    selection.seriesTickers.length > 0 ||
    selection.eventTickers.length > 0 ||
    selection.marketUrls.length > 0
  );
}

function applyKalshiSelectorInput(
  selection: KalshiCoinSelection,
  rawInput: string,
): void {
  const entries = rawInput
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);

  const addTicker = (value: string) => {
    const normalized = normalizeKalshiTicker(value);
    if (!normalized) return;
    selection.tickers.push(normalized);
    const derived = deriveSeriesTickerFromMarket(normalized);
    if (derived) selection.seriesTickers.push(derived);
  };

  const addSeries = (value: string) => {
    const normalized = normalizeKalshiTicker(value);
    if (!normalized) return;
    selection.seriesTickers.push(normalized);
  };

  const addEvent = (value: string) => {
    const normalized = normalizeKalshiTicker(value);
    if (!normalized) return;
    selection.eventTickers.push(normalized);
    const derived = deriveSeriesTickerFromMarket(normalized);
    if (derived) selection.seriesTickers.push(derived);
  };

  const addUrl = (value: string) => {
    const trimmed = value.trim();
    if (!trimmed) return;
    selection.marketUrls.push(trimmed);
    const parsed = parseKalshiMarketUrl(trimmed);
    if (parsed.marketTicker) addTicker(parsed.marketTicker);
    if (parsed.seriesTicker) addSeries(parsed.seriesTicker);
  };

  for (const entry of entries) {
    if (entry.toLowerCase().startsWith("http")) {
      addUrl(entry);
      continue;
    }

    const prefixMatch = entry.match(/^([a-zA-Z]+)\s*:\s*(.+)$/);
    if (prefixMatch) {
      const prefix = prefixMatch[1]?.trim().toLowerCase();
      const value = prefixMatch[2]?.trim() ?? "";
      if (!value) continue;
      if (prefix === "series") {
        addSeries(value);
        continue;
      }
      if (prefix === "event") {
        addEvent(value);
        continue;
      }
      if (prefix === "market" || prefix === "ticker") {
        addTicker(value);
        continue;
      }
      if (prefix === "url") {
        addUrl(value);
        continue;
      }
    }

    if (looksLikeKalshiMarketTicker(entry)) {
      addTicker(entry);
      continue;
    }

    addSeries(entry);
  }

  selection.tickers = Array.from(new Set(selection.tickers));
  selection.seriesTickers = Array.from(new Set(selection.seriesTickers));
  selection.eventTickers = Array.from(new Set(selection.eventTickers));
  selection.marketUrls = Array.from(new Set(selection.marketUrls));
}

function setupProfileAndCoinNavigation(
  getProfileCount: () => number,
  getProfileIndex: () => number,
  setProfileIndex: (nextIndex: number) => void,
  getCoinCount: () => number,
  getCoinIndex: () => number,
  setCoinIndex: (nextIndex: number) => void,
): () => void {
  let keyBuffer = Buffer.alloc(0);

  const handleData = (data: Buffer | string) => {
    const keyStr = typeof data === "string" ? data : data.toString();
    keyBuffer = Buffer.concat([
      keyBuffer,
      typeof data === "string" ? Buffer.from(data) : data,
    ]);

    if (keyBuffer[0] == 0x1b) {
      if (keyBuffer.length >= 3 && keyBuffer[1] == 0x5b) {
        if (keyBuffer[2] == 0x43) {
          const coinCount = getCoinCount();
          if (coinCount > 1) {
            const next = (getCoinIndex() + 1) % coinCount;
            setCoinIndex(next);
          }
          keyBuffer = Buffer.alloc(0);
          return;
        }
        if (keyBuffer[2] == 0x44) {
          const coinCount = getCoinCount();
          if (coinCount > 1) {
            const current = getCoinIndex();
            const next = current === 0 ? coinCount - 1 : current - 1;
            setCoinIndex(next);
          }
          keyBuffer = Buffer.alloc(0);
          return;
        }
        if (keyBuffer[2] == 0x42) {
          const profileCount = getProfileCount();
          if (profileCount > 1) {
            const next = Math.min(profileCount - 1, getProfileIndex() + 1);
            setProfileIndex(next);
          }
          keyBuffer = Buffer.alloc(0);
          return;
        }
        if (keyBuffer[2] == 0x41) {
          const profileCount = getProfileCount();
          if (profileCount > 1) {
            const next = Math.max(0, getProfileIndex() - 1);
            setProfileIndex(next);
          }
          keyBuffer = Buffer.alloc(0);
          return;
        }
      }
      if (keyBuffer.length > 10) {
        keyBuffer = Buffer.alloc(0);
      }
      return;
    }

    keyBuffer = Buffer.alloc(0);

    if (keyStr.toLowerCase() === "w" || keyStr === "k") {
      const profileCount = getProfileCount();
      if (profileCount > 1) {
        const next = Math.max(0, getProfileIndex() - 1);
        setProfileIndex(next);
      }
      return;
    }

    if (keyStr.toLowerCase() === "s" || keyStr === "j") {
      const profileCount = getProfileCount();
      if (profileCount > 1) {
        const next = Math.min(profileCount - 1, getProfileIndex() + 1);
        setProfileIndex(next);
      }
      return;
    }

    if (keyStr === "\x03") {
      process.emit("SIGINT", "SIGINT");
    }
  };

  const cleanup = () => {
    process.stdin.removeListener("data", handleData);
    if (process.stdin.isRaw) {
      process.stdin.setRawMode(false);
    }
  };

  if ((getProfileCount() > 1 || getCoinCount() > 1) && process.stdin.isTTY) {
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.setEncoding("utf-8");
    process.stdin.on("data", handleData);
  }

  return cleanup;
}

export async function fakeTradeRoute(): Promise<void> {
  return fakeTradeRouteWithOptions();
}

export async function fakeTradeRouteWithOptions(
  options: FakeTradeRouteOptions = {},
): Promise<void> {
  if (options.provider) {
    console.log(
      "Provider selection is ignored for arbitrage mode (uses Polymarket + Kalshi).",
    );
  }

  let profiles: ReturnType<typeof loadArbitrageConfig>["profiles"] = [];
  let coinOptions: CoinSymbol[] = [];
  let kalshiSelectorsByCoin: Map<CoinSymbol, KalshiCoinSelection> | undefined;
  let polyCoinOptions: CoinSymbol[] = [];
  let kalshiCoinOptions: CoinSymbol[] = [];
  try {
    const arbLoaded = loadArbitrageConfig();
    profiles = arbLoaded.profiles;
    coinOptions = arbLoaded.coinOptions;

    const polyLoaded = loadProviderConfig("polymarket");
    const kalshiLoaded = loadProviderConfig("kalshi");
    polyCoinOptions = polyLoaded.coinOptions;
    kalshiCoinOptions = kalshiLoaded.coinOptions;
    kalshiSelectorsByCoin = kalshiLoaded.kalshiSelectorsByCoin;
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load config.json.";
    console.log(message);
    return;
  }
  if (profiles.length == 0) {
    console.log("No arbitrage profiles found in config.json.");
    return;
  }

  const profileNames = profiles.map((profile) => profile.name);
  const profileLookup = new Map(
    profileNames.map((name) => [name.toLowerCase(), name]),
  );
  let selectedProfiles: string[] | null = null;

  if (options.profiles && options.profiles.length > 0) {
    const chosen = new Set<string>();
    for (const name of options.profiles) {
      const match = profileLookup.get(name.toLowerCase().trim());
      if (match) {
        chosen.add(match);
      } else {
        console.log(`Unknown profile: ${name}`);
      }
    }
    if (chosen.size > 0) {
      selectedProfiles = Array.from(chosen);
    }
  }

  if (
    !selectedProfiles &&
    options.profiles &&
    options.profiles.length > 0 &&
    !options.autoSelect
  ) {
    console.log("No valid profiles matched --profiles.");
    return;
  }

  if (!selectedProfiles && options.autoSelect) {
    selectedProfiles = profileNames;
  }

  if (!selectedProfiles) {
    if (!process.stdin.isTTY) {
      console.log("No profiles selected. Use --profiles or --auto.");
      return;
    }
    selectedProfiles = await selectMany(
      "Select profiles",
      profileNames.map((name) => ({ title: name, value: name })),
    );
    if (selectedProfiles.length === 0) {
      console.log("No profiles selected. Exiting.");
      return;
    }
  }
  const resolvedProfiles = selectedProfiles ?? [];

  const fallbackCoins: CoinSymbol[] = ["eth", "btc", "sol", "xrp"];
  const polySet = new Set(polyCoinOptions);
  const kalshiSet = new Set(kalshiCoinOptions);
  const intersection = coinOptions.filter(
    (coin) => polySet.has(coin) && kalshiSet.has(coin),
  );
  const coinsToSelect =
    intersection.length > 0
      ? intersection
      : coinOptions.length > 0
        ? coinOptions
        : fallbackCoins;
  let selectedCoins: CoinSymbol[] | null = null;

  if (options.coins && options.coins.length > 0) {
    const normalized = options.coins.map((coin) => coin.toLowerCase().trim());
    if (normalized.includes("all")) {
      selectedCoins = coinsToSelect;
    } else {
      selectedCoins = normalized
        .map((coin) => normalizeCoinKey(coin))
        .filter((coin): coin is CoinSymbol => !!coin);
    }
  }

  if (
    !selectedCoins &&
    options.coins &&
    options.coins.length > 0 &&
    !options.autoSelect
  ) {
    console.log("No valid coins matched --coins.");
    return;
  }

  if (!selectedCoins && options.autoSelect) {
    selectedCoins = coinsToSelect;
  }

  if (!selectedCoins) {
    if (!process.stdin.isTTY) {
      console.log("No coins selected. Use --coins or --auto.");
      return;
    }
    const selectedCoinsRaw = await selectMany(
      "Select coins",
      coinsToSelect.map((coin) => ({
        title: coin.toUpperCase(),
        value: coin.toUpperCase(),
      })),
    );
    if (selectedCoinsRaw.length === 0) {
      console.log("No coins selected. Exiting.");
      return;
    }
    selectedCoins = selectedCoinsRaw
      .map((coin) => normalizeCoinKey(coin))
      .filter((coin): coin is CoinSymbol => !!coin);
  }

  if (selectedCoins.length === 0) {
    console.log("No valid coins selected. Exiting.");
    return;
  }
  const resolvedCoins = selectedCoins ?? [];

  if (!kalshiSelectorsByCoin) {
    kalshiSelectorsByCoin = new Map<CoinSymbol, KalshiCoinSelection>();
  }

  const canPromptSelectors = process.stdin.isTTY && !options.headless;
  for (const coin of resolvedCoins) {
    const selection = ensureKalshiSelection(kalshiSelectorsByCoin, coin);
    if (selection.autoDiscover !== false) continue;
    if (hasKalshiSelection(selection)) continue;
    if (!canPromptSelectors) continue;

    const response = await promptText(
      `Enter Kalshi market URLs, tickers, or series for ${coin.toUpperCase()} (comma-separated). Prefix with series:/event:/market: to disambiguate, or leave blank to skip:`,
    );
    if (response && response.trim().length > 0) {
      applyKalshiSelectorInput(selection, response);
    }
  }

  const { runDir, runId } = getNextRunDir();
  const systemLogger = new RunLogger(join(runDir, "system.log"), 200, {
    stdout: options.headless === true,
  });
  const mismatchLogger = new RunLogger(join(runDir, "mismatch.log"), 200, {
    stdout: options.headless === true,
  });

  let kalshiConfig;
  try {
    kalshiConfig = getKalshiEnvConfig();
  } catch (error) {
    const message = error instanceof Error ? error.message : "Kalshi config error.";
    systemLogger.log(`STARTUP: ${message}`, "ERROR");
    console.log(message);
    return;
  }

  const polyHub = new MarketDataHub(systemLogger, { requireCryptoPrice: false });
  const kalshiHub = new KalshiMarketDataHub(
    systemLogger,
    kalshiConfig,
    kalshiSelectorsByCoin,
    { requireCryptoPrice: false },
  );

  try {
    await polyHub.start(resolvedCoins);
  } catch (err) {
    const msg = err instanceof Error ? err.message : "unknown";
    systemLogger.log(`STARTUP: Polymarket hub start failed: ${msg}`, "ERROR");
    // Continue -- partial data is better than no data
  }

  try {
    await kalshiHub.start(resolvedCoins);
  } catch (err) {
    const msg = err instanceof Error ? err.message : "unknown";
    systemLogger.log(`STARTUP: Kalshi hub start failed: ${msg}`, "ERROR");
    // Continue -- partial data is better than no data
  }

  // ── Startup health check ──────────────────────────────────────
  systemLogger.log(
    `STARTUP: profiles=[${resolvedProfiles.join(",")}] coins=[${resolvedCoins.join(",")}] headless=${options.headless ?? false}`,
  );
  for (const profile of profiles) {
    if (!resolvedProfiles.includes(profile.name)) continue;
    for (const coin of resolvedCoins) {
      const cfg = profile.coins.get(coin);
      if (cfg) {
        systemLogger.log(
          `STARTUP: ${profile.name}/${coin.toUpperCase()} minGap=${cfg.minGap} fillUsd=${cfg.fillUsd} tradeAllowed=${cfg.tradeAllowedTimeLeft}s tradeStop=${cfg.tradeStopTimeLeft ?? "null"}`,
        );
      }
    }
  }
  // Log initial market selections
  const polyInitialSnaps = polyHub.getSnapshots();
  const kalshiInitialSnaps = kalshiHub.getSnapshots();
  for (const coin of resolvedCoins) {
    const polySnap = polyInitialSnaps.get(coin);
    const kalshiSnap = kalshiInitialSnaps.get(coin);
    systemLogger.log(
      `STARTUP: ${coin.toUpperCase()} polyMarket=${polySnap?.slug ?? "pending"} kalshiMarket=${kalshiSnap?.slug ?? "pending"} polyThreshold=${polySnap?.priceToBeat ?? "n/a"} kalshiThreshold=${kalshiSnap?.priceToBeat ?? "n/a"}`,
    );
  }

  const kalshiOutcomeClient = new KalshiClient(kalshiConfig);
  const profileEngines: ArbitrageEngine[] = [];
  const profileCoinsByName = new Map<string, CoinSymbol[]>();
  const activeCoinIndexByProfile = new Map<string, number>();
  for (const profile of profiles) {
    if (!resolvedProfiles.includes(profile.name)) {
      continue;
    }

    const filtered = new Map<CoinSymbol, ArbitrageCoinConfig>();
    for (const coin of resolvedCoins) {
      const cfg = profile.coins.get(coin);
      if (cfg) {
        filtered.set(coin, cfg);
      }
    }
    const profileCoins = resolvedCoins.filter((coin) => filtered.has(coin));

    if (filtered.size === 0) {
      systemLogger.log(
        `Profile ${profile.name} has no configs for selected coins, skipping.`,
        "WARN",
      );
      continue;
    }

    const profileLogName = `${sanitizeProfileName(profile.name)}.log`;
    const profileLogger = new RunLogger(join(runDir, profileLogName), 200, {
      stdout: options.headless === true,
    });
    profileEngines.push(
      new ArbitrageEngine(profile.name, filtered, profileLogger, {
        kalshiOutcomeClient,
        mismatchLogger,
        headlessSummary: options.headless === true,
      }),
    );
    profileCoinsByName.set(profile.name, profileCoins);
    if (!activeCoinIndexByProfile.has(profile.name)) {
      activeCoinIndexByProfile.set(profile.name, 0);
    }
  }

  const polyOddsHistoryByCoin = new Map<CoinSymbol, number[]>();
  const kalshiOddsHistoryByCoin = new Map<CoinSymbol, number[]>();
  for (const coin of resolvedCoins) {
    polyOddsHistoryByCoin.set(coin, []);
    kalshiOddsHistoryByCoin.set(coin, []);
  }

  if (profileEngines.length === 0) {
    systemLogger.log("No profiles eligible for selected coins.", "WARN");
    polyHub.stop();
    kalshiHub.stop();
    return;
  }

  const dashboard = options.headless ? null : new ArbitrageDashboard();
  let activeProfileIndex = 0;

  const cleanupNavigation = dashboard
    ? setupProfileAndCoinNavigation(
        () => profileEngines.length,
        () => activeProfileIndex,
        (nextIndex) => {
          if (profileEngines.length === 0) return;
          const clamped = Math.max(
            0,
            Math.min(nextIndex, profileEngines.length - 1),
          );
          activeProfileIndex = clamped;
          const profileName = profileEngines[activeProfileIndex]?.getName();
          if (!profileName) return;
          const coins = profileCoinsByName.get(profileName) ?? [];
          const current = activeCoinIndexByProfile.get(profileName) ?? 0;
          if (coins.length === 0) {
            activeCoinIndexByProfile.set(profileName, 0);
          } else if (current < 0 || current >= coins.length) {
            activeCoinIndexByProfile.set(profileName, 0);
          }
        },
        () => {
          const profileName = profileEngines[activeProfileIndex]?.getName();
          const coins = profileName ? profileCoinsByName.get(profileName) ?? [] : [];
          return coins.length;
        },
        () => {
          const profileName = profileEngines[activeProfileIndex]?.getName();
          if (!profileName) return 0;
          const coins = profileCoinsByName.get(profileName) ?? [];
          const stored = activeCoinIndexByProfile.get(profileName) ?? 0;
          if (coins.length === 0) return 0;
          return Math.max(0, Math.min(stored, coins.length - 1));
        },
        (nextIndex) => {
          const profileName = profileEngines[activeProfileIndex]?.getName();
          if (!profileName) return;
          const coins = profileCoinsByName.get(profileName) ?? [];
          if (coins.length === 0) {
            activeCoinIndexByProfile.set(profileName, 0);
            return;
          }
          const clamped = Math.max(0, Math.min(nextIndex, coins.length - 1));
          activeCoinIndexByProfile.set(profileName, clamped);
        },
      )
    : () => {};

  // ── Evaluation loop ─────────────────────────────────────────────
  // Default 10ms is fast enough to catch arb opportunities (market data
  // arrives at ~50-100ms granularity via WS) while being much more
  // CPU-friendly than the previous 1ms interval.
  const ARB_EVAL_INTERVAL_MS = Math.max(
    1,
    Number(process.env.ARB_EVAL_INTERVAL_MS) || 10,
  );
  const evalTimer = setInterval(() => {
    try {
      const polySnapshots = polyHub.getSnapshots();
      const kalshiSnapshots = kalshiHub.getSnapshots();
      const now = Date.now();
      for (const engine of profileEngines) {
        engine.evaluate(polySnapshots, kalshiSnapshots, now);
      }
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown error in eval loop.";
      systemLogger.log(`Arbitrage eval error: ${message}`, "ERROR");
    }
  }, ARB_EVAL_INTERVAL_MS);

  // ── Render loop (250ms) ────────────────────────────────────────
  // Dashboard rendering and odds history are visual-only; 4 fps for snappy UI.
  const renderTimer = setInterval(() => {
    try {
      const polySnapshots = polyHub.getSnapshots();
      const kalshiSnapshots = kalshiHub.getSnapshots();
      for (const coin of resolvedCoins) {
        const polySnap = polySnapshots.get(coin);
        if (polySnap) {
          const upTokenId = polySnap.upTokenId;
          const bid = upTokenId ? polySnap.bestBid.get(upTokenId) ?? null : null;
          const ask = upTokenId ? polySnap.bestAsk.get(upTokenId) ?? null : null;
          const mid = computeOddsMid(bid ?? null, ask ?? null);
          if (mid != null) {
            const arr = polyOddsHistoryByCoin.get(coin);
            if (arr) {
              arr.push(mid);
              if (arr.length > ODDS_HISTORY_LIMIT) arr.shift();
            }
          }
        }

        const kalshiSnap = kalshiSnapshots.get(coin);
        if (kalshiSnap) {
          const arr = kalshiOddsHistoryByCoin.get(coin);
          if (arr) {
            if (
              arr.length === 0 &&
              kalshiSnap.kalshiMarketPriceHistory &&
              kalshiSnap.kalshiMarketPriceHistory.length > 0
            ) {
              arr.push(
                ...kalshiSnap.kalshiMarketPriceHistory.slice(-ODDS_HISTORY_LIMIT),
              );
            }
            const odds =
              kalshiSnap.kalshiMarketPrice ?? kalshiSnap.kalshiLastPrice ?? null;
            if (odds != null) {
              const last = arr[arr.length - 1];
              if (last !== odds) {
                arr.push(odds);
                if (arr.length > ODDS_HISTORY_LIMIT) arr.shift();
              }
            }
          }
        }
      }

      if (!dashboard) {
        return;
      }

      const profileViews = profileEngines.map((engine) => ({
        name: engine.getName(),
        summary: engine.getSummary(),
        markets: engine.getMarketViews(),
        logs: engine.getLogs(),
        pnlHistory: engine.getPnlHistory(),
      }));

      const activeProfile = profileViews[activeProfileIndex];
      if (!activeProfile) {
        return;
      }

      const activeProfileName = activeProfile.name;
      const profileCoins = profileCoinsByName.get(activeProfileName) ?? [];
      const storedIndex = activeCoinIndexByProfile.get(activeProfileName) ?? 0;
      const safeIndex =
        profileCoins.length > 0
          ? Math.max(0, Math.min(storedIndex, profileCoins.length - 1))
          : 0;
      if (storedIndex !== safeIndex) {
        activeCoinIndexByProfile.set(activeProfileName, safeIndex);
      }
      const activeCoin =
        profileCoins[safeIndex] ??
        activeProfile.markets[0]?.coin ??
        resolvedCoins[0] ??
        null;

      dashboard.update({
        runId,
        modeLabel: "Arbitrage Bot (paper)",
        activeProfileIndex,
        profiles: profileViews,
        coins: profileCoins,
        activeCoinIndex: safeIndex,
        activeCoin,
        polySnapshots,
        kalshiSnapshots,
        polyOddsHistoryByCoin,
        kalshiOddsHistoryByCoin,
      });
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown error in loop.";
      systemLogger.log(`Arbitrage loop error: ${message}`, "ERROR");
    }
  }, 250);

  process.on("SIGINT", () => {
    clearInterval(evalTimer);
    clearInterval(renderTimer);
    cleanupNavigation();

    // Log final summary before shutdown
    for (const engine of profileEngines) {
      const summary = engine.getSummary();
      systemLogger.log(
        `SHUTDOWN: ${engine.getName()} trades=${summary.totalTrades} wins=${summary.wins} losses=${summary.losses} profit=${summary.totalProfit.toFixed(2)} runtime=${summary.runtimeSec.toFixed(0)}s`,
      );
    }
    systemLogger.log("SHUTDOWN: graceful exit");

    polyHub.stop();
    kalshiHub.stop();
    process.exit(0);
  });
}
