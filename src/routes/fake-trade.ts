import { existsSync } from "fs";
import { join } from "path";
import { MarketDataHub } from "../services/market-data-hub";
import { KalshiMarketDataHub } from "../services/kalshi-market-data-hub";
import {
  ProfileEngine,
  type TimedTradeConfig,
} from "../services/profile-engine";
import { RunLogger } from "../services/run-logger";
import { ProfileDashboard } from "../cli/profile-dashboard";
import type { CoinSymbol } from "../services/auto-market";
import { promptText, selectMany, selectOne } from "../cli/prompts";
import {
  loadProviderConfig,
  resolveProfileConfigForCoin,
  normalizeCoinKey,
  sanitizeProfileName,
  type ProviderProfileDefinition,
  type KalshiCoinSelection,
} from "../services/profile-config";
import { resolveMarketProvider, type MarketProvider } from "../providers/provider";
import { resolveMarketGroup, type MarketGroupDefinition } from "../services/market-groups";
import { getKalshiEnvConfig } from "../clients/kalshi/kalshi-config";
import {
  deriveSeriesTickerFromMarket,
  looksLikeKalshiMarketTicker,
  normalizeKalshiTicker,
  parseKalshiMarketUrl,
} from "../clients/kalshi/kalshi-url";

export interface FakeTradeRouteOptions {
  profiles?: string[];
  coins?: string[];
  autoSelect?: boolean;
  provider?: MarketProvider;
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

function setupProfileNavigation(
  profileCount: number,
  getIndex: () => number,
  setIndex: (nextIndex: number) => void,
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
          const next = (getIndex() + 1) % profileCount;
          setIndex(next);
          keyBuffer = Buffer.alloc(0);
          return;
        }
        if (keyBuffer[2] == 0x44) {
          const current = getIndex();
          const next = current === 0 ? profileCount - 1 : current - 1;
          setIndex(next);
          keyBuffer = Buffer.alloc(0);
          return;
        }
        if (keyBuffer[2] == 0x42) {
          const next = Math.min(profileCount - 1, getIndex() + 1);
          setIndex(next);
          keyBuffer = Buffer.alloc(0);
          return;
        }
        if (keyBuffer[2] == 0x41) {
          const next = Math.max(0, getIndex() - 1);
          setIndex(next);
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
      const next = Math.max(0, getIndex() - 1);
      setIndex(next);
      return;
    }

    if (keyStr.toLowerCase() === "s" || keyStr === "j") {
      const next = Math.min(profileCount - 1, getIndex() + 1);
      setIndex(next);
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

  if (profileCount > 1) {
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
  let provider: MarketProvider = options.provider ?? resolveMarketProvider();
  const envProvider = process.env.MARKET_PROVIDER?.trim();
  if (!envProvider && process.stdin.isTTY && !options.provider && !options.headless) {
    const selected = await selectOne<MarketProvider>(
      "Select provider",
      [
        { title: "Polymarket", value: "polymarket" },
        { title: "Kalshi", value: "kalshi" },
      ],
      provider === "kalshi" ? 1 : 0,
    );
    if (!selected) {
      console.log("No provider selected. Exiting.");
      return;
    }
    provider = selected;
  }

  let profiles: ProviderProfileDefinition[] = [];
  let coinOptions: CoinSymbol[] = [];
  let marketGroups: MarketGroupDefinition[] = [];
  let kalshiSelectorsByCoin: Map<CoinSymbol, KalshiCoinSelection> | undefined;
  try {
    const loaded = loadProviderConfig(provider);
    profiles = loaded.profiles;
    coinOptions = loaded.coinOptions;
    marketGroups = loaded.marketGroups;
    kalshiSelectorsByCoin = loaded.kalshiSelectorsByCoin;
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load config.json.";
    console.log(message);
    return;
  }
  if (profiles.length == 0) {
    console.log("No profiles found in config.json.");
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

  const fallbackCoins: CoinSymbol[] = ["eth", "btc", "sol", "xrp"];
  const coinsToSelect = coinOptions.length > 0 ? coinOptions : fallbackCoins;
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

  if (provider === "kalshi" && kalshiSelectorsByCoin) {
    const canPromptSelectors = process.stdin.isTTY && !options.headless;
    for (const coin of selectedCoins) {
      const selection = ensureKalshiSelection(kalshiSelectorsByCoin, coin);
      if (hasKalshiSelection(selection)) continue;
      if (!canPromptSelectors) continue;

      const response = await promptText(
        `Enter Kalshi market URLs, tickers, or series for ${coin.toUpperCase()} (comma-separated). Prefix with series:/event:/market: to disambiguate, or leave blank to skip:`,
      );
      if (response && response.trim().length > 0) {
        applyKalshiSelectorInput(selection, response);
      }
    }

    const filteredCoins = selectedCoins.filter((coin) => {
      const selection = kalshiSelectorsByCoin?.get(coin);
      if (!hasKalshiSelection(selection)) {
        console.log(
          `No Kalshi market selectors configured for ${coin.toUpperCase()}, skipping.`,
        );
        return false;
      }
      return true;
    });
    if (filteredCoins.length === 0) {
      console.log("No Kalshi coins have market selectors configured. Exiting.");
      return;
    }
    selectedCoins = filteredCoins;
  }

  const { runDir, runId } = getNextRunDir();
  const systemLogger = new RunLogger(join(runDir, "system.log"));

  let hub: MarketDataHub | KalshiMarketDataHub;
  if (provider === "kalshi") {
    if (!kalshiSelectorsByCoin) {
      console.log("Kalshi market selectors are missing from config.json.");
      return;
    }
    let kalshiConfig;
    try {
      kalshiConfig = getKalshiEnvConfig();
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Kalshi config error.";
      console.log(message);
      return;
    }
    hub = new KalshiMarketDataHub(
      systemLogger,
      kalshiConfig,
      kalshiSelectorsByCoin,
    );
  } else {
    hub = new MarketDataHub(systemLogger);
  }

  await hub.start(selectedCoins);

  const profileEngines: ProfileEngine[] = [];
  for (const profile of profiles) {
    if (!selectedProfiles.includes(profile.name)) {
      continue;
    }

    const filtered = new Map<CoinSymbol, TimedTradeConfig>();
    for (const coin of selectedCoins) {
      const cfg = resolveProfileConfigForCoin(profile, coin, "default");
      if (cfg) {
        filtered.set(coin, cfg);
      }
    }

    if (filtered.size === 0) {
      systemLogger.log(
        `Profile ${profile.name} has no configs for selected coins, skipping.`,
        "WARN",
      );
      continue;
    }

    const profileLogName = `${sanitizeProfileName(profile.name)}.log`;
    const profileLogger = new RunLogger(join(runDir, profileLogName));
    profileEngines.push(
      new ProfileEngine(profile.name, filtered, profileLogger, undefined, {
        advancedSignals: true,
        signalDebug: true,
        configResolver: (coin, snapshot) => {
          const groupId = resolveMarketGroup(provider, snapshot, marketGroups);
          return resolveProfileConfigForCoin(profile, coin, groupId);
        },
      }),
    );
  }

  if (profileEngines.length === 0) {
    systemLogger.log("No profiles eligible for selected coins.", "WARN");
    hub.stop();
    return;
  }

  const dashboard = options.headless ? null : new ProfileDashboard();
  let activeProfileIndex = 0;

  const cleanupNavigation = dashboard
    ? setupProfileNavigation(
        profileEngines.length,
        () => activeProfileIndex,
        (nextIndex) => {
          activeProfileIndex = nextIndex;
        },
      )
    : () => {};

  const renderTimer = setInterval(() => {
    const snapshots = hub.getSnapshots();
    for (const engine of profileEngines) {
      engine.evaluate(snapshots, Date.now());
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
    const activeCoin =
      activeProfile.markets.length > 0
        ? activeProfile.markets[0]?.coin ?? null
        : selectedCoins[0] || null;
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

    dashboard.update({
      runId,
      activeProfileIndex,
      profiles: profileViews,
      activeCoin,
      activeCoinPriceHistory: activeCoinHistory ?? [],
      activeCoinPriceLabel,
      useCandleGraph: true,
    });
  }, 250);

  process.on("SIGINT", () => {
    clearInterval(renderTimer);
    cleanupNavigation();
    hub.stop();
    process.exit(0);
  });
}
