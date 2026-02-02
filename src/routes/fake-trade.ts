import { existsSync } from "fs";
import { join } from "path";
import { MarketDataHub } from "../services/market-data-hub";
import {
  ProfileEngine,
  type TimedTradeConfig,
} from "../services/profile-engine";
import { RunLogger } from "../services/run-logger";
import { ProfileDashboard } from "../cli/profile-dashboard";
import type { CoinSymbol } from "../services/auto-market";
import { selectMany } from "../cli/prompts";
import {
  loadProfilesFromConfig,
  normalizeCoinKey,
  sanitizeProfileName,
  type ProfileDefinition,
} from "../services/profile-config";

export interface FakeTradeRouteOptions {
  profiles?: string[];
  coins?: string[];
  autoSelect?: boolean;
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
  let profiles: ProfileDefinition[] = [];
  let coinOptions: CoinSymbol[] = [];
  try {
    const loaded = loadProfilesFromConfig();
    profiles = loaded.profiles;
    coinOptions = loaded.coinOptions;
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

  const coinsToSelect =
    coinOptions.length > 0 ? coinOptions : ["eth", "btc", "sol", "xrp"];
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

  const { runDir, runId } = getNextRunDir();
  const systemLogger = new RunLogger(join(runDir, "system.log"));
  const hub = new MarketDataHub(systemLogger);
  await hub.start(selectedCoins);

  const profileEngines: ProfileEngine[] = [];
  for (const profile of profiles) {
    if (!selectedProfiles.includes(profile.name)) {
      continue;
    }

    const filtered = new Map<CoinSymbol, TimedTradeConfig>();
    for (const coin of selectedCoins) {
      const cfg = profile.configs.get(coin);
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
      }),
    );
  }

  if (profileEngines.length === 0) {
    systemLogger.log("No profiles eligible for selected coins.", "WARN");
    hub.stop();
    return;
  }

  const dashboard = new ProfileDashboard();
  let activeProfileIndex = 0;

  const cleanupNavigation = setupProfileNavigation(
    profileEngines.length,
    () => activeProfileIndex,
    (nextIndex) => {
      activeProfileIndex = nextIndex;
    },
  );

  const renderTimer = setInterval(() => {
    const snapshots = hub.getSnapshots();
    for (const engine of profileEngines) {
      engine.evaluate(snapshots, Date.now());
    }

    const profileViews = profileEngines.map((engine) => ({
      name: engine.getName(),
      summary: engine.getSummary(),
      markets: engine.getMarketViews(),
      logs: engine.getLogs(),
      pnlHistory: engine.getPnlHistory(),
    }));

    const activeProfile = profileViews[activeProfileIndex];
    const activeCoin =
      activeProfile && activeProfile.markets.length > 0
        ? activeProfile.markets[0].coin
        : selectedCoins[0] || null;
    const activeCoinHistory = activeCoin
      ? snapshots.get(activeCoin)?.priceHistory || []
      : [];

    dashboard.update({
      runId,
      activeProfileIndex,
      profiles: profileViews,
      activeCoin,
      activeCoinPriceHistory: activeCoinHistory,
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
