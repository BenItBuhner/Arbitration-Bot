import { join } from "path";
import type { CoinSymbol } from "../services/auto-market";
import { loadProfilesFromConfig, sanitizeProfileName } from "../services/profile-config";
import { RunLogger } from "../services/run-logger";
import { ProfileEngine } from "../services/profile-engine";
import type { TimedTradeConfig } from "../services/profile-engine";
import { BacktestHub } from "./backtest-hub";
import { BacktestRunner } from "./backtest-runner";
import { loadBacktestData } from "../routes/backtest";

type CoinWorkerRequest = {
  coin: CoinSymbol;
  dataDir: string;
  startMs: number;
  endMs: number;
  runDir: string;
  selectedProfiles: string[];
  latencyMs: number;
};

type CoinWorkerResponse =
  | {
      ok: true;
      coin: CoinSymbol;
      summaries: Array<{
        name: string;
        summary: ReturnType<ProfileEngine["getSummary"]>;
      }>;
    }
  | {
      ok: false;
      coin: CoinSymbol;
      error: string;
    };

const ctx: any = self as any;

ctx.onmessage = async (event: MessageEvent<CoinWorkerRequest>) => {
  const { coin, dataDir, startMs, endMs, runDir, selectedProfiles, latencyMs } =
    event.data;

  try {
    const loaded = loadProfilesFromConfig();
    const profiles = loaded.profiles.filter((profile) =>
      selectedProfiles.includes(profile.name),
    );

    if (profiles.length === 0) {
      const response: CoinWorkerResponse = {
        ok: false,
        coin,
        error: "No profiles available for this worker.",
      };
      ctx.postMessage(response);
      return;
    }

    const data = await loadBacktestData(dataDir, [coin], startMs, endMs);

    const hub = new BacktestHub({
      marketsByCoin: data.marketsByCoin,
      tradeFilesBySlug: data.tradeFilesBySlug,
      cryptoTickFilesByCoin: data.cryptoTickFilesByCoin,
      tradeRangesBySlug: data.tradeRangesBySlug,
      tickRangesByCoin: data.tickRangesByCoin,
      latencyMs,
    });

    const profileEngines: ProfileEngine[] = [];
    for (const profile of profiles) {
      const filtered = new Map<CoinSymbol, TimedTradeConfig>();
      const cfg = profile.configs.get(coin);
      if (cfg) {
        filtered.set(coin, cfg);
      }
      if (filtered.size === 0) continue;

      const profileLogName = `${sanitizeProfileName(profile.name)}-${coin}.log`;
      const profileLogger = new RunLogger(join(runDir, profileLogName));
      profileEngines.push(
        new ProfileEngine(profile.name, filtered, profileLogger, hub.getStartTimeMs(), {
          advancedSignals: true,
          decisionLatencyMs: 250,
          crossDebug: true,
          crossAllowNoFlip: true,
        }),
      );
    }

    if (profileEngines.length === 0) {
      const response: CoinWorkerResponse = {
        ok: false,
        coin,
        error: "No profile configs match this coin.",
      };
      ctx.postMessage(response);
      return;
    }

    const dashboard = { update: () => {} } as any;
    const runner = new BacktestRunner(hub, profileEngines, dashboard, {
      speed: 0,
      runId: `${coin}-worker`,
      modeLabel: "Backtest Fast Mode (worker)",
      activeProfileIndex: () => 0,
      setActiveProfileIndex: () => {},
      selectedCoins: [coin],
      render: false,
      headless: true,
    });

    runner.start();
    hub.close();

    const summaries = profileEngines.map((engine) => ({
      name: engine.getName(),
      summary: engine.getSummary(),
    }));

    const response: CoinWorkerResponse = { ok: true, coin, summaries };
    ctx.postMessage(response);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Coin worker failed.";
    const response: CoinWorkerResponse = { ok: false, coin, error: message };
    ctx.postMessage(response);
  }
};
