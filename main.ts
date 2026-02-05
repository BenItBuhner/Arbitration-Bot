// poly-saturate-bot\cli.ts

import { watchMarketRoute } from "./src/routes/watch-market";
import { fakeTradeRouteWithOptions } from "./src/routes/fake-trade";
import { crossPlatformAnalysisRoute } from "./src/routes/cross-platform-analysis";
import { priceDiffDetectionRoute } from "./src/routes/price-diff-detection";
import { backtestRoute } from "./src/routes/backtest";
import { selectOne } from "./src/cli/prompts";

type CoinSymbol = "eth" | "btc" | "sol" | "xrp";
const COINS: CoinSymbol[] = ["eth", "btc", "sol", "xrp"];
function normalizeCoins(values: string[] | undefined): CoinSymbol[] | undefined {
  if (!values || values.length === 0) return undefined;
  const out = values
    .map((v) => v.trim().toLowerCase())
    .filter((v) => COINS.includes(v as CoinSymbol)) as CoinSymbol[];
  return out.length > 0 ? out : undefined;
}

type CLIMode =
  | "fake-trade"
  | "watch-market"
  | "cross-platform-analysis"
  | "price-diff-detection"
  | "backtest";
type Provider = "polymarket" | "kalshi";

interface CLIArgs {
  mode?: CLIMode;
  profiles?: string[];
  coins?: string[];
  market?: string;
  auto?: boolean;
  provider?: Provider;
  speed?: number;
  dataDir?: string;
  start?: string;
  end?: string;
  backtestMode?: "fast" | "visual";
  headless?: boolean;
  headlessSummary?: boolean;
  realisticFill?: boolean;
  fillUsd?: number;
  help?: boolean;
  validate?: boolean;
}

function normalizeMode(value: string | undefined): CLIMode | undefined {
  if (!value) return undefined;
  const normalized = value.toLowerCase().replace(/_/g, "-");
  if (
    normalized === "fake-trade" ||
    normalized === "fake" ||
    normalized === "arbitrage" ||
    normalized === "arb"
  )
    return "fake-trade";
  if (normalized === "watch-market" || normalized === "watch") return "watch-market";
  if (normalized === "cross-platform-analysis" || normalized === "cross-platform" || normalized === "outcome-analysis" || normalized === "analysis") return "cross-platform-analysis";
  if (normalized === "price-diff-detection" || normalized === "price-diff" || normalized === "diff") return "price-diff-detection";
  if (normalized === "backtest" || normalized === "historical") return "backtest";
  return undefined;
}

function splitList(value: string | undefined): string[] {
  if (!value) return [];
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function normalizeBacktestMode(
  value: string | undefined,
): "fast" | "visual" | undefined {
  if (!value) return undefined;
  const normalized = value.toLowerCase().trim();
  if (normalized === "fast" || normalized === "max") return "fast";
  if (normalized === "visual" || normalized === "slow" || normalized === "debug") {
    return "visual";
  }
  return undefined;
}

function normalizeProvider(value: string | undefined): Provider | undefined {
  if (!value) return undefined;
  const normalized = value.toLowerCase().trim();
  if (normalized === "polymarket" || normalized === "poly") return "polymarket";
  if (normalized === "kalshi") return "kalshi";
  return undefined;
}

function parseArgs(argv: string[]): CLIArgs {
  const args: CLIArgs = { profiles: [], coins: [] };

  for (let i = 0; i < argv.length; i++) {
    const raw = argv[i] ?? "";
    if (!raw) continue;

    if (raw === "--help" || raw === "-h") {
      args.help = true;
      continue;
    }

    if (raw === "--auto") {
      args.auto = true;
      continue;
    }

    if (raw === "--fake-trade") {
      args.mode = "fake-trade";
      continue;
    }

    if (raw === "--watch-market" || raw === "--watch") {
      args.mode = "watch-market";
      continue;
    }

    if (raw === "--cross-platform-analysis" || raw === "--cross-platform" || raw === "--outcome-analysis" || raw === "--analysis") {
      args.mode = "cross-platform-analysis";
      continue;
    }

    if (raw === "--price-diff-detection" || raw === "--price-diff" || raw === "--diff") {
      args.mode = "price-diff-detection";
      continue;
    }

    if (raw === "--backtest") {
      args.mode = "backtest";
      continue;
    }

    if (raw === "--fast") {
      args.backtestMode = "fast";
      continue;
    }

    if (raw === "--visual") {
      args.backtestMode = "visual";
      continue;
    }

    if (raw === "--headless") {
      args.headless = true;
      continue;
    }

    if (raw === "--validate" || raw === "--check") {
      args.validate = true;
      continue;
    }

    if (
      raw === "--headless-summary" ||
      raw === "--summary-logs" ||
      raw === "--summary-log"
    ) {
      args.headlessSummary = true;
      args.headless = true;
      continue;
    }

    if (raw === "--realistic-fill") {
      args.realisticFill = true;
      continue;
    }

    if (raw === "--no-realistic-fill") {
      args.realisticFill = false;
      continue;
    }

    if (raw === "--kalshi") {
      args.provider = "kalshi";
      continue;
    }

    if (raw === "--polymarket" || raw === "--poly") {
      args.provider = "polymarket";
      continue;
    }

    if (raw.startsWith("--provider=") || raw.startsWith("--platform=")) {
      const value = raw.includes("=") ? raw.split("=").slice(1).join("=") : "";
      args.provider = normalizeProvider(value);
      continue;
    }

    if (raw === "--provider" || raw === "--platform") {
      args.provider = normalizeProvider(argv[i + 1]);
      i += 1;
      continue;
    }

    if (raw.startsWith("--backtest-mode=")) {
      args.backtestMode = normalizeBacktestMode(
        raw.slice("--backtest-mode=".length),
      );
      continue;
    }

    if (raw === "--backtest-mode") {
      args.backtestMode = normalizeBacktestMode(argv[i + 1]);
      i += 1;
      continue;
    }

    if (raw.startsWith("--mode=")) {
      args.mode = normalizeMode(raw.slice("--mode=".length));
      continue;
    }

    if (raw === "--mode") {
      args.mode = normalizeMode(argv[i + 1]);
      i += 1;
      continue;
    }

    if (raw.startsWith("--profiles=")) {
      args.profiles?.push(...splitList(raw.slice("--profiles=".length)));
      continue;
    }

    if (raw === "--profiles") {
      args.profiles?.push(...splitList(argv[i + 1]));
      i += 1;
      continue;
    }

    if (raw.startsWith("--coins=")) {
      args.coins?.push(...splitList(raw.slice("--coins=".length)));
      continue;
    }

    if (raw === "--coins") {
      args.coins?.push(...splitList(argv[i + 1]));
      i += 1;
      continue;
    }

    if (raw.startsWith("--market=")) {
      args.market = raw.slice("--market=".length).trim();
      continue;
    }

    if (raw === "--market" || raw === "--query" || raw === "--search") {
      args.market = (argv[i + 1] ?? "").trim();
      i += 1;
      continue;
    }

    if (raw.startsWith("--speed=")) {
      const value = raw.slice("--speed=".length).trim();
      args.speed = value === "max" ? 0 : Number(value);
      continue;
    }

    if (raw === "--speed") {
      const value = (argv[i + 1] ?? "").trim();
      args.speed = value === "max" ? 0 : Number(value);
      i += 1;
      continue;
    }

    if (raw.startsWith("--fill-usd=")) {
      const value = raw.slice("--fill-usd=".length).trim();
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        args.fillUsd = parsed;
      }
      continue;
    }

    if (raw === "--fill-usd") {
      const value = (argv[i + 1] ?? "").trim();
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        args.fillUsd = parsed;
      }
      i += 1;
      continue;
    }

    if (raw.startsWith("--data-dir=")) {
      args.dataDir = raw.slice("--data-dir=".length).trim();
      continue;
    }

    if (raw === "--data-dir") {
      args.dataDir = (argv[i + 1] ?? "").trim();
      i += 1;
      continue;
    }

    if (raw.startsWith("--start=")) {
      args.start = raw.slice("--start=".length).trim();
      continue;
    }

    if (raw === "--start") {
      args.start = (argv[i + 1] ?? "").trim();
      i += 1;
      continue;
    }

    if (raw.startsWith("--end=")) {
      args.end = raw.slice("--end=".length).trim();
      continue;
    }

    if (raw === "--end") {
      args.end = (argv[i + 1] ?? "").trim();
      i += 1;
      continue;
    }

    if (!raw.startsWith("-") && !args.market) {
      args.market = raw.trim();
    }
  }

  if (args.profiles && args.profiles.length === 0) {
    delete args.profiles;
  }
  if (args.coins && args.coins.length === 0) {
    delete args.coins;
  }

  return args;
}

function parseTime(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return numeric;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? undefined : parsed;
}

function printUsage(): void {
  const lines = [
    "Usage:",
    "  bun run main.ts",
    "  bun run main.ts -- --mode fake-trade --profiles arbPaperV1 --coins eth,btc",
    "  bun run main.ts -- --mode fake-trade --auto",
    "  bun run main.ts -- --mode watch-market --market \"https://polymarket.com/event/...\"",
    "  bun run main.ts -- --mode cross-platform-analysis",
    "  bun run main.ts -- --mode price-diff-detection",
    "  bun run main.ts -- --mode backtest --auto --data-dir backtest-data --speed max",
    "  bun run main.ts -- --mode backtest --auto --backtest-mode fast",
    "",
    "Flags:",
    "  --mode <fake-trade|watch-market|cross-platform-analysis|price-diff-detection|backtest>",
    "  --fake-trade | --watch-market | --cross-platform-analysis | --price-diff-detection | --backtest",
    "  --profiles <name1,name2>   (arbitrage bot)",
    "  --coins <eth,btc,sol,xrp>  (arbitrage bot, cross-platform-analysis, price-diff-detection)",
    "  --auto                     (arbitrage bot: select all profiles/coins)",
    "  --provider <polymarket|kalshi> (watch-market)",
    "  --kalshi | --polymarket    (provider shortcut)",
    "  --market <keyword|url>     (watch-market)",
    "  --data-dir <path>          (backtest)",
    "  --speed <n|max>            (backtest)",
    "  --backtest-mode <fast|visual> (backtest)",
    "  --fast                    (backtest alias for fast)",
    "  --visual                  (backtest alias for visual)",
    "  --headless                (backtest/arbitrage/cross-platform-analysis/price-diff-detection: disable dashboard UI)",
    "  --headless-summary        (cross-platform-analysis/price-diff-detection: headless + concise summary logs)",
    "  --realistic-fill          (price-diff-detection: book-walk fill simulation)",
    "  --no-realistic-fill       (price-diff-detection: disable fill simulation)",
    "  --fill-usd <amount>       (price-diff-detection: USD budget for fill simulation)",
    "  --start <iso|ms>           (backtest)",
    "  --end <iso|ms>             (backtest)",
    "  --validate               (check config, env vars, and API connectivity)",
    "  --help",
  ];
  console.log(lines.join("\n"));
}

const menuChoices: Array<{ title: string; value: CLIMode }> = [
  { title: "Start arbitrage bot (paper)", value: "fake-trade" },
  { title: "Start cross-platform outcome analyzation", value: "cross-platform-analysis" },
  { title: "Price diff detection", value: "price-diff-detection" },
  { title: "Watch market", value: "watch-market" },
  { title: "Backtest (historical)", value: "backtest" },
];

async function promptMainMenu(): Promise<CLIMode | null> {
  return selectOne("Select a mode", menuChoices, 0);
}

const argv = process.argv.slice(2);
const cliArgs = parseArgs(argv);
if (cliArgs.provider) {
  process.env.MARKET_PROVIDER = cliArgs.provider;
}

async function runValidation(): Promise<void> {
  console.log("=== Arbitrage Bot Validation ===\n");
  let issues = 0;
  let warnings = 0;

  // 1. Config file
  try {
    const { loadArbitrageConfig } = await import("./src/services/arbitrage-config");
    const config = loadArbitrageConfig();
    console.log(`[OK] config.json: ${config.profiles.length} profile(s), coins: ${config.coinOptions.join(", ")}`);
    for (const profile of config.profiles) {
      const coins = Array.from(profile.coins.keys());
      console.log(`     Profile "${profile.name}": ${coins.join(", ")}`);
      for (const [coin, cfg] of profile.coins) {
        if (cfg.fillUsd && cfg.fillUsd > cfg.maxSpendTotal) {
          console.log(`  [WARN] ${profile.name}/${coin}: fillUsd (${cfg.fillUsd}) > maxSpendTotal (${cfg.maxSpendTotal})`);
          warnings++;
        }
      }
    }
  } catch (err) {
    console.log(`[FAIL] config.json: ${err instanceof Error ? err.message : "load failed"}`);
    issues++;
  }

  // 2. Provider configs
  try {
    const { loadProviderConfig } = await import("./src/services/profile-config");
    const poly = loadProviderConfig("polymarket");
    console.log(`[OK] Polymarket config: coins=${poly.coinOptions.join(", ")}`);
  } catch (err) {
    console.log(`[FAIL] Polymarket config: ${err instanceof Error ? err.message : "load failed"}`);
    issues++;
  }

  try {
    const { loadProviderConfig } = await import("./src/services/profile-config");
    const kalshi = loadProviderConfig("kalshi");
    console.log(`[OK] Kalshi config: coins=${kalshi.coinOptions.join(", ")}, selectors=${kalshi.kalshiSelectorsByCoin?.size ?? 0}`);
  } catch (err) {
    console.log(`[FAIL] Kalshi config: ${err instanceof Error ? err.message : "load failed"}`);
    issues++;
  }

  // 3. Environment variables
  const kalshiKey = process.env.KALSHI_API_KEY;
  const kalshiKeyPath = process.env.KALSHI_PRIVATE_KEY_PATH;
  const kalshiKeyPem = process.env.KALSHI_PRIVATE_KEY_PEM;
  const kalshiEnv = process.env.KALSHI_ENV || "demo";

  if (kalshiKey) {
    console.log(`[OK] KALSHI_API_KEY: set (${kalshiKey.slice(0, 8)}...)`);
  } else {
    console.log("[FAIL] KALSHI_API_KEY: not set");
    issues++;
  }

  if (kalshiKeyPath) {
    const { existsSync } = await import("fs");
    if (existsSync(kalshiKeyPath)) {
      console.log(`[OK] KALSHI_PRIVATE_KEY_PATH: ${kalshiKeyPath} (exists)`);
    } else {
      console.log(`[FAIL] KALSHI_PRIVATE_KEY_PATH: ${kalshiKeyPath} (FILE NOT FOUND)`);
      issues++;
    }
  } else if (kalshiKeyPem) {
    console.log(`[OK] KALSHI_PRIVATE_KEY_PEM: set (${kalshiKeyPem.length} chars)`);
  } else {
    console.log("[FAIL] Neither KALSHI_PRIVATE_KEY_PATH nor KALSHI_PRIVATE_KEY_PEM set");
    issues++;
  }

  console.log(`[INFO] KALSHI_ENV: ${kalshiEnv}`);

  // 4. Kalshi auth test
  if (kalshiKey && (kalshiKeyPath || kalshiKeyPem)) {
    try {
      const { getKalshiEnvConfig } = await import("./src/clients/kalshi/kalshi-config");
      const config = getKalshiEnvConfig();
      console.log(`[OK] Kalshi auth config: baseUrl=${config.baseUrl}`);

      // Try a basic API call
      try {
        const { KalshiClient } = await import("./src/clients/kalshi/kalshi-client");
        const client = new KalshiClient(config);
        const markets = await client.getMarkets({ limit: 1, status: "open" });
        console.log(`[OK] Kalshi API connectivity: ${markets.markets.length > 0 ? "working" : "no markets found"}`);
      } catch (apiErr) {
        console.log(`[WARN] Kalshi API test: ${apiErr instanceof Error ? apiErr.message : "failed"}`);
        warnings++;
      }
    } catch (err) {
      console.log(`[FAIL] Kalshi auth: ${err instanceof Error ? err.message : "failed"}`);
      issues++;
    }
  }

  // 5. Summary
  console.log(`\n=== Summary: ${issues} issue(s), ${warnings} warning(s) ===`);
  if (issues === 0) {
    console.log("Ready to run. Use --mode fake-trade --profiles arbBotV1 --coins all --headless");
  } else {
    console.log("Fix the issues above before running.");
  }
}

const run = async () => {
  if (cliArgs.help) {
    printUsage();
    return;
  }

  if (cliArgs.validate) {
    await runValidation();
    return;
  }

  if (cliArgs.mode === "fake-trade") {
    await fakeTradeRouteWithOptions({
      profiles: cliArgs.profiles,
      coins: cliArgs.coins,
      autoSelect: cliArgs.auto,
      provider: cliArgs.provider,
      headless: cliArgs.headless || cliArgs.headlessSummary,
    });
    return;
  }

  if (cliArgs.mode === "watch-market") {
    await watchMarketRoute({ searchTerm: cliArgs.market });
    return;
  }

  if (cliArgs.mode === "cross-platform-analysis") {
    await crossPlatformAnalysisRoute({
      coins: normalizeCoins(cliArgs.coins),
      headless: cliArgs.headless,
      headlessSummary: cliArgs.headlessSummary,
    });
    return;
  }

  if (cliArgs.mode === "price-diff-detection") {
    await priceDiffDetectionRoute({
      coins: normalizeCoins(cliArgs.coins),
      headless: cliArgs.headless,
      headlessSummary: cliArgs.headlessSummary,
      realisticFill: cliArgs.realisticFill,
      fillUsd: cliArgs.fillUsd,
    });
    return;
  }

  if (cliArgs.mode === "backtest") {
    await backtestRoute({
      profiles: cliArgs.profiles,
      coins: cliArgs.coins,
      autoSelect: cliArgs.auto,
      dataDir: cliArgs.dataDir,
      speed: cliArgs.speed,
      mode: cliArgs.backtestMode,
      headless: cliArgs.headless,
      startMs: parseTime(cliArgs.start),
      endMs: parseTime(cliArgs.end),
    });
    return;
  }

  if (argv.length > 0 || !process.stdin.isTTY) {
    printUsage();
    process.exit(1);
  }

  const selectedMode = await promptMainMenu();
  if (!selectedMode) {
    console.log("\nExiting...");
    return;
  }

  if (selectedMode === "fake-trade") {
    await fakeTradeRouteWithOptions();
    return;
  }

  if (selectedMode === "watch-market") {
    await watchMarketRoute();
    return;
  }

  if (selectedMode === "cross-platform-analysis") {
    await crossPlatformAnalysisRoute();
    return;
  }

  if (selectedMode === "price-diff-detection") {
    await priceDiffDetectionRoute({
      realisticFill: cliArgs.realisticFill,
      fillUsd: cliArgs.fillUsd,
    });
    return;
  }

  if (selectedMode === "backtest") {
    await backtestRoute();
  }
};

// Global error handlers to prevent silent crashes
process.on("unhandledRejection", (reason) => {
  const message = reason instanceof Error ? reason.message : String(reason);
  console.error(`[FATAL] Unhandled rejection: ${message}`);
  if (reason instanceof Error && reason.stack) {
    console.error(reason.stack);
  }
});

process.on("uncaughtException", (error) => {
  console.error(`[FATAL] Uncaught exception: ${error.message}`);
  if (error.stack) {
    console.error(error.stack);
  }
  process.exit(1);
});

void run();
