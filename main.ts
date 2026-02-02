// poly-saturate-bot\cli.ts

import { watchMarketRoute } from "./src/routes/watch-market";
import { fakeTradeRouteWithOptions } from "./src/routes/fake-trade";
import { backtestRoute } from "./src/routes/backtest";
import { selectOne } from "./src/cli/prompts";

type CLIMode = "fake-trade" | "watch-market" | "trading" | "backtest";

interface CLIArgs {
  mode?: CLIMode;
  profiles?: string[];
  coins?: string[];
  market?: string;
  auto?: boolean;
  speed?: number;
  dataDir?: string;
  start?: string;
  end?: string;
  backtestMode?: "fast" | "visual";
  headless?: boolean;
  help?: boolean;
}

function normalizeMode(value: string | undefined): CLIMode | undefined {
  if (!value) return undefined;
  const normalized = value.toLowerCase().replace(/_/g, "-");
  if (normalized === "fake-trade" || normalized === "fake") return "fake-trade";
  if (normalized === "watch-market" || normalized === "watch") return "watch-market";
  if (normalized === "trading" || normalized === "trade") return "trading";
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

    if (raw === "--trading") {
      args.mode = "trading";
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
    "  bun run main.ts -- --mode fake-trade --profiles calmTrader,aggresiveTrader --coins eth,btc",
    "  bun run main.ts -- --mode fake-trade --auto",
    "  bun run main.ts -- --mode watch-market --market \"https://polymarket.com/event/...\"",
    "  bun run main.ts -- --mode backtest --auto --data-dir backtest-data --speed max",
    "  bun run main.ts -- --mode backtest --auto --backtest-mode fast",
    "",
    "Flags:",
    "  --mode <fake-trade|watch-market|trading|backtest>",
    "  --fake-trade | --watch-market | --trading | --backtest",
    "  --profiles <name1,name2>   (fake-trade)",
    "  --coins <eth,btc,sol,xrp>  (fake-trade)",
    "  --auto                     (fake-trade: select all profiles/coins)",
    "  --market <keyword|url>     (watch-market)",
    "  --data-dir <path>          (backtest)",
    "  --speed <n|max>            (backtest)",
    "  --backtest-mode <fast|visual> (backtest)",
    "  --fast                    (backtest alias for fast)",
    "  --visual                  (backtest alias for visual)",
    "  --headless                (backtest: disable dashboard, print logs)",
    "  --start <iso|ms>           (backtest)",
    "  --end <iso|ms>             (backtest)",
    "  --help",
  ];
  console.log(lines.join("\n"));
}

const menuChoices: Array<{ title: string; value: CLIMode }> = [
  { title: "Start fake trade", value: "fake-trade" },
  { title: "Start trading", value: "trading" },
  { title: "Watch market", value: "watch-market" },
  { title: "Backtest (historical)", value: "backtest" },
];

async function promptMainMenu(): Promise<CLIMode | null> {
  return selectOne("Select a mode", menuChoices, 0);
}

const argv = process.argv.slice(2);
const cliArgs = parseArgs(argv);

const run = async () => {
  if (cliArgs.help) {
    printUsage();
    return;
  }

  if (cliArgs.mode === "fake-trade") {
    await fakeTradeRouteWithOptions({
      profiles: cliArgs.profiles,
      coins: cliArgs.coins,
      autoSelect: cliArgs.auto,
    });
    return;
  }

  if (cliArgs.mode === "watch-market") {
    await watchMarketRoute({ searchTerm: cliArgs.market });
    return;
  }

  if (cliArgs.mode === "trading") {
    console.log("\nStarting trading bot...");
    console.log(
      "This will route to a future CLI file for trading configuration and execution.\n",
    );
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

  if (selectedMode === "trading") {
    console.log("\nStarting trading bot...");
    console.log(
      "This will route to a future CLI file for trading configuration and execution.\n",
    );
    return;
  }

  if (selectedMode === "backtest") {
    await backtestRoute();
  }
};

void run();
