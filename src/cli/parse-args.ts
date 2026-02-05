type CLIMode =
  | "fake-trade"
  | "watch-market"
  | "cross-platform-analysis"
  | "price-diff-detection"
  | "backtest";
type Provider = "polymarket" | "kalshi";

export interface CLIArgs {
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

export function normalizeMode(value: string | undefined): CLIMode | undefined {
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

export function parseArgs(argv: string[]): CLIArgs {
  const args: CLIArgs = { profiles: [], coins: [] };

  for (let i = 0; i < argv.length; i++) {
    const raw = argv[i] ?? "";
    if (!raw) continue;

    if (raw === "--help" || raw === "-h") { args.help = true; continue; }
    if (raw === "--auto") { args.auto = true; continue; }
    if (raw === "--fake-trade") { args.mode = "fake-trade"; continue; }
    if (raw === "--watch-market" || raw === "--watch") { args.mode = "watch-market"; continue; }
    if (raw === "--cross-platform-analysis" || raw === "--cross-platform" || raw === "--outcome-analysis" || raw === "--analysis") { args.mode = "cross-platform-analysis"; continue; }
    if (raw === "--price-diff-detection" || raw === "--price-diff" || raw === "--diff") { args.mode = "price-diff-detection"; continue; }
    if (raw === "--backtest") { args.mode = "backtest"; continue; }
    if (raw === "--fast") { args.backtestMode = "fast"; continue; }
    if (raw === "--visual") { args.backtestMode = "visual"; continue; }
    if (raw === "--headless") { args.headless = true; continue; }
    if (raw === "--validate" || raw === "--check") { args.validate = true; continue; }
    if (raw === "--headless-summary" || raw === "--summary-logs" || raw === "--summary-log") { args.headlessSummary = true; args.headless = true; continue; }
    if (raw === "--realistic-fill") { args.realisticFill = true; continue; }
    if (raw === "--no-realistic-fill") { args.realisticFill = false; continue; }
    if (raw === "--kalshi") { args.provider = "kalshi"; continue; }
    if (raw === "--polymarket" || raw === "--poly") { args.provider = "polymarket"; continue; }

    if (raw.startsWith("--provider=") || raw.startsWith("--platform=")) {
      const value = raw.includes("=") ? raw.split("=").slice(1).join("=") : "";
      args.provider = normalizeProvider(value);
      continue;
    }
    if (raw === "--provider" || raw === "--platform") { args.provider = normalizeProvider(argv[i + 1]); i += 1; continue; }
    if (raw.startsWith("--backtest-mode=")) { args.backtestMode = normalizeBacktestMode(raw.slice("--backtest-mode=".length)); continue; }
    if (raw === "--backtest-mode") { args.backtestMode = normalizeBacktestMode(argv[i + 1]); i += 1; continue; }
    if (raw.startsWith("--mode=")) { args.mode = normalizeMode(raw.slice("--mode=".length)); continue; }
    if (raw === "--mode") { args.mode = normalizeMode(argv[i + 1]); i += 1; continue; }
    if (raw.startsWith("--profiles=")) { args.profiles?.push(...splitList(raw.slice("--profiles=".length))); continue; }
    if (raw === "--profiles") { args.profiles?.push(...splitList(argv[i + 1])); i += 1; continue; }
    if (raw.startsWith("--coins=")) { args.coins?.push(...splitList(raw.slice("--coins=".length))); continue; }
    if (raw === "--coins") { args.coins?.push(...splitList(argv[i + 1])); i += 1; continue; }
    if (raw.startsWith("--market=")) { args.market = raw.slice("--market=".length).trim(); continue; }
    if (raw === "--market" || raw === "--query" || raw === "--search") { args.market = (argv[i + 1] ?? "").trim(); i += 1; continue; }
    if (raw.startsWith("--speed=")) { const value = raw.slice("--speed=".length).trim(); args.speed = value === "max" ? 0 : Number(value); continue; }
    if (raw === "--speed") { const value = (argv[i + 1] ?? "").trim(); args.speed = value === "max" ? 0 : Number(value); i += 1; continue; }
    if (raw.startsWith("--fill-usd=")) { const parsed = Number(raw.slice("--fill-usd=".length).trim()); if (Number.isFinite(parsed)) args.fillUsd = parsed; continue; }
    if (raw === "--fill-usd") { const parsed = Number((argv[i + 1] ?? "").trim()); if (Number.isFinite(parsed)) args.fillUsd = parsed; i += 1; continue; }
    if (raw.startsWith("--data-dir=")) { args.dataDir = raw.slice("--data-dir=".length).trim(); continue; }
    if (raw === "--data-dir") { args.dataDir = (argv[i + 1] ?? "").trim(); i += 1; continue; }
    if (raw.startsWith("--start=")) { args.start = raw.slice("--start=".length).trim(); continue; }
    if (raw === "--start") { args.start = (argv[i + 1] ?? "").trim(); i += 1; continue; }
    if (raw.startsWith("--end=")) { args.end = raw.slice("--end=".length).trim(); continue; }
    if (raw === "--end") { args.end = (argv[i + 1] ?? "").trim(); i += 1; continue; }

    if (!raw.startsWith("-") && !args.market) {
      args.market = raw.trim();
    }
  }

  if (args.profiles && args.profiles.length === 0) delete args.profiles;
  if (args.coins && args.coins.length === 0) delete args.coins;

  return args;
}
