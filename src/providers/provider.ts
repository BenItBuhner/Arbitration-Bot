export type MarketProvider = "polymarket" | "kalshi";

export function resolveMarketProvider(
  env: Partial<NodeJS.ProcessEnv> = process.env,
): MarketProvider {
  const raw = env.MARKET_PROVIDER?.trim().toLowerCase();
  if (raw === "kalshi") {
    return "kalshi";
  }
  return "polymarket";
}
