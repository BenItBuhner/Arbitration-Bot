import type { MarketSnapshot } from "./market-data-hub";
import type { MarketProvider } from "../providers/provider";

export interface PolymarketGroupMatch {
  slugRegex?: string;
  seriesSlug?: string;
  outcomes?: string[];
  questionRegex?: string;
}

export interface KalshiGroupMatch {
  tickerPrefix?: string;
  eventTicker?: string;
  titleRegex?: string;
}

export type MarketGroupMatch = PolymarketGroupMatch | KalshiGroupMatch;

export interface MarketGroupDefinition {
  id: string;
  match?: MarketGroupMatch;
}

function testRegex(pattern: string, value: string): boolean {
  try {
    const regex = new RegExp(pattern, "i");
    return regex.test(value);
  } catch {
    return false;
  }
}

function matchesPolymarket(
  snapshot: MarketSnapshot,
  match: PolymarketGroupMatch,
): boolean {
  if (match.slugRegex && !testRegex(match.slugRegex, snapshot.slug)) {
    return false;
  }
  if (match.seriesSlug && match.seriesSlug !== snapshot.seriesSlug) {
    return false;
  }
  if (match.questionRegex && !testRegex(match.questionRegex, snapshot.marketName)) {
    return false;
  }
  if (match.outcomes && match.outcomes.length > 0) {
    const lower = match.outcomes.map((o) => o.toLowerCase());
    const snapshotOutcomes = [
      snapshot.upOutcome?.toLowerCase?.() ?? "",
      snapshot.downOutcome?.toLowerCase?.() ?? "",
    ];
    for (const outcome of lower) {
      if (!snapshotOutcomes.includes(outcome)) {
        return false;
      }
    }
  }
  return true;
}

function matchesKalshi(
  snapshot: MarketSnapshot,
  match: KalshiGroupMatch,
): boolean {
  const ticker = snapshot.marketTicker ?? snapshot.slug;
  if (match.tickerPrefix && !ticker.startsWith(match.tickerPrefix)) {
    return false;
  }
  if (match.eventTicker && match.eventTicker !== snapshot.eventTicker) {
    return false;
  }
  if (match.titleRegex && !testRegex(match.titleRegex, snapshot.marketName)) {
    return false;
  }
  return true;
}

export function resolveMarketGroup(
  provider: MarketProvider,
  snapshot: MarketSnapshot,
  groups: MarketGroupDefinition[],
): string | null {
  for (const group of groups) {
    const match = group.match ?? {};
    const isMatch =
      provider === "polymarket"
        ? matchesPolymarket(snapshot, match as PolymarketGroupMatch)
        : matchesKalshi(snapshot, match as KalshiGroupMatch);
    if (isMatch) {
      return group.id;
    }
  }

  const defaultGroup = groups.find((group) => group.id === "default");
  return defaultGroup ? defaultGroup.id : null;
}
