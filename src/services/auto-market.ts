import {
  extractOutcomes,
  getMarketBySlug,
  getSeriesBySlug,
  isMarketTradeable,
  searchByKeyword,
  type EventDetails,
  type MarketDetails,
} from "./market-service";

const UPDOWN_SLUG_RE =
  /^(eth|btc|sol|xrp)-(?:updown|up-or-down)-(\d+[hm])-(\d{9,13})/i;

function parseEnvNumber(
  name: string,
  defaultValue: number,
  minValue: number,
): number {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const parsed = Number(raw);
  return Number.isFinite(parsed)
    ? Math.max(minValue, parsed)
    : defaultValue;
}

const MIN_LIQUIDITY = parseEnvNumber("AUTO_MARKET_MIN_LIQUIDITY", 0, 0);
const MIN_VOLUME_24H = parseEnvNumber("AUTO_MARKET_MIN_VOLUME_24H", 0, 0);

export type CoinSymbol = "eth" | "btc" | "sol" | "xrp";

export interface AutoMarketResult {
  market: MarketDetails;
  startMs: number | null;
}

export function parseUpDownSlugStartMs(slug: string): number | null {
  const match = slug.match(UPDOWN_SLUG_RE);
  if (!match) return null;
  const ts = match[3];
  if (!ts) return null;
  const tsNum = Number(ts);
  if (!Number.isFinite(tsNum)) return null;
  if (ts.length >= 13) return tsNum;
  return tsNum * 1000;
}

export function resolveMarketStartMs(market: MarketDetails): number | null {
  if (market.startTime) {
    const parsed = Date.parse(market.startTime);
    if (!Number.isNaN(parsed)) return parsed;
  }
  if (market.slug) {
    const parsed = parseUpDownSlugStartMs(market.slug);
    if (parsed) return parsed;
  }
  if (market.startDate) {
    const parsed = Date.parse(market.startDate);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return null;
}

function isUpDownMarket(market: MarketDetails): boolean {
  const outcomes = extractOutcomes(market).map((o) => o.toLowerCase());
  if (outcomes.includes("up") && outcomes.includes("down")) {
    return true;
  }
  return UPDOWN_SLUG_RE.test(market.slug || "");
}

function passesLiquidityFilters(market: MarketDetails): boolean {
  if (MIN_LIQUIDITY <= 0 && MIN_VOLUME_24H <= 0) return true;
  const liquidity = market.liquidityClob ?? 0;
  const volume = market.volume24hrClob ?? 0;
  if (MIN_LIQUIDITY > 0 && liquidity < MIN_LIQUIDITY) return false;
  if (MIN_VOLUME_24H > 0 && volume < MIN_VOLUME_24H) return false;
  return true;
}

function resolveEventStartMs(event: EventDetails): number | null {
  if (event.startTime) {
    const parsed = Date.parse(event.startTime);
    if (!Number.isNaN(parsed)) return parsed;
  }
  if (event.slug) {
    const parsed = parseUpDownSlugStartMs(event.slug);
    if (parsed) return parsed;
  }
  if (event.startDate) {
    const parsed = Date.parse(event.startDate);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return null;
}

async function resolveMarketFromEvent(
  event: EventDetails,
): Promise<MarketDetails | null> {
  if (event.markets && Array.isArray(event.markets) && event.markets.length > 0) {
    const tradeable =
      event.markets.find(
        (m) => isMarketTradeable(m) && passesLiquidityFilters(m),
      ) || event.markets[0];
    return tradeable || null;
  }
  if (event.slug) {
    return getMarketBySlug(event.slug);
  }
  return null;
}

function pickClosestEvent(
  events: EventDetails[],
  nowMs: number,
  minStartMs?: number,
): { event: EventDetails; startMs: number | null } | null {
  const withStart = events.map((event) => ({
    event,
    startMs: resolveEventStartMs(event),
  }));

  const filtered = minStartMs
    ? withStart.filter(
        (item) => item.startMs !== null && item.startMs > minStartMs,
      )
    : withStart;

  const past = filtered.filter(
    (item) => item.startMs !== null && item.startMs <= nowMs,
  );
  if (past.length > 0) {
    past.sort((a, b) => (b.startMs || 0) - (a.startMs || 0));
    return past[0] || null;
  }

  const future = filtered.filter(
    (item) => item.startMs !== null && item.startMs > nowMs,
  );
  if (future.length > 0) {
    future.sort((a, b) => (a.startMs || 0) - (b.startMs || 0));
    return future[0] || null;
  }

  if (filtered.length > 0) {
    return { event: filtered[0].event, startMs: filtered[0].startMs };
  }

  return null;
}

function pickClosestMarket(
  candidates: MarketDetails[],
  nowMs: number,
  minStartMs?: number,
): AutoMarketResult | null {
  const withStart = candidates.map((market) => ({
    market,
    startMs: resolveMarketStartMs(market),
  }));

  const filtered = minStartMs
    ? withStart.filter(
        (item) => item.startMs !== null && item.startMs > minStartMs,
      )
    : withStart;

  const past = filtered.filter(
    (item) => item.startMs !== null && item.startMs <= nowMs,
  );
  if (past.length > 0) {
    past.sort((a, b) => (b.startMs || 0) - (a.startMs || 0));
    return past[0] || null;
  }

  const future = filtered.filter(
    (item) => item.startMs !== null && item.startMs > nowMs,
  );
  if (future.length > 0) {
    future.sort((a, b) => (a.startMs || 0) - (b.startMs || 0));
    return future[0] || null;
  }

  if (filtered.length > 0) {
    return filtered[0] || null;
  }

  return null;
}

export async function findLatestUpDownMarket(
  coin: CoinSymbol,
  duration: "15m" = "15m",
  minStartMs?: number,
): Promise<AutoMarketResult | null> {
  const seriesSlugs = [
    `${coin}-up-or-down-${duration}`,
    `${coin}-updown-${duration}`,
  ];
  for (const seriesSlug of seriesSlugs) {
    const series = await getSeriesBySlug(seriesSlug);
    if (series?.events && series.events.length > 0) {
      const selection = pickClosestEvent(series.events, Date.now(), minStartMs);
      if (selection) {
        const market = await resolveMarketFromEvent(selection.event);
        if (market) {
          return { market, startMs: selection.startMs };
        }
      }
    }
  }

  const term = `${coin} updown ${duration}`;
  const search = await searchByKeyword(term, 50);

  const slugPattern = new RegExp(
    `^${coin}-(?:updown|up-or-down)-${duration}-`,
    "i",
  );
  const candidates = search.markets.filter(
    (market) =>
      slugPattern.test(market.slug || "") &&
      isUpDownMarket(market) &&
      isMarketTradeable(market) &&
      passesLiquidityFilters(market),
  );

  if (candidates.length === 0) {
    return null;
  }

  const selection = pickClosestMarket(candidates, Date.now(), minStartMs);
  if (!selection) return null;

  return selection;
}
