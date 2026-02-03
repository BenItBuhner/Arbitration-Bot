/**
 * Market Search and Identification Module
 *
 * Provides robust functionality to resolve Polymarket market identifiers
 * (slugs, IDs, or URLs) into market metadata and CLOB Token IDs.
 */

export interface MarketToken {
  tokenId: string;
  outcome: string;
  price?: number;
}

export interface MarketDetails {
  id: string;
  slug: string;
  question: string;
  description: string;
  conditionId?: string;
  clobTokenIds: string | string[]; // Gamma API returns this as a JSON string or array
  outcomes: string; // JSON string
  outcomePrices: string; // JSON string
  active: boolean;
  closed: boolean;
  marketState?: string;
  volume24hrClob?: number;
  liquidityClob?: number;
  acceptingOrders?: boolean;
  groupItemThreshold?: string; // The "price to beat" for crypto/stock markets (e.g., "4000" for "ETH above $4000")
  groupItemTitle?: string; // Title of the group this market belongs to (often contains formatted price like "88,000")
  // Event-level fields for Up/Down markets
  startTime?: string; // ISO timestamp when the market's time window begins (for Up/Down markets)
  endDate?: string; // ISO timestamp when the market ends
  startDate?: string; // ISO timestamp when the market was created/started accepting orders
  referencePrice?: number; // The "price to beat" captured at startTime for Up/Down markets
  seriesSlug?: string; // The series this market belongs to (e.g., "eth-up-or-down-4h")
}

export interface MarketSearchResult {
  markets: MarketDetails[];
  total: number;
}

export interface EventDetails {
  id: string;
  slug: string;
  title: string;
  description: string;
  startTime?: string; // ISO timestamp when the market's time window begins (for Up/Down markets)
  startDate?: string;
  endDate?: string;
  seriesSlug?: string;
  markets?: MarketDetails[];
}

export interface SeriesDetails {
  id: string;
  slug: string;
  title: string;
  events?: EventDetails[];
}

const GAMMA_API_BASE = "https://gamma-api.polymarket.com/markets";
const GAMMA_EVENTS_BASE = "https://gamma-api.polymarket.com/events";
const GAMMA_SERIES_BASE = "https://gamma-api.polymarket.com/series";

/**
 * Extracts a market slug from a Polymarket URL.
 * Only processes URLs that belong to polymarket.com.
 * Handles both /event/ and /market/ paths.
 */
export function extractSlugFromUrl(url: string): string {
  const trimmed = url.trim();
  if (!trimmed.startsWith("http")) {
    return trimmed;
  }

  try {
    const parsed = new URL(trimmed);
    // Ensure we only extract from Polymarket domains
    if (!parsed.hostname.endsWith("polymarket.com")) {
      return trimmed;
    }

    const pathParts = parsed.pathname.split("/").filter(Boolean);
    // Polymarket patterns are typically /event/slug or /market/slug
    if (
      pathParts.length >= 2 &&
      (pathParts[0] === "event" || pathParts[0] === "market")
    ) {
      return pathParts[1] ?? trimmed;
    }
    // Fallback to last segment if it's a non-standard Polymarket link
    return pathParts[pathParts.length - 1] || trimmed;
  } catch {
    return trimmed;
  }
}

/**
 * Fetches market details by slug.
 */
export async function getMarketBySlug(
  slug: string,
): Promise<MarketDetails | null> {
  try {
    const url = `${GAMMA_API_BASE}?slug=${encodeURIComponent(slug)}`;
    const response = await fetch(url);
    if (!response.ok) return null;

    const data = await response.json();
    if (Array.isArray(data) && data.length > 0) {
      // Find exact slug match in case API returns multiple
      return data.find((m: MarketDetails) => m.slug === slug) || data[0];
    }
    return null;
  } catch (error) {
    return null;
  }
}

/**
 * Fetches market details by ID.
 * Validates that the ID is numeric to prevent 422 errors from the Gamma API.
 */
export async function getMarketById(id: string): Promise<MarketDetails | null> {
  if (!/^\d+$/.test(id)) return null;

  try {
    const url = `${GAMMA_API_BASE}?id=${id}`;
    const response = await fetch(url);
    if (!response.ok) return null;

    const data = await response.json();
    if (Array.isArray(data) && data.length > 0) {
      return data[0];
    }
    return null;
  } catch (error) {
    return null;
  }
}

/**
 * Searches for markets by keyword.
 * Prioritizes open (non-closed) markets over closed ones.
 */
export async function searchByKeyword(
  keyword: string,
  limit: number = 10,
): Promise<MarketSearchResult> {
  try {
    // Search for open markets first (closed=false)
    const url = `${GAMMA_API_BASE}?query=${encodeURIComponent(keyword)}&limit=${limit}&closed=false`;
    const response = await fetch(url);
    if (!response.ok) return { markets: [], total: 0 };

    const data = await response.json();
    let markets = Array.isArray(data) ? data : [];

    // Filter to ensure we only get markets that are actually open and accepting orders
    markets = markets.filter(
      (m: MarketDetails) => m.closed === false && m.active === true,
    );

    // If no open markets found, try broader search
    if (markets.length === 0) {
      const fallbackUrl = `${GAMMA_API_BASE}?query=${encodeURIComponent(keyword)}&limit=${limit}`;
      const fallbackResponse = await fetch(fallbackUrl);
      if (fallbackResponse.ok) {
        const fallbackData = await fallbackResponse.json();
        markets = Array.isArray(fallbackData) ? fallbackData : [];
        // Still prefer non-closed markets
        const openMarkets = markets.filter((m: MarketDetails) => !m.closed);
        if (openMarkets.length > 0) {
          markets = openMarkets;
        }
      }
    }

    return {
      markets,
      total: markets.length,
    };
  } catch (error) {
    return { markets: [], total: 0 };
  }
}

/**
 * Safely resolves the outcomes list from a market object.
 * Handles cases where Gamma returns outcomes as a JSON string or an array.
 * Returns an array of outcome names (e.g., ["Yes", "No"] or ["Up", "Down"]).
 */
export function extractOutcomes(market: MarketDetails): string[] {
  if (!market.outcomes) return [];
  if (Array.isArray(market.outcomes)) return market.outcomes;
  try {
    const parsed = JSON.parse(market.outcomes);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

/**
 * Safely resolves a token ID list from a market object.
 * Handles cases where Gamma returns clobTokenIds as a JSON string or an array.
 */
export function extractTokenIds(market: MarketDetails): string[] {
  if (!market.clobTokenIds) return [];
  if (Array.isArray(market.clobTokenIds)) return market.clobTokenIds;
  try {
    const parsed = JSON.parse(market.clobTokenIds);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

/**
 * Extracts the "price to beat" threshold from a market.
 * This is the target price that determines market resolution for crypto/stock prediction markets.
 *
 * The threshold can be stored in:
 * 1. `groupItemThreshold` as a numeric string (e.g., "50000")
 * 2. `groupItemTitle` as a formatted string (e.g., "88,000" or "$4,000")
 *
 * @returns The numeric threshold value, or 0 if not found/applicable
 */
export function extractPriceToBeat(market: MarketDetails): number {
  // First try parsing groupItemTitle (e.g., "88,000" or "$4,000" or "90,000")
  // This is preferred because groupItemThreshold is often a sort index (0, 1, 2, etc.)
  // rather than the actual price threshold
  if (market.groupItemTitle) {
    // Remove currency symbols, commas, and other non-numeric characters except decimal point
    const cleaned = market.groupItemTitle.replace(/[^0-9.]/g, "");
    const parsed = parseFloat(cleaned);
    // Only use if it looks like a real price (> 100 to distinguish from sort indices)
    if (!isNaN(parsed) && parsed > 100) {
      return parsed;
    }
  }

  // Fall back to groupItemThreshold if groupItemTitle didn't yield a valid price
  // Only use if it looks like a real price threshold (> 100)
  if (market.groupItemThreshold) {
    const threshold = parseFloat(market.groupItemThreshold);
    if (!isNaN(threshold) && threshold > 100) {
      return threshold;
    }
  }

  return 0;
}

/**
 * Fetches event details by slug which may contain additional data like startTime.
 * Events are the parent container for markets and have more metadata for Up/Down style markets.
 */
export async function getEventBySlug(
  slug: string,
): Promise<EventDetails | null> {
  try {
    const url = `${GAMMA_EVENTS_BASE}?slug=${encodeURIComponent(slug)}`;
    const response = await fetch(url);
    if (!response.ok) return null;

    const data = await response.json();
    if (Array.isArray(data) && data.length > 0) {
      return data.find((e: EventDetails) => e.slug === slug) || data[0];
    }
    return null;
  } catch (error) {
    return null;
  }
}

/**
 * Fetches event details by market slug - tries to find the parent event for a market.
 * This is useful for Up/Down markets where the event contains startTime info.
 */
export async function getEventForMarket(
  marketSlug: string,
): Promise<EventDetails | null> {
  // First try direct event lookup
  let event = await getEventBySlug(marketSlug);
  if (event) return event;

  // For Up/Down markets, the slug pattern is often like "eth-updown-4h-1234567890" or "eth-updown-15m-1234567890"
  // Try to extract series slug and look up via series endpoint
  const seriesMatch = marketSlug.match(/^(eth|btc|sol)-updown-(\d+[hm])-/i);
  if (seriesMatch && seriesMatch[1] && seriesMatch[2]) {
    const coin = seriesMatch[1];
    const duration = seriesMatch[2];
    const seriesSlug = `${coin.toLowerCase()}-up-or-down-${duration}`;
    const seriesEvent = await getEventFromSeries(seriesSlug, marketSlug);
    if (seriesEvent) return seriesEvent;
  }

  // Try to find the event by searching events endpoint
  try {
    const url = `${GAMMA_EVENTS_BASE}?active=true&closed=false&limit=100`;
    const response = await fetch(url);
    if (!response.ok) return null;

    const data = await response.json();
    if (Array.isArray(data)) {
      // Look for an event that contains this market
      for (const evt of data) {
        if (evt.markets && Array.isArray(evt.markets)) {
          const foundMarket = evt.markets.find(
            (m: MarketDetails) => m.slug === marketSlug,
          );
          if (foundMarket) {
            return evt;
          }
        }
      }
    }
    return null;
  } catch (error) {
    return null;
  }
}

/**
 * Fetches event data from a series endpoint.
 * Series contain multiple events with startTime data for Up/Down style markets.
 */
export async function getEventFromSeries(
  seriesSlug: string,
  marketSlug?: string,
): Promise<EventDetails | null> {
  try {
    const url = `${GAMMA_SERIES_BASE}?slug=${encodeURIComponent(seriesSlug)}`;
    const response = await fetch(url);
    if (!response.ok) return null;

    const data = await response.json();
    if (Array.isArray(data) && data.length > 0) {
      const series = data[0] as SeriesDetails;
      if (series.events && series.events.length > 0) {
        // If a specific market slug is provided, find its event
        if (marketSlug) {
          for (const evt of series.events) {
            if (evt.slug === marketSlug) {
              return evt;
            }
            // Also check nested markets array
            if (evt.markets) {
              const foundMarket = evt.markets.find(
                (m: MarketDetails) => m.slug === marketSlug,
              );
              if (foundMarket) {
                return evt;
              }
            }
          }
        }
        // Return the first active/open event if no specific slug
        for (const evt of series.events) {
          if (!evt.endDate || new Date(evt.endDate) > new Date()) {
            return evt;
          }
        }
        // Fallback to first event
        return series.events[0] || null;
      }
    }
    return null;
  } catch (error) {
    return null;
  }
}

/**
 * Fetches a series by slug for direct event inspection.
 */
export async function getSeriesBySlug(
  slug: string,
): Promise<SeriesDetails | null> {
  try {
    const url = `${GAMMA_SERIES_BASE}?slug=${encodeURIComponent(slug)}`;
    const response = await fetch(url);
    if (!response.ok) return null;
    const data = await response.json();
    if (Array.isArray(data) && data.length > 0) {
      return data[0] as SeriesDetails;
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Validates if the market is currently active and tradeable via CLOB.
 */
export function isMarketTradeable(market: MarketDetails): boolean {
  const tokenIds = extractTokenIds(market);
  return (
    market.active === true &&
    market.closed === false &&
    tokenIds.length > 0 &&
    market.acceptingOrders !== false
  );
}

/**
 * Top-level lookup function that tries multiple strategies to find a market.
 * Sequence: Slug -> ID (if numeric) -> Keyword Search.
 *
 * Includes verification to ensure that search results actually match specific
 * identifiers like URLs or slugs to avoid false-positive "featured" market results.
 */
export async function quickMarketLookup(
  identifier: string,
): Promise<MarketDetails> {
  const term = extractSlugFromUrl(identifier);
  const isSpecificIdentifier =
    identifier.includes("/") || identifier.includes("-") || /^\d+$/.test(term);

  // Attempt 1: Direct Slug match (Most reliable)
  let market = await getMarketBySlug(term);

  // Attempt 2: Direct ID match
  if (!market && /^\d+$/.test(term)) {
    market = await getMarketById(term);
  }

  // Attempt 3: Keyword search (Fallback)
  if (!market) {
    const search = await searchByKeyword(term, 5);

    // Check if any search result is an exact slug match first
    const exactMatch = search.markets.find((m) => m.slug === term);
    if (exactMatch) {
      market = exactMatch;
    } else if (search.markets.length > 0) {
      // If we provided a specific slug/URL, but search returned unexpected results,
      // and we didn't find an exact slug match above, we should be skeptical.
      if (!isSpecificIdentifier) {
        // If it was just a general word like "bitcoin", take the top search result.
        const first = search.markets[0];
        if (first) {
          market = first;
        }
      }
    }
  }

  if (!market) {
    throw new Error(
      `Market not found: ${identifier}. (Term resolved to: ${term}). Tried slug, ID, and keyword search.`,
    );
  }

  // For Up/Down markets, try to fetch event data to get startTime
  const outcomes = extractOutcomes(market);
  const outcomesLower = outcomes.map((o) => o.toLowerCase());
  if (outcomesLower.includes("up") && outcomesLower.includes("down")) {
    const event = await getEventForMarket(market.slug);
    if (event) {
      // Copy event-level data to market
      if (event.startTime) {
        market.startTime = event.startTime;
      }
      if (event.seriesSlug) {
        market.seriesSlug = event.seriesSlug;
      }
    }
  }

  // Final sanity check: If we were looking for a specific slug and got a result,
  // but the result slug doesn't contain our term, it's likely a Gamma API "featured" false positive.
  if (
    isSpecificIdentifier &&
    !market.slug.includes(term) &&
    !market.id.includes(term)
  ) {
    throw new Error(
      `Market mismatch: Searching for "${term}" but found "${market.slug}". This is likely a false positive from the search engine.`,
    );
  }

  return market;
}

/**
 * Fetches current high-volume markets from Gamma.
 */
export async function getHighVolumeMarkets(
  limit: number = 10,
): Promise<MarketSearchResult> {
  try {
    const url = `${GAMMA_API_BASE}?active=true&limit=${limit}&order=volume24hrClob&ascending=false`;
    const response = await fetch(url);
    if (!response.ok) return { markets: [], total: 0 };
    const data = await response.json();
    const markets = Array.isArray(data) ? data : [];
    return {
      markets,
      total: markets.length,
    };
  } catch {
    return { markets: [], total: 0 };
  }
}

/**
 * Helper to format market summary for CLI output.
 */
export function formatMarketInfo(market: MarketDetails): string {
  const tokenIds = extractTokenIds(market);
  return `
Question: ${market.question}
Slug:     ${market.slug}
ID:       ${market.id}
Active:   ${market.active}
Tokens:   ${tokenIds.length} IDs available
Link:     https://polymarket.com/event/${market.slug}
`;
}
