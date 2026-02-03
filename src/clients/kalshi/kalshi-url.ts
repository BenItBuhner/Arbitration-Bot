export interface KalshiUrlParseResult {
  marketTicker: string | null;
  seriesTicker: string | null;
}

export function normalizeKalshiTicker(value: string): string {
  return value.trim().toUpperCase();
}

export function looksLikeKalshiMarketTicker(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) return false;
  return /^[A-Z0-9]+-[A-Z0-9]/.test(trimmed.toUpperCase());
}

export function deriveSeriesTickerFromMarket(marketTicker: string): string | null {
  const normalized = normalizeKalshiTicker(marketTicker);
  const index = normalized.indexOf("-");
  if (index <= 0) return null;
  const prefix = normalized.slice(0, index).trim();
  return prefix.length > 0 ? prefix : null;
}

export function parseKalshiMarketUrl(value: string): KalshiUrlParseResult {
  if (!value) {
    return { marketTicker: null, seriesTicker: null };
  }

  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return { marketTicker: null, seriesTicker: null };
  }

  const parts = url.pathname.split("/").filter(Boolean);
  if (parts.length === 0) {
    return { marketTicker: null, seriesTicker: null };
  }

  const marketSegment = parts[parts.length - 1] ?? "";
  const marketTicker = marketSegment ? normalizeKalshiTicker(marketSegment) : null;

  let seriesTicker: string | null = null;
  const marketsIndex = parts.findIndex(
    (segment) => segment.toLowerCase() === "markets",
  );
  if (marketsIndex >= 0 && parts.length > marketsIndex + 1) {
    const seriesSegment = parts[marketsIndex + 1] ?? "";
    if (seriesSegment) {
      seriesTicker = normalizeKalshiTicker(seriesSegment);
    }
  }

  if (!seriesTicker && marketTicker) {
    seriesTicker = deriveSeriesTickerFromMarket(marketTicker);
  }

  return { marketTicker, seriesTicker };
}
