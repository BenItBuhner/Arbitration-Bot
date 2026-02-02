export interface HtmlReferencePrice {
  openPrice: number;
  closePrice: number | null;
  startMs: number;
  endMs: number;
}

interface HtmlReferenceOptions {
  slug: string;
  symbol: string;
  startMs: number;
  endMs: number;
  timeoutMs?: number;
  matchToleranceMs?: number;
}

const DEFAULT_TIMEOUT_MS = 10000;
const DEFAULT_MATCH_TOLERANCE_MS = 30000;
const NEXT_DATA_MARKER = "\"__N_SSG\"";

function parseMs(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function extractNextData(html: string): unknown | null {
  const markerIndex = html.indexOf(NEXT_DATA_MARKER);
  if (markerIndex === -1) return null;
  const scriptStart = html.lastIndexOf("<script", markerIndex);
  const scriptEnd = html.indexOf("</script>", markerIndex);
  if (scriptStart === -1 || scriptEnd === -1) return null;
  const script = html.slice(scriptStart, scriptEnd);
  const match = script.match(/<script[^>]*>([\s\S]*)/);
  if (!match) return null;
  try {
    return JSON.parse(match[1]);
  } catch {
    return null;
  }
}

function findCryptoPriceQuery(
  nextData: any,
  symbol: string,
  startMs: number,
  endMs: number,
  toleranceMs: number,
): HtmlReferencePrice | null {
  const queries = nextData?.props?.pageProps?.dehydratedState?.queries;
  if (!Array.isArray(queries)) return null;

  const targetSymbol = symbol.toUpperCase();
  for (const query of queries) {
    const key = query?.queryKey;
    if (!Array.isArray(key)) continue;
    if (key[0] !== "crypto-prices" || key[1] !== "price") continue;
    if (typeof key[2] !== "string") continue;
    if (key[2].toUpperCase() !== targetSymbol) continue;

    const startKeyMs = parseMs(key[3]);
    const endKeyMs = parseMs(key[5]);
    if (startKeyMs === null || endKeyMs === null) continue;

    const startDiff = Math.abs(startKeyMs - startMs);
    const endDiff = Math.abs(endKeyMs - endMs);
    if (startDiff > toleranceMs || endDiff > toleranceMs) continue;

    const data = query?.state?.data;
    const openPrice = Number(data?.openPrice);
    if (!Number.isFinite(openPrice) || openPrice <= 0) continue;
    const closeRaw = Number(data?.closePrice);

    return {
      openPrice,
      closePrice: Number.isFinite(closeRaw) ? closeRaw : null,
      startMs: startKeyMs,
      endMs: endKeyMs,
    };
  }

  return null;
}

async function fetchHtml(url: string, timeoutMs: number): Promise<string | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      headers: {
        accept: "text/html",
        "user-agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      },
      signal: controller.signal,
    });
    if (!response.ok) return null;
    return await response.text();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

export async function fetchPolymarketHtmlReferencePrice(
  options: HtmlReferenceOptions,
): Promise<HtmlReferencePrice | null> {
  const {
    slug,
    symbol,
    startMs,
    endMs,
    timeoutMs = DEFAULT_TIMEOUT_MS,
    matchToleranceMs = DEFAULT_MATCH_TOLERANCE_MS,
  } = options;

  const urls = [
    `https://polymarket.com/event/${slug}`,
    `https://polymarket.com/event/${slug}/${slug}`,
  ];

  for (const url of urls) {
    const html = await fetchHtml(url, timeoutMs);
    if (!html) continue;
    const nextData = extractNextData(html);
    if (!nextData) continue;
    const found = findCryptoPriceQuery(
      nextData,
      symbol,
      startMs,
      endMs,
      matchToleranceMs,
    );
    if (found) return found;
  }

  return null;
}
