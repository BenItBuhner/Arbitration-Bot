export interface KalshiHtmlReference {
  strikePrice?: number;
  underlyingValue?: number;
}

export interface KalshiHtmlReferenceOptions {
  urls: string[];
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 10000;

const STRIKE_KEYS = new Set([
  "strike_price_dollars",
  "strike_price_decimal",
  "strike_price",
  "strike_price_cents",
  "strike",
  "floor_strike",
  "cap_strike",
  "lower_strike",
  "upper_strike",
  "price_to_beat",
  "priceToBeat",
  "reference_price",
  "referencePrice",
]);

const UNDERLYING_KEYS = new Set([
  "underlying_value",
  "underlyingValue",
  "underlying_value_dollars",
]);

function parseNumericString(value: string): number | null {
  const cleaned = value.replace(/[$,%]/g, "").replace(/,/g, "").trim();
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") return parseNumericString(value);
  return null;
}

function extractNumericValue(value: unknown): number | null {
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const candidate = record.value ?? record.display_value;
    const parsed = toNumber(candidate);
    if (parsed !== null) return parsed;
  }
  return toNumber(value);
}

function findNumericByKeys(value: unknown, keys: Set<string>): number | null {
  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = findNumericByKeys(entry, keys);
      if (found !== null) return found;
    }
    return null;
  }
  if (!value || typeof value !== "object") return null;

  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (keys.has(key)) {
      const parsed = extractNumericValue(entry);
      if (parsed !== null && parsed > 0) return parsed;
    }
  }

  for (const entry of Object.values(value as Record<string, unknown>)) {
    const found = findNumericByKeys(entry, keys);
    if (found !== null) return found;
  }

  return null;
}

function extractNextData(html: string): unknown | null {
  const match = html.match(
    /<script[^>]*id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/,
  );
  if (!match || !match[1]) return null;
  try {
    return JSON.parse(match[1]);
  } catch {
    return null;
  }
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

export async function fetchKalshiHtmlReference(
  options: KalshiHtmlReferenceOptions,
): Promise<KalshiHtmlReference | null> {
  const { urls, timeoutMs = DEFAULT_TIMEOUT_MS } = options;
  if (!urls || urls.length === 0) return null;

  for (const url of urls) {
    const html = await fetchHtml(url, timeoutMs);
    if (!html) continue;
    const nextData = extractNextData(html);
    if (!nextData) continue;

    const strikePrice = findNumericByKeys(nextData, STRIKE_KEYS);
    const underlyingValue = findNumericByKeys(nextData, UNDERLYING_KEYS);
    if (strikePrice !== null || underlyingValue !== null) {
      return {
        strikePrice: strikePrice ?? undefined,
        underlyingValue: underlyingValue ?? undefined,
      };
    }
  }

  return null;
}
