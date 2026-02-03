import { Configuration, MarketApi, EventsApi } from "kalshi-typescript";
import type { KalshiEnvConfig } from "./kalshi-config";

export interface KalshiMarket {
  ticker?: string;
  market_ticker?: string;
  event_ticker?: string;
  series_ticker?: string;
  title?: string;
  subtitle?: string;
  status?: string;
  close_time?: string;
  closeTime?: string;
  yes_sub_title?: string;
  no_sub_title?: string;
  strike_set_time?: string;
  strike_price?: number | string | null;
  strike_price_decimal?: number | string | null;
  strike_price_cents?: number | string | null;
  strike?: number | string | null;
  strike_price_dollars?: number | string | { value?: string; display_value?: string } | null;
  strike_type?: string;
  floor_strike?: number | string | null;
  cap_strike?: number | string | null;
  lower_strike?: number | string | null;
  upper_strike?: number | string | null;
  strike?: {
    strike_type?: string;
    floor_strike?: number | string | null;
    cap_strike?: number | string | null;
    lower_strike?: number | string | null;
    upper_strike?: number | string | null;
    strike_price?: number | string | null;
    strike_price_decimal?: number | string | null;
    strike_price_cents?: number | string | null;
    strike_price_dollars?: number | string | { value?: string; display_value?: string } | null;
  } | null;
  underlying_value?: number | string | null;
  underlying_value_ts?: number | string | null;
  last_price?: number | string | null;
  last_price_dollars?: number | string | { value?: string; display_value?: string } | null;
  [key: string]: unknown;
}

export class KalshiClient {
  private marketsApi: MarketApi;
  private eventsApi: EventsApi;

  constructor(config: KalshiEnvConfig) {
    const configuration = new Configuration({
      apiKey: config.apiKey,
      privateKeyPath: config.privateKeyPath,
      privateKeyPem: config.privateKeyPem,
      basePath: config.baseUrl,
    });

    this.marketsApi = new MarketApi(configuration);
    this.eventsApi = new EventsApi(configuration);
  }

  private unwrapResponse<T>(response: unknown): T {
    const payload = response as any;
    return (payload?.data ?? payload?.body ?? payload) as T;
  }

  async getMarket(marketTicker: string): Promise<KalshiMarket | null> {
    const response = await this.marketsApi.getMarket(marketTicker);
    const payload = this.unwrapResponse<any>(response);
    const market = payload?.market ?? payload;
    if (!market) return null;
    return market as KalshiMarket;
  }

  async getMarkets(params: {
    limit?: number;
    cursor?: string;
    status?: string;
    eventTicker?: string;
    tickers?: string;
    seriesTicker?: string;
    minCreatedTs?: number;
    maxCreatedTs?: number;
    minUpdatedTs?: number;
    maxCloseTs?: number;
    minCloseTs?: number;
    minSettledTs?: number;
    maxSettledTs?: number;
    mveFilter?: string;
  }): Promise<{ markets: KalshiMarket[]; cursor?: string }> {
    const response = await this.marketsApi.getMarkets(
      params.limit,
      params.cursor,
      params.eventTicker,
      params.seriesTicker,
      params.minCreatedTs,
      params.maxCreatedTs,
      params.minUpdatedTs,
      params.maxCloseTs,
      params.minCloseTs,
      params.minSettledTs,
      params.maxSettledTs,
      params.status as any,
      params.tickers,
      params.mveFilter as any,
    );
    const payload = this.unwrapResponse<any>(response);
    const markets = Array.isArray(payload?.markets) ? payload.markets : [];
    return {
      markets: markets as KalshiMarket[],
      cursor: payload?.cursor ?? payload?.next_cursor ?? payload?.nextCursor,
    };
  }

  async searchMarkets(
    term: string,
    options?: { limit?: number; status?: string },
  ): Promise<KalshiMarket[]> {
    const normalized = term.trim().toLowerCase();
    const limit = options?.limit ?? 200;
    const status = options?.status;
    const seen = new Set<string>();
    const results: KalshiMarket[] = [];
    let cursor: string | undefined;

    for (let page = 0; page < 3; page += 1) {
      const pageResult = await this.getMarkets({
        limit,
        cursor,
        status,
      });
      for (const market of pageResult.markets) {
        const tickerRaw =
          typeof market.ticker === "string"
            ? market.ticker
            : typeof market.market_ticker === "string"
              ? market.market_ticker
              : "";
        if (!tickerRaw) continue;
        if (seen.has(tickerRaw)) continue;
        seen.add(tickerRaw);
        const title = market.title?.toLowerCase() ?? "";
        const subtitle = market.subtitle?.toLowerCase() ?? "";
        const ticker = tickerRaw.toLowerCase();
        if (
          title.includes(normalized) ||
          subtitle.includes(normalized) ||
          ticker.includes(normalized)
        ) {
          results.push(market);
        }
      }
      cursor = pageResult.cursor;
      if (!cursor) break;
    }

    return results;
  }

  async getEvent(eventTicker: string): Promise<any | null> {
    const response = await this.eventsApi.getEvent(eventTicker);
    const payload = this.unwrapResponse<any>(response);
    return payload?.event ?? payload ?? null;
  }
}
