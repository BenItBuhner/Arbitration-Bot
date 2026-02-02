import type { CoinSymbol } from "../services/auto-market";

export type BacktestTradeSide = "BUY" | "SELL";

export interface BacktestTradeLevel {
  price: number;
  size: number;
  side: BacktestTradeSide;
  tokenId: string;
}

export interface BacktestMarketMeta {
  slug: string;
  coin: CoinSymbol;
  marketName: string;
  marketId?: string;
  conditionId?: string;
  startMs: number;
  endMs: number;
  upTokenId: string;
  downTokenId: string;
  priceToBeat?: number;
}

export interface BacktestTradeEvent {
  timestamp: number;
  tokenId: string;
  price: number;
  size: number;
  side?: BacktestTradeSide;
  tradeId?: string;
  takerOrderId?: string;
  bucketIndex?: number;
  makerOrders?: BacktestTradeLevel[];
}

export interface BacktestCryptoTick {
  symbol: string;
  timestamp: number;
  value: number;
}
