export type SignalTradeSide = "BUY" | "SELL";

export interface TradeLike {
  timestamp: number;
  price: number;
  size: number;
  side?: SignalTradeSide;
  tokenId?: string;
}

export interface TokenSignal {
  bestBid: number | null;
  bestAsk: number | null;
  spread: number | null;
  midPrice: number | null;
  depthValue: number | null;
  slippage: number | null;
  bookImbalance: number | null;
  totalBidValue: number | null;
  totalAskValue: number | null;
}

export interface SignalSnapshot {
  tokenSignals: Map<string, TokenSignal>;
  priceMomentum: number | null;
  priceVolatility: number | null;
  tradeVelocity: number | null;
  tradeFlowImbalance: number | null;
  priceStalenessSec: number | null;
  referenceQuality: number;
  updatedAtMs: number;
}

export interface SignalInput {
  orderBooks: Map<
    string,
    {
      bids: Array<{ price: number; size: number }>;
      asks: Array<{ price: number; size: number }>;
      totalBidValue: number;
      totalAskValue: number;
    }
  >;
  bestBid: Map<string, number>;
  bestAsk: Map<string, number>;
  priceHistory: number[];
  cryptoPriceTimestamp: number;
  referenceSource:
    | "price_to_beat"
    | "historical"
    | "html"
    | "kalshi_underlying"
    | "kalshi_html"
    | "missing";
  priceToBeat: number;
  referencePrice: number;
}

export interface SignalOptions {
  depthLevels?: number;
  slippageNotional?: number;
  tradeWindowMs?: number;
}

const DEFAULT_DEPTH_LEVELS = 3;
const DEFAULT_SLIPPAGE_NOTIONAL = 50;
const DEFAULT_TRADE_WINDOW_MS = 5 * 60 * 1000;

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function computeVolatility(values: number[]): number | null {
  if (values.length < 2) return null;
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  let variance = 0;
  for (const value of values) {
    const diff = value - mean;
    variance += diff * diff;
  }
  variance /= values.length;
  return Math.sqrt(variance);
}

function computeEWMomentum(values: number[], alpha: number): number | null {
  if (values.length < 2) return null;
  const safeAlpha = clamp(alpha, 0.01, 0.99);
  let ema = values[0] ?? 0;
  for (let i = 1; i < values.length; i += 1) {
    const value = values[i] ?? ema;
    ema = safeAlpha * value + (1 - safeAlpha) * ema;
  }
  const last = values[values.length - 1] ?? ema;
  return last - ema;
}

function computeLogReturnVolatility(values: number[]): number | null {
  if (values.length < 2) return null;
  const returns: number[] = [];
  for (let i = 1; i < values.length; i += 1) {
    const prev = values[i - 1];
    const next = values[i];
    if (!prev || !next || prev <= 0 || next <= 0) continue;
    returns.push(Math.log(next / prev));
  }
  if (returns.length < 2) return null;
  return computeVolatility(returns);
}

function computeDepthValue(
  asks: Array<{ price: number; size: number }>,
  depthLevels: number,
): number | null {
  if (!asks.length) return null;
  let total = 0;
  const capped = Math.max(1, depthLevels);
  for (let i = 0; i < asks.length && i < capped; i += 1) {
    const ask = asks[i];
    if (!ask) continue;
    total += ask.price * ask.size;
  }
  return total > 0 ? total : null;
}

function computeSlippage(
  asks: Array<{ price: number; size: number }>,
  bestAsk: number | null,
  notional: number,
): number | null {
  if (!asks.length || bestAsk === null || notional <= 0) return null;
  let remaining = notional;
  let cost = 0;
  let shares = 0;

  for (const ask of asks) {
    if (ask.size <= 0) continue;
    const availableValue = ask.price * ask.size;
    if (availableValue <= 0) continue;
    const useValue = Math.min(remaining, availableValue);
    shares += useValue / ask.price;
    cost += useValue;
    remaining -= useValue;
    if (remaining <= 0) break;
  }

  if (remaining > 0 || shares <= 0) return null;
  const avgPrice = cost / shares;
  return avgPrice - bestAsk;
}

function computeTradeMetrics(
  trades: TradeLike[] | undefined,
  nowMs: number,
  windowMs: number,
): { tradeVelocity: number | null; tradeFlowImbalance: number | null } {
  if (!trades || trades.length === 0) {
    return { tradeVelocity: null, tradeFlowImbalance: null };
  }

  const minTs = nowMs - windowMs;
  let count = 0;
  let buyValue = 0;
  let sellValue = 0;

  for (const trade of trades) {
    if (trade.timestamp < minTs) continue;
    count += 1;
    if (trade.side === "BUY") {
      buyValue += trade.price * trade.size;
    } else if (trade.side === "SELL") {
      sellValue += trade.price * trade.size;
    }
  }

  const minutes = windowMs / 60000;
  const tradeVelocity = minutes > 0 ? count / minutes : null;
  const totalValue = buyValue + sellValue;
  const tradeFlowImbalance =
    totalValue > 0 ? (buyValue - sellValue) / totalValue : null;

  return { tradeVelocity, tradeFlowImbalance };
}

export function computeSignals(
  input: SignalInput,
  nowMs: number,
  trades?: TradeLike[],
  options: SignalOptions = {},
): SignalSnapshot {
  const depthLevels = options.depthLevels ?? DEFAULT_DEPTH_LEVELS;
  const slippageNotional = options.slippageNotional ?? DEFAULT_SLIPPAGE_NOTIONAL;
  const tradeWindowMs = options.tradeWindowMs ?? DEFAULT_TRADE_WINDOW_MS;

  const tokenSignals = new Map<string, TokenSignal>();
  const tokenIds = new Set<string>();
  for (const key of input.orderBooks.keys()) tokenIds.add(key);
  for (const key of input.bestBid.keys()) tokenIds.add(key);
  for (const key of input.bestAsk.keys()) tokenIds.add(key);

  for (const tokenId of tokenIds) {
    const book = input.orderBooks.get(tokenId);
    const bids = book?.bids ?? [];
    const asks = book?.asks ?? [];
    const bestBid = input.bestBid.get(tokenId) ?? bids[0]?.price ?? null;
    const bestAsk = input.bestAsk.get(tokenId) ?? asks[0]?.price ?? null;
    const spread =
      bestBid !== null && bestAsk !== null ? bestAsk - bestBid : null;
    const midPrice =
      bestBid !== null && bestAsk !== null ? (bestAsk + bestBid) / 2 : null;
    const totalBidValue = book?.totalBidValue ?? null;
    const totalAskValue = book?.totalAskValue ?? null;
    const denom =
      totalBidValue !== null && totalAskValue !== null
        ? totalBidValue + totalAskValue
        : null;
    const bookImbalance =
      denom && denom > 0 && totalBidValue !== null
        ? totalBidValue / denom
        : null;
    const depthValue = computeDepthValue(asks, depthLevels);
    const slippage = computeSlippage(asks, bestAsk, slippageNotional);

    tokenSignals.set(tokenId, {
      bestBid,
      bestAsk,
      spread,
      midPrice,
      depthValue,
      slippage,
      bookImbalance,
      totalBidValue,
      totalAskValue,
    });
  }

  const priceMomentum = computeEWMomentum(input.priceHistory, 0.3);
  const priceVolatility = computeLogReturnVolatility(input.priceHistory);

  const { tradeVelocity, tradeFlowImbalance } = computeTradeMetrics(
    trades,
    nowMs,
    tradeWindowMs,
  );

  const priceStalenessSec =
    input.cryptoPriceTimestamp > 0
      ? Math.max(0, (nowMs - input.cryptoPriceTimestamp) / 1000)
      : null;

  const referenceQuality =
    input.referenceSource === "kalshi_underlying"
      ? 1
      : input.referenceSource === "kalshi_html"
        ? 0.8
        : input.priceToBeat > 0
          ? 1
          : input.referenceSource === "html"
            ? 1
            : input.referenceSource === "historical"
              ? 0.6
              : 0;

  return {
    tokenSignals,
    priceMomentum,
    priceVolatility,
    tradeVelocity,
    tradeFlowImbalance,
    priceStalenessSec,
    referenceQuality: clamp(referenceQuality, 0, 1),
    updatedAtMs: nowMs,
  };
}
