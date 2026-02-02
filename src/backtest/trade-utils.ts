import type { BacktestTradeEvent } from "./types";

export function compareTrades(a: BacktestTradeEvent, b: BacktestTradeEvent): number {
  if (a.timestamp !== b.timestamp) {
    return a.timestamp - b.timestamp;
  }
  const aBucket = a.bucketIndex ?? 0;
  const bBucket = b.bucketIndex ?? 0;
  if (aBucket !== bBucket) {
    return aBucket - bBucket;
  }
  const aId = a.tradeId ?? "";
  const bId = b.tradeId ?? "";
  if (aId !== bId) {
    return aId.localeCompare(bId);
  }
  const aTaker = a.takerOrderId ?? "";
  const bTaker = b.takerOrderId ?? "";
  if (aTaker !== bTaker) {
    return aTaker.localeCompare(bTaker);
  }
  return 0;
}

export function sortTradesChronologically(
  trades: BacktestTradeEvent[],
  slug: string,
): BacktestTradeEvent[] {
  if (trades.length === 0) return trades;

  const withIndex = trades
    .map((trade, index) => {
      const timestamp = Number(trade.timestamp);
      if (!Number.isFinite(timestamp)) return null;
      if (trade.timestamp !== timestamp) {
        trade.timestamp = timestamp;
      }
      return { trade, index };
    })
    .filter(
      (entry): entry is { trade: BacktestTradeEvent; index: number } =>
        entry !== null,
    );

  if (withIndex.length !== trades.length) {
    const skipped = trades.length - withIndex.length;
    console.warn(
      `[backtest] Dropped ${skipped} trade(s) with invalid timestamp for ${slug}.`,
    );
  }

  withIndex.sort((a, b) => {
    const order = compareTrades(a.trade, b.trade);
    if (order !== 0) return order;
    // Preserve original file order for true timestamp ties.
    return a.index - b.index;
  });

  return withIndex.map(({ trade }) => trade);
}
