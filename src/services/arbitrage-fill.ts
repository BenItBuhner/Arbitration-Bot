import type { MarketSnapshot, OrderBookLevel } from "./market-data-hub";

export interface FillEstimate {
  shares: number;
  avgPoly: number;
  avgKalshi: number;
  costPoly: number;
  costKalshi: number;
  totalCost: number;
  gap: number;
}

export function randomDelayMs(minMs: number, maxMs: number): number {
  const min = Math.max(0, Math.floor(minMs));
  const max = Math.max(min, Math.floor(maxMs));
  return min + Math.floor(Math.random() * (max - min + 1));
}

export function normalizeAsks(levels: OrderBookLevel[]): OrderBookLevel[] {
  return levels
    .filter(
      (level) =>
        Number.isFinite(level.price) &&
        level.price > 0 &&
        Number.isFinite(level.size) &&
        level.size > 0,
    )
    .map((level) => ({
      price: level.price,
      size: Math.max(0, Math.floor(level.size)),
    }))
    .filter((level) => level.size > 0)
    .sort((a, b) => a.price - b.price);
}

export function resolveAsks(
  snapshot: MarketSnapshot | undefined,
  tokenId: string | null | undefined,
): OrderBookLevel[] {
  if (!snapshot || !tokenId) return [];
  const book = snapshot.orderBooks.get(tokenId);
  if (!book || !book.asks) return [];
  return normalizeAsks(book.asks);
}

export function findMaxEqualShares(
  asksA: OrderBookLevel[],
  asksB: OrderBookLevel[],
  budgetUsd: number,
): { shares: number; costA: number; costB: number } | null {
  if (budgetUsd <= 0) return null;
  let indexA = 0;
  let indexB = 0;
  let remainingA = 0;
  let remainingB = 0;
  let priceA = 0;
  let priceB = 0;
  let costA = 0;
  let costB = 0;
  let shares = 0;

  const advanceA = (): boolean => {
    while (indexA < asksA.length) {
      const level = asksA[indexA];
      indexA += 1;
      if (!level || level.size <= 0) continue;
      remainingA = level.size;
      priceA = level.price;
      return true;
    }
    return false;
  };

  const advanceB = (): boolean => {
    while (indexB < asksB.length) {
      const level = asksB[indexB];
      indexB += 1;
      if (!level || level.size <= 0) continue;
      remainingB = level.size;
      priceB = level.price;
      return true;
    }
    return false;
  };

  if (!advanceA() || !advanceB()) return null;

  while (true) {
    if (remainingA <= 0 && !advanceA()) break;
    if (remainingB <= 0 && !advanceB()) break;
    if (remainingA <= 0 || remainingB <= 0) break;

    const nextCostA = costA + priceA;
    const nextCostB = costB + priceB;
    if (nextCostA + nextCostB > budgetUsd) break;

    costA = nextCostA;
    costB = nextCostB;
    remainingA -= 1;
    remainingB -= 1;
    shares += 1;
  }

  if (shares <= 0) return null;
  return { shares, costA, costB };
}

export function computeFillEstimate(
  asksPoly: OrderBookLevel[],
  asksKalshi: OrderBookLevel[],
  budgetUsd: number,
): FillEstimate | null {
  const result = findMaxEqualShares(asksPoly, asksKalshi, budgetUsd);
  if (!result) return null;
  const avgPoly = result.costA / result.shares;
  const avgKalshi = result.costB / result.shares;
  const totalCost = result.costA + result.costB;
  return {
    shares: result.shares,
    avgPoly,
    avgKalshi,
    costPoly: result.costA,
    costKalshi: result.costB,
    totalCost,
    gap: 1 - (avgPoly + avgKalshi),
  };
}
