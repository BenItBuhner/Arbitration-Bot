export interface HistoricalPriceResult {
  price: number;
  timestamp: number;
}

/**
 * Fetch historical crypto price at a specific time.
 * Uses CoinGecko API as a fallback for historical prices.
 */
export async function fetchHistoricalCryptoPrice(
  symbol: string,
  targetTime: Date,
): Promise<HistoricalPriceResult | null> {
  try {
    const coinId = symbol.includes("eth")
      ? "ethereum"
      : symbol.includes("btc")
        ? "bitcoin"
        : symbol.includes("sol")
          ? "solana"
          : symbol.includes("xrp")
            ? "ripple"
            : null;

    if (!coinId) {
      return null;
    }

    const timestamp = Math.floor(targetTime.getTime() / 1000);
    const url = `https://api.coingecko.com/api/v3/coins/${coinId}/market_chart/range?vs_currency=usd&from=${timestamp - 300}&to=${timestamp + 300}`;

    const response = await fetch(url);
    if (!response.ok) return null;

    const data = (await response.json()) as {
      prices?: Array<[number, number]>;
    };

    if (!data.prices || data.prices.length === 0) {
      return null;
    }

    const firstPrice = data.prices[0];
    if (!firstPrice) return null;

    let closestPrice: [number, number] = firstPrice;
    let minDiff = Math.abs(firstPrice[0] - targetTime.getTime());

    for (const pricePoint of data.prices) {
      const ts = pricePoint[0];
      const diff = Math.abs(ts - targetTime.getTime());
      if (diff < minDiff) {
        minDiff = diff;
        closestPrice = pricePoint;
      }
    }

    return {
      price: closestPrice[1],
      timestamp: closestPrice[0],
    };
  } catch {
    return null;
  }
}
