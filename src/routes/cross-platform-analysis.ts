/**
 * Cross-platform outcome analysis: compare Polymarket vs Kalshi for the same coin/timeframe.
 * Runs both hubs side-by-side, normalizes outcomes, and tracks resolution match % per market.
 */

import { existsSync, mkdirSync } from "fs";
import { join } from "path";
import { MarketDataHub } from "../services/market-data-hub";
import { KalshiMarketDataHub } from "../services/kalshi-market-data-hub";
import { RunLogger } from "../services/run-logger";
import { loadProviderConfig } from "../services/profile-config";
import { getKalshiEnvConfig } from "../clients/kalshi/kalshi-config";
import type { CoinSymbol } from "../services/auto-market";
import {
  computeFavoredOutcome,
  computeOddsMid,
  createAccuracyState,
  updateAccuracy,
  type NormalizedOutcome,
  type AccuracyState,
} from "../services/cross-platform-compare";
import { CrossPlatformDashboard } from "../cli/cross-platform-dashboard";

const ODDS_HISTORY_LIMIT = 180;
const RENDER_INTERVAL_MS = 500;

export interface CrossPlatformAnalysisRouteOptions {
  coins?: CoinSymbol[];
  headless?: boolean;
}

interface LastComparison {
  polyOutcome: NormalizedOutcome;
  kalshiOutcome: NormalizedOutcome;
  matched: boolean;
  comparedAtMs: number;
  polySlug: string;
  kalshiSlug: string;
}

export async function crossPlatformAnalysisRoute(
  options: CrossPlatformAnalysisRouteOptions = {},
): Promise<void> {
  const polyConfig = loadProviderConfig("polymarket");
  const kalshiConfigResult = loadProviderConfig("kalshi");
  const coins: CoinSymbol[] =
    (options.coins?.length ?? 0) > 0
      ? options.coins!.filter(
          (c) =>
            polyConfig.coinOptions.includes(c) &&
            kalshiConfigResult.coinOptions.includes(c),
        )
      : polyConfig.coinOptions.filter((c) =>
          kalshiConfigResult.coinOptions.includes(c),
        );

  if (coins.length === 0) {
    console.log(
      "No coins in common between Polymarket and Kalshi config. Add at least one (e.g. eth) to both providers in config.json.",
    );
    return;
  }

  let kalshiEnvConfig;
  try {
    kalshiEnvConfig = getKalshiEnvConfig(process.env);
  } catch (e) {
    console.log(
      "Kalshi env not configured. Set KALSHI_API_KEY, KALSHI_PRIVATE_KEY_PATH, and optionally KALSHI_ENV.",
    );
    return;
  }

  const kalshiSelectors = kalshiConfigResult.kalshiSelectorsByCoin;
  if (!kalshiSelectors) {
    console.log("Kalshi market selectors missing in config.");
    return;
  }

  const runDir = join(process.cwd(), "logs", "cross-platform");
  if (!existsSync(runDir)) {
    mkdirSync(runDir, { recursive: true });
  }
  const logger = new RunLogger(join(runDir, "analysis.log"));

  const polyHub = new MarketDataHub(logger);
  const kalshiHub = new KalshiMarketDataHub(
    logger,
    kalshiEnvConfig,
    kalshiSelectors,
  );

  await polyHub.start(coins);
  await kalshiHub.start(coins);

  const accuracyByCoin = new Map<CoinSymbol, AccuracyState>();
  for (const coin of coins) {
    accuracyByCoin.set(coin, createAccuracyState());
  }
  const lastComparisonByCoin = new Map<CoinSymbol, LastComparison>();
  const lastComparedKeyByCoin = new Map<CoinSymbol, string>();

  const polyOddsHistoryByCoin = new Map<CoinSymbol, number[]>();
  const kalshiOddsHistoryByCoin = new Map<CoinSymbol, number[]>();
  for (const coin of coins) {
    polyOddsHistoryByCoin.set(coin, []);
    kalshiOddsHistoryByCoin.set(coin, []);
  }

  const dashboard = options.headless ? null : new CrossPlatformDashboard();

  const timer = setInterval(() => {
    const polySnapshots = polyHub.getSnapshots();
    const kalshiSnapshots = kalshiHub.getSnapshots();

    for (const coin of coins) {
      const polySnap = polySnapshots.get(coin);
      const kalshiSnap = kalshiSnapshots.get(coin);

      if (polySnap) {
        const upTokenId = polySnap.upTokenId;
        const bid = upTokenId ? polySnap.bestBid.get(upTokenId) ?? null : null;
        const ask = upTokenId ? polySnap.bestAsk.get(upTokenId) ?? null : null;
        const mid = computeOddsMid(bid ?? null, ask ?? null);
        if (mid != null) {
          const arr = polyOddsHistoryByCoin.get(coin)!;
          arr.push(mid);
          if (arr.length > ODDS_HISTORY_LIMIT) arr.shift();
        }
      }

      if (kalshiSnap) {
        const arr = kalshiOddsHistoryByCoin.get(coin)!;
        if (
          arr.length === 0 &&
          kalshiSnap.kalshiMarketPriceHistory &&
          kalshiSnap.kalshiMarketPriceHistory.length > 0
        ) {
          arr.push(
            ...kalshiSnap.kalshiMarketPriceHistory.slice(-ODDS_HISTORY_LIMIT),
          );
        }
        const odds =
          kalshiSnap.kalshiMarketPrice ??
          kalshiSnap.kalshiLastPrice ??
          null;
        if (odds != null) {
          const last = arr[arr.length - 1];
          if (last !== odds) {
            arr.push(odds);
            if (arr.length > ODDS_HISTORY_LIMIT) arr.shift();
          }
        }
      }

      const polyClosed =
        polySnap?.timeLeftSec !== null &&
        polySnap?.timeLeftSec !== undefined &&
        polySnap.timeLeftSec <= 0;
      const kalshiClosed =
        kalshiSnap?.timeLeftSec !== null &&
        kalshiSnap?.timeLeftSec !== undefined &&
        kalshiSnap.timeLeftSec <= 0;

      if (polySnap && kalshiSnap && polyClosed && kalshiClosed) {
        const polyOutcome = computeFavoredOutcome(polySnap);
        const kalshiOutcome = computeFavoredOutcome(kalshiSnap);
        if (polyOutcome !== "UNKNOWN" && kalshiOutcome !== "UNKNOWN") {
          const pairKey = `${polySnap.slug}|${kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi"}`;
          const lastKey = lastComparedKeyByCoin.get(coin);
          if (lastKey !== pairKey) {
            const acc = accuracyByCoin.get(coin);
            if (acc) updateAccuracy(acc, polyOutcome, kalshiOutcome);
            lastComparedKeyByCoin.set(coin, pairKey);
            lastComparisonByCoin.set(coin, {
              polyOutcome,
              kalshiOutcome,
              matched: polyOutcome === kalshiOutcome,
              comparedAtMs: Date.now(),
              polySlug: polySnap.slug,
              kalshiSlug: kalshiSnap.slug ?? kalshiSnap.marketTicker ?? "kalshi",
            });
          }
        }
      }
    }

    if (dashboard) {
      dashboard.update({
        polySnapshots,
        kalshiSnapshots,
        coins,
        accuracyByCoin,
        polyOddsHistoryByCoin,
        kalshiOddsHistoryByCoin,
        lastComparisonByCoin,
      });
    }
  }, RENDER_INTERVAL_MS);

  process.on("SIGINT", () => {
    clearInterval(timer);
    polyHub.stop();
    kalshiHub.stop();
    process.exit(0);
  });
}
