# Arbitration Bot (Polymarket + Kalshi)

Paper arbitrage engine and analytics for crypto Up/Down markets on Polymarket and Kalshi. Includes live dashboards, cross-platform outcome analysis, price gap detection, and historical backtesting.

## Quick Start

```bash
# Install dependencies
bun install

# Copy env and fill in keys
cp .env.example .env
# Windows PowerShell:
# copy .env.example .env

# Interactive mode (menu)
bun run main.ts

# View all CLI options
bun run main.ts -- --help
```

## Features

- Paper arbitrage engine with per-coin rules and PnL tracking
- Cross-platform outcome analysis with official resolution matching
- Real-time price diff detection with realistic fill simulation
- Live dashboards with profile tabs and coin tabs
- Historical backtesting
- Headless modes for server and Docker use

## Modes

### Arbitrage Bot (paper)
Simulated arbitrage engine that consumes Polymarket + Kalshi order books, computes realistic fills, and tracks PnL and win rate. Uses the `arbitrage` section in `config.json`.

Mode flags:
- `--mode fake-trade` (aliases: `fake`, `arbitrage`, `arb`)
- `--fake-trade`

Examples:
```bash
bun run main.ts -- --mode fake-trade --profiles arbBotV1 --coins eth,btc
bun run main.ts -- --mode fake-trade --auto
bun run main.ts -- --fake-trade --headless
```

Controls:
- Up/Down: switch profile
- Left/Right: switch coin tab

Logs:
- `logs/run*` with `system.log`, `mismatch.log`, and one log per profile (e.g. `arbBotV1.log`)

### Cross-Platform Outcome Analysis
Compares Polymarket vs Kalshi outcomes for matching markets, tracks accuracy, and logs mismatches.

Mode flags:
- `--mode cross-platform-analysis` (aliases: `cross-platform`, `outcome-analysis`, `analysis`)
- `--cross-platform-analysis`

Examples:
```bash
bun run main.ts -- --mode cross-platform-analysis --coins eth,btc
bun run main.ts -- --cross-platform-analysis --headless-summary
```

Logs:
- `logs/cross-platform/run*` with `system.log`, `mismatch.log`, and `debug.log` in headless summary mode

### Price Diff Detection
Flags large gaps between `PolyUp + KalshiNo` and `PolyDown + KalshiYes`, tracks how often each market meets the threshold, and optionally runs realistic fill simulation with post-delay confirmation.

Mode flags:
- `--mode price-diff-detection` (aliases: `price-diff`, `diff`)
- `--price-diff-detection`

Examples:
```bash
bun run main.ts -- --mode price-diff-detection --coins eth,sol --realistic-fill
bun run main.ts -- --price-diff-detection --headless-summary --fill-usd 500
```

Logs:
- `logs/price-diff/run*` with `system.log` and `debug.log` in headless summary mode

### Watch Market
Monitor a single market in real time.

Mode flags:
- `--mode watch-market` (alias: `watch`)
- `--watch-market`

Examples:
```bash
bun run main.ts -- --mode watch-market --market "bitcoin"
bun run main.ts -- --watch --provider polymarket --market "https://polymarket.com/event/..."
```

### Backtest (historical)
Replay markets from stored data, with optional visualization.

Mode flags:
- `--mode backtest` (alias: `historical`)
- `--backtest`

Examples:
```bash
bun run main.ts -- --mode backtest --auto --data-dir backtest-data --speed max
bun run main.ts -- --backtest --backtest-mode visual --start "2024-01-01T00:00:00Z" --end "2024-01-07T23:59:59Z"
```

## CLI Flags (full list)
```
--mode <fake-trade|watch-market|cross-platform-analysis|price-diff-detection|backtest>
--fake-trade | --watch-market | --cross-platform-analysis | --price-diff-detection | --backtest
--profiles <name1,name2>          (arbitrage bot)
--coins <eth,btc,sol,xrp>         (arbitrage, cross-platform-analysis, price-diff-detection)
--auto                            (arbitrage/backtest: select all profiles/coins)
--provider <polymarket|kalshi>    (watch-market)
--kalshi | --polymarket           (provider shortcut)
--market <keyword|url>            (watch-market)
--data-dir <path>                 (backtest)
--speed <n|max>                   (backtest)
--backtest-mode <fast|visual>     (backtest)
--fast                            (backtest alias for fast)
--visual                          (backtest alias for visual)
--headless                        (backtest/arbitrage/cross-platform/price-diff: disable dashboard UI)
--headless-summary                (cross-platform/price-diff: headless + concise summary logs)
--realistic-fill                  (price-diff-detection: book-walk fill simulation)
--no-realistic-fill               (price-diff-detection: disable fill simulation)
--fill-usd <amount>               (price-diff-detection: USD budget for fill simulation)
--start <iso|ms>                   (backtest)
--end <iso|ms>                     (backtest)
--help
```

## Configuration (`config.json`)

`config.json` is provider-first with a dedicated `arbitrage` section for the paper bot. Coins must exist in both providers for cross-platform and arbitrage modes to run.

Minimal example (valid JSON):
```json
{
  "schemaVersion": 2,
  "providers": {
    "polymarket": {
      "coins": ["eth", "btc", "sol"],
      "marketGroups": [{ "id": "default", "match": {} }],
      "profiles": {}
    },
    "kalshi": {
      "coins": {
        "eth": {
          "tickers": [],
          "seriesTickers": ["KXETH15M"],
          "eventTickers": [],
          "marketUrls": [],
          "autoDiscover": true
        },
        "btc": {
          "tickers": [],
          "seriesTickers": ["KXBTC15M"],
          "eventTickers": [],
          "marketUrls": [],
          "autoDiscover": true
        },
        "sol": {
          "tickers": [],
          "seriesTickers": ["KXSOL15M"],
          "eventTickers": [],
          "marketUrls": [],
          "autoDiscover": true
        }
      },
      "marketGroups": [{ "id": "default", "match": {} }],
      "profiles": {}
    }
  },
  "arbitrage": {
    "profiles": {
      "arbBotV1": {
        "coins": {
          "eth": {
            "tradeAllowedTimeLeft": 750,
            "tradeStopTimeLeft": 1,
            "minGap": 0.04,
            "maxSpendTotal": 500,
            "minSpendTotal": 10,
            "maxSpread": null,
            "minDepthValue": null,
            "maxPriceStalenessSec": null,
            "fillUsd": 500
          },
          "btc": {
            "tradeAllowedTimeLeft": 750,
            "tradeStopTimeLeft": 1,
            "minGap": 0.04,
            "maxSpendTotal": 500,
            "minSpendTotal": 10,
            "maxSpread": null,
            "minDepthValue": null,
            "maxPriceStalenessSec": null,
            "fillUsd": 500
          },
          "sol": {
            "tradeAllowedTimeLeft": 750,
            "tradeStopTimeLeft": 1,
            "minGap": 0.04,
            "maxSpendTotal": 500,
            "minSpendTotal": 10,
            "maxSpread": null,
            "minDepthValue": null,
            "maxPriceStalenessSec": null,
            "fillUsd": 500
          }
        }
      }
    }
  }
}
```

Kalshi selector notes:
- `tickers`: explicit market tickers
- `seriesTickers`: series-level tickers (auto-discover open markets)
- `eventTickers`: event-level tickers (auto-discover open markets)
- `marketUrls`: full market URLs (series/market tickers are extracted)
- `autoDiscover`: when true, rotates to the latest open market

### Arbitrage coin config fields
- `tradeAllowedTimeLeft` (sec): trades only when time left is at or below this value
- `tradeStopTimeLeft` (sec | null): stop trading when time left is at or below this value
- `minGap`: minimum profit gap required (`1 - (polyAsk + kalshiAsk)`)
- `maxSpendTotal`: hard max total cost across both legs
- `minSpendTotal`: minimum total cost required to place a trade
- `maxSpread` (optional): maximum allowed spread per book
- `minDepthValue` (optional): minimum total ask depth per book
- `maxPriceStalenessSec` (optional): max allowed age for spot prices
- `fillUsd`: USD budget used for the fill estimate (book-walk)

`fillUsd` is the budget used to estimate shares and average prices. `maxSpendTotal` is the hard ceiling the trade cannot exceed. The estimate is computed from `fillUsd` and then validated against min/max spend.

## Execution Delay Model
Both the arbitrage bot and price diff detection use the same execution delay model:
1. Detect a valid opportunity and commit immediately.
2. Apply a random delay between `EXECUTION_DELAY_MIN_MS` and `EXECUTION_DELAY_MAX_MS`.
3. Re-check the books and finalize the fill using post-delay prices.
4. No abort is allowed after commitment; slippage is recorded.

## Environment Variables

### Required
- `POLYMARKET_TRADES_PROVIDER` (e.g. `data-api`)
- `POLYMARKET_CLOB_PRIVATE_KEY`
- `POLYMARKET_CLOB_API_KEY`
- `POLYMARKET_CLOB_API_SECRET`
- `KALSHI_ENV` (e.g. `demo`)
- `KALSHI_API_KEY`
- `KALSHI_PRIVATE_KEY_PATH`

### Core optional
- `MARKET_PROVIDER` (default provider for watch-market)
- `TEST_PRICE_DIFF_REQ` (gap threshold for price diff detection)
- `PRICE_DIFF_FILL_USD` (default fill budget when not provided)
- `EXECUTION_DELAY_MIN_MS` / `EXECUTION_DELAY_MAX_MS` (post-commit delay range)
- `TUI_ALT_SCREEN` (set `false` to disable alternate screen buffer)
- `KALSHI_PRIVATE_KEY_PEM` (optional inline key instead of file path)
- `KALSHI_BASE_URL` / `KALSHI_WS_URL` (override Kalshi endpoints)
- `KALSHI_WEB_EMAIL` / `KALSHI_WEB_PASSWORD` (optional web session for v1/forecast_history)

### Cross-platform analysis tuning (optional)
- `CROSS_ANALYSIS_FINAL_WINDOW_MS`
- `CROSS_ANALYSIS_FINAL_GRACE_MS`
- `CROSS_ANALYSIS_FINAL_MIN_POINTS`
- `CROSS_ANALYSIS_OFFICIAL_WAIT_MS`
- `CROSS_ANALYSIS_MATCH_TIME_TOLERANCE_MS`
- `CROSS_ANALYSIS_SLOT_DURATION_MS`
- `CROSS_ANALYSIS_SLOT_TOLERANCE_MS`
- `CROSS_ANALYSIS_STALE_LOG_MS`
- `CROSS_ANALYSIS_PRICE_STALE_MS`
- `CROSS_ANALYSIS_BOOK_STALE_MS`
- `CROSS_ANALYSIS_SUMMARY_LOG_MS`
- `CROSS_ANALYSIS_OFFICIAL_RETRY_BASE_MS`
- `CROSS_ANALYSIS_OFFICIAL_RETRY_MAX_MS`
- `CROSS_ANALYSIS_OFFICIAL_RETRIES`
- `CROSS_ANALYSIS_VERBOSE`
- `CROSS_ANALYSIS_MISMATCH_VERBOSE`

### Price diff detection tuning (optional)
- `PRICE_DIFF_LOG_INTERVAL_MS`
- `PRICE_DIFF_SUMMARY_LOG_MS`
- `PRICE_DIFF_BOOK_STALE_MS`
- `PRICE_DIFF_STALE_LOG_MS`

### Market data tuning (optional)
- `PM_HTML_REF_REFRESH_MS`
- `PM_HTML_REF_RETRY_BASE_MS`
- `PM_HTML_REF_RETRY_MAX_MS`
- `PM_HTML_REF_TIMEOUT_MS`
- `PM_HTML_REF_MATCH_TOLERANCE_MS`
- `PM_HTML_REF_ENABLED`
- `LIVE_SIGNAL_PREP`
- `AUTO_MARKET_MIN_LIQUIDITY`
- `AUTO_MARKET_MIN_VOLUME_24H`

### Reliability / WebSocket reconnection (optional)
All reconnect-attempts vars accept `-1` for infinite retries (recommended for servers).

**Polymarket market WS:**
- `PM_WS_RECONNECT_ATTEMPTS` (default `-1`)
- `PM_WS_RECONNECT_DELAY_MS` (default `3000`)
- `PM_WS_PING_INTERVAL_MS` (default `30000`)

**Polymarket crypto WS:**
- `CRYPTO_WS_RECONNECT_ATTEMPTS` (default `-1`)
- `CRYPTO_WS_RECONNECT_DELAY_MS` (default `3000`)

**Kalshi WS:**
- `KALSHI_WS_RECONNECT_ATTEMPTS` (default `-1`)
- `KALSHI_WS_RECONNECT_DELAY_MS` (default `3000`)

**Kalshi crypto WS (Chainlink feed):**
- `KALSHI_CRYPTO_WS_RECONNECT_ATTEMPTS` (default `-1`)
- `KALSHI_CRYPTO_WS_RECONNECT_DELAY_MS` (default `3000`)

### Data-freshness thresholds (optional)
Any WS event (orderbook snapshot, price change, last trade, ticker) resets the freshness timer. Data is only marked stale when zero events of any kind arrive within the threshold. WS resets only fire when the connection is actually dead, not when a market is simply quiet.

**Polymarket:**
- `PM_BOOK_STALE_MS` (default `45000`) -- order book considered stale after this many ms with no events
- `PM_BOOK_RESET_MS` (default `90000`) -- trigger WS reset after this many ms stale (only if WS disconnected)
- `PM_WS_RESET_COOLDOWN_MS` (default `45000`) -- min time between WS resets
- `PM_PRICE_STALE_MS` (default `45000`) -- crypto price considered stale
- `PM_PRICE_RESET_MS` (default `90000`) -- trigger crypto WS reset (only if WS disconnected)
- `PM_CRYPTO_RESET_COOLDOWN_MS` (default `45000`) -- min time between crypto WS resets
- `PM_DATA_STARTUP_GRACE_MS` (default `20000`) -- ignore staleness for this long after startup
- `PM_MARKET_RESELECT_MS` (default `60000`) -- trigger market reselect after this many ms stale
- `PM_MARKET_RESELECT_COOLDOWN_MS` (default `60000`) -- min time between reselects

**Kalshi:**
- `KALSHI_BOOK_STALE_MS` (default `45000`)
- `KALSHI_PRICE_STALE_MS` (default `45000`)
- `KALSHI_DATA_STARTUP_GRACE_MS` (default `20000`)
- `KALSHI_MARKET_RESELECT_MS` (default `60000`)
- `KALSHI_MARKET_RESELECT_COOLDOWN_MS` (default `60000`)
