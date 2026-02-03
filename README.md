# Poly-Saturate-Bot

A Polymarket simulated trading bot with backtesting capabilities. Run trading strategies, monitor markets in real-time, and backtest on historical data.

## Quick Start

```bash
# Install dependencies
bun install

# Interactive mode (recommended for first-time users)
bun run main.ts

# View all CLI options
bun run main.ts -- --help

# Quick automated fake trade
bun run main.ts -- --fake-trade -- --auto

# Fake trade on Kalshi (provider prompt or flag)
MARKET_PROVIDER=kalshi bun run main.ts -- --fake-trade -- --auto
bun run main.ts -- --fake-trade -- --provider kalshi -- --auto
```

## Features

- **Fake Trade Mode**: Simulate trading with configurable strategies on live Polymarket markets
- **Watch Market Mode**: Monitor any Polymarket market with real-time WebSocket data
- **Backtesting**: Test strategies on historical market data
- **Multi-Profile Trading**: Run multiple strategies simultaneously
- **Real-Time Dashboard**: Live monitoring of trades, prices, and performance

## Modes

### 1. Fake Trade Mode
Run simulated trading with predefined strategies. Automatically finds active Up/Down markets for selected cryptocurrencies.

```bash
# Interactive fake trade
bun run main.ts -- --fake-trade

# Auto-select all profiles and coins
bun run main.ts -- --fake-trade -- --auto

# Specific profiles and coins
bun run main.ts -- --fake-trade -- --profiles aggressiveTraderV2,V3 -- --coins eth,btc
```

**Trading Profiles**: `aggressiveTrader`, `aggressiveTraderV2`, `aggressiveTraderV3`, `aggressiveTraderV4`

**Supported Coins**: `eth`, `btc`, `sol`, `xrp`

### 2. Watch Market Mode
Monitor any Polymarket market in real-time with live order book data.

```bash
# Interactive search
bun run main.ts -- --watch-market

# Monitor specific market by keyword or URL
bun run main.ts -- --watch-market -- --market "bitcoin"
bun run main.ts -- --watch-market -- --market "https://polymarket.com/event/..."
```

### 3. Backtesting Mode
Test strategies on historical market data to evaluate performance.

```bash
# Interactive backtest
bun run main.ts -- --backtest

# Fast backtesting (no dashboard)
bun run main.ts -- --backtest -- --auto -- --fast

# Visual backtesting (step-by-step)
bun run main.ts -- --backtest -- --auto -- --visual

# Headless backtest (logs to console)
bun run main.ts -- --backtest -- --headless

# Specific date range
bun run main.ts -- --backtest -- --data-dir backtest-data -- --start "2024-01-01T00:00:00Z" -- --end "2024-01-07T23:59:59Z"

# Control simulation speed
bun run main.ts -- --backtest -- --speed max    # Maximum speed
bun run main.ts -- --backtest -- --speed 2      # 2x speed
```

### 4. Trading Mode
Placeholder for future real-money trading implementation.

```bash
bun run main.ts -- --trading
```

## CLI Flags

### Mode Selection
```
--mode <fake-trade|watch-market|trading|backtest>
--fake-trade | --fake               # Start fake trade mode
--watch-market | --watch            # Start watch market mode
--backtest | --historical           # Start backtest mode
--trading | --trade                 # Start trading mode
--help | -h                         # Show usage information
```

### Fake Trade Options
```
--profiles <name1,name2>            # Select trading profiles
--coins <eth,btc,sol,xrp>           # Select cryptocurrencies
--auto                              # Auto-select all profiles/coins
--provider <polymarket|kalshi>      # Select market provider
--kalshi | --polymarket             # Provider shortcuts
--headless                          # Disable dashboard UI
```

### Watch Market Options
```
--market <keyword|url>              # Market keyword or Polymarket URL
--query <keyword>                   # Alias for --market
--search <keyword>                  # Alias for --market
```

### Backtest Options
```
--data-dir <path>                   # Directory containing historical data
--speed <n|max>                     # Simulation speed (max = unlimited)
--fast                              # Alias for fast backtest mode
--visual                            # Alias for visual backtest mode
--backtest-mode <fast|visual>       # Explicit backtest mode selection
--headless                          # Disable dashboard, print logs
--start <iso|ms>                    # Start time (ISO format or milliseconds)
--end <iso|ms>                      # End time (ISO format or milliseconds)
```

## Configuration

Edit `config.json` to customize trading strategies. The config is provider-first with market groups.

```json
{
  "schemaVersion": 2,
  "providers": {
    "polymarket": {
      "coins": ["eth", "btc"],
      "marketGroups": [
        { "id": "updown-15m", "match": { "slugRegex": "^(eth|btc)-.*-15m-" } },
        { "id": "default", "match": {} }
      ],
      "profiles": {
        "aggressiveTrader": {
          "markets": {
            "updown-15m": {
              "eth": {
                "120": {
                  "minimumPriceDifference": 5,
                  "maximumSharePrice": 0.99,
                  "minimumSharePrice": 0.8,
                  "maximumSpend": 50,
                  "minimumSpend": 1.5
                },
                "tradeAllowedTimeLeft": 480
              }
            }
          }
        }
      }
    },
    "kalshi": {
      "coins": {
        "eth": {
          "tickers": ["KXETH15M-26FEB021730"],
          "seriesTickers": ["KXETH15M"],
          "eventTickers": [],
          "marketUrls": [
            "https://kalshi.com/markets/kxeth15m/eth-15m-price-up-down/kxeth15m-26feb021730"
          ],
          "autoDiscover": true
        }
      },
      "marketGroups": [{ "id": "default", "match": {} }],
      "profiles": {
        "aggressiveTrader": {
          "markets": {
            "default": {
              "btc": { "...": "same TimedTradeConfig shape" }
            }
          }
        }
      }
    }
  }
}
```

Kalshi selector notes:
- `tickers`: explicit market tickers (bot will prefer these if open)
- `seriesTickers`: series-level tickers to auto-discover the current open market
- `eventTickers`: event-level tickers to auto-discover the current open market
- `marketUrls`: full Kalshi market URLs (the bot extracts the series/market tickers)
- `autoDiscover`: when `true`, the bot uses Kalshi's market list APIs to rotate to the latest open market as older ones close

### Advanced Configuration Fields
Additional fields available per time bracket:
- `maxSpread` - Maximum price spread allowed
- `minBookImbalance` - Minimum order book imbalance
- `minDepthValue` - Minimum depth value in order book
- `minTradeVelocity` - Minimum trade velocity
- `minMomentum` - Minimum price momentum
- `minVolatility` - Minimum price volatility
- `maxPriceStalenessSec` - Max time price can be stale
- `minConfidence` - Minimum confidence score
- `sizeStrategy` - Position sizing strategy ("fixed" | "edge")
- `sizeScale` - Scale factor for position sizing
- `maxOpenExposure` - Maximum open exposure

#### Cross & Double-Down (Optional)
Profiles can include a `cross` block per coin to allow a single opposing-side trade when the favored outcome flips late.
Each `cross` tier supports the standard rule fields plus:
- `minRecoveryMultiple` - Minimum recovery multiple vs realized loss (e.g., 2)
- `minLossToTrigger` - Minimum realized loss to allow crossing

### Trading Criteria
Each strategy evaluates trades based on:
1. **Time Criteria** - Only trades if sufficient time remains
2. **Price Gap Criteria** - Requires minimum price difference
3. **Share Price Criteria** - Restricts trades to acceptable price ranges
4. **Liquidity Criteria** - (Optional) Minimum order book depth
