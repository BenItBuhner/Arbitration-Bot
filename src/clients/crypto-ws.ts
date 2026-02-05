/**
 * Crypto Price WebSocket Client
 *
 * Connects to Polymarket's Real-Time Data Socket (RTDS) to receive
 * live cryptocurrency prices from Chainlink oracles and Binance.
 */

// Types for crypto price events
export interface CryptoPricePayload {
  symbol: string;
  timestamp: number;
  value: number;
}

export interface CryptoPriceMessage {
  topic: "crypto_prices" | "crypto_prices_chainlink";
  type: "update";
  timestamp: number;
  payload: CryptoPricePayload;
}

export interface SubscriptionMessage {
  action: "subscribe" | "unsubscribe";
  subscriptions: Array<{
    topic: string;
    type: string;
    filters?: string;
  }>;
}

// Callback types
export type PriceCallback = (price: CryptoPricePayload) => void;
export type ConnectionCallback = (connected: boolean) => void;
export type ErrorCallback = (error: Error) => void;

// Configuration
export interface CryptoWSConfig {
  reconnectAttempts?: number;
  reconnectDelay?: number;
  pingInterval?: number;
  source?: "chainlink" | "binance" | "both";
}

const DEFAULT_CONFIG: CryptoWSConfig = {
  reconnectAttempts: 5,
  reconnectDelay: 3000,
  pingInterval: 30000,
  source: "chainlink",
};

const RTDS_WS_URL = "wss://ws-live-data.polymarket.com";

/**
 * WebSocket client for Polymarket RTDS crypto prices
 */
export class CryptoWS {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private isManuallyClosed = false;
  private pingInterval: NodeJS.Timeout | null = null;
  private subscribedSymbols: Set<string> = new Set();

  // Callbacks
  private onPriceUpdate: PriceCallback;
  private onConnectionChange: ConnectionCallback;
  private onError: ErrorCallback;
  private config: CryptoWSConfig;

  // Price cache for latest values
  private priceCache: Map<string, CryptoPricePayload> = new Map();

  constructor(
    onPriceUpdate: PriceCallback = () => {},
    onConnectionChange: ConnectionCallback = () => {},
    onError: ErrorCallback = () => {},
    config: CryptoWSConfig = {},
  ) {
    this.onPriceUpdate = onPriceUpdate;
    this.onConnectionChange = onConnectionChange;
    this.onError = onError;
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Connect to RTDS WebSocket
   */
  connect(): void {
    try {
      this.ws = new WebSocket(RTDS_WS_URL);

      this.ws.onopen = () => {
        this.reconnectAttempts = 0;
        this.startPing();
        this.onConnectionChange(true);

        // Re-subscribe to any previous subscriptions
        if (this.subscribedSymbols.size > 0) {
          this.subscribeToSymbols(Array.from(this.subscribedSymbols));
        }
      };

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          this.handleMessage(data);
        } catch (error) {
          // Ignore non-JSON messages (like PONG)
        }
      };

      this.ws.onerror = () => {
        this.onError(new Error("RTDS WebSocket connection error"));
      };

      this.ws.onclose = () => {
        this.stopPing();
        this.onConnectionChange(false);

        if (!this.isManuallyClosed) {
          this.attemptReconnect();
        }
      };
    } catch (error) {
      this.onError(error instanceof Error ? error : new Error(String(error)));
    }
  }

  /**
   * Handle incoming WebSocket messages
   */
  private handleMessage(data: any): void {
    if (!data || !data.topic) return;

    // Handle crypto price updates
    if (
      (data.topic === "crypto_prices" ||
        data.topic === "crypto_prices_chainlink") &&
      data.type === "update" &&
      data.payload
    ) {
      const payload = data.payload as CryptoPricePayload;

      // Normalize symbol format (e.g., "eth/usd" -> "ETH/USD")
      payload.symbol = payload.symbol.toUpperCase().replace("/", "/");

      // Update cache
      this.priceCache.set(payload.symbol, payload);

      // Notify callback
      this.onPriceUpdate(payload);
    }
  }

  /**
   * Subscribe to cryptocurrency price updates
   */
  subscribe(symbols?: string[]): void {
    if (symbols) {
      symbols.forEach((s) => this.subscribedSymbols.add(s.toLowerCase()));
    }

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.subscribeToSymbols(symbols);
    }
  }

  /**
   * Internal method to send subscription messages
   */
  private subscribeToSymbols(symbols?: string[]): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

    const subscriptions: SubscriptionMessage["subscriptions"] = [];

    // Subscribe to Chainlink prices
    if (this.config.source === "chainlink" || this.config.source === "both") {
      if (symbols && symbols.length > 0) {
        for (const symbol of symbols) {
          subscriptions.push({
            topic: "crypto_prices_chainlink",
            type: "*",
            filters: JSON.stringify({ symbol: symbol.toLowerCase() }),
          });
        }
      } else {
        subscriptions.push({
          topic: "crypto_prices_chainlink",
          type: "*",
          filters: "",
        });
      }
    }

    // Subscribe to Binance prices
    if (this.config.source === "binance" || this.config.source === "both") {
      const filters =
        symbols && symbols.length > 0
          ? symbols.map((s) => s.toLowerCase().replace("/", "")).join(",")
          : undefined;

      subscriptions.push({
        topic: "crypto_prices",
        type: "update",
        ...(filters && { filters }),
      });
    }

    const message: SubscriptionMessage = {
      action: "subscribe",
      subscriptions,
    };

    try {
      this.ws.send(JSON.stringify(message));
    } catch (error) {
      this.onError(
        error instanceof Error ? error : new Error("Failed to subscribe"),
      );
    }
  }

  /**
   * Unsubscribe from price updates
   */
  unsubscribe(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

    const subscriptions: SubscriptionMessage["subscriptions"] = [];

    if (this.config.source === "chainlink" || this.config.source === "both") {
      subscriptions.push({
        topic: "crypto_prices_chainlink",
        type: "*",
      });
    }

    if (this.config.source === "binance" || this.config.source === "both") {
      subscriptions.push({
        topic: "crypto_prices",
        type: "update",
      });
    }

    const message: SubscriptionMessage = {
      action: "unsubscribe",
      subscriptions,
    };

    try {
      this.ws.send(JSON.stringify(message));
      this.subscribedSymbols.clear();
    } catch (error) {
      // Ignore unsubscribe errors
    }
  }

  /**
   * Start heartbeat ping
   */
  private startPing(): void {
    this.stopPing();
    this.pingInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        try {
          this.ws.send(JSON.stringify({ event: "ping" }));
        } catch (error) {
          // Ignore ping errors
        }
      }
    }, this.config.pingInterval || 30000);
  }

  /**
   * Stop heartbeat
   */
  private stopPing(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
  }

  /**
   * Attempt reconnection with exponential backoff.
   * When reconnectAttempts is -1, retries indefinitely.
   */
  private attemptReconnect(): void {
    const maxAttempts = this.config.reconnectAttempts ?? 5;
    const infinite = maxAttempts < 0;
    if (!infinite && this.reconnectAttempts >= maxAttempts) {
      this.onError(new Error("Max reconnection attempts reached"));
      return;
    }

    this.reconnectAttempts++;
    const delay = Math.min(
      (this.config.reconnectDelay || 3000) *
        Math.pow(2, this.reconnectAttempts - 1),
      30000,
    );

    setTimeout(() => {
      if (!this.isManuallyClosed) {
        this.connect();
      }
    }, delay);
  }

  /**
   * Disconnect from WebSocket
   */
  disconnect(): void {
    this.isManuallyClosed = true;
    this.stopPing();

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }

    this.subscribedSymbols.clear();
  }

  /**
   * Get current connection status
   */
  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  /**
   * Get latest cached price for a symbol
   */
  getPrice(symbol: string): CryptoPricePayload | undefined {
    return this.priceCache.get(symbol.toUpperCase());
  }

  /**
   * Get all cached prices
   */
  getAllPrices(): Map<string, CryptoPricePayload> {
    return new Map(this.priceCache);
  }
}

/**
 * Simple price tracker that maintains latest prices for multiple symbols
 */
export class CryptoPriceTracker {
  private ws: CryptoWS;
  private prices: Map<string, number> = new Map();
  private timestamps: Map<string, number> = new Map();
  private callbacks: Map<string, Set<(price: number) => void>> = new Map();

  constructor(source: "chainlink" | "binance" | "both" = "chainlink") {
    this.ws = new CryptoWS(
      (payload) => this.handlePrice(payload),
      () => {},
      () => {},
      { source },
    );
  }

  /**
   * Handle incoming price update
   */
  private handlePrice(payload: CryptoPricePayload): void {
    const symbol = payload.symbol.toUpperCase();
    this.prices.set(symbol, payload.value);
    this.timestamps.set(symbol, payload.timestamp);

    // Notify symbol-specific callbacks
    const symbolCallbacks = this.callbacks.get(symbol);
    if (symbolCallbacks) {
      symbolCallbacks.forEach((cb) => cb(payload.value));
    }
  }

  /**
   * Start tracking prices
   */
  start(symbols?: string[]): void {
    this.ws.connect();
    this.ws.subscribe(symbols);
  }

  /**
   * Stop tracking
   */
  stop(): void {
    this.ws.disconnect();
  }

  /**
   * Get current price for a symbol
   */
  getPrice(symbol: string): number | undefined {
    return this.prices.get(symbol.toUpperCase());
  }

  /**
   * Get timestamp of last update for a symbol
   */
  getTimestamp(symbol: string): number | undefined {
    return this.timestamps.get(symbol.toUpperCase());
  }

  /**
   * Register a callback for price updates on a specific symbol
   */
  onPrice(symbol: string, callback: (price: number) => void): void {
    const key = symbol.toUpperCase();
    if (!this.callbacks.has(key)) {
      this.callbacks.set(key, new Set());
    }
    this.callbacks.get(key)!.add(callback);
  }

  /**
   * Remove a callback
   */
  offPrice(symbol: string, callback: (price: number) => void): void {
    const key = symbol.toUpperCase();
    this.callbacks.get(key)?.delete(callback);
  }
}

/**
 * Convenience function to quickly get ETH price
 */
export async function getEthPrice(
  timeoutMs: number = 5000,
): Promise<number | null> {
  return new Promise((resolve) => {
    const ws = new CryptoWS(
      (payload) => {
        if (
          payload.symbol.toUpperCase() === "ETH/USD" ||
          payload.symbol.toLowerCase() === "ethusdt"
        ) {
          ws.disconnect();
          resolve(payload.value);
        }
      },
      () => {},
      () => resolve(null),
      { source: "chainlink" },
    );

    ws.connect();
    ws.subscribe(["eth/usd"]);

    setTimeout(() => {
      ws.disconnect();
      resolve(null);
    }, timeoutMs);
  });
}

/**
 * Convenience function to quickly get BTC price
 */
export async function getBtcPrice(
  timeoutMs: number = 5000,
): Promise<number | null> {
  return new Promise((resolve) => {
    const ws = new CryptoWS(
      (payload) => {
        if (
          payload.symbol.toUpperCase() === "BTC/USD" ||
          payload.symbol.toLowerCase() === "btcusdt"
        ) {
          ws.disconnect();
          resolve(payload.value);
        }
      },
      () => {},
      () => resolve(null),
      { source: "chainlink" },
    );

    ws.connect();
    ws.subscribe(["btc/usd"]);

    setTimeout(() => {
      ws.disconnect();
      resolve(null);
    }, timeoutMs);
  });
}

/**
 * Convenience function to quickly get SOL price
 */
export async function getSolPrice(
  timeoutMs: number = 5000,
): Promise<number | null> {
  return new Promise((resolve) => {
    const ws = new CryptoWS(
      (payload) => {
        if (
          payload.symbol.toUpperCase() === "SOL/USD" ||
          payload.symbol.toLowerCase() === "solusdt"
        ) {
          ws.disconnect();
          resolve(payload.value);
        }
      },
      () => {},
      () => resolve(null),
      { source: "chainlink" },
    );

    ws.connect();
    ws.subscribe(["sol/usd"]);

    setTimeout(() => {
      ws.disconnect();
      resolve(null);
    }, timeoutMs);
  });
}
