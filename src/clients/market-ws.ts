/**
 * Market WebSocket Client
 *
 * WebSocket client for real-time Polymarket market data including
 * price changes, order books, and last trade prices.
 */

// Types for WebSocket events
export interface PriceEvent {
  market: string;
  price_changes: PriceChange[];
  timestamp: string;
  event_type: "price_change";
}

export interface PriceChange {
  asset_id: string;
  price: string;
  size: string;
  side: "BUY" | "SELL";
  hash: string;
  best_bid: string;
  best_ask: string;
}

export interface OrderBookEvent {
  market: string;
  asset_id: string;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
  hash: string;
  timestamp: string;
  event_type: "book";
  last_trade_price?: string;
}

export interface OrderBookLevel {
  price: string;
  size: string;
}

export interface LastTradeEvent {
  market: string;
  asset_id: string;
  price: string;
  size: string;
  fee_rate_bps?: string;
  side?: "BUY" | "SELL";
  timestamp: string;
  transaction_hash?: string;
  event_type: "last_trade_price";
}

export type MarketEvent = PriceEvent | OrderBookEvent | LastTradeEvent;

// Callback types
export type EventCallback = (event: MarketEvent) => void;
export type ConnectionCallback = (connected: boolean) => void;
export type ErrorCallback = (error: Error) => void;

// Configuration interface
export interface WSConfig {
  reconnectAttempts?: number;
  reconnectDelay?: number;
  pingInterval?: number;
  silent?: boolean;
  debug?: boolean;
}

const DEFAULT_CONFIG: WSConfig = {
  reconnectAttempts: 5,
  reconnectDelay: 3000,
  pingInterval: 30000,
  silent: false,
  debug: false,
};

const WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/**
 * WebSocket client for Polymarket market data
 */
export class MarketWS {
  private ws: WebSocket | null = null;
  private subscriptions: Set<string> = new Set();
  private reconnectAttempts = 0;
  private isManuallyClosed = false;
  private pingInterval: NodeJS.Timeout | null = null;

  // Callbacks
  private onPriceUpdate: EventCallback;
  private onOrderBookUpdate: EventCallback;
  private onLastTrade: EventCallback;
  private onConnectionChange: ConnectionCallback;
  private onError: ErrorCallback;
  private config: WSConfig;

  constructor(
    onPriceUpdate: EventCallback = () => {},
    onOrderBookUpdate: EventCallback = () => {},
    onLastTrade: EventCallback = () => {},
    onConnectionChange: ConnectionCallback = () => {},
    onError: ErrorCallback = () => {},
    config: WSConfig = {},
  ) {
    this.onPriceUpdate = onPriceUpdate;
    this.onOrderBookUpdate = onOrderBookUpdate;
    this.onLastTrade = onLastTrade;
    this.onConnectionChange = onConnectionChange;
    this.onError = onError;
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Connect to Polymarket WebSocket
   */
  connect(): void {
    try {
      this.ws = new WebSocket(WS_URL);

      this.ws.onopen = () => {
        this.reconnectAttempts = 0;
        this.startPing();
        this.onConnectionChange(true);

        // Re-subscribe to any previous subscriptions after reconnection
        if (this.subscriptions.size > 0) {
          const tokens = Array.from(this.subscriptions);
          this.sendSubscription(tokens);
        }
      };

      this.ws.onmessage = (event) => {
        // Handle raw string messages like "PONG" first
        if (typeof event.data === "string") {
          const trimmed = event.data.trim();
          if (trimmed === "PONG") {
            return;
          }
        }

        try {
          const data = JSON.parse(event.data);
          this.handleMessage(data);
        } catch (error) {
          if (typeof event.data === "string") {
            return;
          }
          this.onError(
            error instanceof Error ? error : new Error(String(error)),
          );
        }
      };

      this.ws.onerror = () => {
        this.onError(new Error("WebSocket connection error"));
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
    if (typeof data === "object" && data !== null) {
      if (this.config.debug) {
        console.log(
          "DEBUG - Received event:",
          JSON.stringify(data, null, 2),
        );
      }

      if (Array.isArray(data)) {
        for (const item of data) {
          this.processEvent(item);
        }
      } else {
        this.processEvent(data);
      }
    }
  }

  /**
   * Process a single event
   */
  private processEvent(data: any): void {
    if (!data || !data.event_type) return;

    switch (data.event_type) {
      case "price_change":
        this.onPriceUpdate(data as PriceEvent);
        break;

      case "book":
        this.onOrderBookUpdate(data as OrderBookEvent);
        break;

      case "last_trade_price":
        this.onLastTrade(data as LastTradeEvent);
        break;

      case "subscription_confirmed":
        // Quietly handle subscription confirmations
        break;
    }
  }

  /**
   * Subscribe to specific token IDs
   */
  subscribe(tokenIds: string[]): void {
    if (!Array.isArray(tokenIds) || tokenIds.length === 0) {
      return;
    }

    tokenIds.forEach((id) => {
      if (id && typeof id === "string") {
        this.subscriptions.add(id);
      }
    });

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendSubscription(Array.from(this.subscriptions));
    }
  }

  /**
   * Send subscription message to WebSocket
   */
  private sendSubscription(tokenIds: string[]): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }

    const message = {
      type: "market",
      assets_ids: tokenIds,
    };

    try {
      this.ws.send(JSON.stringify(message));
    } catch (error) {
      if (!this.config.silent) {
        console.error("Failed to send subscription:", error);
      }
    }
  }

  /**
   * Unsubscribe from specific token IDs
   */
  unsubscribe(tokenIds: string[]): void {
    tokenIds.forEach((id) => this.subscriptions.delete(id));

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      const message = {
        assets_ids: tokenIds,
        operation: "unsubscribe",
      };

      try {
        this.ws.send(JSON.stringify(message));
        if (!this.config.silent) {
          console.log(`Unsubscribed from ${tokenIds.length} token(s)`);
        }
      } catch (error) {
        if (!this.config.silent) {
          console.error("Failed to send unsubscription:", error);
        }
      }
    }
  }

  /**
   * Start heartbeat (PING) to keep connection alive
   */
  private startPing(): void {
    this.stopPing();

    this.pingInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        try {
          this.ws.send("PING");
        } catch (error) {
          if (!this.config.silent) {
            console.error("Failed to send PING:", error);
          }
        }
      }
    }, this.config.pingInterval);
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
   * Attempt to reconnect after connection loss.
   * When reconnectAttempts is -1, retries indefinitely with exponential backoff.
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

    if (!this.config.silent) {
      const label = infinite ? "infinite" : `${this.reconnectAttempts}/${maxAttempts}`;
      console.log(`MarketWS reconnect ${label} in ${delay}ms`);
    }

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

    this.subscriptions.clear();
  }

  /**
   * Get current connection status
   */
  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  /**
   * Get current subscriptions
   */
  getSubscriptions(): string[] {
    return Array.from(this.subscriptions);
  }

  /**
   * Clear all subscriptions
   */
  clearSubscriptions(): void {
    const allSubs = Array.from(this.subscriptions);
    this.unsubscribe(allSubs);
    this.subscriptions.clear();
  }

  /**
   * Replace all current subscriptions with a new set of token IDs.
   * Unsubscribes removed tokens, subscribes new ones, avoids full reconnect.
   */
  replaceSubscriptions(newTokenIds: string[]): void {
    const newSet = new Set(newTokenIds.filter((id) => id && typeof id === "string"));
    const oldSet = this.subscriptions;

    const toRemove = Array.from(oldSet).filter((id) => !newSet.has(id));
    const toAdd = Array.from(newSet).filter((id) => !oldSet.has(id));

    if (toRemove.length > 0) {
      this.unsubscribe(toRemove);
    }
    if (toAdd.length > 0) {
      this.subscribe(toAdd);
    }
  }
}

/**
 * Trading bot integration wrapper
 */
export class TradingBotWS {
  private wsClient: MarketWS;
  private priceHistory: Map<string, number[]> = new Map();
  private position: Map<string, { side: "BUY" | "SELL"; size: number }> =
    new Map();
  private config: WSConfig;

  constructor(
    private buyThreshold: number = 0.5,
    private sellThreshold: number = 0.8,
    private maxPositionSize: number = 10,
    config: WSConfig = {},
  ) {
    this.config = config;
    this.wsClient = new MarketWS(
      this.handlePriceChange.bind(this),
      this.handleOrderBook.bind(this),
      this.handleLastTrade.bind(this),
      this.handleConnectionChange.bind(this),
      this.handleError.bind(this),
      config,
    );
  }

  private handlePriceChange(event: MarketEvent): void {
    if (event.event_type !== "price_change") return;
    if (!event.price_changes || event.price_changes.length === 0) return;

    const change = event.price_changes[0];
    if (!change) return;
    const tokenId = change.asset_id;
    const price = parseFloat(change.price);

    if (!this.priceHistory.has(tokenId)) {
      this.priceHistory.set(tokenId, []);
    }
    const history = this.priceHistory.get(tokenId)!;
    history.push(price);

    if (history.length > 100) {
      history.shift();
    }

    this.evaluateTradeOpportunity(tokenId, price);
  }

  private handleOrderBook(event: MarketEvent): void {
    // Could add logic to detect large orders or liquidity shifts
  }

  private handleLastTrade(event: MarketEvent): void {
    // Handle last trade price updates
  }

  private handleConnectionChange(connected: boolean): void {
    if (!connected && !this.config.silent) {
      console.log(`🔌 Connection status: OFFLINE`);
    }
  }

  private handleError(error: Error): void {
    console.error(`💥 Trading bot error: ${error.message}`);
  }

  private evaluateTradeOpportunity(
    tokenId: string,
    currentPrice: number,
  ): void {
    const currentPosition = this.position.get(tokenId);

    if (!currentPosition && currentPrice < this.buyThreshold) {
      if (!this.config.silent) {
        console.log(
          `🟢 BUY SIGNAL: ${tokenId.substring(0, 8)}... @ $${currentPrice.toFixed(4)}`,
        );
      }
    }

    if (currentPosition?.side === "BUY" && currentPrice > this.sellThreshold) {
      if (!this.config.silent) {
        console.log(
          `🔴 SELL SIGNAL: ${tokenId.substring(0, 8)}... @ $${currentPrice.toFixed(4)}`,
        );
      }
    }
  }

  public connect() {
    this.wsClient.connect();
  }

  public subscribe(tokenIds: string[]) {
    this.wsClient.subscribe(tokenIds);
  }

  public disconnect() {
    this.wsClient.disconnect();
  }

  public getAveragePrice(tokenId: string): number {
    const history = this.priceHistory.get(tokenId);
    if (!history || history.length === 0) return 0;
    return history.reduce((sum, price) => sum + price, 0) / history.length;
  }

  public getPriceHistory(tokenId: string): number[] {
    return this.priceHistory.get(tokenId) || [];
  }
}
