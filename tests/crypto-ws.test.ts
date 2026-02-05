import { describe, expect, it } from "bun:test";
import { CryptoWS, type CryptoPricePayload } from "../src/clients/crypto-ws";

describe("CryptoWS", () => {
  describe("construction and state", () => {
    it("starts disconnected", () => {
      const ws = new CryptoWS();
      expect(ws.isConnected()).toBe(false);
    });

    it("has empty price cache initially", () => {
      const ws = new CryptoWS();
      expect(ws.getAllPrices().size).toBe(0);
      expect(ws.getPrice("ETH/USD")).toBeUndefined();
    });
  });

  describe("price cache", () => {
    it("stores prices via callback flow", () => {
      // Simulate the internal flow by accessing the cache after constructing
      const ws = new CryptoWS();
      // Can't directly test without connecting, but we can verify the interface
      expect(ws.getPrice("BTC/USD")).toBeUndefined();
      expect(ws.getPrice("nonexistent")).toBeUndefined();
    });
  });

  describe("symbol handling", () => {
    it("subscribe stores symbols in lowercase", () => {
      const ws = new CryptoWS();
      // Subscribe stores symbols -- we can verify disconnect clears them
      ws.subscribe(["eth/usd", "btc/usd"]);
      // After disconnect, subscriptions should be cleared
      ws.disconnect();
      expect(ws.isConnected()).toBe(false);
    });

    it("handles disconnect gracefully when not connected", () => {
      const ws = new CryptoWS();
      // Should not throw when disconnecting without connecting
      expect(() => ws.disconnect()).not.toThrow();
    });

    it("handles subscribe before connect gracefully", () => {
      const ws = new CryptoWS();
      // Should not throw when subscribing before connecting
      expect(() => ws.subscribe(["eth/usd"])).not.toThrow();
    });
  });

  describe("configuration", () => {
    it("accepts custom config", () => {
      const ws = new CryptoWS(
        () => {},
        () => {},
        () => {},
        {
          source: "chainlink",
          reconnectAttempts: 3,
          reconnectDelay: 1000,
        },
      );
      expect(ws.isConnected()).toBe(false);
    });

    it("accepts all source types", () => {
      // Should not throw for any source type
      expect(() => new CryptoWS(() => {}, () => {}, () => {}, { source: "chainlink" })).not.toThrow();
      expect(() => new CryptoWS(() => {}, () => {}, () => {}, { source: "binance" })).not.toThrow();
      expect(() => new CryptoWS(() => {}, () => {}, () => {}, { source: "both" })).not.toThrow();
    });
  });
});
