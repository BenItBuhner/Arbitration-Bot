import { describe, expect, it } from "bun:test";
import { parseArgs, normalizeMode } from "../src/cli/parse-args";

describe("normalizeMode", () => {
  it("normalizes standard mode names", () => {
    expect(normalizeMode("fake-trade")).toBe("fake-trade");
    expect(normalizeMode("watch-market")).toBe("watch-market");
    expect(normalizeMode("cross-platform-analysis")).toBe("cross-platform-analysis");
    expect(normalizeMode("price-diff-detection")).toBe("price-diff-detection");
    expect(normalizeMode("backtest")).toBe("backtest");
  });

  it("normalizes aliases", () => {
    expect(normalizeMode("fake")).toBe("fake-trade");
    expect(normalizeMode("arb")).toBe("fake-trade");
    expect(normalizeMode("arbitrage")).toBe("fake-trade");
    expect(normalizeMode("watch")).toBe("watch-market");
    expect(normalizeMode("cross-platform")).toBe("cross-platform-analysis");
    expect(normalizeMode("analysis")).toBe("cross-platform-analysis");
    expect(normalizeMode("price-diff")).toBe("price-diff-detection");
    expect(normalizeMode("diff")).toBe("price-diff-detection");
    expect(normalizeMode("historical")).toBe("backtest");
  });

  it("is case-insensitive", () => {
    expect(normalizeMode("FAKE-TRADE")).toBe("fake-trade");
    expect(normalizeMode("Backtest")).toBe("backtest");
  });

  it("handles underscores", () => {
    expect(normalizeMode("fake_trade")).toBe("fake-trade");
    expect(normalizeMode("watch_market")).toBe("watch-market");
  });

  it("returns undefined for invalid modes", () => {
    expect(normalizeMode("invalid")).toBeUndefined();
    expect(normalizeMode("")).toBeUndefined();
    expect(normalizeMode(undefined)).toBeUndefined();
  });
});

describe("parseArgs", () => {
  it("parses --mode flag", () => {
    const args = parseArgs(["--mode", "fake-trade"]);
    expect(args.mode).toBe("fake-trade");
  });

  it("parses --mode=value format", () => {
    const args = parseArgs(["--mode=fake-trade"]);
    expect(args.mode).toBe("fake-trade");
  });

  it("parses shortcut flags", () => {
    expect(parseArgs(["--fake-trade"]).mode).toBe("fake-trade");
    expect(parseArgs(["--watch-market"]).mode).toBe("watch-market");
    expect(parseArgs(["--backtest"]).mode).toBe("backtest");
    expect(parseArgs(["--cross-platform-analysis"]).mode).toBe("cross-platform-analysis");
    expect(parseArgs(["--price-diff-detection"]).mode).toBe("price-diff-detection");
  });

  it("parses --profiles", () => {
    const args = parseArgs(["--profiles", "arbBotV1,testProfile"]);
    expect(args.profiles).toEqual(["arbBotV1", "testProfile"]);
  });

  it("parses --coins", () => {
    const args = parseArgs(["--coins", "eth,btc,sol"]);
    expect(args.coins).toEqual(["eth", "btc", "sol"]);
  });

  it("parses --headless", () => {
    const args = parseArgs(["--headless"]);
    expect(args.headless).toBe(true);
  });

  it("parses --validate", () => {
    const args = parseArgs(["--validate"]);
    expect(args.validate).toBe(true);
  });

  it("parses --check (alias for --validate)", () => {
    const args = parseArgs(["--check"]);
    expect(args.validate).toBe(true);
  });

  it("parses --auto", () => {
    const args = parseArgs(["--auto"]);
    expect(args.auto).toBe(true);
  });

  it("parses --help", () => {
    const args = parseArgs(["--help"]);
    expect(args.help).toBe(true);
  });

  it("parses -h", () => {
    const args = parseArgs(["-h"]);
    expect(args.help).toBe(true);
  });

  it("parses --speed", () => {
    const args = parseArgs(["--speed", "5"]);
    expect(args.speed).toBe(5);
  });

  it("parses --speed=max", () => {
    const args = parseArgs(["--speed=max"]);
    expect(args.speed).toBe(0);
  });

  it("parses --fill-usd", () => {
    const args = parseArgs(["--fill-usd", "500"]);
    expect(args.fillUsd).toBe(500);
  });

  it("parses --realistic-fill and --no-realistic-fill", () => {
    expect(parseArgs(["--realistic-fill"]).realisticFill).toBe(true);
    expect(parseArgs(["--no-realistic-fill"]).realisticFill).toBe(false);
  });

  it("parses --provider", () => {
    expect(parseArgs(["--provider", "kalshi"]).provider).toBe("kalshi");
    expect(parseArgs(["--kalshi"]).provider).toBe("kalshi");
    expect(parseArgs(["--polymarket"]).provider).toBe("polymarket");
  });

  it("parses complex combination of args", () => {
    const args = parseArgs([
      "--mode", "fake-trade",
      "--profiles", "arbBotV1",
      "--coins", "eth,btc",
      "--headless",
      "--auto",
    ]);
    expect(args.mode).toBe("fake-trade");
    expect(args.profiles).toEqual(["arbBotV1"]);
    expect(args.coins).toEqual(["eth", "btc"]);
    expect(args.headless).toBe(true);
    expect(args.auto).toBe(true);
  });

  it("handles empty argv", () => {
    const args = parseArgs([]);
    expect(args.mode).toBeUndefined();
    expect(args.profiles).toBeUndefined();
    expect(args.coins).toBeUndefined();
  });

  it("parses --backtest-mode", () => {
    expect(parseArgs(["--fast"]).backtestMode).toBe("fast");
    expect(parseArgs(["--visual"]).backtestMode).toBe("visual");
    expect(parseArgs(["--backtest-mode", "fast"]).backtestMode).toBe("fast");
  });

  it("parses --market / --search", () => {
    const args = parseArgs(["--market", "bitcoin"]);
    expect(args.market).toBe("bitcoin");
  });

  it("parses --headless-summary (implies headless)", () => {
    const args = parseArgs(["--headless-summary"]);
    expect(args.headless).toBe(true);
    expect(args.headlessSummary).toBe(true);
  });
});
