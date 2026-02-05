import { describe, expect, it } from "bun:test";
import { normalizeCoinKey, sanitizeProfileName, stripJsonComments } from "../src/services/profile-config";

describe("normalizeCoinKey", () => {
  it("normalizes standard coin keys", () => {
    expect(normalizeCoinKey("eth")).toBe("eth");
    expect(normalizeCoinKey("btc")).toBe("btc");
    expect(normalizeCoinKey("sol")).toBe("sol");
    expect(normalizeCoinKey("xrp")).toBe("xrp");
  });

  it("normalizes full names", () => {
    expect(normalizeCoinKey("ethereum")).toBe("eth");
    expect(normalizeCoinKey("bitcoin")).toBe("btc");
    expect(normalizeCoinKey("solana")).toBe("sol");
    expect(normalizeCoinKey("ripple")).toBe("xrp");
  });

  it("is case-insensitive", () => {
    expect(normalizeCoinKey("ETH")).toBe("eth");
    expect(normalizeCoinKey("BTC")).toBe("btc");
    expect(normalizeCoinKey("Bitcoin")).toBe("btc");
    expect(normalizeCoinKey("ETHEREUM")).toBe("eth");
  });

  it("trims whitespace", () => {
    expect(normalizeCoinKey("  eth  ")).toBe("eth");
    expect(normalizeCoinKey(" btc")).toBe("btc");
  });

  it("returns null for unknown coins", () => {
    expect(normalizeCoinKey("doge")).toBeNull();
    expect(normalizeCoinKey("avax")).toBeNull();
    expect(normalizeCoinKey("")).toBeNull();
    expect(normalizeCoinKey("   ")).toBeNull();
  });
});

describe("sanitizeProfileName", () => {
  it("preserves alphanumeric and dash/underscore", () => {
    expect(sanitizeProfileName("arbBotV1")).toBe("arbBotV1");
    expect(sanitizeProfileName("my-profile_2")).toBe("my-profile_2");
  });

  it("replaces special characters with underscore", () => {
    expect(sanitizeProfileName("my profile!")).toBe("my_profile_");
    expect(sanitizeProfileName("test@bot#1")).toBe("test_bot_1");
  });
});

describe("stripJsonComments", () => {
  it("strips line comments", () => {
    const input = '{\n  "key": "value" // this is a comment\n}';
    const result = stripJsonComments(input);
    expect(JSON.parse(result)).toEqual({ key: "value" });
  });

  it("strips block comments", () => {
    const input = '{\n  /* comment */\n  "key": "value"\n}';
    const result = stripJsonComments(input);
    expect(JSON.parse(result)).toEqual({ key: "value" });
  });

  it("preserves strings containing comment-like content", () => {
    const input = '{\n  "url": "https://example.com" // real comment\n}';
    const result = stripJsonComments(input);
    expect(JSON.parse(result)).toEqual({ url: "https://example.com" });
  });

  it("handles empty input", () => {
    expect(stripJsonComments("")).toBe("");
  });

  it("handles input with no comments", () => {
    const input = '{"key": "value"}';
    expect(stripJsonComments(input)).toBe(input);
  });
});
