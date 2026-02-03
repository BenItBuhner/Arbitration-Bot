import { describe, expect, it } from "bun:test";
import {
  buildKalshiSignatureMessage,
  createKalshiAuthHeaders,
  signKalshiRequest,
} from "../../src/clients/kalshi/kalshi-auth";
import { constants, generateKeyPairSync, verify } from "crypto";

describe("kalshi-auth", () => {
  it("signs and verifies RSA-PSS signatures", () => {
    const { privateKey, publicKey } = generateKeyPairSync("rsa", {
      modulusLength: 2048,
      publicKeyEncoding: { type: "spki", format: "pem" },
      privateKeyEncoding: { type: "pkcs1", format: "pem" },
    });

    const timestamp = 1710000000000;
    const message = buildKalshiSignatureMessage(
      timestamp,
      "GET",
      "/trade-api/ws/v2",
    );
    const signature = signKalshiRequest({
      privateKeyPem: privateKey,
      timestampMs: timestamp,
      method: "GET",
      path: "/trade-api/ws/v2",
    });

    const isValid = verify(
      "RSA-SHA256",
      Buffer.from(message),
      {
        key: publicKey,
        padding: constants.RSA_PKCS1_PSS_PADDING,
        saltLength: constants.RSA_PSS_SALTLEN_DIGEST,
      },
      Buffer.from(signature, "base64"),
    );

    expect(isValid).toBe(true);
  });

  it("creates auth headers with required keys", () => {
    const { privateKey } = generateKeyPairSync("rsa", {
      modulusLength: 2048,
      publicKeyEncoding: { type: "spki", format: "pem" },
      privateKeyEncoding: { type: "pkcs1", format: "pem" },
    });

    const headers = createKalshiAuthHeaders({
      apiKey: "test-key",
      privateKeyPem: privateKey,
      method: "GET",
      path: "/trade-api/ws/v2",
      timestampMs: 1710000000000,
    });

    expect(headers["KALSHI-ACCESS-KEY"]).toBe("test-key");
    expect(headers["KALSHI-ACCESS-SIGNATURE"]).toBeDefined();
    expect(headers["KALSHI-ACCESS-TIMESTAMP"]).toBe("1710000000000");
  });
});
