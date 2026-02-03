import { readFileSync } from "fs";
import {
  constants,
  createPrivateKey,
  createSign,
  KeyObject,
} from "crypto";

export interface KalshiSignatureInput {
  privateKeyPem: string;
  timestampMs: number;
  method: string;
  path: string;
}

export interface KalshiAuthHeadersInput {
  apiKey: string;
  privateKeyPem: string;
  method: string;
  path: string;
  timestampMs?: number;
}

export function loadPrivateKeyPem(path: string): string {
  return readFileSync(path, "utf-8");
}

function normalizePath(path: string): string {
  if (!path) return "/";
  const trimmed = path.trim();
  const noQuery = trimmed.split("?")[0] ?? trimmed;
  return noQuery.startsWith("/") ? noQuery : `/${noQuery}`;
}

function buildSignatureMessage(
  timestampMs: number,
  method: string,
  path: string,
): string {
  const normalizedMethod = method.trim().toUpperCase();
  const normalizedPath = normalizePath(path);
  return `${timestampMs}${normalizedMethod}${normalizedPath}`;
}

function getKeyObject(privateKeyPem: string): KeyObject {
  return createPrivateKey({
    key: privateKeyPem,
  });
}

export function signKalshiRequest({
  privateKeyPem,
  timestampMs,
  method,
  path,
}: KalshiSignatureInput): string {
  const signer = createSign("RSA-SHA256");
  const message = buildSignatureMessage(timestampMs, method, path);
  signer.update(message);
  signer.end();
  const signature = signer.sign({
    key: getKeyObject(privateKeyPem),
    padding: constants.RSA_PKCS1_PSS_PADDING,
    saltLength: constants.RSA_PSS_SALTLEN_DIGEST,
  });
  return signature.toString("base64");
}

export function createKalshiAuthHeaders({
  apiKey,
  privateKeyPem,
  method,
  path,
  timestampMs,
}: KalshiAuthHeadersInput): Record<string, string> {
  const ts = timestampMs ?? Date.now();
  const signature = signKalshiRequest({
    privateKeyPem,
    timestampMs: ts,
    method,
    path,
  });

  return {
    "Content-Type": "application/json",
    "KALSHI-ACCESS-KEY": apiKey,
    "KALSHI-ACCESS-SIGNATURE": signature,
    "KALSHI-ACCESS-TIMESTAMP": String(ts),
  };
}

export function buildKalshiSignatureMessage(
  timestampMs: number,
  method: string,
  path: string,
): string {
  return buildSignatureMessage(timestampMs, method, path);
}
