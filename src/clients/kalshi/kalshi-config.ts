export interface KalshiEnvConfig {
  apiKey: string;
  privateKeyPath?: string;
  privateKeyPem?: string;
  baseUrl: string;
  wsUrl: string;
}

function stripEnvValue(value: string | undefined): string | undefined {
  if (!value) return undefined;
  let trimmed = value.trim();
  if (!trimmed) return undefined;
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed[trimmed.length - 1];
    if ((first === "\"" && last === "\"") || (first === "'" && last === "'")) {
      trimmed = trimmed.slice(1, -1).trim();
    }
  }
  const lowered = trimmed.toLowerCase();
  if (lowered === "null" || lowered === "undefined") return undefined;
  return trimmed;
}

export const KALSHI_DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2";
export const KALSHI_DEMO_WS = "wss://demo-api.kalshi.co/trade-api/ws/v2";
export const KALSHI_PROD_BASE = "https://api.elections.kalshi.com/trade-api/v2";
export const KALSHI_PROD_WS = "wss://api.elections.kalshi.com/trade-api/ws/v2";

function resolveDefaultUrls(env: NodeJS.ProcessEnv): {
  baseUrl: string;
  wsUrl: string;
} {
  const envName = env.KALSHI_ENV?.trim().toLowerCase();
  const baseUrl = stripEnvValue(env.KALSHI_BASE_URL);
  const wsUrl = stripEnvValue(env.KALSHI_WS_URL);

  if (baseUrl || wsUrl) {
    return {
      baseUrl: baseUrl ?? KALSHI_DEMO_BASE,
      wsUrl: wsUrl ?? KALSHI_DEMO_WS,
    };
  }

  if (envName === "prod" || envName === "production") {
    return { baseUrl: KALSHI_PROD_BASE, wsUrl: KALSHI_PROD_WS };
  }

  return { baseUrl: KALSHI_DEMO_BASE, wsUrl: KALSHI_DEMO_WS };
}

export function getKalshiEnvConfig(
  env: NodeJS.ProcessEnv = process.env,
): KalshiEnvConfig {
  const apiKey = stripEnvValue(env.KALSHI_API_KEY);
  const privateKeyPath = stripEnvValue(env.KALSHI_PRIVATE_KEY_PATH);
  const privateKeyPem = stripEnvValue(env.KALSHI_PRIVATE_KEY_PEM);
  const { baseUrl, wsUrl } = resolveDefaultUrls(env);

  if (!apiKey) {
    throw new Error("Missing KALSHI_API_KEY environment variable.");
  }
  if (!privateKeyPath && !privateKeyPem) {
    throw new Error(
      "Missing Kalshi private key. Set KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY_PEM.",
    );
  }

  return {
    apiKey,
    privateKeyPath,
    privateKeyPem,
    baseUrl,
    wsUrl,
  };
}
