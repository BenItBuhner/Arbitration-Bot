let tuiEnabled = false;
let cleanupRegistered = false;

function parseEnvFlag(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const normalized = raw.trim().toLowerCase();
  if (["false", "0", "off", "no"].includes(normalized)) return false;
  if (["true", "1", "on", "yes"].includes(normalized)) return true;
  return defaultValue;
}

function shouldEnableTui(): boolean {
  if (!process.stdout.isTTY) return false;
  return parseEnvFlag("TUI_ALT_SCREEN", true);
}

function cleanupTui(): void {
  if (!tuiEnabled) return;
  tuiEnabled = false;
  if (process.stdout.isTTY) {
    process.stdout.write("\x1b[?25h\x1b[?1049l");
  }
}

export function enableTui(): void {
  if (tuiEnabled || !shouldEnableTui()) return;
  tuiEnabled = true;
  process.stdout.write("\x1b[?1049h\x1b[?25l");

  if (!cleanupRegistered) {
    cleanupRegistered = true;
    process.on("exit", cleanupTui);
    process.on("SIGINT", cleanupTui);
    process.on("SIGTERM", cleanupTui);
    process.on("SIGHUP", cleanupTui);
  }
}

export function disableTui(): void {
  cleanupTui();
}
