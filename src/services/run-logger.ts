import { appendFileSync, mkdirSync, existsSync, statSync, renameSync } from "fs";
import { dirname } from "path";

type LogLevel = "INFO" | "WARN" | "ERROR";

const DEFAULT_MAX_FILE_BYTES = 10 * 1024 * 1024; // 10 MB
const ROTATION_CHECK_INTERVAL = 500; // check every N writes

interface RunLoggerOptions {
  stdout?: boolean;
  maxFileBytes?: number;
}

export class RunLogger {
  private lines: string[] = [];
  private maxLines: number;
  private logPath: string;
  private alsoStdout: boolean;
  private maxFileBytes: number;
  private writeCount: number = 0;

  constructor(logPath: string, maxLines: number = 200, options?: RunLoggerOptions) {
    this.logPath = logPath;
    this.maxLines = maxLines;
    this.alsoStdout = options?.stdout === true;
    this.maxFileBytes = options?.maxFileBytes ?? DEFAULT_MAX_FILE_BYTES;
    this.ensureDir();
    this.writeLine(`--- Run started ${new Date().toISOString()} ---`);
  }

  log(message: string, level: LogLevel = "INFO"): void {
    const ts = new Date().toISOString();
    const line = `[${ts}] [${level}] ${message}`;
    this.lines.push(line);
    if (this.lines.length > this.maxLines) {
      this.lines.shift();
    }
    this.writeLine(line);
  }

  getRecentLines(limit?: number): string[] {
    const cap = typeof limit === "number" ? limit : this.maxLines;
    if (cap <= 0) return [];
    return this.lines.slice(-cap);
  }

  clearRecentLines(): void {
    this.lines = [];
  }

  private ensureDir(): void {
    const dir = dirname(this.logPath);
    if (!dir || dir === "." || dir === "\\") {
      return;
    }
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  private writeLine(line: string): void {
    try {
      appendFileSync(this.logPath, line + "\n", { encoding: "utf8" });
    } catch {
      // If write fails (disk full, permissions), don't crash the bot
    }
    if (this.alsoStdout) {
      process.stdout.write(line + "\n");
    }
    this.writeCount += 1;
    if (this.writeCount % ROTATION_CHECK_INTERVAL === 0) {
      this.maybeRotate();
    }
  }

  private maybeRotate(): void {
    try {
      if (!existsSync(this.logPath)) return;
      const stat = statSync(this.logPath);
      if (stat.size < this.maxFileBytes) return;

      // Rotate: current → .1, delete .2 if exists
      const rotatedPath = `${this.logPath}.1`;
      const oldRotated = `${this.logPath}.2`;
      try {
        if (existsSync(oldRotated)) {
          // Best-effort delete of oldest rotation
          require("fs").unlinkSync(oldRotated);
        }
      } catch { /* ignore */ }
      try {
        if (existsSync(rotatedPath)) {
          renameSync(rotatedPath, oldRotated);
        }
      } catch { /* ignore */ }
      try {
        renameSync(this.logPath, rotatedPath);
      } catch { /* ignore */ }

      // Start fresh
      this.writeLine(`--- Log rotated ${new Date().toISOString()} ---`);
    } catch {
      // Rotation is best-effort; never crash the bot for logging
    }
  }
}
