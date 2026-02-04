import { appendFileSync, mkdirSync, existsSync } from "fs";
import { dirname } from "path";

type LogLevel = "INFO" | "WARN" | "ERROR";

interface RunLoggerOptions {
  stdout?: boolean;
}

export class RunLogger {
  private lines: string[] = [];
  private maxLines: number;
  private logPath: string;
  private alsoStdout: boolean;

  constructor(logPath: string, maxLines: number = 200, options?: RunLoggerOptions) {
    this.logPath = logPath;
    this.maxLines = maxLines;
    this.alsoStdout = options?.stdout === true;
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
    appendFileSync(this.logPath, line + "\n", { encoding: "utf8" });
    if (this.alsoStdout) {
      process.stdout.write(line + "\n");
    }
  }
}
