import { openSync, readSync, closeSync } from "fs";

type ParseFn<T> = (line: string) => T | null;
type ErrorFn = (error: Error, line: string) => void;

interface JsonlStreamOptions<T> {
  chunkSize?: number;
  bufferLines?: number;
  parse?: ParseFn<T>;
  onError?: ErrorFn;
}

export class JsonlSyncReader<T> {
  private fd: number | null;
  private position = 0;
  private leftover = "";
  private buffer: T[] = [];
  private eof = false;
  private errorLogged = false;
  private readonly chunkSize: number;
  private readonly bufferLines: number;
  private readonly parseLine: ParseFn<T>;
  private readonly onError?: ErrorFn;
  private readonly chunkBuffer: Buffer;

  constructor(filePath: string, options: JsonlStreamOptions<T> = {}) {
    this.fd = openSync(filePath, "r");
    this.chunkSize = Math.max(4096, options.chunkSize ?? 1 << 20);
    this.bufferLines = Math.max(1, options.bufferLines ?? 2000);
    this.parseLine =
      options.parse ??
      ((line: string) => {
        return JSON.parse(line) as T;
      });
    this.onError = options.onError;
    this.chunkBuffer = Buffer.alloc(this.chunkSize);
  }

  peek(): T | null {
    this.fillBuffer(1);
    return this.buffer.length > 0 ? this.buffer[0]! : null;
  }

  shift(): T | null {
    this.fillBuffer(1);
    return this.buffer.length > 0 ? this.buffer.shift()! : null;
  }

  close(): void {
    if (this.fd !== null) {
      closeSync(this.fd);
      this.fd = null;
    }
  }

  private fillBuffer(minLines: number): void {
    if (this.eof || this.fd === null) return;
    while (this.buffer.length < Math.max(minLines, this.bufferLines) && !this.eof) {
      const bytesRead = readSync(
        this.fd,
        this.chunkBuffer,
        0,
        this.chunkBuffer.length,
        this.position,
      );
      if (bytesRead <= 0) {
        this.eof = true;
        this.flushLeftover();
        return;
      }
      const chunk = this.chunkBuffer.subarray(0, bytesRead).toString("utf8");
      this.position += bytesRead;
      this.consumeChunk(chunk);
    }
  }

  private consumeChunk(chunk: string): void {
    const combined = this.leftover + chunk;
    const lines = combined.split(/\r?\n/);
    this.leftover = lines.pop() ?? "";
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      this.pushParsed(trimmed);
    }
  }

  private flushLeftover(): void {
    const trimmed = this.leftover.trim();
    if (!trimmed) return;
    this.pushParsed(trimmed);
    this.leftover = "";
  }

  private pushParsed(line: string): void {
    try {
      const parsed = this.parseLine(line);
      if (parsed !== null) {
        this.buffer.push(parsed);
      }
    } catch (error) {
      if (this.onError && !this.errorLogged) {
        this.errorLogged = true;
        const err = error instanceof Error ? error : new Error("JSONL parse failed.");
        this.onError(err, line);
      }
    }
  }
}
