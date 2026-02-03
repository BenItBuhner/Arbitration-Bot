import { readFileSync } from "fs";

declare const self: any;

type JsonlWorkerRequest = {
  id: number;
  filePath: string;
  sortByTimestamp?: boolean;
};

type JsonlWorkerResponse =
  | { id: number; ok: true; records: unknown[] }
  | { id: number; ok: false; error: string };

const ctx: any = self as any;

function parseJsonlFile(filePath: string): unknown[] {
  const raw = readFileSync(filePath, "utf8");
  const lines = raw.split(/\r?\n/);
  const records: unknown[] = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    records.push(JSON.parse(trimmed));
  }
  return records;
}

function sortByTimestamp(records: unknown[]): void {
  records.sort((a, b) => {
    const at = (a as { timestamp?: number }).timestamp ?? 0;
    const bt = (b as { timestamp?: number }).timestamp ?? 0;
    return at - bt;
  });
}

ctx.onmessage = (event: MessageEvent<JsonlWorkerRequest>) => {
  const { id, filePath, sortByTimestamp: shouldSort } = event.data;
  try {
    const records = parseJsonlFile(filePath);
    if (shouldSort) {
      sortByTimestamp(records);
    }
    const response: JsonlWorkerResponse = { id, ok: true, records };
    ctx.postMessage(response);
  } catch (error) {
    const message = error instanceof Error ? error.message : "JSONL parse failed.";
    const response: JsonlWorkerResponse = { id, ok: false, error: message };
    ctx.postMessage(response);
  }
};
