import { appendFileSync, readFileSync, writeFileSync, mkdirSync } from "fs";
import { dirname } from "path";

export function readJsonlFile<T>(filePath: string): T[] {
  const raw = readFileSync(filePath, "utf8");
  const lines = raw.split(/\r?\n/);
  const records: T[] = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    records.push(JSON.parse(trimmed) as T);
  }

  return records;
}

export function writeJsonlLines<T>(
  filePath: string,
  items: T[],
  options: { append?: boolean } = {},
): void {
  if (items.length === 0) return;
  const dir = dirname(filePath);
  if (dir && dir !== "." && dir !== "\\") {
    mkdirSync(dir, { recursive: true });
  }
  const lines = items.map((item) => JSON.stringify(item)).join("\n") + "\n";
  if (options.append) {
    appendFileSync(filePath, lines, { encoding: "utf8" });
  } else {
    writeFileSync(filePath, lines, { encoding: "utf8" });
  }
}
