import {
  closeSync,
  openSync,
  renameSync,
  unlinkSync,
  existsSync,
  writeSync,
} from "fs";
import type { BacktestCryptoTick, BacktestTradeEvent } from "./types";
import { JsonlSyncReader } from "./jsonl-stream";
import { compareTrades } from "./trade-utils";

interface MergeResult {
  count: number;
  minTs: number | null;
  maxTs: number | null;
}

function writeLine(fd: number, value: unknown): void {
  const line = JSON.stringify(value) + "\n";
  const buffer = Buffer.from(line);
  let offset = 0;
  while (offset < buffer.length) {
    const written = writeSync(fd, buffer, offset, buffer.length - offset);
    offset += written;
  }
}

function createTradeReader(filePath: string): JsonlSyncReader<BacktestTradeEvent> {
  return new JsonlSyncReader<BacktestTradeEvent>(filePath, {
    bufferLines: 2000,
    parse: (line) => {
      const record = JSON.parse(line) as BacktestTradeEvent;
      if (!record || typeof record !== "object") return null;
      const timestamp = Number(record.timestamp);
      if (!Number.isFinite(timestamp)) return null;
      if (record.timestamp !== timestamp) record.timestamp = timestamp;
      return record;
    },
  });
}

function createTickReader(filePath: string): JsonlSyncReader<BacktestCryptoTick> {
  return new JsonlSyncReader<BacktestCryptoTick>(filePath, {
    bufferLines: 5000,
    parse: (line) => {
      const record = JSON.parse(line) as BacktestCryptoTick;
      if (!record || typeof record !== "object") return null;
      const timestamp = Number(record.timestamp);
      if (!Number.isFinite(timestamp)) return null;
      const value = Number(record.value);
      if (!Number.isFinite(value)) return null;
      if (record.timestamp !== timestamp) record.timestamp = timestamp;
      if (record.value !== value) record.value = value;
      return record;
    },
  });
}

function tradeKey(trade: BacktestTradeEvent): string {
  return [
    trade.timestamp,
    trade.tokenId,
    trade.price,
    trade.size,
    trade.side ?? "",
    trade.tradeId ?? "",
    trade.takerOrderId ?? "",
  ].join("|");
}

function tickKey(tick: BacktestCryptoTick): string {
  return `${tick.timestamp}|${tick.symbol}`;
}

export function mergeSortedTradeFiles(
  prevPath: string,
  nextPath: string,
  outPath: string,
): MergeResult {
  const outTemp = `${outPath}.merge.tmp`;
  const outFd = openSync(outTemp, "w");
  const left = createTradeReader(prevPath);
  const right = createTradeReader(nextPath);

  let leftTrade = left.peek();
  let rightTrade = right.peek();
  let lastKey: string | null = null;
  let count = 0;
  let minTs: number | null = null;
  let maxTs: number | null = null;

  while (leftTrade || rightTrade) {
    let takeLeft = false;
    if (!rightTrade) {
      takeLeft = true;
    } else if (!leftTrade) {
      takeLeft = false;
    } else {
      takeLeft = compareTrades(leftTrade, rightTrade) <= 0;
    }

    const trade = takeLeft ? leftTrade! : rightTrade!;
    const key = tradeKey(trade);
    if (key !== lastKey) {
      writeLine(outFd, trade);
      lastKey = key;
      count += 1;
      minTs = minTs === null ? trade.timestamp : Math.min(minTs, trade.timestamp);
      maxTs = maxTs === null ? trade.timestamp : Math.max(maxTs, trade.timestamp);
    }

    if (takeLeft) {
      left.shift();
      leftTrade = left.peek();
    } else {
      right.shift();
      rightTrade = right.peek();
    }
  }

  left.close();
  right.close();
  closeSync(outFd);

  if (existsSync(outPath)) {
    unlinkSync(outPath);
  }
  renameSync(outTemp, outPath);

  return { count, minTs, maxTs };
}

export function mergeSortedTickFiles(
  prevPath: string,
  nextPath: string,
  outPath: string,
): MergeResult {
  const outTemp = `${outPath}.merge.tmp`;
  const outFd = openSync(outTemp, "w");
  const left = createTickReader(prevPath);
  const right = createTickReader(nextPath);

  let leftTick = left.peek();
  let rightTick = right.peek();
  let lastKey: string | null = null;
  let count = 0;
  let minTs: number | null = null;
  let maxTs: number | null = null;

  while (leftTick || rightTick) {
    let takeLeft = false;
    if (!rightTick) {
      takeLeft = true;
    } else if (!leftTick) {
      takeLeft = false;
    } else {
      takeLeft = leftTick.timestamp <= rightTick.timestamp;
    }

    const tick = takeLeft ? leftTick! : rightTick!;
    const key = tickKey(tick);
    if (key !== lastKey) {
      writeLine(outFd, tick);
      lastKey = key;
      count += 1;
      minTs = minTs === null ? tick.timestamp : Math.min(minTs, tick.timestamp);
      maxTs = maxTs === null ? tick.timestamp : Math.max(maxTs, tick.timestamp);
    }

    if (takeLeft) {
      left.shift();
      leftTick = left.peek();
    } else {
      right.shift();
      rightTick = right.peek();
    }
  }

  left.close();
  right.close();
  closeSync(outFd);

  if (existsSync(outPath)) {
    unlinkSync(outPath);
  }
  renameSync(outTemp, outPath);

  return { count, minTs, maxTs };
}
