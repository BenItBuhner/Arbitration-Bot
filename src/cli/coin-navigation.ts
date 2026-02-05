export function setupCoinNavigation(
  coinCount: number,
  getIndex: () => number,
  setIndex: (nextIndex: number) => void,
): () => void {
  let keyBuffer = Buffer.alloc(0);

  const handleData = (data: Buffer | string) => {
    const keyStr = typeof data === "string" ? data : data.toString();
    keyBuffer = Buffer.concat([
      keyBuffer,
      typeof data === "string" ? Buffer.from(data) : data,
    ]);

    if (keyBuffer[0] == 0x1b) {
      if (keyBuffer.length >= 3 && keyBuffer[1] == 0x5b) {
        if (keyBuffer[2] == 0x43) {
          const next = (getIndex() + 1) % coinCount;
          setIndex(next);
          keyBuffer = Buffer.alloc(0);
          return;
        }
        if (keyBuffer[2] == 0x44) {
          const current = getIndex();
          const next = current === 0 ? coinCount - 1 : current - 1;
          setIndex(next);
          keyBuffer = Buffer.alloc(0);
          return;
        }
      }
      if (keyBuffer.length > 10) {
        keyBuffer = Buffer.alloc(0);
      }
      return;
    }

    keyBuffer = Buffer.alloc(0);

    if (keyStr === "\x03") {
      process.emit("SIGINT", "SIGINT");
    }
  };

  const cleanup = () => {
    process.stdin.removeListener("data", handleData);
    if (process.stdin.isRaw) {
      process.stdin.setRawMode(false);
    }
  };

  if (coinCount > 1 && process.stdin.isTTY) {
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.setEncoding("utf-8");
    process.stdin.on("data", handleData);
  }

  return cleanup;
}
