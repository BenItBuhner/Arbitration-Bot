/**
 * Crypto Price Graph - Unicode Chart
 *
 * Visualizes price action as a 2D grid with trend-colored bars or candles.
 * Line mode: each price update corresponds to one column.
 * Candle mode: multiple ticks are bucketed into a single column.
 */

export interface PriceGraphConfig {
  /** Maximum number of history points (columns) to show (default: auto) */
  maxPoints?: number;
  /** Height of the graph in lines (default: 12) */
  height?: number;
  /** Width of the graph in characters (default: auto) */
  width?: number | "auto";
  /** Render mode (default: line) */
  mode?: "line" | "candles";
}

export class PriceGraph {
  private prices: number[] = [];
  private height: number;
  private width: number;
  private maxPoints: number;
  private mode: "line" | "candles";

  private colors = {
    up: "\x1b[32m", // Green
    down: "\x1b[31m", // Red
    flat: "\x1b[90m", // Gray
    border: "\x1b[90m", // Dim Gray
    labels: "\x1b[90m", // Dim Gray
    reset: "\x1b[0m",
  };

  constructor(config: PriceGraphConfig = {}) {
    this.height = config.height ?? 12;
    this.width =
      config.width === "auto" ? this.getAdjustedWidth() : (config.width ?? 60);
    this.mode = config.mode ?? "line";
    const defaultMax =
      this.mode === "candles" ? this.width * 4 : this.width;
    this.maxPoints = Math.max(this.width, config.maxPoints ?? defaultMax);
  }

  private getAdjustedWidth(): number {
    try {
      const cols = process.stdout.columns || 80;
      return Math.max(20, Math.floor(cols * 0.75) - 15);
    } catch {
      return 60;
    }
  }

  addPrice(price: number): void {
    const len = this.prices.length;
    if (len === 0) {
      this.prices.push(price);
    } else {
      this.prices.push(price);
      if (len >= this.maxPoints) {
        this.prices.shift();
      }
    }
  }

  addPrices(prices: number[]): void {
    const startIndex = Math.max(0, prices.length - this.maxPoints);
    const pricesToAdd = prices.slice(startIndex);

    for (const price of pricesToAdd) {
      this.prices.push(price);
      if (this.prices.length > this.maxPoints) {
        this.prices.shift();
      }
    }
  }

  hasData(): boolean {
    return this.prices.length > 0;
  }

  clear(): void {
    this.prices = [];
  }

  private formatPrice(p: number): string {
    return p.toLocaleString("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  render(): string {
    if (this.mode === "candles") {
      return this.renderCandles();
    }
    return this.renderLine();
  }

  private renderLine(): string {
    const count = this.prices.length;
    if (count === 0) {
      return `  ${this.colors.labels}(Waiting for price data...)${this.colors.reset}\n`;
    }

    const firstPrice = this.prices[0];
    if (firstPrice === undefined || Number.isNaN(firstPrice)) {
      return `  ${this.colors.labels}(Invalid price data)${this.colors.reset}\n`;
    }
    let min = firstPrice;
    let max = min;
    for (let i = 1; i < count; i++) {
      const p = this.prices[i];
      if (p !== undefined && p < min) min = p;
      if (p !== undefined && p > max) max = p;
    }

    let range = max - min;
    if (range === 0) {
      const padding = Math.max(0.01, Math.abs(min) * 0.01);
      min -= padding;
      max += padding;
      range = max - min;
    }

    const normalized = new Array(count);
    const heightMinus1 = this.height - 1;
    for (let i = 0; i < count; i++) {
      const p = this.prices[i] ?? min;
      const value = Number.isFinite(p) ? p : min;
      normalized[i] = Math.floor(((value - min) / range) * heightMinus1);
    }

    const grid: string[][] = [];
    for (let r = 0; r < this.height; r++) {
      const row = new Array(count);
      for (let c = 0; c < count; c++) {
        row[c] = " ";
      }
      grid.push(row);
    }

    const colors = this.colors;
    const prices = this.prices;

    for (let c = 0; c < count; c++) {
      const h = normalized[c];
      const currPrice = prices[c];
      const prevPrice =
        c > 0 && prices[c - 1] !== undefined ? prices[c - 1] : currPrice;

      if (currPrice === undefined || prevPrice === undefined) continue;

      const color =
        currPrice > prevPrice
          ? colors.up
          : currPrice < prevPrice
            ? colors.down
            : colors.flat;

      const colStr = `${color}█${colors.reset}`;
      for (let r = 0; r <= h; r++) {
        const row = grid[r];
        if (row) row[c] = colStr;
      }
    }

    return this.renderGrid(grid, min, max);
  }

  private renderCandles(): string {
    const count = this.prices.length;
    if (count === 0) {
      return `  ${this.colors.labels}(Waiting for price data...)${this.colors.reset}\n`;
    }

    const bucketSize = Math.max(1, Math.ceil(count / this.width));
    const candles: Array<{
      open: number;
      close: number;
      high: number;
      low: number;
    }> = [];

    for (let i = 0; i < count; i += bucketSize) {
      const slice = this.prices.slice(i, i + bucketSize);
      if (slice.length === 0) continue;
      let high = slice[0]!;
      let low = slice[0]!;
      for (const value of slice) {
        if (value > high) high = value;
        if (value < low) low = value;
      }
      candles.push({
        open: slice[0]!,
        close: slice[slice.length - 1]!,
        high,
        low,
      });
    }

    const candleCount = candles.length;
    if (candleCount === 0) {
      return `  ${this.colors.labels}(Waiting for price data...)${this.colors.reset}\n`;
    }

    let min = candles[0]!.low;
    let max = candles[0]!.high;
    for (const candle of candles) {
      if (candle.low < min) min = candle.low;
      if (candle.high > max) max = candle.high;
    }

    let range = max - min;
    if (range === 0) {
      const padding = Math.max(0.01, Math.abs(min) * 0.01);
      min -= padding;
      max += padding;
      range = max - min;
    }

    const toRow = (value: number) => {
      const normalized = (value - min) / range;
      return Math.floor(normalized * (this.height - 1));
    };

    const grid: string[][] = [];
    for (let r = 0; r < this.height; r++) {
      const row = new Array(candleCount);
      row.fill(" ");
      grid.push(row);
    }

    const colors = this.colors;
    const bodyChar = "█";
    let prevClose: number | null = null;
    let prevCloseRow: number | null = null;

    for (let c = 0; c < candleCount; c++) {
      const candle = candles[c]!;
      const trendBase = prevClose ?? candle.open;
      const color =
        candle.close > trendBase
          ? colors.up
          : candle.close < trendBase
            ? colors.down
            : colors.flat;
      const body = `${color}${bodyChar}${colors.reset}`;

      const highRow = toRow(candle.high);
      const lowRow = toRow(candle.low);
      const closeRow = toRow(candle.close);
      const bridgeTop =
        prevCloseRow === null ? highRow : Math.max(highRow, prevCloseRow);
      const bridgeBottom =
        prevCloseRow === null ? lowRow : Math.min(lowRow, prevCloseRow);
      const bodyTop = Math.max(bridgeTop, closeRow);
      const bodyBottom = Math.min(bridgeBottom, closeRow);
      for (let r = bodyBottom; r <= bodyTop; r++) {
        grid[r]![c] = body;
      }
      prevClose = candle.close;
      prevCloseRow = closeRow;
    }

    return this.renderGrid(grid, min, max);
  }

  private renderGrid(grid: string[][], min: number, max: number): string {
    const count = grid[0]?.length ?? 0;
    if (count === 0) {
      return `  ${this.colors.labels}(Waiting for price data...)${this.colors.reset}\n`;
    }

    const outputParts: string[] = [];
    const border = this.colors.border;
    const reset = this.colors.reset;
    const labels = this.colors.labels;
    const labelPad = " ".repeat(12);
    const borderLine = "─".repeat(count);

    const maxLabel = labels + this.formatPrice(max).padStart(12) + reset + " ";
    const minLabel = labels + this.formatPrice(min).padStart(12) + reset + " ";

    outputParts.push(`  ${labelPad} ${border}╭${borderLine}╮${reset}\n`);

    for (let r = this.height - 1; r >= 0; r--) {
      const row = grid[r]!;
      const rowStr = row.join("");

      if (r === this.height - 1) {
        outputParts.push(
          `  ${maxLabel}${border}│${reset}${rowStr}${border}│${reset}\n`,
        );
      } else if (r === 0) {
        outputParts.push(
          `  ${minLabel}${border}│${reset}${rowStr}${border}│${reset}\n`,
        );
      } else {
        outputParts.push(
          `  ${labelPad} ${border}│${reset}${rowStr}${border}│${reset}\n`,
        );
      }
    }

    outputParts.push(`  ${labelPad} ${border}╰${borderLine}╯${reset}\n`);

    return outputParts.join("");
  }
}
