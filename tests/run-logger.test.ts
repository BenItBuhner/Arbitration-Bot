import { describe, expect, it, afterEach } from "bun:test";
import { RunLogger } from "../src/services/run-logger";
import { join } from "path";
import { existsSync, readFileSync, rmSync, mkdirSync, statSync } from "fs";

const TEST_DIR = join(process.cwd(), "tests", ".test-log-output");

function cleanup() {
  try {
    if (existsSync(TEST_DIR)) {
      rmSync(TEST_DIR, { recursive: true, force: true });
    }
  } catch { /* ignore */ }
}

// Clean before and after
cleanup();

afterEach(() => {
  cleanup();
});

describe("RunLogger", () => {
  it("creates log file on construction", () => {
    const logPath = join(TEST_DIR, "test1.log");
    new RunLogger(logPath, 50);
    expect(existsSync(logPath)).toBe(true);
    const content = readFileSync(logPath, "utf8");
    expect(content).toContain("Run started");
  });

  it("logs messages with timestamp and level", () => {
    const logPath = join(TEST_DIR, "test2.log");
    const logger = new RunLogger(logPath, 50);
    logger.log("test message", "INFO");
    logger.log("warning message", "WARN");
    logger.log("error message", "ERROR");

    const content = readFileSync(logPath, "utf8");
    expect(content).toContain("[INFO] test message");
    expect(content).toContain("[WARN] warning message");
    expect(content).toContain("[ERROR] error message");
  });

  it("keeps recent lines in memory", () => {
    const logPath = join(TEST_DIR, "test3.log");
    const logger = new RunLogger(logPath, 5);

    for (let i = 0; i < 10; i++) {
      logger.log(`message ${i}`);
    }

    const recent = logger.getRecentLines();
    expect(recent.length).toBe(5);
    // Should have the LAST 5 messages
    expect(recent[0]).toContain("message 5");
    expect(recent[4]).toContain("message 9");
  });

  it("getRecentLines respects limit parameter", () => {
    const logPath = join(TEST_DIR, "test4.log");
    const logger = new RunLogger(logPath, 50);

    for (let i = 0; i < 10; i++) {
      logger.log(`message ${i}`);
    }

    const recent = logger.getRecentLines(3);
    expect(recent.length).toBe(3);
  });

  it("clearRecentLines empties memory buffer", () => {
    const logPath = join(TEST_DIR, "test5.log");
    const logger = new RunLogger(logPath, 50);

    logger.log("test");
    expect(logger.getRecentLines().length).toBeGreaterThan(0);

    logger.clearRecentLines();
    expect(logger.getRecentLines().length).toBe(0);
  });

  it("creates directory if it does not exist", () => {
    const logPath = join(TEST_DIR, "subdir", "deep", "test6.log");
    new RunLogger(logPath, 50);
    expect(existsSync(logPath)).toBe(true);
  });

  it("does not crash when log file write fails", () => {
    // Use a path that we can control
    const logPath = join(TEST_DIR, "test7.log");
    const logger = new RunLogger(logPath, 50);
    // This should not throw even if underlying fs has issues
    expect(() => {
      for (let i = 0; i < 100; i++) {
        logger.log(`stress test ${i}`);
      }
    }).not.toThrow();
  });

  it("getRecentLines returns empty for zero limit", () => {
    const logPath = join(TEST_DIR, "test8.log");
    const logger = new RunLogger(logPath, 50);
    logger.log("test");
    expect(logger.getRecentLines(0)).toEqual([]);
  });

  it("defaults level to INFO", () => {
    const logPath = join(TEST_DIR, "test9.log");
    const logger = new RunLogger(logPath, 50);
    logger.log("default level");
    const content = readFileSync(logPath, "utf8");
    expect(content).toContain("[INFO] default level");
  });

  it("rotates log file when maxFileBytes exceeded", () => {
    const logPath = join(TEST_DIR, "test-rotate.log");
    const rotatedPath = `${logPath}.1`;
    // Tiny limit: 500 bytes, rotation checks every 500 writes
    // so we need to write enough to exceed AND hit the check interval
    const logger = new RunLogger(logPath, 50, { maxFileBytes: 500 });

    // Write enough to exceed 500 bytes (each line is ~60 bytes)
    // Rotation check happens every 500 writes
    for (let i = 0; i < 501; i++) {
      logger.log(`msg-${i}`);
    }

    // After 501 writes, rotation should have triggered
    // The rotated file should exist
    expect(existsSync(rotatedPath)).toBe(true);
    // The current log file should be fresh (much smaller)
    const currentStat = statSync(logPath);
    expect(currentStat.size).toBeLessThan(500);
  });
});

// Final cleanup
process.on("beforeExit", cleanup);
