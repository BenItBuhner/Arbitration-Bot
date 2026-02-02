type JsonlWorkerRequest = {
  id: number;
  filePath: string;
  sortByTimestamp?: boolean;
};

type JsonlWorkerResponse =
  | { id: number; ok: true; records: unknown[] }
  | { id: number; ok: false; error: string };

interface WorkerTask {
  request: JsonlWorkerRequest;
  resolve: (records: unknown[]) => void;
  reject: (error: Error) => void;
}

interface WorkerSlot {
  worker: Worker;
  busy: boolean;
}

export class JsonlWorkerPool {
  private slots: WorkerSlot[] = [];
  private queue: WorkerTask[] = [];
  private nextId = 1;

  constructor(size: number) {
    const count = Math.max(1, Math.floor(size));
    for (let i = 0; i < count; i += 1) {
      const worker = new Worker(
        new URL("./jsonl-worker.ts", import.meta.url),
        { type: "module" },
      );
      this.slots.push({ worker, busy: false });
    }
  }

  parseJsonl(filePath: string, options?: { sortByTimestamp?: boolean }): Promise<unknown[]> {
    const request: JsonlWorkerRequest = {
      id: this.nextId++,
      filePath,
      sortByTimestamp: options?.sortByTimestamp,
    };

    return new Promise((resolve, reject) => {
      this.queue.push({ request, resolve, reject });
      this.drainQueue();
    });
  }

  close(): void {
    for (const slot of this.slots) {
      slot.worker.terminate();
    }
    this.slots = [];
    this.queue = [];
  }

  private drainQueue(): void {
    for (const slot of this.slots) {
      if (slot.busy) continue;
      const task = this.queue.shift();
      if (!task) return;
      this.runTask(slot, task);
    }
  }

  private runTask(slot: WorkerSlot, task: WorkerTask): void {
    slot.busy = true;

    const handleMessage = (event: MessageEvent<JsonlWorkerResponse>) => {
      const response = event.data;
      if (response.id !== task.request.id) return;
      cleanup();
      if (response.ok) {
        task.resolve(response.records);
      } else {
        task.reject(new Error(response.error));
      }
    };

    const handleError = (error: ErrorEvent) => {
      cleanup();
      task.reject(error.error ?? new Error(error.message));
    };

    const cleanup = () => {
      slot.busy = false;
      slot.worker.removeEventListener("message", handleMessage);
      slot.worker.removeEventListener("error", handleError);
      this.drainQueue();
    };

    slot.worker.addEventListener("message", handleMessage);
    slot.worker.addEventListener("error", handleError);
    slot.worker.postMessage(task.request);
  }
}
