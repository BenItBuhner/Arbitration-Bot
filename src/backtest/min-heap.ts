export interface HeapNode<T> {
  key: string;
  time: number;
  value: T;
}

export class MinHeap<T> {
  private nodes: HeapNode<T>[] = [];
  private indexByKey: Map<string, number> = new Map();

  size(): number {
    return this.nodes.length;
  }

  peek(): HeapNode<T> | null {
    return this.nodes.length > 0 ? this.nodes[0]! : null;
  }

  upsert(key: string, time: number, value: T): void {
    if (!Number.isFinite(time)) {
      this.remove(key);
      return;
    }

    const existing = this.indexByKey.get(key);
    if (existing === undefined) {
      const node: HeapNode<T> = { key, time, value };
      this.nodes.push(node);
      const idx = this.nodes.length - 1;
      this.indexByKey.set(key, idx);
      this.bubbleUp(idx);
      return;
    }

    const node = this.nodes[existing]!;
    node.time = time;
    node.value = value;
    this.bubbleUp(existing);
    this.bubbleDown(existing);
  }

  remove(key: string): void {
    const index = this.indexByKey.get(key);
    if (index === undefined) return;

    const lastIndex = this.nodes.length - 1;
    if (index !== lastIndex) {
      this.swap(index, lastIndex);
    }

    this.nodes.pop();
    this.indexByKey.delete(key);

    if (index < this.nodes.length) {
      this.bubbleUp(index);
      this.bubbleDown(index);
    }
  }

  private bubbleUp(index: number): void {
    let idx = index;
    while (idx > 0) {
      const parent = Math.floor((idx - 1) / 2);
      if (this.compare(this.nodes[idx]!, this.nodes[parent]!) >= 0) break;
      this.swap(idx, parent);
      idx = parent;
    }
  }

  private bubbleDown(index: number): void {
    let idx = index;
    const length = this.nodes.length;
    while (true) {
      const left = idx * 2 + 1;
      const right = left + 1;
      let smallest = idx;

      if (left < length && this.compare(this.nodes[left]!, this.nodes[smallest]!) < 0) {
        smallest = left;
      }
      if (right < length && this.compare(this.nodes[right]!, this.nodes[smallest]!) < 0) {
        smallest = right;
      }
      if (smallest === idx) break;
      this.swap(idx, smallest);
      idx = smallest;
    }
  }

  private swap(a: number, b: number): void {
    const temp = this.nodes[a]!;
    this.nodes[a] = this.nodes[b]!;
    this.nodes[b] = temp;
    this.indexByKey.set(this.nodes[a]!.key, a);
    this.indexByKey.set(this.nodes[b]!.key, b);
  }

  private compare(a: HeapNode<T>, b: HeapNode<T>): number {
    if (a.time !== b.time) return a.time - b.time;
    return a.key.localeCompare(b.key);
  }
}
