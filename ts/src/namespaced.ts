/**
 * Namespaced: a key-prefix view over a `Staged`-shaped store.
 *
 * Wraps any store that exposes async reads + sync buffered writes
 * (notably `Staged`, but also another `Namespaced`). All operations
 * route through a `<namespace>/<key>` prefix, so an enclosing
 * application can carve out independent sub-namespaces over a single
 * shared store.
 *
 * Nesting flattens: `new Namespaced(new Namespaced(staged, 'a'), 'b')`
 * reads/writes keys under `a/b/` rather than building a deeper
 * prefix-of-prefix chain.
 */

/**
 * The minimal shape `Namespaced` needs from its underlying store.
 *
 * Both `Staged` and `Namespaced` itself implement this naturally;
 * other stores can opt in by providing the same surface.
 */
export interface NamespaceableStore {
  get<T = unknown>(key: string): Promise<T | undefined>
  has(key: string): Promise<boolean>
  set<T = unknown>(key: string, value: T): void
  delete(key: string): void
  keys(): AsyncIterable<string>
}

export class Namespaced implements NamespaceableStore {
  /** The full prefix this view is namespaced under (without trailing slash). */
  readonly namespace: string

  private readonly store: NamespaceableStore

  constructor(store: NamespaceableStore, namespace: string) {
    if (namespace.includes('/')) {
      throw new Error("Namespace names cannot contain '/'")
    }
    if (store instanceof Namespaced) {
      this.namespace = `${store.namespace}/${namespace}`
      // Flatten: reach past the parent Namespaced to the underlying
      // store so we don't build a chain of indirection on each access.
      this.store = (store as Namespaced).store
    } else {
      this.namespace = namespace
      this.store = store
    }
  }

  private prefixed(key: string): string {
    return `${this.namespace}/${key}`
  }

  // --- Reads ---

  async get<T = unknown>(key: string): Promise<T | undefined> {
    return this.store.get<T>(this.prefixed(key))
  }

  async has(key: string): Promise<boolean> {
    return this.store.has(this.prefixed(key))
  }

  /** Direct child keys in this namespace (excluding nested sub-namespaces). */
  async *keys(): AsyncIterable<string> {
    const prefix = `${this.namespace}/`
    for await (const k of this.store.keys()) {
      if (!k.startsWith(prefix)) continue
      const remainder = k.slice(prefix.length)
      if (remainder.length === 0 || remainder.includes('/')) continue
      yield remainder
    }
  }

  /** All keys under this namespace, including those in nested sub-namespaces. */
  async *descendantKeys(): AsyncIterable<string> {
    const prefix = `${this.namespace}/`
    for await (const k of this.store.keys()) {
      if (k.startsWith(prefix)) yield k.slice(prefix.length)
    }
  }

  // --- Writes ---

  set<T = unknown>(key: string, value: T): void {
    this.store.set(this.prefixed(key), value)
  }

  delete(key: string): void {
    this.store.delete(this.prefixed(key))
  }
}
