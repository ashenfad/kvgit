/**
 * Namespaced: a key-prefix view over a worktree.
 *
 * Wraps any store shaped like a `Worktree` (async reads, pending
 * writes), another `Namespaced` included. All operations route through
 * a `<namespace>/<key>` prefix, so an enclosing application can carve
 * out independent sub-namespaces over a single shared worktree.
 *
 * Nesting flattens: `new Namespaced(new Namespaced(wt, 'a'), 'b')`
 * reads/writes keys under `a/b/` rather than building a deeper
 * prefix-of-prefix chain.
 */

/**
 * The shape `Namespaced` needs from the store under it: a `Worktree`'s.
 * `Namespaced` itself has it too.
 */
export interface NamespaceableStore<V = unknown> {
  get(key: string): Promise<V | undefined>
  has(key: string): Promise<boolean>
  set(key: string, value: V): void
  delete(key: string): Promise<boolean>
  keys(): Promise<string[]>
}

export class Namespaced<V = unknown> implements NamespaceableStore<V> {
  /** The full prefix this view is namespaced under (without trailing slash). */
  readonly namespace: string

  private readonly store: NamespaceableStore<V>

  constructor(store: NamespaceableStore<V>, namespace: string) {
    if (namespace.includes('/')) {
      throw new Error("Namespace names cannot contain '/'")
    }
    if (store instanceof Namespaced) {
      this.namespace = `${store.namespace}/${namespace}`
      // Flatten: reach past the parent Namespaced to the underlying
      // store so we don't build a chain of indirection on each access.
      this.store = (store as Namespaced<V>).store
    } else {
      this.namespace = namespace
      this.store = store
    }
  }

  private prefixed(key: string): string {
    return `${this.namespace}/${key}`
  }

  // --- Reads ---

  async get(key: string): Promise<V | undefined> {
    return this.store.get(this.prefixed(key))
  }

  async has(key: string): Promise<boolean> {
    return this.store.has(this.prefixed(key))
  }

  /** Direct child keys in this namespace (excluding nested
   *  sub-namespaces), sorted. */
  async keys(): Promise<string[]> {
    const prefix = `${this.namespace}/`
    const out: string[] = []
    for (const k of await this.store.keys()) {
      if (!k.startsWith(prefix)) continue
      const remainder = k.slice(prefix.length)
      if (remainder.length === 0 || remainder.includes('/')) continue
      out.push(remainder)
    }
    return out
  }

  /** All keys under this namespace, including those in nested
   *  sub-namespaces, sorted. */
  async descendantKeys(): Promise<string[]> {
    const prefix = `${this.namespace}/`
    const out: string[] = []
    for (const k of await this.store.keys()) {
      if (k.startsWith(prefix)) out.push(k.slice(prefix.length))
    }
    return out
  }

  // --- Writes ---

  set(key: string, value: V): void {
    this.store.set(this.prefixed(key), value)
  }

  /** Delete a key; false if it was not there. */
  delete(key: string): Promise<boolean> {
    return this.store.delete(this.prefixed(key))
  }
}
