/**
 * ContentCache — disposable local copy of content-addressed bytes.
 *
 * Not to be confused with MessageLog. MessageLog is the source of truth
 * for "did I see this message" and is never evicted casually. ContentCache
 * is the opposite: everything in it is, by construction, re-fetchable from
 * contentStore/the swarm by its CID, so losing an entry is a performance
 * regression, never a data-loss event. This is the local half of the
 * "browser cache that fetches through it" piece named in xpacenode's own
 * ROADMAP.md Milestone 2 — given a real seam here rather than invented
 * twice.
 *
 * IndexedDB-backed (works for Blob/ArrayBuffer values, unlike localStorage),
 * with a basic max-entry LRU eviction so it can't grow unbounded.
 *
 * `resolver(cid)` is supplied by the consumer app — this package does not
 * depend on contentStore's client directly, only on being told how to ask
 * for a CID it doesn't have cached yet.
 */
const DB_NAME    = 'xpacesync-cache'
const DB_VERSION = 1
const STORE      = 'content'

function openDb () {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION)
    req.onupgradeneeded = () => {
      const db = req.result
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { keyPath: 'cid' })
      }
    }
    req.onsuccess = () => resolve(req.result)
    req.onerror   = () => reject(req.error)
  })
}

export class ContentCache {
  #dbPromise = null
  #resolver
  #maxEntries
  // Monotonic counter, not Date.now() — recency needs strict ordering, and
  // two operations can land in the same millisecond, which would make the
  // "least recently used" comparison a coin flip between ties.
  #seq = 0

  /**
   * @param {(cid: string) => Promise<any>} resolver Fetches bytes for a CID on a cache miss.
   * @param {number} [maxEntries] Evict the least-recently-used entry once past this count.
   */
  constructor (resolver, { maxEntries = 200 } = {}) {
    if (typeof resolver !== 'function') {
      throw new Error('ContentCache requires a resolver(cid) function for cache misses')
    }
    this.#resolver   = resolver
    this.#maxEntries = maxEntries
  }

  #db () {
    this.#dbPromise ??= openDb()
    return this.#dbPromise
  }

  /** Get bytes for `cid` — served from cache when present, resolved and cached on a miss. */
  async get (cid) {
    const cached = await this.#read(cid)
    if (cached !== undefined) {
      await this.#touch(cid)
      return cached
    }
    const value = await this.#resolver(cid)
    await this.#write(cid, value)
    return value
  }

  /** True if `cid` is already cached locally — does not trigger a fetch. */
  async has (cid) {
    return (await this.#read(cid)) !== undefined
  }

  async #read (cid) {
    const db = await this.#db()
    return new Promise((resolve, reject) => {
      const tx  = db.transaction(STORE, 'readonly')
      const req = tx.objectStore(STORE).get(cid)
      req.onsuccess = () => resolve(req.result?.value)
      req.onerror   = () => reject(req.error)
    })
  }

  async #write (cid, value) {
    const db = await this.#db()
    await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, 'readwrite')
      tx.objectStore(STORE).put({ cid, value, lastUsed: this.#seq++ })
      tx.oncomplete = () => resolve()
      tx.onerror    = () => reject(tx.error)
    })
    await this.#evictIfNeeded()
  }

  async #touch (cid) {
    const db = await this.#db()
    return new Promise((resolve, reject) => {
      const tx    = db.transaction(STORE, 'readwrite')
      const store = tx.objectStore(STORE)
      const req   = store.get(cid)
      req.onsuccess = () => {
        const row = req.result
        if (row) store.put({ ...row, lastUsed: this.#seq++ })
        resolve()
      }
      req.onerror = () => reject(req.error)
    })
  }

  async #evictIfNeeded () {
    const db = await this.#db()
    const all = await new Promise((resolve, reject) => {
      const tx  = db.transaction(STORE, 'readonly')
      const req = tx.objectStore(STORE).getAll()
      req.onsuccess = () => resolve(req.result)
      req.onerror   = () => reject(req.error)
    })
    if (all.length <= this.#maxEntries) return

    const toEvict = all
      .sort((a, b) => a.lastUsed - b.lastUsed)
      .slice(0, all.length - this.#maxEntries)

    const tx    = db.transaction(STORE, 'readwrite')
    const store = tx.objectStore(STORE)
    for (const row of toEvict) store.delete(row.cid)
  }
}
