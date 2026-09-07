/**
 * OutboundQueue — store-carry-forward for a message with nowhere to go yet.
 *
 * This is Milestone 2 of REALTIME_LAYER.md: the same Delay-Tolerant
 * Networking lineage RESILIENCE.md already cites (NASA's Bundle Protocol,
 * RFC 5050). A message sent while completely isolated (zero live peers) is
 * queued locally, not dropped, and gets delivered to the next peer who
 * shows up — even hours later.
 *
 * Deliberately NOT the same thing as MessageLog. MessageLog answers "did I
 * ever see this message" (app history, kept regardless of delivery
 * outcome). OutboundQueue answers "does the room still need to hear this"
 * (retry state, expires on its own). A chat message sent while offline is
 * typically in both at once — logged locally right away so the sender
 * sees their own message, AND queued for the room — but they answer
 * different questions and are cleared independently.
 *
 * Room-scoped, not peer-scoped, and delivery is non-destructive: this
 * transport has no multi-hop relay (a peer doesn't forward what it
 * receives to a third peer) — every recipient has to reach the room
 * directly. So a message stays available to EVERY peer who joins while
 * it's still fresh, not just the first one, and is only actually removed
 * once it expires (default 24h — long enough to matter for "network was
 * down for a while," short enough that a queue isn't silently immortal).
 */
const DEFAULT_DB_NAME     = 'xpacesync-outbound'
const DB_VERSION          = 1
const STORE               = 'queued'
const DEFAULT_MAX_AGE_MS  = 24 * 60 * 60 * 1000

function openDb (dbName) {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(dbName, DB_VERSION)
    req.onupgradeneeded = () => {
      const db = req.result
      if (!db.objectStoreNames.contains(STORE)) {
        const store = db.createObjectStore(STORE, { keyPath: 'id', autoIncrement: true })
        store.createIndex('by_room', 'roomId')
      }
    }
    req.onsuccess = () => resolve(req.result)
    req.onerror   = () => reject(req.error)
  })
}

export class OutboundQueue {
  #dbPromise = null
  #dbName
  #maxAgeMs

  /**
   * @param {object} [opts]
   * @param {string} [opts.dbName] Override for test isolation — see MessageLog.
   * @param {number} [opts.maxAgeMs] How long a queued message stays deliverable.
   */
  constructor ({ dbName = DEFAULT_DB_NAME, maxAgeMs = DEFAULT_MAX_AGE_MS } = {}) {
    this.#dbName   = dbName
    this.#maxAgeMs = maxAgeMs
  }

  #db () {
    this.#dbPromise ??= openDb(this.#dbName)
    return this.#dbPromise
  }

  /** Queue `message` for the room — delivered to every peer who joins while it's still fresh. */
  async enqueue (roomId, message) {
    const db = await this.#db()
    await new Promise((resolve, reject) => {
      const tx    = db.transaction(STORE, 'readwrite')
      const store = tx.objectStore(STORE)
      store.add({ roomId, message, ts: Date.now() })
      tx.oncomplete = () => resolve()
      tx.onerror    = () => reject(tx.error)
    })
    await this.#pruneExpired(roomId)
  }

  /**
   * Every still-fresh queued message for `roomId`, oldest first — call this
   * whenever a peer joins and send each returned message directly to them.
   * Non-destructive: the next peer to join sees the same messages, until
   * they expire.
   */
  async peek (roomId) {
    await this.#pruneExpired(roomId)
    const db = await this.#db()
    return new Promise((resolve, reject) => {
      const tx    = db.transaction(STORE, 'readonly')
      const index = tx.objectStore(STORE).index('by_room')
      const req   = index.getAll(IDBKeyRange.only(roomId))
      req.onsuccess = () => resolve(req.result.map(row => row.message))
      req.onerror   = () => reject(req.error)
    })
  }

  async #pruneExpired (roomId) {
    const db     = await this.#db()
    const cutoff = Date.now() - this.#maxAgeMs
    return new Promise((resolve, reject) => {
      const tx    = db.transaction(STORE, 'readwrite')
      const index = tx.objectStore(STORE).index('by_room')
      const req   = index.openCursor(IDBKeyRange.only(roomId))
      req.onsuccess = () => {
        const cursor = req.result
        if (!cursor) return resolve()
        if (cursor.value.ts < cutoff) cursor.delete()
        cursor.continue()
      }
      req.onerror = () => reject(req.error)
    })
  }
}
