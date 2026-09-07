/**
 * MessageLog — durable local history for message types marked `persist`.
 *
 * IndexedDB-backed, keyed by (roomId, type), ordered by insertion. This is
 * the piece that was actually missing before: `spacework`'s chat had a live
 * WebRTC path but nowhere to remember a message once received, so a full
 * page discard (the common case on mobile — most browsers discard a
 * backgrounded tab rather than merely suspending it) lost history that
 * reconnection alone could never have recovered.
 *
 * Deliberately dumb: it does not know what a message means, does not
 * dedupe beyond an optional caller-supplied id, and does not talk to the
 * network. RealtimeChannel decides when to call it, per MessageRegistry's
 * `persist` flag.
 */
const DEFAULT_DB_NAME = 'xpacesync'
const DB_VERSION      = 1
const STORE           = 'messages'

function openDb (dbName) {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(dbName, DB_VERSION)
    req.onupgradeneeded = () => {
      const db = req.result
      if (!db.objectStoreNames.contains(STORE)) {
        const store = db.createObjectStore(STORE, { keyPath: 'id', autoIncrement: true })
        store.createIndex('by_room_type', ['roomId', 'type'])
      }
    }
    req.onsuccess = () => resolve(req.result)
    req.onerror   = () => reject(req.error)
  })
}

export class MessageLog {
  #dbPromise = null
  #dbName

  /**
   * @param {object} [opts]
   * @param {string} [opts.dbName] Override the IndexedDB database name.
   *   Real apps should leave this at the default (one shared local log
   *   per device/browser profile) — it exists mainly so tests simulating
   *   multiple independent peers in one process don't cross-pollute a
   *   single fake IndexedDB instance.
   */
  constructor ({ dbName = DEFAULT_DB_NAME } = {}) {
    this.#dbName = dbName
  }

  #db () {
    this.#dbPromise ??= openDb(this.#dbName)
    return this.#dbPromise
  }

  /**
   * Append one message to the durable log for (roomId, type).
   * @param {string} roomId
   * @param {string} type
   * @param {*} entry Any JSON-serializable value — MessageLog doesn't
   *   interpret it, only stores and replays it back verbatim.
   * @returns {Promise<void>}
   */
  async append (roomId, type, entry) {
    const db = await this.#db()
    return /** @type {Promise<void>} */ (new Promise((resolve, reject) => {
      const tx    = db.transaction(STORE, 'readwrite')
      const store = tx.objectStore(STORE)
      store.add({ roomId, type, entry, ts: Date.now() })
      tx.oncomplete = () => resolve()
      tx.onerror    = () => reject(tx.error)
    }))
  }

  /**
   * Replay every stored message for (roomId, type), oldest first.
   * `since` (a timestamp) returns only entries appended after it — used to
   * page or to resume rather than reload the entire history every time.
   * @param {string} roomId
   * @param {string} type
   * @param {object} [opts]
   * @param {number} [opts.since]
   * @returns {Promise<any[]>}
   */
  async replay (roomId, type, { since = 0 } = {}) {
    const db = await this.#db()
    return new Promise((resolve, reject) => {
      const tx    = db.transaction(STORE, 'readonly')
      const store = tx.objectStore(STORE)
      const index = store.index('by_room_type')
      const range = IDBKeyRange.only([roomId, type])
      const out   = /** @type {any[]} */ ([])

      const req = index.openCursor(range)
      req.onsuccess = () => {
        const cursor = req.result
        if (!cursor) return resolve(out)
        if (cursor.value.ts > since) out.push(cursor.value.entry)
        cursor.continue()
      }
      req.onerror = () => reject(req.error)
    })
  }

  /**
   * Remove all logged messages for a room (every type). Used when a user leaves for good.
   * @param {string} roomId
   * @returns {Promise<void>}
   */
  async clearRoom (roomId) {
    const db = await this.#db()
    return /** @type {Promise<void>} */ (new Promise((resolve, reject) => {
      const tx    = db.transaction(STORE, 'readwrite')
      const store = tx.objectStore(STORE)
      const req   = store.openCursor()
      req.onsuccess = () => {
        const cursor = req.result
        if (!cursor) return resolve()
        if (cursor.value.roomId === roomId) cursor.delete()
        cursor.continue()
      }
      req.onerror = () => reject(req.error)
    }))
  }
}
