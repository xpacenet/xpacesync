/**
 * XpaceNodePool — WebSocket signaling transport to one xpacenode.
 *
 * Extracted verbatim from spacework/client/src/sync/remote.js (v3). This
 * class was already fully generic — no SpaceWork-specific code — it just
 * hadn't been given anywhere else to live yet.
 *
 * Sole responsibility: keep one WebSocket connection to an xpacenode alive,
 * reconnecting with backoff on drop, and dispatch inbound `{ t, ... }`
 * frames to registered handlers by type. It knows nothing about rooms,
 * peers, or message payloads beyond that shape.
 */
// When the socket's unsent backlog (bufferedAmount) exceeds this, a
// coalescible send is held in a latest-only slot instead of being queued
// behind data that may take seconds to drain on a weak uplink. ~17 presence
// ticks' worth — a bounded, sub-second backlog instead of an unbounded one.
const COALESCE_HIGH_WATER_BYTES = 4096
const COALESCE_DRAIN_CHECK_MS = 150

export class XpaceNodePool {
  #ws        = null
  #url       = ''
  #handlers  = new Map()   // type → [cb]
  #ready     = false
  #closed    = false       // true after explicit close() — suppresses reconnect
  #queue     = []          // { s, coalesceKey? } buffered before connection opens
  #retries   = 0
  #onOpenCb  = null        // fires after every (re)connect — see onOpen()
  #pendingCoalesce = new Map()  // coalesceKey → msg object — latest-only holds while backed up
  #drainTimer = null

  /**
   * Register a callback that runs every time the WebSocket establishes a
   * connection — the very first connect() AND every automatic reconnect
   * after a drop (phone sleep, tab suspend, network blip).
   *
   * This is the pool's one lifecycle hook. A dropped-and-reopened
   * WebSocket is, from the bridge's point of view, a brand-new connection
   * that knows nothing about any room or peer — so anything that depended
   * on that state (re-announcing presence, re-requesting a roster)
   * belongs here, not sprinkled through reconnect-specific branches.
   */
  onOpen (cb) { this.#onOpenCb = cb }

  /**
   * @param {string} url
   * @returns {Promise<void>}
   */
  async connect (url) {
    this.#url = url
    return /** @type {Promise<void>} */ (new Promise((resolve, reject) => {
      const ws = new WebSocket(url)
      this.#ws = ws

      const timeout = setTimeout(() => reject(new Error('xpacenode connect timeout')), 10_000)

      ws.onopen = () => {
        clearTimeout(timeout)
        this.#ready   = true
        this.#retries = 0
        // Drain buffered messages — offline-queued first (already
        // latest-only per coalesceKey, see send()), then anything a
        // backed-up socket had parked in the coalesce slots.
        const q = this.#queue.splice(0)
        q.forEach(m => ws.send(m.s))
        this.#flushPendingCoalesce()
        this.#onOpenCb?.()
        resolve()
      }

      ws.onmessage = ({ data }) => {
        try {
          const msg = JSON.parse(data)
          this.#dispatch(msg)
        } catch { /* malformed — drop */ }
      }

      ws.onerror = err => {
        clearTimeout(timeout)
        reject(err)
      }

      ws.onclose = () => {
        this.#ready = false
        this.#reconnect()
      }
    }))
  }

  /**
   * Send one `{ t, ... }` frame.
   *
   * @param {object} msg
   * @param {object} [opts]
   * @param {string} [opts.coalesceKey] Marks this frame LATEST-ONLY: a newer
   *   frame with the same key fully supersedes an unsent older one. Used for
   *   state snapshots (presence ticks) where delivering a stale frame after
   *   a newer one exists is strictly worse than not delivering it at all.
   *   Three behaviors hang off it: (1) while disconnected, the offline
   *   buffer holds one frame per key instead of an unbounded run of stale
   *   ones; (2) while the socket's unsent backlog is past
   *   COALESCE_HIGH_WATER_BYTES (a weak uplink mid-stall), the frame parks
   *   in a latest-only slot that drains when the socket does, so head-of-
   *   line blocking cannot grow the tail without bound; (3) a healthy send
   *   clears any parked older frame for the key. Frames without a key keep
   *   the exact pre-existing semantics.
   */
  send (msg, { coalesceKey } = {}) {
    if (this.#ready && this.#ws?.readyState === WebSocket.OPEN) {
      if (coalesceKey && this.#ws.bufferedAmount > COALESCE_HIGH_WATER_BYTES) {
        this.#pendingCoalesce.set(coalesceKey, msg)
        this.#ensureDrainTimer()
        return
      }
      if (coalesceKey) this.#pendingCoalesce.delete(coalesceKey)  // superseded
      this.#ws.send(JSON.stringify(msg))
    } else {
      const entry = { s: JSON.stringify(msg), coalesceKey }
      if (coalesceKey) {
        const existing = this.#queue.findIndex(q => q.coalesceKey === coalesceKey)
        if (existing >= 0) { this.#queue[existing] = entry; return }
      }
      this.#queue.push(entry)   // buffer until reconnect
    }
  }

  // ── Latest-only drain for coalesced frames (see send()) ─────────────────
  #ensureDrainTimer () {
    if (this.#drainTimer) return
    this.#drainTimer = setInterval(() => {
      if (this.#pendingCoalesce.size === 0) {
        clearInterval(this.#drainTimer)
        this.#drainTimer = null
        return
      }
      if (this.#ready && this.#ws?.readyState === WebSocket.OPEN &&
          this.#ws.bufferedAmount <= COALESCE_HIGH_WATER_BYTES) {
        this.#flushPendingCoalesce()
      }
    }, COALESCE_DRAIN_CHECK_MS)
  }

  #flushPendingCoalesce () {
    if (this.#pendingCoalesce.size === 0) return
    const pending = [...this.#pendingCoalesce.values()]
    this.#pendingCoalesce.clear()
    for (const msg of pending) {
      if (this.#ws?.readyState === WebSocket.OPEN) this.#ws.send(JSON.stringify(msg))
    }
  }

  on (type, cb) {
    if (!this.#handlers.has(type)) this.#handlers.set(type, [])
    this.#handlers.get(type).push(cb)
  }

  close () {
    this.#closed = true    // prevent reconnect loop after intentional close
    this.#ready  = false
    clearInterval(this.#drainTimer)
    this.#drainTimer = null
    this.#pendingCoalesce.clear()
    this.#ws?.close()
  }

  #dispatch (msg) {
    const cbs = this.#handlers.get(msg.t)
    cbs?.forEach(cb => cb(msg))
  }

  #reconnect () {
    if (this.#closed) return    // explicit close — do not reconnect
    const delay = Math.min(1000 * 2 ** this.#retries++, 30_000)
    console.warn(`[xpacesync] xpacenode disconnected — reconnecting in ${delay}ms`)
    setTimeout(() => {
      if (this.#closed) return
      this.connect(this.#url).catch(() => { /* next retry handles it */ })
    }, delay)
  }
}
