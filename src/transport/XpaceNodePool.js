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
export class XpaceNodePool {
  #ws        = null
  #url       = ''
  #handlers  = new Map()   // type → [cb]
  #ready     = false
  #closed    = false       // true after explicit close() — suppresses reconnect
  #queue     = []          // messages buffered before connection opens
  #retries   = 0
  #onOpenCb  = null        // fires after every (re)connect — see onOpen()

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
        // Drain buffered messages
        const q = this.#queue.splice(0)
        q.forEach(m => ws.send(m))
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

  send (msg) {
    const s = JSON.stringify(msg)
    if (this.#ready && this.#ws?.readyState === WebSocket.OPEN) {
      this.#ws.send(s)
    } else {
      this.#queue.push(s)   // buffer until reconnect
    }
  }

  on (type, cb) {
    if (!this.#handlers.has(type)) this.#handlers.set(type, [])
    this.#handlers.get(type).push(cb)
  }

  close () {
    this.#closed = true    // prevent reconnect loop after intentional close
    this.#ready  = false
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
