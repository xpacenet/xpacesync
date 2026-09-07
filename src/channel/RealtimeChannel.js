import { MessageRegistry } from '../registry/MessageRegistry.js'
import { MessageLog }      from '../persistence/MessageLog.js'
import { OutboundQueue }   from '../queue/OutboundQueue.js'

/**
 * RealtimeChannel — the consumer-facing API. Wraps a PeerMesh with the
 * message-type registry from REALTIME_LAYER.md, so an app deals in
 * `send(type, payload)` / `on(type, cb)` and never touches signaling,
 * WebRTC, or persistence bookkeeping directly.
 *
 * This is where a message's registered strategy actually gets acted on:
 *   - every outbound/inbound message of a `persist: true` type is appended
 *     to MessageLog, so `history(type)` can replay it after a reload;
 *   - a `transport: 'contentStore'` type is expected to carry a CID in its
 *     payload, not inline bytes — resolving those bytes is ContentCache's
 *     job (wired in separately by the consumer app, see README), not this
 *     channel's, since it needs contentStore itself to resolve a miss;
 *   - a `queueOnFail: true` type sent while completely isolated (zero live
 *     peers) goes to OutboundQueue instead of nowhere, and gets delivered
 *     to the next peer who joins — REALTIME_LAYER.md's Milestone 2
 *     resilience requirement, proven on the same abstraction as Milestone 1
 *     rather than a parallel system.
 *
 * Adding a new message type is one `registry.register(type, strategy)`
 * call — this class never grows a case for it.
 */
export class RealtimeChannel {
  #mesh
  #registry
  #log
  #queue
  #handlers = new Map()   // type → [cb]

  /**
   * @param {import('../mesh/PeerMesh.js').PeerMesh} mesh
   * @param {object} [opts]
   * @param {MessageRegistry} [opts.registry]
   * @param {MessageLog} [opts.log]
   * @param {OutboundQueue} [opts.queue]
   */
  constructor (mesh, { registry, log, queue } = {}) {
    this.#mesh     = mesh
    this.#registry = registry ?? new MessageRegistry()
    this.#log      = log ?? new MessageLog()
    this.#queue    = queue ?? new OutboundQueue()

    this.#mesh.addEventListener('message', ({ detail }) => {
      const { from, payload } = detail
      this.#receive(from, payload)
    })

    // A route just ACTUALLY opened — not merely 'peer:join', which only
    // means signaling learned this peer exists; the data channel itself
    // negotiates asynchronously afterward and isn't ready yet at that
    // point (mesh.send() would silently no-op). 'peer:open' fires once
    // the channel is genuinely usable. Hand this peer anything queued for
    // the room while no one was reachable. Non-destructive (see
    // OutboundQueue): the next peer to join sees the same backlog, until
    // it expires, since nothing here relays a message on to a third peer.
    this.#mesh.addEventListener('peer:open', ({ detail: { peerId } }) => {
      this.#queue.peek(this.#mesh.roomId).then(queued => {
        for (const message of queued) this.#mesh.send(peerId, message)
      })
    })
  }

  get registry () { return this.#registry }
  get mesh ()     { return this.#mesh }

  /** Broadcast `payload` as a message of `type` to every connected peer. */
  send (type, payload) {
    const strategy = this.#registry.get(type)
    const message  = { type, payload, meta: { from: this.#mesh.selfId, ts: Date.now() } }

    this.#mesh.broadcast(message)
    if (strategy.persist) this.#log.append(this.#mesh.roomId, type, message)
    // Isolated (no one to broadcast to right now) — queue it for the room
    // rather than let it vanish the instant it left the sender's memory.
    if (strategy.queueOnFail && this.#mesh.peerIds.length === 0) {
      this.#queue.enqueue(this.#mesh.roomId, message)
    }
    return message
  }

  /**
   * Send `payload` as a message of `type` to exactly one peer — for
   * protocols that are inherently point-to-point (a state request/response
   * handshake, for instance) rather than something every peer needs.
   * Persistence follows the same registry strategy as send(). Store-carry-
   * forward does not apply here — `queueOnFail` is a room-broadcast concept
   * (see OutboundQueue); a point-to-point protocol that fails simply fails,
   * the same as today.
   */
  sendTo (peerId, type, payload) {
    const strategy = this.#registry.get(type)
    const message  = { type, payload, meta: { from: this.#mesh.selfId, ts: Date.now() } }

    this.#mesh.send(peerId, message)
    if (strategy.persist) this.#log.append(this.#mesh.roomId, type, message)
    return message
  }

  /** Subscribe to inbound messages of `type`. Returns an unsubscribe function. */
  on (type, cb) {
    if (!this.#handlers.has(type)) this.#handlers.set(type, [])
    this.#handlers.get(type).push(cb)
    return () => {
      this.#handlers.set(type, this.#handlers.get(type).filter(h => h !== cb))
    }
  }

  /**
   * Replay this room's durable history for `type` (only meaningful for
   * types registered `persist: true` — others were never logged).
   */
  async history (type, opts) {
    return this.#log.replay(this.#mesh.roomId, type, opts)
  }

  #receive (from, message) {
    const { type, payload, meta } = message ?? {}
    if (!type) return
    const strategy = this.#registry.get(type)
    if (strategy.persist) this.#log.append(this.#mesh.roomId, type, message)
    this.#handlers.get(type)?.forEach(cb => cb({ from, payload, meta }))
  }
}
