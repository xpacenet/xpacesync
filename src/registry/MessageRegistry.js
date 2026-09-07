/**
 * MessageRegistry — the core abstraction from REALTIME_LAYER.md, in code.
 *
 * A message type declares its own strategy once, at registration time.
 * RealtimeChannel consults this registry to decide what to do with a
 * message — it never hardcodes a switch statement over message types.
 * Adding a new type (a new row in REALTIME_LAYER.md's table) means calling
 * `register()` once, not editing the channel or the mesh.
 *
 * Strategy shape (all fields optional — see defaults below):
 *   {
 *     persist:   boolean   Append every message of this type to MessageLog
 *                          so it survives a reload. Default false — most
 *                          message types (position, presence) are correctly
 *                          ephemeral; only mark the ones that carry real
 *                          history (chat) as persist: true.
 *     transport: string    Which transport carries this type's payload.
 *                          'mesh' (default) — payload rides the PeerMesh
 *                          data channel directly, small JSON only.
 *                          'contentStore' — payload is a CID reference;
 *                          the actual bytes live in contentStore and get
 *                          resolved through ContentCache, not sent inline.
 *     queueOnFail: boolean REALTIME_LAYER.md's Milestone 2 resilience
 *                          requirement: if this type's send() reaches zero
 *                          live peers, queue it (OutboundQueue) instead of
 *                          dropping it, and deliver it to the next peer who
 *                          joins. Default false — only meaningful for types
 *                          where a delayed delivery still means something
 *                          (chat); a stale position update queued for
 *                          hours would just be wrong once delivered.
 *   }
 */
const DEFAULT_STRATEGY = Object.freeze({ persist: false, transport: 'mesh', queueOnFail: false })

export class MessageRegistry {
  #types = new Map()

  /** Declare how messages of `type` should be carried and persisted. */
  register (type, strategy = {}) {
    if (!type) throw new Error('MessageRegistry.register requires a type name')
    this.#types.set(type, { ...DEFAULT_STRATEGY, ...strategy })
    return this
  }

  /** Strategy for `type`, or the default (mesh transport, not persisted) if never registered. */
  get (type) {
    return this.#types.get(type) ?? DEFAULT_STRATEGY
  }

  has (type) { return this.#types.has(type) }

  types () { return [...this.#types.keys()] }
}
