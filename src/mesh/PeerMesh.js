import { XpaceNodePool } from '../transport/XpaceNodePool.js'
import { RTCPeer }       from '../transport/RTCPeer.js'

const DEFAULT_ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
]

/**
 * PeerMesh — room membership and peer-connection lifecycle, nothing else.
 *
 * This is the generalized core of spacework's RemoteSync (v3), with every
 * SpaceWork-specific concept removed: no avatar, no position, no presence
 * snapshots. What's left is the actual reusable mechanism — connect to an
 * xpacenode, join a room, keep a live WebRTC connection to every peer in it,
 * and rebuild any connection that goes stale on reconnect (the exact fix
 * that closed spacework's original peer-rediscovery bug).
 *
 * PeerMesh does not interpret message contents. Every data-channel message
 * is handed up as a raw `{ from, payload }` pair via the 'message' event —
 * deciding what a message TYPE means, how it's routed, and whether it's
 * persisted is `RealtimeChannel`'s job, layered on top. This split is the
 * point: new message types never require touching this file.
 *
 * Events (EventTarget CustomEvents):
 *   peer:join    detail: { peerId, announced }  — signaling learned of this peer
 *                                                  (announced = whatever extra
 *                                                  fields the caller's introPayload
 *                                                  puts on the wire, e.g. a display
 *                                                  name — PeerMesh doesn't interpret it)
 *   peer:open    detail: { peerId }             — the data channel to this peer is
 *                                                  actually open and ready to send/receive
 *   peer:intro   detail: { peerId, announced }  — the peer's own intro frame arrived
 *                                                  over the data channel (same shape as
 *                                                  peer:join's announced, confirmed
 *                                                  peer-to-peer rather than via signaling)
 *   peer:leave   detail: { peerId }
 *   message      detail: { from, payload }      — any non-protocol data-channel message
 */
export class PeerMesh extends EventTarget {
  #pool            = null
  #peers           = new Map()   // peerId → { peer: RTCPeer, hasBeenOpen: boolean }
  #selfId          = ''
  #roomId          = ''
  #nodeUrl         = ''
  #iceServers      = DEFAULT_ICE_SERVERS
  #heartbeatTimer  = null
  #onIntro         = null        // optional payload merged into every announce/intro frame
  #localTracks     = []
  #trackCb         = null

  /**
   * @param {object} [opts]
   * @param {string} [opts.selfId]     Stable peer identity (any string, e.g. an Ed25519 hex
   *   id) — required in practice (the constructor throws without one); typed optional here
   *   only because the parameter object itself defaults to `{}`.
   * @param {Array<{urls: string, username?: string, credential?: string}>} [opts.iceServers]
   *   Override the default STUN-only ICE server list.
   * @param {() => object} [opts.introPayload] Called each announce/intro — extra fields to merge in
   *   (e.g. a display name). Keeps PeerMesh from needing to know what an "identity" contains.
   */
  constructor ({ selfId, iceServers, introPayload } = {}) {
    super()
    if (!selfId) throw new Error('PeerMesh requires selfId')
    this.#selfId     = selfId
    if (iceServers) this.#iceServers = iceServers
    this.#onIntro    = introPayload ?? (() => ({}))
  }

  get selfId ()  { return this.#selfId }
  get roomId ()  { return this.#roomId }
  get nodeUrl () { return this.#nodeUrl }
  get peerIds () { return [...this.#peers.keys()] }

  /** Connect to one xpacenode and join `roomId`. Resolves once the socket is open. */
  async join (nodeUrl, roomId) {
    this.#roomId  = roomId
    this.#nodeUrl = nodeUrl
    this.#pool    = new XpaceNodePool()

    // Re-announce on every (re)connect, including automatic reconnects after
    // a drop — this is the one lifecycle hook that makes reconnection and
    // first-connection share the same path instead of needing separate logic.
    this.#pool.onOpen(() => this.#announce())

    this.#pool.on('peer_join', async msg => {
      // `t` and `roomId` are the signaling envelope; anything else the
      // bridge relayed (e.g. the introPayload fields the peer announced
      // itself with) is forwarded as-is — PeerMesh doesn't know or care
      // what those fields mean, only that they arrived alongside peerId.
      const { peerId, t: _t, roomId: _roomId, ...announced } = msg
      if (peerId === this.#selfId) return
      this.dispatchEvent(new CustomEvent('peer:join', { detail: { peerId, announced } }))
      // A peer_join for someone we already track is normal on every
      // reconnect (both sides restate presence). Only rebuild the
      // connection if it's actually dead.
      await this.#ensureLivePeer(peerId)
    })

    this.#pool.on('peer_leave', msg => this.#teardownPeer(msg.peerId))

    this.#pool.on('signal', async msg => {
      const { from, payload } = msg
      if (!from || !payload) return
      await this.#ensureLivePeer(from)
      await this.#peers.get(from)?.peer.handleSignal(payload)
    })

    await this.#pool.connect(nodeUrl)

    // Keep-alive: prevents server-side peer pruning while idle.
    this.#heartbeatTimer = setInterval(() => {
      this.#pool.send({ t: 'hb', roomId: this.#roomId })
    }, 30_000)
  }

  leave () {
    clearInterval(this.#heartbeatTimer)
    this.#pool?.send({ t: 'leave', roomId: this.#roomId })
    this.#pool?.close()
    for (const { peer } of this.#peers.values()) peer.close()
    this.#peers.clear()
    this.#pool = null
  }

  /**
   * Send `payload` (any JSON-serializable object) to one peer.
   * @returns {boolean} true if an open data channel actually carried it,
   *   false if there's no live connection to `peerId` right now (unknown
   *   peer, or a connection that exists but isn't open yet/anymore) — the
   *   signal RealtimeChannel's store-carry-forward fallback acts on.
   */
  send (peerId, payload) {
    const entry = this.#peers.get(peerId)
    if (!entry?.peer.connected) return false
    entry.peer.send(payload)
    return true
  }

  /**
   * Raw RTCPeerConnection per currently-connected peer, keyed by peerId.
   * Exists for callers that need WebRTC-level access PeerMesh doesn't wrap
   * itself — e.g. inspecting existing inbound media receivers for a track
   * that arrived before a consumer's onTrack callback was registered.
   */
  getPeerConnections () {
    const out = {}
    for (const [peerId, { peer }] of this.#peers) out[peerId] = peer.pc
    return out
  }

  /** Send `payload` to every peer currently connected. */
  broadcast (payload) {
    for (const { peer } of this.#peers.values()) peer.send(payload)
  }

  /** Attach a local media track to every current and future peer connection. */
  addTrack (track, stream) {
    this.#localTracks.push({ track, stream })
    for (const { peer } of this.#peers.values()) peer.addTrack(track, stream)
  }

  onTrack (cb) { this.#trackCb = cb }

  // ── Internal ────────────────────────────────────────────────────────────

  async #createPeer (peerId, isPolite) {
    const peer  = new RTCPeer(isPolite, this.#iceServers)
    const entry = { peer, hasBeenOpen: false }
    this.#peers.set(peerId, entry)

    peer.addEventListener('signal', (/** @type {CustomEvent} */ { detail }) => {
      this.#pool?.send({ t: 'signal', roomId: this.#roomId, to: peerId, payload: detail })
    })

    peer.onMessage(payload => {
      if (payload?.__xpacesync === 'intro') {
        const { __xpacesync: _tag, from: _from, ...announced } = payload
        this.dispatchEvent(new CustomEvent('peer:intro', { detail: { peerId, announced } }))
        return
      }
      this.dispatchEvent(new CustomEvent('message', { detail: { from: peerId, payload } }))
    })

    peer.onTrack((track, stream) => this.#trackCb?.(track, stream, peerId))

    peer.addEventListener('open', () => {
      // Marks this connection as having been live at least once — lets
      // #ensureLivePeer tell "dead, was working before" (rebuild) apart
      // from "still negotiating for the first time" (leave alone).
      entry.hasBeenOpen = true
      peer.send({ __xpacesync: 'intro', from: this.#selfId, ...this.#onIntro() })
      this.dispatchEvent(new CustomEvent('peer:open', { detail: { peerId } }))
    })

    peer.addEventListener('failed', () => this.#teardownPeer(peerId))

    for (const { track, stream } of this.#localTracks) peer.addTrack(track, stream)

    return peer
  }

  #teardownPeer (peerId) {
    const entry = this.#peers.get(peerId)
    if (!entry) return
    entry.peer.close()
    this.#peers.delete(peerId)
    this.dispatchEvent(new CustomEvent('peer:leave', { detail: { peerId } }))
  }

  #announce () {
    this.#pool.send({
      t: 'hello', roomId: this.#roomId, peerId: this.#selfId, ...this.#onIntro(),
    })
  }

  /**
   * Ensure a live WebRTC connection to `peerId`, rebuilding it if the
   * existing one has gone dead (the phone-went-to-sleep / tab-suspend case).
   * A connection that has never opened yet is left alone — that's normal
   * first-time negotiation, not a stale session.
   */
  async #ensureLivePeer (peerId) {
    const entry = this.#peers.get(peerId)
    if (entry) {
      const isDead = entry.hasBeenOpen && !entry.peer.connected
      if (!isDead) return
      entry.peer.close()
      this.#peers.delete(peerId)
    }
    const isPolite = this.#selfId < peerId
    await this.#createPeer(peerId, isPolite)
  }
}
