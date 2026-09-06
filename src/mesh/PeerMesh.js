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
 *   peer:join    detail: { peerId }
 *   peer:leave   detail: { peerId }
 *   message      detail: { from, payload }
 *   status       detail: { peerCount }
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

  /**
   * @param {object} opts
   * @param {string} opts.selfId       Stable peer identity (any string, e.g. an Ed25519 hex id)
   * @param {object} [opts.iceServers] Override the default STUN-only ICE server list
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
      const { peerId } = msg
      if (peerId === this.#selfId) return
      this.dispatchEvent(new CustomEvent('peer:join', { detail: { peerId } }))
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

  /** Send `payload` (any JSON-serializable object) to one peer. No-op if not connected. */
  send (peerId, payload) {
    this.#peers.get(peerId)?.peer.send(payload)
  }

  /** Send `payload` to every peer currently connected. */
  broadcast (payload) {
    for (const { peer } of this.#peers.values()) peer.send(payload)
  }

  /** Attach a local media track to every current and future peer connection. */
  addTrack (track, stream) {
    this.#localTracks ??= []
    this.#localTracks.push({ track, stream })
    for (const { peer } of this.#peers.values()) peer.addTrack(track, stream)
  }

  onTrack (cb) { this.#trackCb = cb }

  // ── Internal ────────────────────────────────────────────────────────────

  async #createPeer (peerId, isPolite) {
    const peer  = new RTCPeer(isPolite, this.#iceServers)
    const entry = { peer, hasBeenOpen: false }
    this.#peers.set(peerId, entry)

    peer.addEventListener('signal', ({ detail }) => {
      this.#pool?.send({ t: 'signal', roomId: this.#roomId, to: peerId, payload: detail })
    })

    peer.onMessage(payload => {
      this.dispatchEvent(new CustomEvent('message', { detail: { from: peerId, payload } }))
    })

    peer.onTrack((track, stream) => this.#trackCb?.(track, stream, peerId))

    peer.addEventListener('open', () => {
      // Marks this connection as having been live at least once — lets
      // #ensureLivePeer tell "dead, was working before" (rebuild) apart
      // from "still negotiating for the first time" (leave alone).
      entry.hasBeenOpen = true
      peer.send({ __xpacesync: 'intro', from: this.#selfId, ...this.#onIntro() })
    })

    peer.addEventListener('failed', () => this.#teardownPeer(peerId))

    for (const { track, stream } of this.#localTracks ?? []) peer.addTrack(track, stream)

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
