import { XpaceNodePool } from '../transport/XpaceNodePool.js'
import { RTCPeer }       from '../transport/RTCPeer.js'

// Last-resort fallback only — used when a node has no STUN/TURN of its own
// configured (see xpacenode's turnCredentials.js) AND the caller passed no
// explicit override. Google's public STUN is read-only (no relay, no
// credential, a client just learns its own address) and free with no
// realistic reliability concern the way a TURN *relay* has — this is not
// the same class of dependency as the TURN relay this network no longer
// depends on a third party for. Still replaced automatically the moment a
// node reports its own STUN/TURN in its 'welcome' message — see join().
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
  // peerId → tail of that peer's serialized task chain. See #withPeerLock:
  // inbound signaling for ONE peer must be applied one at a time, in arrival
  // order, and nothing in the XpaceNodePool dispatch path provides that.
  #peerLocks       = new Map()
  #iceServersFixed = false       // true once the caller explicitly passed iceServers — see join()

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
    if (iceServers) {
      this.#iceServers      = iceServers
      this.#iceServersFixed = true   // an explicit override always wins — a node's own welcome never replaces it
    }
    this.#onIntro    = introPayload ?? (() => ({}))
  }

  get selfId ()  { return this.#selfId }
  get roomId ()  { return this.#roomId }
  get nodeUrl () { return this.#nodeUrl }

  /**
   * Peer IDs this mesh can actually reach right now — i.e. their data
   * channel is open. Deliberately NOT every peer signaling has told us
   * about: a peer we've just learned of via `peer:join` (or a `signal`
   * relay) is added to `#peers` immediately so reconnection bookkeeping
   * has somewhere to live, but its RTCPeerConnection is still negotiating
   * at that point — `entry.peer.connected` is false until the data channel
   * itself fires `open`. Callers that need "who is genuinely live" (e.g.
   * RealtimeChannel's store-carry-forward isolation check) must see that
   * in-progress peer as absent, not present — otherwise a message sent in
   * that window is neither broadcast (the channel isn't open yet — send()
   * silently no-ops) nor queued (peerIds looked non-empty), and is lost
   * outright with no trace, which is worse than either alternative.
   */
  get peerIds () {
    return [...this.#peers.entries()]
      .filter(([, entry]) => entry.peer.connected)
      .map(([peerId]) => peerId)
  }

  /** Connect to one xpacenode and join `roomId`. Resolves once the socket is open. */
  async join (nodeUrl, roomId) {
    this.#roomId  = roomId
    this.#nodeUrl = nodeUrl
    this.#pool    = new XpaceNodePool()

    // Re-announce on every (re)connect, including automatic reconnects after
    // a drop — this is the one lifecycle hook that makes reconnection and
    // first-connection share the same path instead of needing separate logic.
    this.#pool.onOpen(() => this.#announce())

    // A node hands out its own STUN/TURN in 'welcome' (see xpacenode's
    // turnCredentials.js) — always the first message it sends, before any
    // peer_join/signal could arrive, so this always lands before a single
    // RTCPeer gets constructed. Ignored entirely if the caller passed an
    // explicit iceServers override (#iceServersFixed) — that's a deliberate
    // choice by the consumer app and a node's own opinion never overrides
    // it. A node with nothing configured sends an empty array, which is
    // correctly a no-op here (the DEFAULT_ICE_SERVERS fallback stands).
    this.#pool.on('welcome', msg => {
      if (this.#iceServersFixed) return
      if (Array.isArray(msg.iceServers) && msg.iceServers.length > 0) {
        this.#iceServers = msg.iceServers
      }
    })

    this.#pool.on('peer_join', msg => {
      // `t` and `roomId` are the signaling envelope; anything else the
      // bridge relayed (e.g. the introPayload fields the peer announced
      // itself with) is forwarded as-is — PeerMesh doesn't know or care
      // what those fields mean, only that they arrived alongside peerId.
      const { peerId, t: _t, roomId: _roomId, ...announced } = msg
      if (peerId === this.#selfId) return
      this.dispatchEvent(new CustomEvent('peer:join', { detail: { peerId, announced } }))
      // A peer_join for someone we already track is normal on every
      // reconnect (both sides restate presence). Only rebuild the
      // connection if it's actually dead. Serialized against this peer's
      // inbound signals (see #withPeerLock) because a peer_join and the
      // first offer routinely arrive together, and both build peers.
      this.#withPeerLock(peerId, () => this.#ensureLivePeer(peerId))
    })

    this.#pool.on('peer_leave', msg => this.#teardownPeer(msg.peerId))

    this.#pool.on('signal', msg => {
      const { from, payload } = msg
      if (!from || !payload) return
      this.#withPeerLock(from, async () => {
        await this.#ensureLivePeer(from)
        await this.#peers.get(from)?.peer.handleSignal(payload)
      })
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
    this.#peerLocks.clear()
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

  /**
   * Run `fn` after every task already queued for `peerId`, never alongside
   * one. Returns the chained promise (callers fire-and-forget it).
   *
   * Why this exists — measured, not theorized. XpaceNodePool#dispatch is a
   * synchronous `cbs.forEach(cb => cb(msg))`, so an `async` handler's promise
   * is never awaited by the thing that called it. Every WebSocket frame
   * arrives as its own task, so when two frames land closer together than
   * one handler takes to finish, the handlers run CONCURRENTLY — and
   * `RTCPeer#handleSignal` is a multi-await state machine reading
   * `signalingState`, so a second description applied alongside the first
   * reads a state the first has not finished moving yet.
   *
   * Concretely, with an answer and an offer arriving ~2ms apart — exactly
   * what a call produces, because the caller answers the offer that carried
   * the callee's microphone and then immediately offers its own — the
   * offer's `collision` check still sees 'have-local-offer' left over from
   * the answer that is mid-apply, so an impolite peer discards that offer as
   * glare that never actually happened. Nothing retries it: the sender sits
   * in 'have-local-offer' with no answer ever coming, its track is never
   * negotiated, and `ontrack` never fires at the receiver — whose app then
   * has nothing to attach to a media element, so that peer is never heard
   * for the rest of the call, with the connection otherwise healthy and
   * inbound byte counters still climbing (a sender transmits as soon as its
   * OWN local description says sendrecv, so bytes arrive at a peer that is
   * still sendonly and get counted with nothing audible).
   *
   * Measured on real engines (spacehub's tests/browser/call-cross-engine-
   * media.mjs harness, Chromium and WebKit, strict FIFO relay, no artificial
   * reordering, inter-signal spacing preserved): 5 of 38 Blink<->Blink calls
   * wedged exactly that way with concurrent application — one-way audio when
   * the caller was the polite side, silence in BOTH directions when it was
   * not — against 0 of 32 once application was serialized per peer, which is
   * all this helper does. test/PeerMesh.test.js has the unit-level control:
   * without the lock the two descriptions interleave start:B, start:A,
   * end:B, end:A.
   *
   * Peer CREATION is not part of the hazard, by the way: `#createPeer`
   * registers its entry in `#peers` synchronously before its first await, so
   * two signals for an unknown peer could never build two connections. The
   * lock covers `#ensureLivePeer` anyway so that stays true if that order
   * ever changes.
   *
   * Per-peer rather than one global chain: peers are independent
   * connections, and one slow negotiation must not hold up another's.
   */
  #withPeerLock (peerId, fn) {
    const tail = this.#peerLocks.get(peerId) ?? Promise.resolve()
    const next = tail.then(fn).catch(err => {
      console.warn('[xpacesync] peer task failed', peerId, err)
    })
    this.#peerLocks.set(peerId, next)
    // Don't keep a settled chain around forever — but only drop the entry if
    // nothing else queued behind this link in the meantime.
    next.then(() => {
      if (this.#peerLocks.get(peerId) === next) this.#peerLocks.delete(peerId)
    })
    return next
  }

  #teardownPeer (peerId) {
    const entry = this.#peers.get(peerId)
    if (!entry) return
    entry.peer.close()
    this.#peers.delete(peerId)
    this.#peerLocks.delete(peerId)
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
