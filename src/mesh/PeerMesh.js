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
 *   message      detail: { from, payload, via? } — any non-protocol message. `via: 'relay'`
 *                                                  marks one that arrived through the
 *                                                  xpacenode relay floor instead of a data
 *                                                  channel (see relayTo below); consumers
 *                                                  that don't care never need to look.
 */
export class PeerMesh extends EventTarget {
  #pool            = null
  #peers           = new Map()   // peerId → { peer: RTCPeer, hasBeenOpen: boolean }
  #roster          = new Map()   // peerId → announced — everyone signaling says is in the
                                 // room, INDEPENDENT of whether a WebRTC connection to them
                                 // exists (that's #peers). The relay floor targets this set.
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
  #transportPolicy = 'auto'      // 'auto' (P2P + relay floor) | 'relay-only' (never dial WebRTC)
  #poolFactory     = () => new XpaceNodePool()

  /**
   * @param {object} [opts]
   * @param {string} [opts.selfId]     Stable peer identity (any string, e.g. an Ed25519 hex
   *   id) — required in practice (the constructor throws without one); typed optional here
   *   only because the parameter object itself defaults to `{}`.
   * @param {Array<{urls: string, username?: string, credential?: string}>} [opts.iceServers]
   *   Override the default STUN-only ICE server list.
   * @param {() => object} [opts.introPayload] Called each announce/intro — extra fields to merge in
   *   (e.g. a display name). Keeps PeerMesh from needing to know what an "identity" contains.
   * @param {'auto'|'relay-only'} [opts.transportPolicy] 'auto' (default): negotiate WebRTC
   *   to every room peer, with the xpacenode relay available as a floor for relay-flagged
   *   message types (see MessageRegistry). 'relay-only': never construct an RTCPeer at all —
   *   every message rides the relay. For environments where WebRTC is unavailable or
   *   deliberately disabled, and for QA that needs to prove the relay floor on its own.
   * @param {() => XpaceNodePool} [opts.poolFactory] Injectable signaling-transport factory
   *   (tests substitute an in-memory pool; same posture as the class injection seams
   *   consumers like the Orb's WorldNet already use for this class itself).
   */
  constructor ({ selfId, iceServers, introPayload, transportPolicy, poolFactory } = {}) {
    super()
    if (!selfId) throw new Error('PeerMesh requires selfId')
    this.#selfId     = selfId
    if (iceServers) {
      this.#iceServers      = iceServers
      this.#iceServersFixed = true   // an explicit override always wins — a node's own welcome never replaces it
    }
    this.#onIntro    = introPayload ?? (() => ({}))
    if (transportPolicy === 'relay-only') this.#transportPolicy = 'relay-only'
    if (poolFactory) this.#poolFactory = poolFactory
  }

  get selfId ()  { return this.#selfId }
  get roomId ()  { return this.#roomId }
  get nodeUrl () { return this.#nodeUrl }

  /**
   * The ICE server list new connections are being built with RIGHT NOW —
   * the caller's explicit override, or the freshest set the node handed
   * out ('welcome' at connect, refreshed 'ice' frames afterwards; see
   * join()). For consumers that construct their own RTCPeerConnections
   * outside PeerMesh (e.g. dedicated media links): read this at pc
   * construction time instead of copying config off an old mesh pc, whose
   * embedded TURN credentials may have aged out.
   */
  get iceServers () { return [...this.#iceServers] }

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

  /**
   * Everyone signaling currently says is in this room, whether or not a
   * WebRTC connection to them exists or ever will. `peerIds` answers "who
   * can I reach over a data channel right now"; this answers "who is HERE".
   * The relay floor (relayToUnreached) sends to the difference.
   */
  get knownPeerIds () {
    return [...this.#roster.keys()]
  }

  /** Connect to one xpacenode and join `roomId`. Resolves once the socket is open. */
  async join (nodeUrl, roomId) {
    this.#roomId  = roomId
    this.#nodeUrl = nodeUrl
    this.#pool    = this.#poolFactory()

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

    // Refreshed STUN/TURN credentials, re-issued by the node over the live
    // connection (riding its heartbeat cadence) BEFORE the welcome-issued
    // ones age out. Without this, any RTCPeer constructed after the
    // credential TTL (~1h) on a long-lived session got DEAD TURN — exactly
    // the lazily-created connections a newly-joining peer triggers — so
    // cross-NAT pairs silently lost their relay candidates. Same precedence
    // rule as welcome: an explicit caller override is never replaced. Only
    // RTCPeers created from now on pick the refresh up (an existing
    // RTCPeerConnection's ICE config is fixed at construction) — which is
    // precisely the broken case.
    this.#pool.on('ice', msg => {
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
      this.#roster.set(peerId, announced)
      this.dispatchEvent(new CustomEvent('peer:join', { detail: { peerId, announced } }))
      // A peer_join for someone we already track is normal on every
      // reconnect (both sides restate presence). Only rebuild the
      // connection if it's actually dead. Serialized against this peer's
      // inbound signals (see #withPeerLock) because a peer_join and the
      // first offer routinely arrive together, and both build peers.
      this.#withPeerLock(peerId, () => this.#ensureLivePeer(peerId))
    })

    this.#pool.on('peer_leave', msg => {
      const wasKnown = this.#roster.delete(msg.peerId)
      const hadConnection = this.#peers.has(msg.peerId)
      this.#teardownPeer(msg.peerId)
      // #teardownPeer only dispatches 'peer:leave' when an RTCPeer entry
      // existed — but a peer can be known purely through signaling with no
      // connection at all (relay-only policy, or an RTCPeer already torn
      // down after 'failed'). A room departure must surface either way.
      if (wasKnown && !hadConnection) {
        this.dispatchEvent(new CustomEvent('peer:leave', { detail: { peerId: msg.peerId } }))
      }
    })

    this.#pool.on('signal', msg => {
      const { from, payload } = msg
      if (!from || !payload) return
      this.#withPeerLock(from, async () => {
        await this.#ensureLivePeer(from)
        await this.#peers.get(from)?.peer.handleSignal(payload)
      })
    })

    // The relay floor's inbound half: a message another peer sent through
    // the xpacenode (see relayTo below) arrives here and is handed up as
    // the SAME 'message' event a data-channel delivery produces — layers
    // above (RealtimeChannel and everything on it) need no second code
    // path for it. `via: 'relay'` is informational only.
    this.#pool.on('relay', msg => {
      const { from, payload } = msg
      if (!from || from === this.#selfId || payload === undefined) return
      this.dispatchEvent(new CustomEvent('message', { detail: { from, payload, via: 'relay' } }))
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
    this.#roster.clear()
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

  // ── The relay floor (outbound half) ────────────────────────────────────
  // Why this exists: before it, the ONLY transport for application messages
  // was the P2P data channel — the xpacenode carried signaling and nothing
  // else. Any peer pair whose WebRTC negotiation failed or stalled (NAT
  // combinations ICE can't crack, a mobile engine quirk, UDP-hostile
  // networks) was silently, mutually invisible FOREVER, even though both
  // sides held a live socket to a server that knew they were in the same
  // room. These two methods close that hole: the message rides the already-
  // open signaling socket, the node forwards it (locally or across the
  // swarm via gossip, same as a 'signal' frame), and it arrives as a normal
  // 'message' event on the other side. Per-message opt-in is decided above
  // this class (MessageRegistry's `relay` strategy) — PeerMesh just carries.

  /**
   * Relay `payload` to one peer through the xpacenode instead of a data
   * channel. Fire-and-forget like send(); the pool buffers across
   * reconnects.
   * @param {string} peerId
   * @param {object} payload
   * @param {object} [opts]
   * @param {string} [opts.coalesceKey] Latest-only class (MessageRegistry
   *   `coalesce`): a newer relay with the same key supersedes an unsent
   *   older one, at every hop that can queue — the pool's offline buffer,
   *   the socket's backlog (see XpaceNodePool.send), and the node's own
   *   outbound fanout (the frame carries `ck` so the bridge can do the
   *   same on the receiver's side).
   * @returns {boolean} false only when the mesh has never joined (no pool).
   */
  relayTo (peerId, payload, { coalesceKey } = {}) {
    if (!this.#pool) return false
    this.#pool.send(
      { t: 'relay', roomId: this.#roomId, to: peerId, payload, ...(coalesceKey ? { ck: coalesceKey } : {}) },
      { coalesceKey: coalesceKey ? `${coalesceKey}@${peerId}` : undefined },
    )
    return true
  }

  /**
   * Relay `payload` to every peer signaling says is in the room but whose
   * data channel is NOT open right now — still negotiating, failed, or
   * (under transportPolicy 'relay-only') never attempted. Peers with an
   * open channel are deliberately skipped: the caller already broadcast()
   * to them, and both operations happen in the same synchronous turn, so a
   * channel cannot flip state in between — no peer ever gets both copies.
   * @param {object} payload
   * @param {object} [opts]
   * @param {string} [opts.coalesceKey] See relayTo.
   * @returns {string[]} the peerIds a relay copy was sent toward.
   */
  relayToUnreached (payload, { coalesceKey } = {}) {
    if (!this.#pool) return []
    const relayed = []
    for (const peerId of this.#roster.keys()) {
      if (this.#peers.get(peerId)?.peer.connected) continue
      this.#pool.send(
        { t: 'relay', roomId: this.#roomId, to: peerId, payload, ...(coalesceKey ? { ck: coalesceKey } : {}) },
        { coalesceKey: coalesceKey ? `${coalesceKey}@${peerId}` : undefined },
      )
      relayed.push(peerId)
    }
    return relayed
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
   * still sendonly and get counted with nothing audible — which is why
   * rising inbound bytes are NOT proof of audibility).
   *
   * Measured on real engines (spacehub's tests/browser/call-cross-engine-
   * media.mjs harness, Chromium and WebKit, strict FIFO relay, no artificial
   * reordering, inter-signal spacing preserved): Blink<->Blink calls wedged
   * exactly that way with concurrent application — one-way audio when the
   * caller was the polite side, silence in BOTH directions when it was
   * not — against zero once application was serialized per peer, which is
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
    // relay-only: never dial WebRTC at all — the relay floor carries
    // everything. Inbound 'signal' frames for a peer we never created are
    // already a safe no-op at the call site (optional chaining).
    if (this.#transportPolicy === 'relay-only') return
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
