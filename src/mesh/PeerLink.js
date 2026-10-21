/**
 * PeerLink — ONE connection to one peer, serving however many surfaces need it.
 *
 * THE DEFECT THIS CLOSES (D.3). Before this class, a "surface" and a
 * "connection" were the same object: a PeerMesh owned a private peerId →
 * RTCPeer map, and the only way for a second surface to reach the same person
 * was to build a second RTCPeer. Standing in the 3D world next to someone
 * whose conversation thread is open therefore opened TWO connections to that
 * one person — two DTLS handshakes, two ICE gathering passes, two heartbeats
 * of keep-alive, two sets of TURN relay allocations — that differed in
 * nothing but which room introduced them. Measured in spacehub's
 * tests/comms-connection-census.test.mjs. `DirectPeerChannel`'s `ownsMesh`
 * (D.1) let a second CHANNEL ride one MESH, and PeerMesh's re-join guard
 * (D.2) stopped a second surface from destroying a mesh it was handed. Both
 * were necessary. Neither let two MESHES, in two different rooms, share one
 * CONNECTION — because the peer map was private with no way in.
 *
 * THE MODEL: a connection is a leased resource, not a possession.
 *
 *   registry.acquire(peerId, …) → PeerLease
 *
 * A lease is a surface's handle on a shared connection. Surfaces hold leases;
 * nobody holds the connection. The link closes when the last lease is
 * released, and not one moment sooner. That inverts the old rule — where
 * whoever called close() first won and everyone else lost their connection —
 * into one where no single surface can take the connection away from another.
 *
 * WHY REFERENCE COUNTING, AND WHAT WAS REJECTED.
 *
 *   - An OWNER plus BORROWERS (what `ownsMesh` does one layer up) is right
 *     when one surface genuinely is the principal and the borrow is strictly
 *     shorter-lived — a call borrowing a conversation's mesh. It is wrong
 *     here: the world and a thread are peers, both long-lived, and neither
 *     can be nominated the owner in advance because which one opens first
 *     depends on where the user happened to walk. An owner model forces that
 *     arbitrary choice and then breaks when the "owner" leaves first.
 *   - EXPLICIT TRANSFER (donate/adopt) makes every surface responsible for
 *     knowing who else is attached, which is precisely the knowledge a
 *     shared layer exists to remove from its consumers.
 *   - So: reference counting, with the refcount OPAQUE to consumers. A
 *     surface declares interest by acquiring and withdraws it by releasing;
 *     it is never told, and must never branch on, how many others are
 *     attached. That is what keeps "one system, many views" honest — a
 *     surface's code reads the same whether it is alone or one of three.
 *
 * LANES: one connection, several logical channels.
 *
 * Sharing a connection would be worthless if every surface then had to read
 * every other surface's traffic. Each lease may name a `lane`, and a frame
 * sent on a lane is delivered only to leases on that lane. Lanes are
 * opt-in, and a laneless lease sends exactly the bytes this library has
 * always sent — see #routeMessage for the full compatibility rule, which is
 * load-bearing for an app whose users are not all on the same build.
 *
 * A lane is an isolation hint and a routing optimization. It is NEVER a
 * delivery gate: a tagged frame that matches no lane is delivered to every
 * lease rather than dropped, for the same reason PeerMesh#peerIds documents
 * at length — a silently discarded message is worse than a redundantly
 * delivered one, because the first leaves no trace and the second is
 * filtered by the `type` tag every channel above already applies.
 *
 * MEDIA rides the link, not a lane, because RTP has no lane concept: a
 * remote track arrives with no way to attribute it to one surface. So
 * inbound tracks fan out to every lease and each decides (CallChannel
 * already filters by peer). Outbound media goes through #setTrack, which
 * swaps a track INTO AN EXISTING SENDER instead of adding another one —
 * measured to hold the sender count flat at 2,2,2 across three camera
 * toggles where remove-and-re-add grew it 2→3→4. That measurement is why
 * media belongs on this shared connection at all, and it retires the
 * historical reason for opening a dedicated one.
 */

/**
 * Reserved wire key for lane routing. Reserved namespace, exactly like
 * `__xpacesync` for intro frames. Short because world-position ticks ride
 * this envelope at 10-20Hz per peer.
 */
export const LANE_TAG = '__xpacesync_lane'

/**
 * One surface's handle on a shared connection.
 *
 * Deliberately NOT a PeerMesh and not a channel: it is the smallest thing a
 * surface needs to use a connection it does not own — send, receive, attach
 * media, and let go. Everything product-shaped stays above it.
 *
 * Events: 'open', 'message' ({ payload }), 'track' ({ track, stream }),
 * 'failed', 'close'.
 */
export class PeerLease extends EventTarget {
  #link
  #lane
  #sendSignal
  #released = false
  // 'open' must reach a surface exactly once, however it got there.
  // A lease taken out while the connection is already up receives a REPLAYED
  // open (PeerLink#acquire) and, if the handshake happens to complete in the
  // same turn, the real one too. Two opens means the surface announces itself
  // twice and raises peer:open twice — found by the late-attach test, which
  // measured 2.
  #openEmitted = false

  /** @param {{link: PeerLink, lane: string|null, sendSignal: (payload: object) => boolean}} opts */
  constructor ({ link, lane, sendSignal }) {
    super()
    this.#link       = link
    this.#lane       = lane ?? null
    this.#sendSignal = sendSignal ?? (() => false)
  }

  get peerId ()      { return this.#link.peerId }
  get lane ()        { return this.#lane }
  get released ()    { return this.#released }
  get connected ()   { return !this.#released && this.#link.connected }
  get hasBeenOpen () { return this.#link.hasBeenOpen }
  /** Raw RTCPeerConnection, for callers needing WebRTC-level access. */
  get pc ()          { return this.#link.pc }
  /** True when the connection behind this lease has died and needs rebuilding. */
  get stale ()       { return this.#link.stale }

  /** Send one JSON-serializable payload on this lease's lane. */
  send (payload) {
    if (this.#released) return false
    return this.#link.send(this.#lane, payload)
  }

  /**
   * Feed an inbound signalling frame to the shared connection.
   *
   * ANY lease may do this, which is a correctness fix and not just
   * convenience. The xpacenode bridge dispatches a `to: peerId` frame by
   * peer identity, independent of which room it was sent in — so when one
   * device holds two sockets under one identity (two surfaces, two rooms), a
   * signal for the connection can arrive on EITHER socket. With a private
   * peer map per mesh, a frame landing on the wrong socket reached a
   * DIFFERENT RTCPeer and corrupted its negotiation; spacehub's
   * src/spaceinbox/runtime.js carries a comment about exactly this
   * ("not ours -- e.g. a PeerMesh RTC signal relayed to a different open
   * connection"). One link per peer per device makes the misroute
   * impossible: whichever socket it arrives on, it reaches the one
   * connection it was meant for.
   */
  handleSignal (payload) {
    if (this.#released) return
    return this.#link.handleSignal(payload)
  }

  /** @internal Outbound signalling, routed back through this surface's socket. */
  sendSignal (payload) {
    if (this.#released) return false
    try { return this.#sendSignal(payload) === true } catch { return false }
  }

  /** Attach a local track. Idempotent per track across every lease on the link. */
  addTrack (track, stream) {
    if (this.#released) return false
    return this.#link.addTrack(track, stream)
  }

  /**
   * Put `track` in the named sender slot, replacing whatever was there.
   * `null` empties the slot without giving it up — the constant-sender-count
   * path. See PeerLink#setTrack.
   */
  setTrack (name, track, stream) {
    if (this.#released) return false
    return this.#link.setTrack(name, track, stream)
  }

  /** Take `track` off the connection entirely, freeing its sender. */
  removeTrack (track) {
    if (this.#released) return false
    return this.#link.removeTrack(track)
  }

  /**
   * Withdraw this surface's interest in the connection.
   *
   * Never closes a connection another surface is still using. Idempotent, so
   * a stale handle released twice is harmless — the case that makes a
   * refcount dangerous if it is not handled, since a double release would
   * otherwise under-count and close a live connection out from under its
   * remaining holders.
   */
  release () {
    if (this.#released) return
    this.#released = true
    this.#link.release(this)
    this.dispatchEvent(new CustomEvent('close'))
  }

  /** @internal */
  emit (type, detail) {
    if (this.#released) return
    if (type === 'open') {
      if (this.#openEmitted) return
      this.#openEmitted = true
    }
    this.dispatchEvent(new CustomEvent(type, detail === undefined ? undefined : { detail }))
  }

  /**
   * @internal Called when the connection goes down, so a later re-open is a
   * real event again rather than being swallowed as a duplicate.
   */
  resetOpen () { this.#openEmitted = false }
}

export class PeerLink {
  #peerId
  #peer
  #leases       = new Set()
  #hasBeenOpen  = false
  #failed       = false
  #closed       = false
  #onEmpty
  #onWarn
  #attached     = new Set()  // local tracks already on the connection
  #slots        = new Map()  // slot name → the track currently in it
  #remoteTracks = []         // inbound tracks, replayed to a late-attaching surface
  #signalChain  = Promise.resolve()  // tail of this connection's serialized signal application

  /**
   * @param {object} [opts]
   * @param {string} [opts.peerId] Required in practice (the constructor throws
   *   without one); optional in the type only because the parameter object
   *   itself defaults to `{}`, the same convention PeerMesh uses for selfId.
   * @param {import('../transport/RTCPeer.js').RTCPeer} [opts.peer] Likewise required.
   * @param {() => void} [opts.onEmpty] Called once the last lease is released.
   * @param {(msg: string, ...rest: any[]) => void} [opts.onWarn]
   */
  constructor ({ peerId, peer, onEmpty, onWarn } = {}) {
    if (!peerId) throw new Error('PeerLink requires peerId')
    if (!peer)   throw new Error('PeerLink requires a peer')
    this.#peerId  = peerId
    this.#peer    = peer
    this.#onEmpty = onEmpty ?? (() => {})
    this.#onWarn  = onWarn ?? ((msg, ...rest) => console.warn(msg, ...rest))

    // The transport holds exactly ONE message callback and ONE track callback
    // (see RTCPeer#onMessage/#onTrack). That single-callback shape is why
    // sharing was impossible without this class: a second surface calling
    // onMessage() silently REPLACED the first surface's callback and struck
    // it deaf. RTCPeer is deliberately left untouched — this class claims
    // both seams once and multiplexes them, so the fan-out lives in the new
    // layer rather than changing a primitive every existing caller depends on.
    this.#peer.onMessage(payload => this.#routeMessage(payload))
    this.#peer.onTrack((track, stream) => this.#routeTrack(track, stream))
    this.#peer.addEventListener('signal', (/** @type {any} */ { detail }) => this.#forwardSignal(detail))
    this.#peer.addEventListener('open',   () => this.#onOpen())
    this.#peer.addEventListener('failed', () => this.kill())
    // A data channel that closed and later reopens is a genuine second
    // 'open', so clear the once-only guard rather than swallowing it.
    this.#peer.addEventListener('close',  () => {
      for (const lease of [...this.#leases]) lease.resetOpen()
    })
  }

  get peerId ()      { return this.#peerId }
  get pc ()          { return this.#peer.pc }
  get connected ()   { return !this.#closed && this.#peer.connected }
  get hasBeenOpen () { return this.#hasBeenOpen }
  get closed ()      { return this.#closed }
  get failed ()      { return this.#failed }
  /** How many surfaces are attached. Diagnostics only — consumers must not branch on it. */
  get leaseCount ()  { return this.#leases.size }

  /**
   * Dead and needing a rebuild: it worked once and does not now. A link that
   * has NEVER opened is not stale — that is ordinary first-time negotiation,
   * and tearing it down mid-handshake is the bug that rule exists to avoid.
   */
  get stale () { return this.#closed || this.#failed || (this.#hasBeenOpen && !this.#peer.connected) }

  /** Usable for a fresh acquire. */
  get usable () { return !this.stale }

  /**
   * Attach one surface.
   *
   * LATE ATTACH is the whole reason this is a method and not a constructor
   * argument. A surface that acquires AFTER the connection is already open
   * would otherwise never see the 'open' it is waiting for, and would sit
   * there believing it had no connection while holding a live one. So the
   * current state is REPLAYED to the newcomer: 'open' if the link is open,
   * plus every remote track still live. Replay is deferred by one microtask
   * because the caller cannot attach listeners until after this returns.
   *
   * Ended tracks are deliberately not replayed. Replaying dead media is its
   * own bug class — the one spacehub pinned as a test it could not fix from
   * the app side (see PeerMesh#removeTrack).
   *
   * @param {object} [opts]
   * @param {string|null} [opts.lane] Logical channel for this surface.
   * @param {(payload: object) => boolean} [opts.sendSignal] How this surface
   *   puts a signalling frame on the wire; false when it cannot, so the link
   *   can fail over to another surface still in a room.
   * @returns {PeerLease}
   */
  acquire ({ lane = null, sendSignal } = {}) {
    if (this.#closed) throw new Error('PeerLink is closed')
    const lease = new PeerLease({ link: this, lane, sendSignal })
    this.#leases.add(lease)

    queueMicrotask(() => {
      if (lease.released || this.#closed) return
      // `connected`, not `hasBeenOpen`: a newcomer must only be told the
      // connection is open if it is open NOW. A link that opened once and has
      // since gone quiet is stale, and telling a surface "you are connected"
      // about a dead connection is worse than telling it nothing — it would
      // start sending into a void with send() reporting success.
      if (this.connected) lease.emit('open')
      for (const { track, stream } of this.#remoteTracks) {
        if (track?.readyState === 'ended') continue
        lease.emit('track', { track, stream })
      }
    })

    return lease
  }

  /**
   * Detach one surface. Closes the connection only when it was the last.
   * @internal called by PeerLease#release
   */
  release (lease) {
    if (!this.#leases.delete(lease)) return
    if (this.#leases.size > 0) return
    this.#close()
    this.#onEmpty()
  }

  /**
   * The connection is dead for everyone. Tell every surface, then close.
   *
   * One failure, one notification per surface: each then releases its own
   * lease through its own teardown path, so nobody is left holding a handle
   * to a corpse and no surface has to learn about the others to find out.
   */
  kill () {
    if (this.#closed) return
    this.#failed = true
    const leases = [...this.#leases]
    this.#close()
    this.#onEmpty()
    for (const lease of leases) lease.emit('failed')
  }

  send (lane, payload) {
    if (this.#closed) return false
    // A laneless send puts the payload on the wire bare, byte for byte as
    // this library always has. Only a lane-bearing send is wrapped.
    this.#peer.send(lane === null || lane === undefined ? payload : { [LANE_TAG]: lane, p: payload })
    return true
  }

  /**
   * Apply one inbound signalling frame to this connection, after every frame
   * already queued on it and never alongside one.
   *
   * WHY THE CHAIN LIVES HERE AND NOT ONLY IN PeerMesh. PeerMesh serializes
   * per (surface, peer) with its own `#withPeerLock`, which is enough when a
   * surface is alone on a connection. It stops being enough the moment two
   * surfaces SHARE one — which is the whole point of this class. Each mesh
   * has its own XpaceNodePool, its own socket, and its own lock table, so a
   * frame arriving for the world surface and a frame arriving for the thread
   * surface are serialized against their own siblings and against nothing
   * else. Both then land on `this.#peer` — ONE RTCPeer, one
   * `signalingState` — concurrently, which is exactly the interleaving that
   * made an impolite peer discard a real offer as glare that never happened
   * (see PeerMesh#withPeerLock for the measurement: 5 of 38 calls wedged
   * before serializing, 0 of 32 after).
   *
   * So the two locks are not duplicates of each other. The mesh's lock keeps
   * ensure-then-apply indivisible for one surface; this one makes "one
   * connection applies one description at a time" a property of the
   * connection, true however many surfaces or sockets feed it. Putting the
   * guarantee where the shared mutable state actually is, is what keeps
   * sharing from quietly undoing the glare fix.
   *
   * @param {object} payload
   * @returns {Promise<void>} resolves once THIS frame has been applied.
   */
  handleSignal (payload) {
    const next = this.#signalChain.then(() => {
      if (this.#closed) return
      return this.#peer.handleSignal(payload)
    }).catch(err => {
      this.#onWarn?.('[xpacesync] signal application failed', this.#peerId, err)
    })
    this.#signalChain = next
    return next
  }

  /**
   * Attach a local track once, however many surfaces ask.
   *
   * Idempotent per track by identity — the lesson CallChannel#addLocalStream
   * already learned ("A sender already exists for the track" is a throw, not
   * a no-op), generalized: with several surfaces on one connection, two of
   * them replaying the same track is no longer a caller mistake but the
   * expected case.
   */
  addTrack (track, stream) {
    if (this.#closed || !track) return false
    if (this.#attached.has(track)) return false
    this.#attached.add(track)
    this.#peer.addTrack(track, stream)
    return true
  }

  /**
   * Put `track` in the named sender slot, reusing the sender already there.
   *
   * `name` is a stable ROLE — 'mic', 'camera', 'screen' — never a kind and
   * never an index, so a slot survives the track in it being swapped.
   * Passing `null` empties the slot while KEEPING it, which is how a camera
   * turns off without the sender count moving.
   *
   * This is the measured half of the media question. Swapping in place held
   * the sender count at 2,2,2 across three camera toggles where
   * remove-and-re-add grew it 2→3→4, and that growth was the reason a
   * dedicated media connection ever looked necessary. With the slot stable,
   * media on the shared connection costs nothing it did not already cost —
   * so the two opposing media strategies collapse into this one.
   */
  setTrack (name, track, stream) {
    if (this.#closed || !name) return false
    const previous = this.#slots.get(name)
    if (previous) this.#attached.delete(previous)
    this.#slots.set(name, track ?? null)
    if (track) this.#attached.add(track)
    return this.#peer.setTrack(name, track ?? null, stream)
  }

  /** Take `track` off the connection, freeing its sender. */
  removeTrack (track) {
    if (this.#closed || !track) return false
    this.#attached.delete(track)
    for (const [name, held] of this.#slots) if (held === track) this.#slots.set(name, null)
    this.#remoteTracks = this.#remoteTracks.filter(entry => entry.track !== track)
    return this.#peer.removeTrack(track)
  }

  // ── Internal ────────────────────────────────────────────────────────────

  #close () {
    if (this.#closed) return
    this.#closed = true
    this.#leases.clear()
    this.#attached.clear()
    this.#slots.clear()
    this.#remoteTracks = []
    this.#peer.close()
  }

  #onOpen () {
    this.#hasBeenOpen = true
    for (const lease of [...this.#leases]) lease.emit('open')
  }

  /**
   * THE COMPATIBILITY RULE, and it is graded on purpose.
   *
   *   tagged frame, lanes match     → only those leases. Exact isolation.
   *   tagged frame, no lane matches → every lease. Never dropped.
   *   untagged frame               → every lease. What this library has
   *                                   always done, and what a peer on an
   *                                   older build still sends.
   *
   * The third row is why lanes are opt-in rather than automatic. These apps
   * ship as a PWA and an Android package that do not update in lockstep, so
   * two users are routinely on different builds. If a laneless peer's frames
   * were treated as unroutable, an upgrade would break conversations between
   * a new build and an old one — which for SpaceInbox and SpaceOrb, the two
   * surfaces that must keep working through this, is the only compatibility
   * that matters. Untagged in, untagged out: an old peer sees no change at
   * all, and the moment both ends carry lanes the routing sharpens by itself
   * with no flag day.
   */
  #routeMessage (payload) {
    const lane = payload?.[LANE_TAG]
    if (typeof lane !== 'string') {
      for (const lease of [...this.#leases]) lease.emit('message', { payload })
      return
    }
    const inner  = payload.p
    const onLane = [...this.#leases].filter(lease => lease.lane === lane)
    const target = onLane.length > 0 ? onLane : [...this.#leases]
    for (const lease of target) lease.emit('message', { payload: inner })
  }

  /**
   * Inbound media goes to every surface: RTP carries no lane, so the link
   * cannot attribute a track to one of them and must not guess. Remembered
   * so a surface attaching later still receives tracks that arrived before
   * it existed — the gap PeerMesh#getPeerConnections documents consumers
   * working around by hand today.
   */
  #routeTrack (track, stream) {
    this.#remoteTracks = this.#remoteTracks.filter(entry => entry.track !== track)
    this.#remoteTracks.push({ track, stream })
    for (const lease of [...this.#leases]) lease.emit('track', { track, stream })
  }

  /**
   * Outbound signalling leaves through exactly ONE surface's socket.
   *
   * Not every surface's: the same SDP sent twice reaches the peer twice and
   * is answered twice, which is self-inflicted glare on a connection that
   * had none. So the leases are tried in attachment order and the first one
   * that actually puts the frame on a socket wins.
   *
   * Trying in order, rather than remembering a nominated primary, is what
   * makes this survive the interesting case: the surface that opened the
   * connection leaves the room first. Its sendSignal then reports failure
   * (its pool is gone), the next lease carries the frame instead, and
   * renegotiation keeps working on a connection whose original owner is no
   * longer there — the exact scenario an owner-based design cannot express.
   */
  #forwardSignal (payload) {
    for (const lease of this.#leases) {
      if (lease.sendSignal(payload)) return true
    }
    // Reaching here means something still holds a lease while no surface can
    // reach the signalling network. Negotiation cannot proceed and saying so
    // is the only honest option; silence here was previously indistinguishable
    // from a connection that simply never came up.
    this.#onWarn('[PeerLink] no surface could carry a signal frame', this.#peerId, payload?.type)
    return false
  }
}
