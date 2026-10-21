/**
 * RTCPeer — perfect-negotiation WebRTC peer with one ordered data channel.
 *
 * Extracted verbatim from spacework/client/src/sync/remote.js (v3, itself
 * unchanged since v2). Fully generic already: it knows nothing about rooms,
 * identity, or message semantics — only how to negotiate one connection and
 * move JSON-serializable objects and media tracks across it.
 *
 * ICE servers are passed in by the caller so consumers can supply their own
 * TURN/STUN policy instead of inheriting SpaceWork's.
 */
export class RTCPeer extends EventTarget {
  #pc
  #dc              = null
  #isPolite        = false
  #makingOffer     = false
  #ignoreOffer     = false
  #onMessageCb     = null
  #onTrackCb       = null
  #iceQueue        = []
  #hasRemoteDesc   = false
  // Named sender slots — see setTrack(). Keyed by a stable role ('mic',
  // 'camera', 'screen') so a slot outlives the track currently inside it.
  // Only setTrack() populates this; plain addTrack() stays slotless, exactly
  // as every existing caller already uses it.
  #senders         = new Map()
  // Set whenever #negotiate is asked to run (onnegotiationneeded fires, or a
  // glare-recovery retry is scheduled) while the connection ISN'T in
  // 'stable' -- e.g. addTrack() called for a mid-call video toggle while the
  // call's own initial audio offer/answer, or another track's renegotiation,
  // is still in flight. The spec says onnegotiationneeded re-fires once the
  // connection returns to 'stable' if negotiation is still needed, and
  // that's exactly what this class relied on before this existed -- but
  // that re-fire is engine-internal bookkeeping this class has no visibility
  // into, and the exact same file already documents a real, confirmed gap
  // between what the spec promises and what a WebKit build actually does
  // (implicit rollback -- see handleSignal's own comment on it). Given the
  // real-device report this was added for (a mid-call video track added
  // ONCE, correctly, via addTrack -- confirmed by reading CallChannel's own
  // idempotent-per-track logic -- that then NEVER reaches the peer, on
  // either platform, across an entire test session with no successful
  // renegotiation ever observed) is exactly the symptom of a missed
  // onnegotiationneeded re-fire, this makes the retry EXPLICIT instead of
  // trusting the engine to remember on our behalf: onsignalingstatechange
  // below re-invokes #negotiate the moment 'stable' is reached again, using
  // this flag rather than a fixed timer, so it fires as soon as the
  // connection is actually able to negotiate rather than a guessed delay.
  #negotiationPending = false

  #negotiate = async () => {
    if (this.#makingOffer || this.#pc.signalingState !== 'stable') {
      this.#negotiationPending = true
      return
    }
    this.#negotiationPending = false
    try {
      this.#makingOffer = true
      await this.#pc.setLocalDescription()
      this.dispatchEvent(new CustomEvent('signal', {
        detail: { type: 'offer', sdp: this.#pc.localDescription.sdp },
      }))
    } catch (err) {
      console.warn('[RTCPeer] negotiate error', err)
    } finally {
      this.#makingOffer = false
    }
  }

  constructor (isPolite, iceServers = []) {
    super()
    this.#isPolite = isPolite
    this.#pc = new RTCPeerConnection({ iceServers })

    if (!isPolite) {
      this.#dc = this.#pc.createDataChannel('xpacesync', { ordered: true })
      this.#hookDC(this.#dc)
    }

    this.#pc.ondatachannel = ({ channel }) => {
      this.#dc = channel
      this.#hookDC(channel)
    }

    this.#pc.onicecandidate = ({ candidate }) => {
      if (candidate) {
        this.dispatchEvent(new CustomEvent('signal', {
          detail: { type: 'ice', candidate: candidate.toJSON() },
        }))
      }
    }

    this.#pc.onnegotiationneeded = this.#negotiate

    // Explicit backstop for #negotiationPending (see its own doc comment):
    // the moment the connection is actually able to negotiate again, retry
    // any negotiation #negotiate had to defer instead of trusting the
    // engine's own onnegotiationneeded-refires-on-stable bookkeeping alone.
    this.#pc.onsignalingstatechange = () => {
      if (this.#pc.signalingState === 'stable' && this.#negotiationPending) {
        this.#negotiate()
      }
    }

    this.#pc.ontrack = ({ track, streams }) => {
      const stream = streams[0] ?? new MediaStream([track])
      this.#onTrackCb?.(track, stream)
    }

    this.#pc.onconnectionstatechange = () => {
      if (this.#pc.connectionState === 'failed') {
        this.dispatchEvent(new CustomEvent('failed'))
      }
    }
  }

  #hookDC (dc) {
    dc.onopen    = () => this.dispatchEvent(new CustomEvent('open'))
    dc.onclose   = () => this.dispatchEvent(new CustomEvent('close'))
    dc.onmessage = ({ data }) => {
      try { this.#onMessageCb?.(JSON.parse(data)) } catch {}
    }
  }

  async handleSignal ({ type, sdp, candidate }) {
    try {
      if (type === 'offer') {
        const hadLocalOffer = this.#pc.signalingState === 'have-local-offer'
        const collision     = this.#makingOffer || hadLocalOffer
        this.#ignoreOffer   = !this.#isPolite && collision
        if (this.#ignoreOffer) return

        // Glare, polite side: our own outstanding local offer has to yield
        // to the peer's. setRemoteDescription() below is SPEC'D to perform
        // this rollback implicitly when called with an offer while in
        // 'have-local-offer' — Chrome and Firefox honor that, but not every
        // WebKit/Safari build does (a real, documented cross-engine gap in
        // "implicit rollback" support that predates a lot of iOS still in
        // the field). Relying on it silently means this exact renegotiation
        // — a SECOND offer/answer on an already-open connection, exactly
        // what CallChannel#addLocalStream triggers on accept and on every
        // later mid-call track add — can throw on setRemoteDescription
        // instead of rolling back, which the try/catch around this whole
        // method swallows into a console.warn: the remote offer is never
        // applied, no answer is ever sent back, and the peer that sent it
        // is left stuck in 'have-local-offer' forever (exactly "connects,
        // then nothing exchanges" / "gets stuck mid-call" as reported).
        // Doing the rollback EXPLICITLY first is behaviorally identical on
        // engines that already do it implicitly (rollback then apply is
        // what the implicit path does internally) and is the one extra
        // step that also works on engines that don't.
        if (this.#isPolite && hadLocalOffer) {
          try { await this.#pc.setLocalDescription({ type: 'rollback' }) } catch (err) {
            console.warn('[RTCPeer] rollback before glare offer failed', err)
          }
        }

        await this.#pc.setRemoteDescription({ type: 'offer', sdp })
        this.#hasRemoteDesc = true
        await this.#pc.setLocalDescription()
        this.dispatchEvent(new CustomEvent('signal', {
          detail: { type: 'answer', sdp: this.#pc.localDescription.sdp },
        }))
        await this.#drainIceQueue()

        if (this.#isPolite && hadLocalOffer) setTimeout(this.#negotiate, 200)

      } else if (type === 'answer') {
        if (this.#pc.signalingState === 'have-local-offer') {
          await this.#pc.setRemoteDescription({ type: 'answer', sdp })
          this.#hasRemoteDesc = true
          await this.#drainIceQueue()
        }

      } else if (type === 'ice') {
        if (!this.#hasRemoteDesc) {
          this.#iceQueue.push(candidate)
        } else {
          try { await this.#pc.addIceCandidate(candidate) } catch (err) {
            if (!this.#ignoreOffer) console.warn('[RTCPeer] addIceCandidate', err)
          }
        }
      }
    } catch (err) {
      console.warn('[RTCPeer] handleSignal', type, err)
    }
  }

  async #drainIceQueue () {
    const queued = this.#iceQueue.splice(0)
    for (const c of queued) {
      try { await this.#pc.addIceCandidate(c) } catch {}
    }
  }

  send (msg) {
    if (this.#dc?.readyState === 'open') this.#dc.send(JSON.stringify(msg))
  }

  // v2 (real-device video bug hunt): this used to swallow every addTrack
  // failure with an empty catch -- CallChannel's own doc comment already
  // notes RTCPeerConnection#addTrack throws on a genuine double-add (which
  // CallChannel's idempotent-per-track check now prevents), but ANY OTHER
  // real failure here (a closed connection, an engine-specific rejection on
  // real mobile hardware, anything not yet seen in this sandbox) was
  // previously invisible: the track is silently never sent, with nothing in
  // the console and no event for the app layer to react to -- exactly
  // indistinguishable, from the outside, from "sent fine but the peer's
  // rendering pipeline dropped it," which is the ambiguity blocking a real
  // diagnosis of the founder's "camera turns on locally, never appears on
  // the peer's side" report. Logging every failure (not just the expected
  // double-add case) turns the next real-device console capture into actual
  // evidence instead of another guess.
  addTrack (track, stream) {
    try {
      this.#pc.addTrack(track, stream)
    } catch (err) {
      console.warn('[RTCPeer] addTrack failed -- track was NOT sent to the peer', track?.kind, track?.id, err)
    }
  }

  /**
   * Put `track` in the sender slot named `name`, creating the slot the first
   * time and REUSING it every time after. `null` empties the slot without
   * giving it up.
   *
   * WHY A NAMED SLOT INSTEAD OF ANOTHER addTrack. addTrack appends a sender
   * every time and nothing ever took one away, so each camera toggle left its
   * predecessor behind: measured, the sender count grew 2 → 3 → 4 across
   * three toggles. Swapping the track INSIDE the sender holds it at 2, 2, 2.
   * That difference is the whole reason media can live on a long-lived shared
   * connection instead of needing a second, disposable one opened for it --
   * the historical justification for a dedicated media connection was this
   * growth, and in-place replacement removes it.
   *
   * `name` is a stable ROLE ('mic', 'camera', 'screen'), deliberately not a
   * kind and not an index: a slot has to outlive the track inside it, and both
   * of those change identity whenever media is toggled. A screen share and a
   * camera are both 'video' and must not collide.
   *
   * replaceTrack() renegotiates nothing -- same transceiver, same m-line, no
   * offer/answer. That is also why it cannot CREATE the first sender, hence
   * the addTrack on the empty-slot path, which negotiates exactly once.
   *
   * addTrack() itself is untouched: this is an additional verb, not a changed
   * one, because existing callers depend on its accumulate-and-replay shape.
   * @param {string} name @param {MediaStreamTrack|null} track @param {MediaStream} [stream]
   * @returns {boolean} whether the slot now holds what was asked for.
   */
  setTrack (name, track, stream) {
    if (!name) return false
    const existing = this.#senders.get(name)
    if (existing) {
      try {
        const result = existing.replaceTrack(track ?? null)
        // Async in the real API, synchronous in some test doubles. An
        // unobserved rejection here is an invisible dead camera -- the exact
        // failure class addTrack's logging below was added to make visible.
        if (result && typeof result.catch === 'function') {
          result.catch(err => console.warn('[RTCPeer] replaceTrack failed -- slot', name, 'still holds the old track', err))
        }
        return true
      } catch (err) {
        console.warn('[RTCPeer] replaceTrack threw -- slot', name, 'still holds the old track', err)
        return false
      }
    }
    if (!track) return true   // an empty slot asked to empty: already so
    try {
      const sender = this.#pc.addTrack(track, stream)
      if (sender) this.#senders.set(name, sender)
      return true
    } catch (err) {
      console.warn('[RTCPeer] setTrack could not open slot', name, '-- track was NOT sent', track?.kind, track?.id, err)
      return false
    }
  }

  /**
   * Take `track` off this connection entirely, freeing its sender.
   *
   * The counterpart this class never had. Without it, the only way to stop
   * sending was to stop the TRACK, which leaves a live sender transmitting
   * nothing and leaves the track in whatever list replays onto the NEXT
   * connection -- so a connection rebuilt after a phone woke up was born
   * carrying the dead media of every call before it. See PeerMesh#removeTrack
   * for the layer that uses this to close that gap, and why neither half
   * worked without the other.
   * @returns {boolean} whether a sender was actually removed.
   */
  removeTrack (track) {
    if (!track) return false
    let sender = null
    try {
      sender = this.#pc.getSenders?.().find(s => s.track === track) ?? null
    } catch (err) {
      console.warn('[RTCPeer] getSenders failed during removeTrack', err)
      return false
    }
    if (!sender) return false
    try {
      this.#pc.removeTrack(sender)
    } catch (err) {
      console.warn('[RTCPeer] removeTrack failed -- the sender is still attached', track?.kind, track?.id, err)
      return false
    }
    for (const [name, held] of [...this.#senders]) if (held === sender) this.#senders.delete(name)
    return true
  }

  /**
   * What each named slot currently holds, read from the senders themselves
   * rather than a parallel bookkeeping map — one source of truth, so the
   * report cannot drift from the connection's real state. `null` means the
   * slot exists and is empty (a camera toggled off). Diagnostics and tests.
   * @returns {Map<string, MediaStreamTrack|null>}
   */
  get slotTracks () {
    const out = new Map()
    for (const [name, sender] of this.#senders) out.set(name, sender.track ?? null)
    return out
  }

  onMessage (cb) { this.#onMessageCb = cb }
  onTrack   (cb) { this.#onTrackCb   = cb }

  get pc ()        { return this.#pc }
  get connected () { return this.#dc?.readyState === 'open' }

  close () { try { this.#pc.close() } catch {} }
}
