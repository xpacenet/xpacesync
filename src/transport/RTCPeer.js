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

  onMessage (cb) { this.#onMessageCb = cb }
  onTrack   (cb) { this.#onTrackCb   = cb }

  get pc ()        { return this.#pc }
  get connected () { return this.#dc?.readyState === 'open' }

  close () { try { this.#pc.close() } catch {} }
}
