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

  #negotiate = async () => {
    if (this.#makingOffer) return
    if (this.#pc.signalingState !== 'stable') return
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

  addTrack (track, stream) { try { this.#pc.addTrack(track, stream) } catch {} }

  onMessage (cb) { this.#onMessageCb = cb }
  onTrack   (cb) { this.#onTrackCb   = cb }

  get pc ()        { return this.#pc }
  get connected () { return this.#dc?.readyState === 'open' }

  close () { try { this.#pc.close() } catch {} }
}
