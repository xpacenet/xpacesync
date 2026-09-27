/**
 * CallChannel — ring/accept/reject/hangup signaling plus real audio/video,
 * layered on an ALREADY-JOINED PeerMesh (the same mesh a conversation's
 * message channel already opened) rather than a second connection.
 *
 * Why no new transport: media needs no protocol of its own here. RTCPeer
 * already wraps a real RTCPeerConnection with a working addTrack()/onTrack()
 * pair (see transport/RTCPeer.js), and PeerMesh already fans both of those
 * out to every current/future peer connection it holds (see mesh/PeerMesh.js
 * addTrack()/onTrack()). Calling PeerMesh#addTrack() triggers the SAME
 * onnegotiationneeded → offer/answer → PeerMesh's existing 'signal' relay
 * over xpacenode that already carries this peer's data-channel SDP — so
 * attaching a camera/mic track renegotiates the SAME already-open connection
 * a text conversation is using, with no parallel signaling path to invent
 * or keep in sync.
 *
 * What THIS class actually adds is the missing piece: a small, typed
 * call-lifecycle protocol (ring / accept / reject / hangup), carried the
 * same way 'spaceinbox.envelope.v1' and 'spaceinbox.device-sync.v1' already
 * are — one message `type` tag on the shared mesh, filtered to one peer.
 * DirectPeerChannel is deliberately NOT reused here even though the shape
 * matches: DirectPeerChannel.close() calls mesh.leave(), which is correct
 * for a channel that owns its mesh but wrong here — a CallChannel's mesh is
 * owned by whatever already-open conversation lent it, and ending a call
 * must never tear down that conversation's connection out from under it.
 *
 * Caveat callers must know: PeerMesh#onTrack(cb) holds exactly one callback
 * (see PeerMesh's #trackCb), so only one CallChannel should be active on a
 * given mesh at a time — the real scope this class targets (one call at a
 * time, one peer). A second concurrent CallChannel on the same mesh would
 * silently steal the first one's inbound-track callback.
 */
export class CallChannel extends EventTarget {
  #mesh
  #peerId
  #type
  #closed = false
  #localStream = null
  #state = 'idle' // idle | ringing-outbound | ringing-inbound | active | ended

  /**
   * @param {import('../mesh/PeerMesh.js').PeerMesh} mesh An already-joined mesh
   *   (e.g. the one behind an existing message channel to this same peer).
   * @param {{peerId: string, type?: string}} opts `type` defaults to
   *   'spaceinbox.call.v1' but any app-defined tag works, same convention as
   *   DirectPeerChannel.
   */
  constructor (mesh, { peerId, type = 'spaceinbox.call.v1' }) {
    super()
    if (!mesh) throw new Error('CallChannel requires an already-joined mesh')
    if (!peerId) throw new Error('CallChannel requires peerId')
    this.#mesh = mesh
    this.#peerId = peerId
    this.#type = type
    this.#mesh.addEventListener('message', this.#handleMessage)
    this.#mesh.onTrack(this.#handleTrack)
  }

  get peerId () { return this.#peerId }
  get type () { return this.#type }
  get state () { return this.#state }

  /** Place an outbound call: sends 'ring' describing the requested media. */
  ring ({ video = false } = {}) {
    if (this.#closed) return false
    this.#setState('ringing-outbound')
    return this.#send({ kind: 'ring', video })
  }

  /** Accept an inbound ring. Caller still separately calls addLocalStream(). */
  accept () {
    if (this.#closed) return false
    this.#setState('active')
    return this.#send({ kind: 'accept' })
  }

  /** Decline an inbound ring. */
  reject () {
    if (this.#closed) return false
    const sent = this.#send({ kind: 'reject' })
    this.#setState('ended')
    return sent
  }

  /** End an active or ringing call from either side. */
  hangup () {
    if (this.#closed) return false
    const sent = this.#send({ kind: 'hangup' })
    this.#setState('ended')
    return sent
  }

  /**
   * Attach local media tracks to the shared mesh connection for this peer.
   * Real getUserMedia() acquisition is the app's job (browser API, not
   * network policy) — this just wires the resulting stream's tracks onto
   * the already-open peer connection via PeerMesh#addTrack().
   * @param {MediaStream} stream
   */
  addLocalStream (stream) {
    this.#localStream = stream
    for (const track of stream.getTracks()) this.#mesh.addTrack(track, stream)
  }

  /** Stop local tracks and detach listeners. Never touches the shared mesh's connection/room. */
  close () {
    if (this.#closed) return
    this.#closed = true
    this.#mesh.removeEventListener('message', this.#handleMessage)
    for (const track of this.#localStream?.getTracks() ?? []) track.stop()
    this.#localStream = null
    this.#setState('ended')
  }

  #send (signal) {
    return this.#mesh.send(this.#peerId, {
      type: this.#type,
      payload: signal,
      meta: { from: this.#mesh.selfId, ts: Date.now() },
    })
  }

  #handleMessage = ({ detail }) => {
    const { from, payload: message } = detail
    if (this.#closed || from !== this.#peerId || message?.type !== this.#type) return
    const signal = message.payload
    if (signal?.kind === 'ring') {
      this.#setState('ringing-inbound')
      this.dispatchEvent(new CustomEvent('ring', { detail: { video: !!signal.video } }))
    } else if (signal?.kind === 'accept') {
      this.#setState('active')
      this.dispatchEvent(new CustomEvent('accepted'))
    } else if (signal?.kind === 'reject') {
      this.#setState('ended')
      this.dispatchEvent(new CustomEvent('rejected'))
    } else if (signal?.kind === 'hangup') {
      this.#setState('ended')
      this.dispatchEvent(new CustomEvent('hangup'))
    }
  }

  #handleTrack = (track, stream, fromPeerId) => {
    if (this.#closed || fromPeerId !== this.#peerId) return
    this.dispatchEvent(new CustomEvent('remote-track', { detail: { track, stream } }))
  }

  #setState (state) {
    this.#state = state
    this.dispatchEvent(new CustomEvent('state', { detail: { state } }))
  }
}
