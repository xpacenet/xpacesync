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
  #localTracks = [] // every track ever passed to addLocalStream(), across possibly several calls -- see close()
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

  /**
   * Re-point this channel at a DIFFERENT peer on the SAME already-open mesh,
   * without tearing down localStream, listeners, or call state.
   *
   * Why this exists: a caller places a call knowing only a best-guess
   * "current" peerId for the callee (an app-level cache, e.g. one entry on a
   * relationship record) -- CallChannel itself has no way to know in advance
   * which of the callee's possibly-several devices will actually answer.
   * Once a real accept/ring genuinely arrives from some OTHER peer already
   * live on this mesh (the app layer, not this class, is what notices this --
   * see #handleMessage's strict equality check, which is exactly why a
   * message from the real answering device was being silently ignored before
   * this existed), the app needs to lock this channel onto that ACTUAL
   * pairing for every remaining step: further signals, addLocalStream's
   * already-attached tracks, and -- critically -- #handleTrack's own
   * from-peer filter, which otherwise drops every inbound audio/video track
   * from the real peer because it still only recognizes the original guess.
   *
   * Deliberately NOT a new CallChannel: reconstructing one would mean
   * removeEventListener/addEventListener churn the app layer would have to
   * replicate perfectly, and — the actual reason this is a method here
   * instead of "just close and reopen" — close() stops every local track,
   * which would kill the user's own already-acquired mic/camera mid-call
   * with no way to reacquire the exact same stream. Re-targeting in place
   * keeps localStream, the mesh listeners, and #state completely untouched;
   * only #peerId (and therefore who #handleMessage/#handleTrack/#send
   * address) changes.
   * @param {string} peerId
   */
  retarget (peerId) {
    if (this.#closed || !peerId || peerId === this.#peerId) return
    this.#peerId = peerId
  }

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
   * Tell the peer whether THIS side's video is currently live, independent
   * of ring/accept/reject/hangup. Added for the "both sides turn video off
   * mid-call" bug: turning video off is purely local (setCallVideo just sets
   * `track.enabled = false` -- there is no removeTrack/renegotiation to
   * notice on the receiving end), so before this existed the peer had no
   * way to learn "their camera just went off" versus "their camera track
   * just hasn't sent a new frame in a while" -- the app layer was left
   * inferring peer video state only from whether a video track had EVER
   * arrived (sticky-forever), which is wrong the moment a peer turns their
   * camera back off: the receiving side kept showing a blank/black remote
   * pane instead of falling back to an audio-call UI, because nothing ever
   * told it the peer's video had gone quiet. This is an explicit, real
   * signal for that transition in both directions (on and off), sent
   * whenever the local video toggle changes and additionally whenever
   * screen-share starts/stops (since either can flip whether this side has
   * live video for the other end to react to).
   * @param {boolean} on
   */
  setVideoState (on) {
    if (this.#closed) return false
    return this.#send({ kind: 'video-state', on: !!on })
  }

  /**
   * Attach local media tracks to the shared mesh connection for this peer.
   * Real getUserMedia() acquisition is the app's job (browser API, not
   * network policy) — this just wires the resulting stream's tracks onto
   * the already-open peer connection via PeerMesh#addTrack().
   *
   * Callable more than once per call (e.g. audio at accept-time, then video
   * added mid-call when the user switches it on) — ACCUMULATES every track
   * onto `#localTracks` rather than replacing a single `#localStream`
   * reference outright. The old `this.#localStream = stream` here silently
   * dropped the reference to whatever was attached before: `close()` only
   * stopped whatever `#localStream` currently pointed at, so a mid-call
   * addLocalStream([videoTrack]) call meant hangup left the ORIGINAL mic
   * track running forever (a real leaked-microphone bug, found while
   * tracing the v189 mid-call video-switch regression). Tracked as a plain
   * array rather than a real `MediaStream` so this class stays usable in a
   * plain Node test environment (no DOM/WebRTC globals) the way every other
   * method here already is.
   * Idempotent per track: a track already attached (by identity, not by
   * stream) is never re-passed to PeerMesh#addTrack. PeerMesh#addTrack
   * ultimately reaches RTCPeerConnection#addTrack, which THROWS
   * ("A sender already exists for the track") if called twice for the same
   * track — RTCPeer#addTrack swallows that in a try/catch, so calling
   * addLocalStream twice with an overlapping track used to be a silent
   * no-op rather than a hard error, but it still re-entered addTrack (and
   * therefore PeerMesh's own onnegotiationneeded-triggering path) for a
   * track that was already live, real wasted work on every repeated
   * add/toggle. Found while tracing the founder's real-device report of
   * mid-call video toggling becoming unreliable after being cycled
   * on/off/on several times — this alone was not reproduced as sufficient
   * to explain that report end-to-end (this class has no memory of
   * *disabling* a track, only of ever having attached one, and SpaceHub's
   * own toggle-off path never calls addLocalStream again for a track it
   * already has — see index.html's setCallVideo), but it is a real,
   * independently-worth-fixing correctness gap in its own right, and one
   * fewer redundant renegotiation trigger in a class of bug where
   * renegotiation-storm/glare was the leading suspect.
   * @param {MediaStream} stream
   */
  addLocalStream (stream) {
    for (const track of stream.getTracks()) {
      if (this.#localTracks.includes(track)) continue
      this.#localTracks.push(track)
      this.#mesh.addTrack(track, stream)
    }
  }

  /** Stop local tracks and detach listeners. Never touches the shared mesh's connection/room. */
  close () {
    if (this.#closed) return
    this.#closed = true
    this.#mesh.removeEventListener('message', this.#handleMessage)
    for (const track of this.#localTracks) track.stop()
    this.#localTracks = []
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
    } else if (signal?.kind === 'video-state') {
      this.dispatchEvent(new CustomEvent('video-state', { detail: { on: !!signal.on } }))
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
