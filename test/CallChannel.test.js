import { describe, expect, it, vi } from 'vitest'
import { CallChannel } from '../src/channel/CallChannel.js'

class FakeMesh extends EventTarget {
  constructor () {
    super()
    this.selfId = 'alice'
    this.peerIds = []
    this.sent = []
    this.tracksAdded = []
    this._trackCb = null
  }
  send (peerId, payload) {
    this.sent.push({ peerId, payload })
    return this.peerIds.includes(peerId)
  }
  addTrack (track, stream) { this.tracksAdded.push({ track, stream }) }
  onTrack (cb) { this._trackCb = cb }
  emit (type, detail) { this.dispatchEvent(new CustomEvent(type, { detail })) }
  emitTrack (track, stream, fromPeerId) { this._trackCb?.(track, stream, fromPeerId) }
}

function fakeTrack () { return { stop: vi.fn(), kind: 'video' } }
function fakeStream (tracks) { return { getTracks: () => tracks } }

describe('CallChannel', () => {
  it('sends a typed ring signal only reaching the expected peer, and ignores other peers/types', () => {
    const mesh = new FakeMesh()
    mesh.peerIds = ['bob']
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    const rings = []
    channel.addEventListener('ring', ({ detail }) => rings.push(detail))

    mesh.emit('message', { from: 'mallory', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'ring', video: true } } })
    mesh.emit('message', { from: 'bob', payload: { type: 'different.protocol', payload: { kind: 'ring' } } })
    mesh.emit('message', { from: 'bob', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'ring', video: true } } })

    expect(rings).toEqual([{ video: true }])
    expect(channel.state).toBe('ringing-inbound')

    expect(channel.ring({ video: false })).toBe(true)
    expect(mesh.sent.at(-1)).toMatchObject({ peerId: 'bob', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'ring', video: false } } })
  })

  it('walks the accept/active and reject/ended lifecycle from inbound signals', () => {
    const mesh = new FakeMesh()
    mesh.peerIds = ['bob']
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    const states = []
    channel.addEventListener('state', ({ detail }) => states.push(detail.state))

    mesh.emit('message', { from: 'bob', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'accept' } } })
    expect(channel.state).toBe('active')

    mesh.emit('message', { from: 'bob', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'hangup' } } })
    expect(channel.state).toBe('ended')

    expect(states).toEqual(['active', 'ended'])
  })

  it('accept()/reject()/hangup() send the expected signal and update local state', () => {
    const mesh = new FakeMesh()
    mesh.peerIds = ['bob']
    const channel = new CallChannel(mesh, { peerId: 'bob' })

    expect(channel.accept()).toBe(true)
    expect(channel.state).toBe('active')
    expect(mesh.sent.at(-1).payload.payload).toEqual({ kind: 'accept' })

    const channel2 = new CallChannel(mesh, { peerId: 'bob' })
    expect(channel2.reject()).toBe(true)
    expect(channel2.state).toBe('ended')
    expect(mesh.sent.at(-1).payload.payload).toEqual({ kind: 'reject' })
  })

  it('addLocalStream attaches every track to the shared mesh connection', () => {
    const mesh = new FakeMesh()
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    const t1 = fakeTrack(), t2 = fakeTrack()
    const stream = fakeStream([t1, t2])

    channel.addLocalStream(stream)

    expect(mesh.tracksAdded).toEqual([{ track: t1, stream }, { track: t2, stream }])
  })

  it('surfaces only the expected peer\'s inbound remote tracks', () => {
    const mesh = new FakeMesh()
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    const received = []
    channel.addEventListener('remote-track', ({ detail }) => received.push(detail))

    const track = fakeTrack(), stream = fakeStream([track])
    mesh.emitTrack(track, stream, 'mallory')
    mesh.emitTrack(track, stream, 'bob')

    expect(received).toEqual([{ track, stream }])
  })

  it('retarget() re-points the channel at a different peer without touching localStream/state', () => {
    const mesh = new FakeMesh()
    mesh.peerIds = ['bob-laptop', 'bob-phone']
    const channel = new CallChannel(mesh, { peerId: 'bob-laptop' })
    const track = fakeTrack()
    channel.addLocalStream(fakeStream([track]))
    channel.ring({ video: false })
    expect(channel.state).toBe('ringing-outbound')

    // The guessed device never actually answers -- a DIFFERENT device of
    // the same account does (the real founder scenario: fan-out rang both,
    // the phone answered, the cached relationship pointed at the laptop).
    const accepted = []
    channel.addEventListener('accepted', () => accepted.push(true))
    mesh.emit('message', { from: 'bob-phone', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'accept' } } })
    // Before retargeting, the real answering peer's message is invisible.
    expect(channel.state).toBe('ringing-outbound')
    expect(accepted).toEqual([])

    channel.retarget('bob-phone')
    expect(channel.peerId).toBe('bob-phone')
    expect(track.stop).not.toHaveBeenCalled() // localStream untouched

    mesh.emit('message', { from: 'bob-phone', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'accept' } } })
    expect(channel.state).toBe('active')
    expect(accepted).toEqual([true])

    // Inbound media from the now-correct peer is surfaced; the stale
    // guessed peer's is not.
    const received = []
    channel.addEventListener('remote-track', ({ detail }) => received.push(detail))
    const remoteTrack = fakeTrack(), remoteStream = fakeStream([remoteTrack])
    mesh.emitTrack(remoteTrack, remoteStream, 'bob-laptop')
    mesh.emitTrack(remoteTrack, remoteStream, 'bob-phone')
    expect(received).toEqual([{ track: remoteTrack, stream: remoteStream }])

    // Further sends (e.g. hangup) now correctly address the real peer.
    channel.hangup()
    expect(mesh.sent.at(-1)).toMatchObject({ peerId: 'bob-phone', payload: { payload: { kind: 'hangup' } } })
  })

  it('retarget() is a no-op once closed, and a no-op for the same/empty peerId', () => {
    const mesh = new FakeMesh()
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    channel.retarget('bob') // same id -- no-op, no error
    expect(channel.peerId).toBe('bob')
    channel.close()
    channel.retarget('mallory')
    expect(channel.peerId).toBe('bob') // unchanged -- closed channels never re-point
  })

  it('addLocalStream accumulates tracks across multiple calls (audio at accept, video added mid-call) so close() stops ALL of them', () => {
    const mesh = new FakeMesh()
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    const audioTrack = fakeTrack()
    channel.addLocalStream(fakeStream([audioTrack])) // accept-time audio

    // v189: turning video on mid-call used to call addLocalStream() again
    // with a stream containing ONLY the new video track, which used to
    // silently replace the internal reference to the accept-time audio
    // track -- close() would then never stop it (a real leaked-mic bug).
    const videoTrack = fakeTrack()
    channel.addLocalStream(fakeStream([videoTrack]))

    expect(mesh.tracksAdded).toEqual([
      { track: audioTrack, stream: expect.anything() },
      { track: videoTrack, stream: expect.anything() },
    ])

    channel.close()
    expect(audioTrack.stop).toHaveBeenCalledTimes(1)
    expect(videoTrack.stop).toHaveBeenCalledTimes(1)
  })

  it('addLocalStream is idempotent per track: calling it again with an ALREADY-attached track never re-calls PeerMesh#addTrack', () => {
    // Found while tracing the founder's real-device report of repeated
    // video on/off/on toggling becoming unreliable: PeerMesh#addTrack
    // reaches RTCPeerConnection#addTrack, which throws if called twice for
    // the identical track (RTCPeer#addTrack swallows that in a try/catch,
    // so this was a silent no-op rather than a visible error) -- but it
    // still re-entered the mesh's addTrack/onnegotiationneeded-triggering
    // path for a track that was already live. A real, independently
    // worth-fixing correctness gap even though it wasn't found to be
    // SpaceHub's own toggle path's proximate cause (setCallVideo's "on"
    // path never re-calls addLocalStream for a track it already has).
    const mesh = new FakeMesh()
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    const track = fakeTrack()
    const stream = fakeStream([track])
    channel.addLocalStream(stream)
    channel.addLocalStream(stream) // same track, called again
    expect(mesh.tracksAdded).toEqual([{ track, stream }]) // only once
  })

  it('setVideoState() sends a typed video-state signal, and the peer\'s video-state signal dispatches an event with the real on/off value', () => {
    // Added for the "both sides turn video off mid-call goes blank instead
    // of falling back to an audio UI" bug: setCallVideo's own off path only
    // ever disables the local track (`enabled = false`), which produces no
    // renegotiation and no ontrack event on the peer's side at all -- before
    // this signal existed there was no way for the receiving side to learn
    // "the peer's video just went off" versus "on," only "a video track
    // arrived at some point in the past" (sticky-forever). This is an
    // explicit, symmetric on/off signal independent of ring/accept/hangup.
    const mesh = new FakeMesh()
    mesh.peerIds = ['bob']
    const channel = new CallChannel(mesh, { peerId: 'bob' })

    expect(channel.setVideoState(true)).toBe(true)
    expect(mesh.sent.at(-1)).toMatchObject({ peerId: 'bob', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'video-state', on: true } } })

    const states = []
    channel.addEventListener('video-state', ({ detail }) => states.push(detail))
    mesh.emit('message', { from: 'mallory', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'video-state', on: true } } })
    mesh.emit('message', { from: 'bob', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'video-state', on: true } } })
    mesh.emit('message', { from: 'bob', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'video-state', on: false } } })

    // Only bob's (the real peer's) messages are surfaced, and the boolean
    // round-trips exactly, in order -- both "on" and "off" are real, distinct
    // transitions, not a one-shot/sticky flag.
    expect(states).toEqual([{ on: true }, { on: false }])
    // This signal is purely informational -- it must never touch call state.
    expect(channel.state).toBe('idle')
  })

  it('setVideoState() is a no-op once closed', () => {
    const mesh = new FakeMesh()
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    channel.close()
    expect(channel.setVideoState(true)).toBe(false)
    expect(mesh.sent).toEqual([])
  })

  it('close() stops local tracks, detaches listeners, and never touches the shared mesh connection', () => {
    const mesh = new FakeMesh()
    const channel = new CallChannel(mesh, { peerId: 'bob' })
    const track = fakeTrack()
    channel.addLocalStream(fakeStream([track]))

    const message = vi.fn()
    channel.addEventListener('ring', message)
    channel.close()
    channel.close() // idempotent

    expect(track.stop).toHaveBeenCalledTimes(1)
    expect(mesh.sent).toEqual([]) // close() itself never sends a signal
    mesh.emit('message', { from: 'bob', payload: { type: 'spaceinbox.call.v1', payload: { kind: 'ring' } } })
    expect(message).not.toHaveBeenCalled()
    expect(channel.state).toBe('ended')
  })
})
