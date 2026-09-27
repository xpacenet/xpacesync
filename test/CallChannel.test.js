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
