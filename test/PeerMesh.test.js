import { describe, expect, it, beforeEach, afterEach } from 'vitest'
import { PeerMesh } from '../src/mesh/PeerMesh.js'

// Inbound signaling arrives as independent WebSocket frames dispatched by
// XpaceNodePool with a synchronous forEach over handlers that are async, so
// nothing awaits them. These tests drive that exact path: two frames handed
// to the socket's onmessage back to back, with a deliberately slow
// setRemoteDescription so a second application WOULD overlap the first if
// PeerMesh did not serialize per peer. See PeerMesh#withPeerLock for the
// real-engine measurement behind this.
const delay = ms => new Promise(r => setTimeout(r, ms))

/** Order log shared by the fake peer connections of one test. */
let ORDER = []

class FakeDataChannel {
  constructor () { this.readyState = 'connecting' }
  send () {}
  close () {}
}

class FakePC {
  static instances = []
  constructor () {
    FakePC.instances.push(this)
    this.signalingState  = 'stable'
    this.connectionState = 'new'
    this.localDescription = { type: 'offer', sdp: 'local' }
    this.closed  = false
    this.applied = []
  }
  createDataChannel () { return new FakeDataChannel() }
  async setRemoteDescription (desc) {
    ORDER.push(`start:${desc.sdp}`)
    await delay(20)                       // the real await window a second frame slips into
    this.applied.push(desc.sdp)
    this.signalingState = desc.type === 'offer' ? 'have-remote-offer' : 'stable'
    ORDER.push(`end:${desc.sdp}`)
  }
  async setLocalDescription () { this.signalingState = 'stable' }
  async addIceCandidate () {}
  addTrack () {}
  getSenders () { return [] }
  close () { this.closed = true }
}

class FakeWebSocket {
  static last = null
  constructor (url) {
    this.url = url
    this.readyState = 1
    this.sent = []
    FakeWebSocket.last = this
    setTimeout(() => this.onopen?.(), 0)
  }
  send (data) { this.sent.push(JSON.parse(data)) }
  close () { this.readyState = 3 }
  /** Deliver a signaling frame exactly as XpaceNodePool receives it. */
  deliver (msg) { this.onmessage?.({ data: JSON.stringify(msg) }) }
}

beforeEach(() => {
  ORDER = []
  FakePC.instances = []
  globalThis.RTCPeerConnection = FakePC
  globalThis.WebSocket = FakeWebSocket
  globalThis.WebSocket.OPEN = 1
  globalThis.MediaStream = class { constructor (tracks = []) { this._t = tracks } getTracks () { return this._t } }
})
afterEach(() => {
  delete globalThis.RTCPeerConnection
  delete globalThis.WebSocket
  delete globalThis.MediaStream
})

async function joinedMesh () {
  const mesh = new PeerMesh({ selfId: 'alice' })
  await mesh.join('ws://node.test', 'room-1')
  return { mesh, ws: FakeWebSocket.last }
}

describe('PeerMesh inbound signaling', () => {
  // Property guard, not a demonstration of the defect: this already held
  // before #withPeerLock existed, because #createPeer registers its entry
  // synchronously. It is here so that stays true.
  it('builds exactly ONE connection when two signals for an unknown peer arrive back to back', async () => {
    const { mesh, ws } = await joinedMesh()

    // An offer and its first ICE candidate, which is how first contact
    // genuinely arrives (measured 2-3ms apart on real engines).
    ws.deliver({ t: 'signal', from: 'bob', payload: { type: 'offer', sdp: 'A' } })
    ws.deliver({ t: 'signal', from: 'bob', payload: { type: 'ice', candidate: { candidate: 'c1' } } })
    await delay(120)

    expect(FakePC.instances.length).toBe(1)
    // ...and no orphaned connection was left open behind the live one.
    expect(FakePC.instances.filter(pc => !pc.closed).length).toBe(1)
    expect(FakePC.instances[0].applied).toEqual(['A'])
    mesh.leave()
  })

  it('applies two descriptions from one peer strictly one at a time, never overlapping', async () => {
    const { mesh, ws } = await joinedMesh()

    ws.deliver({ t: 'signal', from: 'bob', payload: { type: 'offer', sdp: 'A' } })
    ws.deliver({ t: 'signal', from: 'bob', payload: { type: 'offer', sdp: 'B' } })
    await delay(200)

    // Interleaved application would read start:A, start:B, end:A, end:B --
    // which is how an offer gets discarded as false glare against a state
    // the previous description has not finished moving.
    expect(ORDER).toEqual(['start:A', 'end:A', 'start:B', 'end:B'])
    expect(FakePC.instances.length).toBe(1)
    expect(FakePC.instances[0].applied).toEqual(['A', 'B'])
    mesh.leave()
  })

  it('does not serialize ACROSS peers -- one slow negotiation never blocks another', async () => {
    const { mesh, ws } = await joinedMesh()

    ws.deliver({ t: 'signal', from: 'bob', payload: { type: 'offer', sdp: 'B1' } })
    ws.deliver({ t: 'signal', from: 'carol', payload: { type: 'offer', sdp: 'C1' } })
    await delay(120)

    // Both started before either finished: independent connections.
    expect(ORDER.indexOf('start:C1')).toBeLessThan(ORDER.indexOf('end:B1'))
    expect(FakePC.instances.length).toBe(2)
    mesh.leave()
  })

  it('serializes a peer_join against the same peer\'s first inbound signal', async () => {
    const { mesh, ws } = await joinedMesh()

    ws.deliver({ t: 'peer_join', roomId: 'room-1', peerId: 'bob', name: 'Bob' })
    ws.deliver({ t: 'signal', from: 'bob', payload: { type: 'offer', sdp: 'A' } })
    await delay(120)

    expect(FakePC.instances.length).toBe(1)
    expect(FakePC.instances[0].applied).toEqual(['A'])
    mesh.leave()
  })

  it('drops a torn-down peer\'s lock so a later signal starts clean', async () => {
    const { mesh, ws } = await joinedMesh()
    ws.deliver({ t: 'signal', from: 'bob', payload: { type: 'offer', sdp: 'A' } })
    await delay(120)
    ws.deliver({ t: 'peer_leave', peerId: 'bob' })
    await delay(10)
    ws.deliver({ t: 'signal', from: 'bob', payload: { type: 'offer', sdp: 'A2' } })
    await delay(120)

    expect(FakePC.instances.length).toBe(2)
    expect(FakePC.instances[1].applied).toEqual(['A2'])
    mesh.leave()
  })
})
