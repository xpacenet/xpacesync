import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { PeerLinkRegistry } from '../src/mesh/PeerLinkRegistry.js'
import { LANE_TAG }         from '../src/mesh/PeerLink.js'
import {
  FakePeerConnection, installWebRTC, fakeTrack, fakeStream, flush,
} from './helpers/fakes.js'

/**
 * D.3 at the unit level: one connection, several surfaces.
 *
 * These tests are the contract. Each names the failure mode it forbids,
 * because every one of them was reachable before this layer existed and
 * several of them were observed in production rather than imagined.
 */

let uninstall = null
beforeEach(() => { uninstall = installWebRTC() })
afterEach(() => { uninstall?.(); uninstall = null })

/** A surface: a lane plus a signalling socket that may or may not be up. */
function surface (registry, peerId, { lane = null, up = true } = {}) {
  const sent = []
  const state = { up }
  const lease = registry.acquire(peerId, {
    lane,
    sendSignal: payload => { if (!state.up) return false; sent.push(payload); return true },
  })
  const seen = { open: 0, message: [], track: [], failed: 0, close: 0 }
  lease.addEventListener('open', () => { seen.open += 1 })
  lease.addEventListener('message', ({ detail }) => seen.message.push(detail.payload))
  lease.addEventListener('track', ({ detail }) => seen.track.push(detail.track))
  lease.addEventListener('failed', () => { seen.failed += 1 })
  lease.addEventListener('close', () => { seen.close += 1 })
  return { lease, sent, seen, state }
}

/**
 * Make the transport produce one real outbound signalling frame, through
 * RTCPeer's own onicecandidate path rather than a synthesized internal event —
 * so these tests exercise the wiring that actually carries SDP in production.
 */
function emitIce (pc) {
  pc.onicecandidate({ candidate: { toJSON: () => ({ candidate: 'candidate:1 1 udp' }) } })
}

describe('PeerLinkRegistry: one connection per peer', () => {
  it('a second surface asking for the same person ADOPTS the connection instead of dialling again', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })

    const world = surface(links, 'bob', { lane: 'plot-7' })
    const thread = surface(links, 'bob', { lane: 'thread-42' })
    await flush()

    // The measured defect was two connections to one person. This is the fix,
    // stated as a number.
    expect(FakePeerConnection.instances.length).toBe(1)
    expect(links.size).toBe(1)
    expect(links.get('bob').leaseCount).toBe(2)
    // Neither surface can tell which of them built it.
    expect(world.lease.peerId).toBe('bob')
    expect(thread.lease.peerId).toBe('bob')
    expect(world.lease.pc).toBe(thread.lease.pc)
  })

  it('a surface releasing does NOT disconnect the others — the D.1 bug, one layer down', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()
    const pc = FakePeerConnection.instances[0]

    world.lease.release()

    // The whole point: the lender keeps working. Before this, the one seam
    // that existed for sharing tore the room down on the lender's behalf.
    expect(pc.closed).toBe(false)
    expect(thread.lease.connected).toBe(true)
    expect(thread.seen.failed).toBe(0)
    expect(links.get('bob').leaseCount).toBe(1)
    expect(thread.lease.send({ hello: 'still here' })).toBe(true)
  })

  it('the LAST release closes the connection, in the same turn, and evicts it from the table', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()
    const pc = FakePeerConnection.instances[0]

    world.lease.release()
    expect(pc.closed).toBe(false)
    thread.lease.release()

    expect(pc.closed).toBe(true)
    expect(links.size).toBe(0)
  })

  it('a double release cannot under-count and close a connection another surface is using', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()
    const pc = FakePeerConnection.instances[0]

    world.lease.release()
    world.lease.release()   // a stale handle, released twice
    world.lease.release()

    // The hazard every refcount has. Three releases from one holder must not
    // spend the other holder's claim.
    expect(pc.closed).toBe(false)
    expect(links.get('bob').leaseCount).toBe(1)
    expect(thread.lease.connected).toBe(true)
  })

  it('a surface attaching LATE is replayed the open it missed, and the live remote tracks', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    await flush()
    expect(world.seen.open).toBe(1)

    const pc = FakePeerConnection.instances[0]
    const camera = fakeTrack('video')
    pc.receiveTrack(camera, fakeStream([camera]))
    expect(world.seen.track).toEqual([camera])

    // Now a thread opens, long after the handshake and after media arrived.
    const thread = surface(links, 'bob')
    await flush()

    // No second connection, and the newcomer is not left waiting for an
    // 'open' that already happened — which is what "late must not mean
    // second connection" has to mean in practice.
    expect(FakePeerConnection.instances.length).toBe(1)
    expect(thread.seen.open).toBe(1)
    expect(thread.seen.track).toEqual([camera])
  })

  it('an ENDED remote track is not replayed to a late surface', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    surface(links, 'bob')
    await flush()
    const pc = FakePeerConnection.instances[0]
    const camera = fakeTrack('video')
    pc.receiveTrack(camera, fakeStream([camera]))
    camera.stop()

    const late = surface(links, 'bob')
    await flush()

    // Replaying dead media is its own bug class, pinned below.
    expect(late.seen.track).toEqual([])
  })

  it('refuses a registry held by a different device identity', () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    expect(() => links.assertSelf('mallory')).toThrow(/ONE device identity/)
    expect(() => links.assertSelf('alice')).not.toThrow()
  })
})

describe('PeerLink: death is collective', () => {
  it('a failed connection notifies EVERY surface on it, and is gone from the table', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()

    FakePeerConnection.instances[0].fail()

    // One connection, so one failure is everybody's failure. The alternative
    // — one surface quietly rebuilding while another holds a corpse — is how
    // duplicates get created.
    expect(world.seen.failed).toBe(1)
    expect(thread.seen.failed).toBe(1)
    expect(links.size).toBe(0)
    expect(FakePeerConnection.instances[0].closed).toBe(true)
  })

  it('a STALE connection is rebuilt once, for everyone, not once per surface', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()
    expect(FakePeerConnection.instances.length).toBe(1)

    // The phone-went-to-sleep shape: open once, then quietly not open, with
    // no 'failed' event to announce it.
    const dead = FakePeerConnection.instances[0]
    dead._dc.readyState = 'closed'
    expect(world.lease.stale).toBe(true)

    // Either surface noticing is enough. Both are told, and ONE replacement
    // connection is built rather than one per surface.
    world.lease.release()
    const rebuilt = surface(links, 'bob')
    await flush()

    expect(thread.seen.failed).toBe(1)
    expect(FakePeerConnection.instances.length).toBe(2)
    expect(links.size).toBe(1)
    expect(rebuilt.lease.connected).toBe(true)
  })

  it('a connection that has NEVER opened is not treated as stale', () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    // Still negotiating for the first time. Tearing this down mid-handshake
    // is the bug `hasBeenOpen` exists to prevent.
    expect(world.lease.stale).toBe(false)
    expect(links.get('bob').usable).toBe(true)
  })
})

describe('PeerLink: signalling leaves through exactly one surface', () => {
  it('one offer goes out once, not once per attached surface', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()

    emitIce(FakePeerConnection.instances[0])

    // The same SDP on the wire twice is self-inflicted glare: the peer
    // answers both and the connection is left negotiating against itself.
    const total = world.sent.length + thread.sent.length
    expect(total).toBe(1)
    expect(world.sent.length).toBe(1)
  })

  it('fails over when the surface that opened the connection leaves the room first', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()

    // The world surface's socket is gone (it left the plot) but the thread is
    // still open. Renegotiation — a camera coming on, say — must still work.
    world.state.up = false
    emitIce(FakePeerConnection.instances[0])

    expect(world.sent.length).toBe(0)
    expect(thread.sent.length).toBe(1)
  })

  it('says so when no surface can carry a frame, instead of stalling silently', async () => {
    const warn = vi.fn()
    const links = new PeerLinkRegistry({ selfId: 'alice', onWarn: warn })
    const only = surface(links, 'bob')
    await flush()
    only.state.up = false

    emitIce(FakePeerConnection.instances[0])

    // Silence here was indistinguishable from a connection that never came
    // up, which is the hardest class of report to act on.
    expect(warn).toHaveBeenCalled()
  })
})

describe('PeerLink: lanes', () => {
  it('a laneless surface puts the payload on the wire bare, byte for byte as before', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const only = surface(links, 'bob')
    await flush()

    only.lease.send({ type: 'spaceinbox.envelope.v1', body: 'hi' })

    expect(FakePeerConnection.instances[0].wire).toEqual([
      { type: 'spaceinbox.envelope.v1', body: 'hi' },
    ])
  })

  it('a lane-bearing surface wraps its frames, and the wrapper is the only difference', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob', { lane: 'plot-7' })
    await flush()

    world.lease.send({ type: 'orb.move.v1', x: 1 })

    expect(FakePeerConnection.instances[0].wire).toEqual([
      { [LANE_TAG]: 'plot-7', p: { type: 'orb.move.v1', x: 1 } },
    ])
  })

  it('a tagged frame reaches ONLY the matching lane: two surfaces stop reading each other', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob', { lane: 'plot-7' })
    const thread = surface(links, 'bob', { lane: 'thread-42' })
    await flush()
    const pc = FakePeerConnection.instances[0]

    pc.receive({ [LANE_TAG]: 'plot-7', p: { type: 'orb.move.v1' } })
    pc.receive({ [LANE_TAG]: 'thread-42', p: { type: 'spaceinbox.envelope.v1' } })

    expect(world.seen.message).toEqual([{ type: 'orb.move.v1' }])
    expect(thread.seen.message).toEqual([{ type: 'spaceinbox.envelope.v1' }])
  })

  it('an UNTAGGED frame still reaches every surface — an older peer is not cut off', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob', { lane: 'plot-7' })
    const thread = surface(links, 'bob', { lane: 'thread-42' })
    await flush()

    // What a peer on a build without lanes sends. These apps ship as a PWA
    // and an Android package that do not update together, so this is the
    // normal case during any rollout, not an edge case.
    FakePeerConnection.instances[0].receive({ type: 'spaceinbox.envelope.v1', body: 'from an old build' })

    expect(world.seen.message.length).toBe(1)
    expect(thread.seen.message.length).toBe(1)
  })

  it('a tagged frame matching NO lane is delivered everywhere rather than dropped', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob', { lane: 'plot-7' })
    await flush()

    FakePeerConnection.instances[0].receive({ [LANE_TAG]: 'some-other-room', p: { type: 'x' } })

    // A lane is an isolation hint, never a delivery gate. A silently dropped
    // message leaves no trace; a redundantly delivered one is filtered by the
    // `type` tag every channel above already applies.
    expect(world.seen.message).toEqual([{ type: 'x' }])
  })
})

describe('PeerLink: media on a shared connection', () => {
  it('SWAPPING a track in place holds the sender count constant across toggles', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const call = surface(links, 'bob')
    await flush()
    const pc = FakePeerConnection.instances[0]

    const mic = fakeTrack('audio')
    call.lease.setTrack('mic', mic, fakeStream([mic]))

    const counts = []
    for (let i = 0; i < 3; i += 1) {
      const cam = fakeTrack('video')
      call.lease.setTrack('camera', cam, fakeStream([cam]))
      counts.push(pc.getSenders().length)
      call.lease.setTrack('camera', null)          // camera off, slot kept
      counts.push(pc.getSenders().length)
    }

    // The measurement this design rests on: 2,2,2 in place where
    // remove-and-re-add grew 2 → 3 → 4. A connection shared between surfaces
    // outlives every call on it, so that growth would have no ceiling.
    expect(counts).toEqual([2, 2, 2, 2, 2, 2])
  })

  it('attaching the same track twice, from two surfaces, attaches it once', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()
    const pc = FakePeerConnection.instances[0]

    const mic = fakeTrack('audio')
    const stream = fakeStream([mic])
    expect(world.lease.addTrack(mic, stream)).toBe(true)
    // With several surfaces replaying their own track lists onto one
    // connection, this is the expected case rather than a caller mistake —
    // and RTCPeerConnection#addTrack throws on a genuine double-add.
    expect(thread.lease.addTrack(mic, stream)).toBe(false)
    expect(pc.getSenders().length).toBe(1)
  })

  it('removeTrack frees the sender', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const call = surface(links, 'bob')
    await flush()
    const pc = FakePeerConnection.instances[0]

    const mic = fakeTrack('audio')
    call.lease.addTrack(mic, fakeStream([mic]))
    expect(pc.getSenders().length).toBe(1)

    expect(call.lease.removeTrack(mic)).toBe(true)
    expect(pc.getSenders().length).toBe(0)
  })

  it('a released lease is inert: it cannot send, attach media, or touch the shared connection', async () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = surface(links, 'bob')
    const thread = surface(links, 'bob')
    await flush()
    const pc = FakePeerConnection.instances[0]

    world.lease.release()

    const mic = fakeTrack('audio')
    expect(world.lease.send({ x: 1 })).toBe(false)
    expect(world.lease.addTrack(mic, fakeStream([mic]))).toBe(false)
    expect(world.lease.setTrack('mic', mic)).toBe(false)
    expect(world.lease.connected).toBe(false)
    expect(pc.getSenders().length).toBe(0)
    // And none of it disturbed the surface still using the connection.
    expect(thread.lease.connected).toBe(true)
  })
})
