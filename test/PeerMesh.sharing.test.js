import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { PeerMesh }         from '../src/mesh/PeerMesh.js'
import { PeerLinkRegistry } from '../src/mesh/PeerLinkRegistry.js'
import {
  FakeNodeServer, FakePool, FakePeerConnection, installWebRTC,
  intervalLedger, fakeTrack, fakeStream, flush,
} from './helpers/fakes.js'

/**
 * D.3 end to end: two real meshes, in two different rooms, reaching one
 * person over ONE connection.
 *
 * This is the shape the defect was measured in. A 3D plot and a conversation
 * thread are different rooms, so they are different meshes — and a mesh used
 * to BE a connection. Standing next to someone whose thread was open
 * therefore opened a second WebRTC connection to that one person, identical
 * to the first in everything but which room introduced them.
 */

let uninstall = null
let ledger = null
beforeEach(() => { uninstall = installWebRTC() })
afterEach(() => {
  ledger?.restore(); ledger = null
  uninstall?.(); uninstall = null
})

const pool = server => () => new FakePool(server)

/** Alice's two surfaces: a 3D plot and a conversation thread. */
async function twoSurfaces ({ shared }) {
  const server = new FakeNodeServer()
  const links = shared ? new PeerLinkRegistry({ selfId: 'alice' }) : undefined
  const world = new PeerMesh({ selfId: 'alice', links, lane: shared ? 'plot-7' : undefined, poolFactory: pool(server) })
  const thread = new PeerMesh({ selfId: 'alice', links, lane: shared ? 'thread-42' : undefined, poolFactory: pool(server) })
  await world.join('ws://node', 'plot-7')
  await thread.join('ws://node', 'thread-42')
  // Bob is in both: he is standing in the plot AND has the thread open.
  const bobInPlot = server.joinPeer('bob', 'plot-7')
  const bobInThread = server.joinPeer('bob', 'thread-42')
  await flush()
  return { server, links, world, thread, bobInPlot, bobInThread }
}

describe('two surfaces, one person', () => {
  it('WITHOUT a shared registry this opens two connections to one person — the measured defect', async () => {
    const { world, thread } = await twoSurfaces({ shared: false })

    // Pinned deliberately rather than deleted: it is the before-number the
    // fix below is measured against, and it is also the guarantee that an
    // existing caller's behaviour did not change. A mesh with no registry
    // handed to it still owns its connections outright.
    expect(FakePeerConnection.live().length).toBe(2)
    expect(world.peerIds).toEqual(['bob'])
    expect(thread.peerIds).toEqual(['bob'])
  })

  it('WITH a shared registry there is exactly ONE connection, and both surfaces work over it', async () => {
    const { links, world, thread } = await twoSurfaces({ shared: true })

    expect(FakePeerConnection.live().length).toBe(1)
    expect(links.size).toBe(1)
    expect(links.get('bob').leaseCount).toBe(2)

    // Both surfaces consider Bob reachable, and both actually reach him.
    expect(world.peerIds).toEqual(['bob'])
    expect(thread.peerIds).toEqual(['bob'])
    expect(world.send('bob', { type: 'orb.move.v1', x: 3 })).toBe(true)
    expect(thread.send('bob', { type: 'spaceinbox.envelope.v1', body: 'hi' })).toBe(true)

    // And the two surfaces' traffic is separated on the wire by lane, not by
    // hoping the other side ignores it.
    const wire = FakePeerConnection.live()[0].wire
    const lanes = wire.map(frame => frame.__xpacesync_lane)
    expect(lanes).toContain('plot-7')
    expect(lanes).toContain('thread-42')
  })

  it('walking out of the plot does NOT drop the conversation', async () => {
    const { links, world, thread } = await twoSurfaces({ shared: true })
    const pc = FakePeerConnection.live()[0]
    const threadLost = []
    thread.addEventListener('peer:leave', ({ detail }) => threadLost.push(detail.peerId))

    world.leave()

    // THE HEADLINE. One surface leaving its room must be invisible to the
    // others on that connection. Before this, the only way a mesh could let
    // go was mesh.leave(), which closed every connection it held — so this
    // was not expressible at all, and the one seam that promised reuse
    // destroyed the lender's connections when used for it.
    expect(pc.closed).toBe(false)
    expect(links.size).toBe(1)
    expect(links.get('bob').leaseCount).toBe(1)
    expect(threadLost).toEqual([])
    expect(thread.peerIds).toEqual(['bob'])
    expect(thread.send('bob', { type: 'spaceinbox.envelope.v1', body: 'still here' })).toBe(true)

    // And the plot genuinely did leave: its own view is empty.
    expect(world.peerIds).toEqual([])
  })

  it('the LAST surface to leave closes the connection, and leaves no heartbeat behind', async () => {
    ledger = intervalLedger()
    const { links, world, thread } = await twoSurfaces({ shared: true })
    const pc = FakePeerConnection.live()[0]

    // One heartbeat per ROOM is correct: two rooms is two sockets. What must
    // never happen is a heartbeat outliving the socket it pings through —
    // the D.2 orphan, which fired every 30s and threw once the pool was null.
    expect(ledger.created.length).toBe(2)

    world.leave()
    expect(pc.closed).toBe(false)
    expect(ledger.orphans().length).toBe(1)   // the thread's, still live

    thread.leave()

    expect(pc.closed).toBe(true)
    expect(links.size).toBe(0)
    // THE ORPHAN CHECK. Not one interval survives either surface leaving, in
    // any order — which is what makes "a heartbeat firing into a null pool"
    // unreachable rather than merely unobserved.
    expect(ledger.orphans().length).toBe(0)
    expect(world.peerIds).toEqual([])
    expect(thread.peerIds).toEqual([])
  })

  it('a surface joining LATE adopts the live connection instead of dialling a second one', async () => {
    const server = new FakeNodeServer()
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const world = new PeerMesh({ selfId: 'alice', links, lane: 'plot-7', poolFactory: pool(server) })
    await world.join('ws://node', 'plot-7')
    server.joinPeer('bob', 'plot-7')
    await flush()
    expect(FakePeerConnection.live().length).toBe(1)

    // Now, long after the handshake, Alice opens Bob's thread.
    const thread = new PeerMesh({ selfId: 'alice', links, lane: 'thread-42', poolFactory: pool(server) })
    const opened = []
    thread.addEventListener('peer:open', ({ detail }) => opened.push(detail.peerId))
    await thread.join('ws://node', 'thread-42')
    server.joinPeer('bob', 'thread-42')
    await flush()

    // No second connection, and the late surface still got its peer:open —
    // so its wiring is identical to one that was there from the start.
    expect(FakePeerConnection.live().length).toBe(1)
    expect(opened).toEqual(['bob'])
    expect(thread.peerIds).toEqual(['bob'])
    expect(links.get('bob').leaseCount).toBe(2)
  })

  it('refuses two different identities on one registry', () => {
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    new PeerMesh({ selfId: 'alice', links })
    expect(() => new PeerMesh({ selfId: 'mallory', links })).toThrow(/ONE device identity/)
  })
})

describe('media on a shared, long-lived connection', () => {
  async function oneSurface () {
    const server = new FakeNodeServer()
    const links = new PeerLinkRegistry({ selfId: 'alice' })
    const mesh = new PeerMesh({ selfId: 'alice', links, poolFactory: pool(server) })
    await mesh.join('ws://node', 'thread-42')
    const bob = server.joinPeer('bob', 'thread-42')
    await flush()
    return { server, links, mesh, bob }
  }

  /** The phone-woke-up path: the connection dies, the peer re-announces. */
  async function forceRebuild (server, bob) {
    FakePeerConnection.live()[0]._dc.readyState = 'closed'
    bob.send({ t: 'hello', roomId: 'thread-42', peerId: 'bob' })
    await flush()
  }

  it('THE §2.5 GAP, CLOSED: removeTrack stops dead tracks replaying onto a rebuilt connection', async () => {
    const { server, mesh, bob } = await oneSurface()

    // Three calls placed and ended, the way a thread accumulates them.
    for (let i = 0; i < 3; i += 1) {
      const mic = fakeTrack('audio')
      mesh.addTrack(mic, fakeStream([mic]))
      mic.stop()
      // The verb the app could not reach. spacehub's own test recorded this
      // as unfixable from that repo: "The real fix is a removeTrack() on
      // PeerMesh, in @xpacenet/xpacesync. It cannot land from this repo."
      expect(mesh.removeTrack(mic)).toBe(1)
    }

    expect(mesh.localTrackCount).toBe(0)
    expect(FakePeerConnection.live()[0].getSenders().length).toBe(0)

    await forceRebuild(server, bob)

    // Before this, the replacement connection was born carrying the ended
    // tracks of every call the surface had ever placed — three dead senders
    // on a connection built seconds ago. On a connection now SHARED between
    // surfaces, which outlives every call on it, that accumulation has no
    // ceiling at all.
    const rebuilt = FakePeerConnection.live().at(-1)
    expect(FakePeerConnection.instances.length).toBe(2)
    expect(rebuilt.getSenders().length).toBe(0)
  })

  it('without the prune, replay is still faithful — addTrack keeps its accumulate-and-replay contract', async () => {
    const { server, mesh, bob } = await oneSurface()

    const mic = fakeTrack('audio')
    mesh.addTrack(mic, fakeStream([mic]))
    await forceRebuild(server, bob)

    // Existing callers depend on a live track being replayed onto a rebuilt
    // connection: that is what keeps a call alive across a reconnect. The fix
    // is a pruning verb, NOT a changed addTrack.
    expect(FakePeerConnection.live().at(-1).getSenders()[0].track).toBe(mic)
  })

  it('setTrack replays the CURRENT track in a slot, not every track that was ever in it', async () => {
    const { server, mesh, bob } = await oneSurface()

    const first = fakeTrack('video')
    const second = fakeTrack('video')
    const third = fakeTrack('video')
    mesh.setTrack('camera', first, fakeStream([first]))
    mesh.setTrack('camera', second, fakeStream([second]))
    mesh.setTrack('camera', third, fakeStream([third]))

    // One slot, one sender, whatever the toggle count.
    expect(FakePeerConnection.live()[0].getSenders().length).toBe(1)
    expect(mesh.localTrackCount).toBe(1)

    await forceRebuild(server, bob)

    const rebuilt = FakePeerConnection.live().at(-1)
    expect(rebuilt.getSenders().length).toBe(1)
    expect(rebuilt.getSenders()[0].track).toBe(third)
  })

  it('an emptied slot replays nothing: a camera turned off does not come back on a rebuild', async () => {
    const { server, mesh, bob } = await oneSurface()

    const cam = fakeTrack('video')
    mesh.setTrack('camera', cam, fakeStream([cam]))
    mesh.setTrack('camera', null)
    expect(mesh.localTrackCount).toBe(0)

    await forceRebuild(server, bob)

    expect(FakePeerConnection.live().at(-1).getSenders().length).toBe(0)
  })

  it('inbound media reaches BOTH surfaces on a shared connection', async () => {
    const { world, thread } = await twoSurfaces({ shared: true })
    const worldTracks = []
    const threadTracks = []
    world.onTrack((track, _stream, from) => worldTracks.push([track.kind, from]))
    thread.onTrack((track, _stream, from) => threadTracks.push([track.kind, from]))

    const camera = fakeTrack('video')
    FakePeerConnection.live()[0].receiveTrack(camera, fakeStream([camera]))

    // RTP carries no lane, so the link cannot attribute a track to one
    // surface and must not guess. A 3D sink and a thread tile legitimately
    // want the same inbound track at once; each filters for itself.
    expect(worldTracks).toEqual([['video', 'bob']])
    expect(threadTracks).toEqual([['video', 'bob']])
  })
})
