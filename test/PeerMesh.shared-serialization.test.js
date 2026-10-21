import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { PeerMesh }         from '../src/mesh/PeerMesh.js'
import { PeerLinkRegistry } from '../src/mesh/PeerLinkRegistry.js'
import {
  FakeNodeServer, FakePool, FakePeerConnection, installWebRTC, flush,
} from './helpers/fakes.js'

/**
 * THE COMPOSITION. Two improvements landed on divergent branches and each is
 * provable on its own:
 *
 *   - signals for one peer are applied one at a time  (PeerMesh#withPeerLock)
 *   - two surfaces reach one person over ONE connection (PeerLinkRegistry)
 *
 * Put together they interact, and the interaction is not benign: sharing
 * REMOVES the thing the per-mesh lock relied on. A mesh's lock table is per
 * surface, and two surfaces have two meshes, two XpaceNodePools and two
 * sockets — so once they share a connection, a frame arriving for the world
 * and a frame arriving for the thread are ordered against their own siblings
 * and against nothing else, while both land on the ONE RTCPeer the link
 * owns. That is the original glare defect back again, reachable only in the
 * shared configuration that the sharing branch introduced and the
 * serialization branch never saw.
 *
 * These tests are the proof that both properties hold AT THE SAME TIME, and
 * they fail on either branch taken alone: on the sharing branch because
 * nothing serializes, and on the serialization branch because
 * PeerLinkRegistry does not exist there.
 */

let uninstall = null
beforeEach(() => { uninstall = installWebRTC() })
afterEach(() => { uninstall?.(); uninstall = null })

const pool = server => () => new FakePool(server)

/** Alice's two surfaces, sharing one registry: a 3D plot and a thread. */
async function sharedSurfaces () {
  const server = new FakeNodeServer()
  const links  = new PeerLinkRegistry({ selfId: 'alice' })
  const world  = new PeerMesh({ selfId: 'alice', links, lane: 'plot-7',   poolFactory: pool(server) })
  const thread = new PeerMesh({ selfId: 'alice', links, lane: 'thread-42', poolFactory: pool(server) })
  await world.join('ws://node', 'plot-7')
  await thread.join('ws://node', 'thread-42')
  // Bob is in both: standing in the plot AND with the thread open.
  const bobInPlot   = server.joinPeer('bob', 'plot-7')
  const bobInThread = server.joinPeer('bob', 'thread-42')
  await flush()
  return { server, links, world, thread, bobInPlot, bobInThread }
}

/**
 * Make the one shared connection's setRemoteDescription slow, and record the
 * window each application occupies. A second application that starts before
 * the first has ended is the defect, and shows up as interleaved entries.
 */
function instrument (pc) {
  const order = []
  pc.setRemoteDescription = async desc => {
    order.push(`start:${desc.sdp}`)
    await new Promise(r => setTimeout(r, 20))
    pc.signalingState = desc.type === 'offer' ? 'have-remote-offer' : 'stable'
    order.push(`end:${desc.sdp}`)
  }
  return order
}

describe('serialized signals AND one shared connection, together', () => {
  it('holds both properties at once: one connection, and its signals applied one at a time', async () => {
    const { links, world, thread, bobInPlot, bobInThread } = await sharedSurfaces()

    // PROPERTY TWO, restated here because the first property is only
    // interesting on a connection that is genuinely shared.
    expect(links.size).toBe(1)
    expect(FakePeerConnection.live().length).toBe(1)
    expect(world.peerIds).toEqual(['bob'])
    expect(thread.peerIds).toEqual(['bob'])

    const pc    = FakePeerConnection.live()[0]
    const order = instrument(pc)

    // PROPERTY ONE, in the configuration that actually threatens it: the two
    // frames arrive on DIFFERENT sockets, so no per-mesh lock can order them
    // against each other. Sent back to back with nothing awaited in between,
    // which is the harshest spacing the transport can produce.
    bobInPlot.send({   t: 'signal', roomId: 'plot-7',    to: 'alice', payload: { type: 'offer', sdp: 'A' } })
    bobInThread.send({ t: 'signal', roomId: 'thread-42', to: 'alice', payload: { type: 'offer', sdp: 'B' } })

    await new Promise(r => setTimeout(r, 120))

    // Both were applied, and neither overlapped the other. The failing shape
    // this excludes is start:A, start:B, end:A, end:B — where B's glare
    // check reads a signalingState that A has not finished moving.
    expect(order).toEqual(['start:A', 'end:A', 'start:B', 'end:B'])
  })

  it('serializes two frames arriving on ONE surface as well, which is the single-surface case', async () => {
    const { bobInPlot } = await sharedSurfaces()
    const pc    = FakePeerConnection.live()[0]
    const order = instrument(pc)

    bobInPlot.send({ t: 'signal', roomId: 'plot-7', to: 'alice', payload: { type: 'offer', sdp: 'A' } })
    bobInPlot.send({ t: 'signal', roomId: 'plot-7', to: 'alice', payload: { type: 'offer', sdp: 'B' } })

    await new Promise(r => setTimeout(r, 120))
    expect(order).toEqual(['start:A', 'end:A', 'start:B', 'end:B'])
  })

  it('a frame that arrives for a killed connection is dropped, not applied to a dead pc', async () => {
    const { links, bobInPlot } = await sharedSurfaces()
    const pc    = FakePeerConnection.live()[0]
    const order = instrument(pc)

    links.get('bob').kill()
    bobInPlot.send({ t: 'signal', roomId: 'plot-7', to: 'alice', payload: { type: 'offer', sdp: 'late' } })
    await new Promise(r => setTimeout(r, 60))

    // The surfaces rebuilt or released as they saw fit; what must NOT happen
    // is the old pc being driven after close.
    expect(order).toEqual([])
    expect(pc.closed).toBe(true)
  })

  it('one peer\'s slow negotiation does not hold up another peer\'s', async () => {
    const { server, world } = await sharedSurfaces()
    const carolInPlot = server.joinPeer('carol', 'plot-7')
    await flush()
    expect(world.peerIds.sort()).toEqual(['bob', 'carol'])

    // Two connections now. Chains are per connection, so carol's frame must
    // not queue behind bob's.
    const pcs = FakePeerConnection.live()
    expect(pcs.length).toBe(2)
    const order = []
    for (const pc of pcs) {
      pc.setRemoteDescription = async desc => {
        order.push(`start:${desc.sdp}`)
        await new Promise(r => setTimeout(r, 20))
        order.push(`end:${desc.sdp}`)
      }
    }

    carolInPlot.send({ t: 'signal', roomId: 'plot-7', to: 'alice', payload: { type: 'offer', sdp: 'carol' } })
    await new Promise(r => setTimeout(r, 120))
    expect(order).toEqual(['start:carol', 'end:carol'])
  })
})
