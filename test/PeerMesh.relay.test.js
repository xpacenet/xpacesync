import { describe, it, expect } from 'vitest'
import { PeerMesh }        from '../src/mesh/PeerMesh.js'
import { RealtimeChannel } from '../src/channel/RealtimeChannel.js'
import { MessageRegistry } from '../src/registry/MessageRegistry.js'
import { MessageLog }      from '../src/persistence/MessageLog.js'

/**
 * In-memory stand-ins for the xpacenode bridge + XpaceNodePool, speaking the
 * exact signaling frames the real pair exchange ('hello' → 'peer_join'
 * fanout + roster push, 'relay' → directed forward, 'leave' → 'peer_leave').
 * This lets REAL PeerMesh instances (transportPolicy 'relay-only', so no
 * RTCPeerConnection is ever constructed — which Node doesn't have anyway:
 * any accidental WebRTC dial in these tests would throw, an implicit
 * assertion in itself) exercise the relay floor end to end in-process.
 */
class FakeNodeServer {
  constructor () { this.clients = new Map() } // peerId → FakePool

  handle (pool, msg) {
    if (msg.t === 'hello') {
      pool.peerId = msg.peerId
      pool.roomId = msg.roomId
      for (const other of this.clients.values()) {
        if (other === pool || other.roomId !== msg.roomId) continue
        other.deliver({ t: 'peer_join', roomId: msg.roomId, peerId: msg.peerId })
        pool.deliver({ t: 'peer_join', roomId: msg.roomId, peerId: other.peerId })
      }
      this.clients.set(msg.peerId, pool)
    } else if (msg.t === 'relay') {
      const target = this.clients.get(msg.to)
      if (!target || target.roomId !== msg.roomId) return
      target.deliver({ t: 'relay', roomId: msg.roomId, from: pool.peerId, payload: msg.payload })
    } else if (msg.t === 'leave') {
      this.clients.delete(pool.peerId)
      for (const other of this.clients.values()) {
        if (other.roomId === msg.roomId) other.deliver({ t: 'peer_leave', roomId: msg.roomId, peerId: pool.peerId })
      }
    }
  }
}

class FakePool {
  #handlers = new Map()
  #openCb   = null
  constructor (server) { this.server = server }
  onOpen (cb) { this.#openCb = cb }
  on (type, cb) {
    if (!this.#handlers.has(type)) this.#handlers.set(type, [])
    this.#handlers.get(type).push(cb)
  }
  send (msg) { this.server.handle(this, msg) }
  deliver (msg) { this.#handlers.get(msg.t)?.forEach(cb => cb(msg)) }
  async connect () { this.#openCb?.() }
  close () {}
}

async function joinTwo (server, roomId = 'room-r') {
  const alice = new PeerMesh({ selfId: 'alice', transportPolicy: 'relay-only', poolFactory: () => new FakePool(server) })
  const bob   = new PeerMesh({ selfId: 'bob',   transportPolicy: 'relay-only', poolFactory: () => new FakePool(server) })
  await alice.join('ws://fake', roomId)
  await bob.join('ws://fake', roomId)
  return { alice, bob }
}

describe('PeerMesh relay floor', () => {
  it('builds a signaling-level roster (knownPeerIds) with zero WebRTC connections', async () => {
    const { alice, bob } = await joinTwo(new FakeNodeServer())
    expect(alice.knownPeerIds).toEqual(['bob'])
    expect(bob.knownPeerIds).toEqual(['alice'])
    // No data channel exists or ever will under relay-only:
    expect(alice.peerIds).toEqual([])
    expect(bob.peerIds).toEqual([])
    alice.leave(); bob.leave()
  })

  it('fires peer:join from signaling alone — the event the Orb now spawns remote avatars from', async () => {
    const server = new FakeNodeServer()
    const alice = new PeerMesh({ selfId: 'alice', transportPolicy: 'relay-only', poolFactory: () => new FakePool(server) })
    const joins = []
    alice.addEventListener('peer:join', ({ detail }) => joins.push(detail.peerId))
    await alice.join('ws://fake', 'room-r')
    const bob = new PeerMesh({ selfId: 'bob', transportPolicy: 'relay-only', poolFactory: () => new FakePool(server) })
    await bob.join('ws://fake', 'room-r')
    expect(joins).toEqual(['bob'])
    alice.leave(); bob.leave()
  })

  it('relayToUnreached reaches every rostered peer and the message lands as a normal message event marked via relay', async () => {
    const { alice, bob } = await joinTwo(new FakeNodeServer())
    const received = []
    bob.addEventListener('message', ({ detail }) => received.push(detail))

    const relayed = alice.relayToUnreached({ type: 'x', payload: { n: 1 } })
    expect(relayed).toEqual(['bob'])
    expect(received).toHaveLength(1)
    expect(received[0].from).toBe('alice')
    expect(received[0].via).toBe('relay')
    expect(received[0].payload).toEqual({ type: 'x', payload: { n: 1 } })
    alice.leave(); bob.leave()
  })

  it('a RealtimeChannel message of a relay:true type crosses two relay-only meshes end to end; a non-relay type does not', async () => {
    const { alice, bob } = await joinTwo(new FakeNodeServer())
    const registry = new MessageRegistry()
      .register('presence.tick', { persist: false, relay: true })
      .register('p2p.only',      { persist: false }) // relay defaults false
    const aliceCh = new RealtimeChannel(alice, { registry, log: new MessageLog() })
    const bobCh   = new RealtimeChannel(bob,   { registry, log: new MessageLog() })

    const ticks = []
    const direct = []
    bobCh.on('presence.tick', msg => ticks.push(msg))
    bobCh.on('p2p.only',      msg => direct.push(msg))

    aliceCh.send('presence.tick', { x: 1, z: 2 })
    aliceCh.send('p2p.only',      { secret: true })

    expect(ticks).toHaveLength(1)
    expect(ticks[0].from).toBe('alice')
    expect(ticks[0].payload).toEqual({ x: 1, z: 2 })
    // The floor is opt-in: an unflagged type must NOT leak through the node.
    expect(direct).toHaveLength(0)
    alice.leave(); bob.leave()
  })

  it('peer_leave shrinks the roster, stops the relay floor targeting the departed peer, AND fires peer:leave even with no RTCPeer (the relay-only departure)', async () => {
    const { alice, bob } = await joinTwo(new FakeNodeServer())
    const leaves = []
    alice.addEventListener('peer:leave', ({ detail }) => leaves.push(detail.peerId))
    expect(alice.knownPeerIds).toEqual(['bob'])
    bob.leave()
    expect(alice.knownPeerIds).toEqual([])
    expect(alice.relayToUnreached({ type: 'x' })).toEqual([])
    // No RTCPeer entry ever existed for bob (relay-only) -- the departure
    // must STILL surface as peer:leave, or consumers never despawn him.
    expect(leaves).toEqual(['bob'])
    alice.leave()
  })
})
