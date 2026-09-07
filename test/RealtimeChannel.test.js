import { describe, it, expect, vi } from 'vitest'
import { RealtimeChannel } from '../src/channel/RealtimeChannel.js'
import { MessageRegistry } from '../src/registry/MessageRegistry.js'
import { MessageLog }      from '../src/persistence/MessageLog.js'
import { OutboundQueue }   from '../src/queue/OutboundQueue.js'

/**
 * A minimal stand-in for PeerMesh — just enough surface (EventTarget +
 * selfId/roomId/peerIds + broadcast()/send()) for RealtimeChannel to be
 * tested without a real WebRTC/xpacenode connection. Two of these, wired to
 * fire each other's 'message' event on broadcast(), simulate a two-peer
 * room already connected. `simulateJoin()` fires the 'peer:open' event
 * RealtimeChannel listens for to deliver anything queued for the room —
 * the real PeerMesh only fires this once a peer's data channel is actually
 * usable, not merely when signaling learns they exist ('peer:join').
 */
class FakeMesh extends EventTarget {
  constructor (selfId, roomId) {
    super()
    this.selfId  = selfId
    this.roomId  = roomId
    this.peer    = null   // the other FakeMesh, wired by the test
    this.peerIds = []
  }
  broadcast (payload) {
    this.peer?.dispatchEvent(new CustomEvent('message', { detail: { from: this.selfId, payload } }))
  }
  send (peerId, payload) {
    if (peerId === this.peer?.selfId) {
      this.peer.dispatchEvent(new CustomEvent('message', { detail: { from: this.selfId, payload } }))
      return true
    }
    return false
  }
  /** Test helper: simulate a peer actually joining — fires the same event PeerMesh does. */
  simulateJoin (peerId) {
    this.peerIds = [...this.peerIds, peerId]
    this.dispatchEvent(new CustomEvent('peer:open', { detail: { peerId } }))
  }
}

function wireTwoPeers (roomId) {
  const alice = new FakeMesh('alice', roomId)
  const bob   = new FakeMesh('bob', roomId)
  alice.peer  = bob
  bob.peer    = alice
  alice.peerIds = ['bob']
  bob.peerIds   = ['alice']
  return { alice, bob }
}

describe('RealtimeChannel', () => {
  it('delivers a message of a registered type to the other peer\'s handler', () => {
    const { alice, bob } = wireTwoPeers('room-x')
    const registry = new MessageRegistry().register('chat', { persist: false })
    const aliceCh  = new RealtimeChannel(alice, { registry, log: new MessageLog() })
    const bobCh    = new RealtimeChannel(bob,   { registry, log: new MessageLog() })

    const received = []
    bobCh.on('chat', msg => received.push(msg))

    aliceCh.send('chat', { text: 'hello bob' })

    expect(received).toHaveLength(1)
    expect(received[0].from).toBe('alice')
    expect(received[0].payload).toEqual({ text: 'hello bob' })
  })

  it('persists a persist:true type on both send and receive, and history() replays it', async () => {
    const { alice, bob } = wireTwoPeers('room-y')
    const registry = new MessageRegistry().register('chat', { persist: true })
    // Distinct dbName per side — in real life alice and bob are separate
    // devices with separate IndexedDBs; without this they'd share the one
    // fake IndexedDB in this test process and each other's appends would
    // bleed into both histories.
    const aliceLog = new MessageLog({ dbName: 'alice-device' })
    const bobLog   = new MessageLog({ dbName: 'bob-device' })
    const aliceCh  = new RealtimeChannel(alice, { registry, log: aliceLog })
    const bobCh    = new RealtimeChannel(bob,   { registry, log: bobLog })

    aliceCh.send('chat', { text: 'logged message' })

    // Both sides should have it durably logged — sender via send(), receiver via #receive()
    const aliceHistory = await aliceCh.history('chat')
    const bobHistory    = await bobCh.history('chat')
    expect(aliceHistory).toHaveLength(1)
    expect(bobHistory).toHaveLength(1)
    expect(aliceHistory[0].payload).toEqual({ text: 'logged message' })
  })

  it('does not persist a message of an unregistered (default) type', async () => {
    const { alice, bob } = wireTwoPeers('room-z')
    const registry = new MessageRegistry()   // 'move' never registered — defaults apply
    const aliceCh  = new RealtimeChannel(alice, { registry, log: new MessageLog() })
    new RealtimeChannel(bob, { registry, log: new MessageLog() })

    aliceCh.send('move', { x: 1, y: 2 })

    expect(await aliceCh.history('move')).toEqual([])
  })

  it('sendTo() reaches only the addressed peer, for point-to-point protocols like a state handshake', () => {
    const { alice, bob } = wireTwoPeers('room-v')
    const registry = new MessageRegistry()
    const aliceCh  = new RealtimeChannel(alice, { registry, log: new MessageLog() })
    const bobCh    = new RealtimeChannel(bob,   { registry, log: new MessageLog() })

    const bobReceived = []
    bobCh.on('state_req', msg => bobReceived.push(msg))

    aliceCh.sendTo('bob', 'state_req', {})

    expect(bobReceived).toHaveLength(1)
    expect(bobReceived[0].from).toBe('alice')
  })

  it('ignores the mesh-internal intro frame — it never reaches an app-level handler', () => {
    const { alice, bob } = wireTwoPeers('room-w')
    const registry = new MessageRegistry()
    const bobCh    = new RealtimeChannel(bob, { registry, log: new MessageLog() })
    const spy      = vi.fn()
    bobCh.on('chat', spy)

    // Simulate PeerMesh's own intro frame, which carries no `type` field
    // in the shape RealtimeChannel expects from app messages.
    alice.dispatchEvent(new CustomEvent('message', {
      detail: { from: 'alice', payload: { __xpacesync: 'intro', from: 'alice' } },
    }))
    bob.dispatchEvent(new CustomEvent('message', {
      detail: { from: 'alice', payload: { __xpacesync: 'intro', from: 'alice' } },
    }))

    expect(spy).not.toHaveBeenCalled()
  })

  it('a queueOnFail type sent while isolated is queued and delivered once a peer joins', async () => {
    const isolated = new FakeMesh('alice', 'room-queue-1')   // peer stays null — truly isolated
    const registry = new MessageRegistry().register('chat', { queueOnFail: true })
    const queue    = new OutboundQueue({ dbName: 'q-rc-1' })
    const aliceCh  = new RealtimeChannel(isolated, { registry, log: new MessageLog(), queue })

    aliceCh.send('chat', { text: 'anyone out there?' })

    // Nothing to receive it yet — but it should be sitting in the queue.
    expect(await queue.peek('room-queue-1')).toHaveLength(1)

    // Now a peer actually joins. Wire it up as a receiver and simulate.
    const bob = new FakeMesh('bob', 'room-queue-1')
    isolated.peer = bob
    const received = []
    new RealtimeChannel(bob, { registry, log: new MessageLog(), queue: new OutboundQueue({ dbName: 'q-rc-1-bob' }) })
      .on('chat', msg => received.push(msg))
    isolated.simulateJoin('bob')

    await vi.waitFor(() => expect(received).toHaveLength(1))
    expect(received[0].payload).toEqual({ text: 'anyone out there?' })
  })

  it('a queueOnFail message reaches a second peer who joins later too, not just the first', async () => {
    const isolated = new FakeMesh('alice', 'room-queue-2')
    const registry = new MessageRegistry().register('chat', { queueOnFail: true })
    const queue    = new OutboundQueue({ dbName: 'q-rc-2' })
    const aliceCh  = new RealtimeChannel(isolated, { registry, log: new MessageLog(), queue })

    aliceCh.send('chat', { text: 'broadcast to whoever shows up' })

    const bob = new FakeMesh('bob', 'room-queue-2')
    isolated.peer = bob
    const bobReceived = []
    new RealtimeChannel(bob, { registry, log: new MessageLog(), queue: new OutboundQueue({ dbName: 'q-rc-2-bob' }) })
      .on('chat', msg => bobReceived.push(msg))
    isolated.simulateJoin('bob')
    await vi.waitFor(() => expect(bobReceived).toHaveLength(1))

    // A second, different peer joins afterward — the backlog is still there for them too.
    const carol = new FakeMesh('carol', 'room-queue-2')
    isolated.peer = carol
    const carolReceived = []
    new RealtimeChannel(carol, { registry, log: new MessageLog(), queue: new OutboundQueue({ dbName: 'q-rc-2-carol' }) })
      .on('chat', msg => carolReceived.push(msg))
    isolated.simulateJoin('carol')

    await vi.waitFor(() => expect(carolReceived).toHaveLength(1))
    expect(carolReceived[0].payload).toEqual({ text: 'broadcast to whoever shows up' })
  })

  it('a type without queueOnFail is simply dropped when isolated, not queued', async () => {
    const isolated = new FakeMesh('alice', 'room-queue-3')
    const registry = new MessageRegistry().register('move', { queueOnFail: false })
    const queue    = new OutboundQueue({ dbName: 'q-rc-3' })
    const aliceCh  = new RealtimeChannel(isolated, { registry, log: new MessageLog(), queue })

    aliceCh.send('move', { x: 1, y: 2 })

    expect(await queue.peek('room-queue-3')).toEqual([])
  })

  it('does not queue when peers are already connected — only the truly isolated case', async () => {
    const { alice } = wireTwoPeers('room-queue-4')   // alice.peerIds = ['bob'], not isolated
    const registry = new MessageRegistry().register('chat', { queueOnFail: true })
    const queue    = new OutboundQueue({ dbName: 'q-rc-4' })
    const aliceCh  = new RealtimeChannel(alice, { registry, log: new MessageLog(), queue })

    aliceCh.send('chat', { text: 'delivered live, no need to queue' })

    expect(await queue.peek('room-queue-4')).toEqual([])
  })
})
