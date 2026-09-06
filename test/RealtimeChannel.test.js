import { describe, it, expect, vi } from 'vitest'
import { RealtimeChannel } from '../src/channel/RealtimeChannel.js'
import { MessageRegistry } from '../src/registry/MessageRegistry.js'
import { MessageLog }      from '../src/persistence/MessageLog.js'

/**
 * A minimal stand-in for PeerMesh — just enough surface (EventTarget +
 * selfId/roomId + broadcast()) for RealtimeChannel to be tested without a
 * real WebRTC/xpacenode connection. Two of these, wired to fire each
 * other's 'message' event on broadcast(), simulate a two-peer room.
 */
class FakeMesh extends EventTarget {
  constructor (selfId, roomId) {
    super()
    this.selfId = selfId
    this.roomId = roomId
    this.peer   = null   // the other FakeMesh, wired by the test
  }
  broadcast (payload) {
    this.peer?.dispatchEvent(new CustomEvent('message', { detail: { from: this.selfId, payload } }))
  }
}

function wireTwoPeers (roomId) {
  const alice = new FakeMesh('alice', roomId)
  const bob   = new FakeMesh('bob', roomId)
  alice.peer  = bob
  bob.peer    = alice
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
})
