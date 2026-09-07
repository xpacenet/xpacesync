import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { OutboundQueue } from '../src/queue/OutboundQueue.js'

// Mock only Date.now (used for the ts stamp on enqueue and for expiry
// comparisons) — NOT vi.useFakeTimers(), which also pauses the real
// setTimeout/microtask scheduling fake-indexeddb's internals depend on to
// ever resolve, hanging every awaited call indefinitely.
let now = 0
describe('OutboundQueue', () => {
  beforeEach(() => {
    now = 1_000_000
    vi.spyOn(Date, 'now').mockImplementation(() => now)
  })
  afterEach(() => { vi.restoreAllMocks() })

  it('peek() returns nothing for a room that never queued anything', async () => {
    const queue = new OutboundQueue({ dbName: 'q-empty' })
    expect(await queue.peek('room-1')).toEqual([])
  })

  it('a queued message is returned by peek()', async () => {
    const queue = new OutboundQueue({ dbName: 'q-basic' })
    await queue.enqueue('room-1', { type: 'chat', payload: { text: 'anyone there?' } })

    const queued = await queue.peek('room-1')
    expect(queued).toEqual([{ type: 'chat', payload: { text: 'anyone there?' } }])
  })

  it('peek() is non-destructive — a second peer joining later sees the same backlog', async () => {
    const queue = new OutboundQueue({ dbName: 'q-nondestructive' })
    await queue.enqueue('room-1', { type: 'chat', payload: { text: 'msg' } })

    const firstJoiner  = await queue.peek('room-1')
    const secondJoiner = await queue.peek('room-1')
    expect(firstJoiner).toEqual(secondJoiner)
    expect(secondJoiner).toHaveLength(1)
  })

  it('keeps rooms independent', async () => {
    const queue = new OutboundQueue({ dbName: 'q-rooms' })
    await queue.enqueue('room-a', { text: 'for a' })
    await queue.enqueue('room-b', { text: 'for b' })

    expect(await queue.peek('room-a')).toEqual([{ text: 'for a' }])
    expect(await queue.peek('room-b')).toEqual([{ text: 'for b' }])
  })

  it('preserves insertion order', async () => {
    const queue = new OutboundQueue({ dbName: 'q-order' })
    await queue.enqueue('room-1', { n: 1 })
    await queue.enqueue('room-1', { n: 2 })
    await queue.enqueue('room-1', { n: 3 })

    expect(await queue.peek('room-1')).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }])
  })

  it('a message older than maxAgeMs is pruned and no longer delivered', async () => {
    const queue = new OutboundQueue({ dbName: 'q-expiry', maxAgeMs: 1000 })
    await queue.enqueue('room-1', { text: 'stale eventually' })

    expect(await queue.peek('room-1')).toHaveLength(1)

    now += 1500
    expect(await queue.peek('room-1')).toEqual([])
  })

  it('expiry is per-message, not all-or-nothing for the room', async () => {
    const queue = new OutboundQueue({ dbName: 'q-partial-expiry', maxAgeMs: 1000 })
    await queue.enqueue('room-1', { text: 'old' })

    now += 800
    await queue.enqueue('room-1', { text: 'newer' })

    now += 400   // old is now 1200ms (expired), newer is 400ms (fresh)
    expect(await queue.peek('room-1')).toEqual([{ text: 'newer' }])
  })
})
