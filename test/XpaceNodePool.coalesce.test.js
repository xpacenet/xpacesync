import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { XpaceNodePool } from '../src/transport/XpaceNodePool.js'

/**
 * Latest-only coalescing in the pool (see XpaceNodePool.send): frames
 * marked with a coalesceKey must never queue behind each other — a newer
 * one supersedes an unsent older one, in the offline buffer AND while the
 * live socket is backed up (bufferedAmount past the high-water mark).
 * This is what keeps a weak uplink from turning 15Hz presence into an
 * unbounded, seconds-deep backlog of stale ticks.
 */
class FakeWS {
  static OPEN = 1
  static instances = []
  constructor (url) {
    FakeWS.instances.push(this)
    this.url = url
    this.sent = []
    this.readyState = 0
    this.bufferedAmount = 0
  }
  send (s) { this.sent.push(JSON.parse(s)) }
  close () { this.onclose?.() }
  simulateOpen () { this.readyState = FakeWS.OPEN; this.onopen?.() }
  frames (t) { return this.sent.filter(m => m.t === t) }
}

describe('XpaceNodePool coalescing', () => {
  beforeEach(() => { FakeWS.instances = []; vi.stubGlobal('WebSocket', FakeWS) })
  afterEach(() => { vi.unstubAllGlobals() })

  async function openPool () {
    const pool = new XpaceNodePool()
    const connecting = pool.connect('ws://fake')
    const ws = FakeWS.instances.at(-1)
    ws.simulateOpen()
    await connecting
    return { pool, ws }
  }

  it('sends coalesced frames straight through while the socket is healthy', async () => {
    const { pool, ws } = await openPool()
    pool.send({ t: 'relay', n: 1 }, { coalesceKey: 'move@bob' })
    pool.send({ t: 'relay', n: 2 }, { coalesceKey: 'move@bob' })
    expect(ws.frames('relay').map(f => f.n)).toEqual([1, 2])
    pool.close()
  })

  it('parks latest-only while bufferedAmount is past the high-water mark, then drains ONE newest frame per key', async () => {
    const { pool, ws } = await openPool()
    ws.bufferedAmount = 1_000_000   // a stalled uplink
    for (let n = 1; n <= 30; n++) pool.send({ t: 'relay', n }, { coalesceKey: 'move@bob' })
    pool.send({ t: 'relay', n: 99, who: 'carol' }, { coalesceKey: 'move@carol' })
    expect(ws.frames('relay')).toHaveLength(0)   // nothing handed to the stalled socket

    ws.bufferedAmount = 0                        // uplink recovered
    await vi.waitFor(() => expect(ws.frames('relay').length).toBeGreaterThan(0), { timeout: 2000 })
    const delivered = ws.frames('relay')
    expect(delivered).toHaveLength(2)            // one per key — 29 stale ticks dropped
    expect(delivered.find(f => !f.who).n).toBe(30)
    expect(delivered.find(f => f.who === 'carol').n).toBe(99)
    pool.close()
  })

  it('a healthy send supersedes a parked frame for the same key (never deliver old after new)', async () => {
    const { pool, ws } = await openPool()
    ws.bufferedAmount = 1_000_000
    pool.send({ t: 'relay', n: 1 }, { coalesceKey: 'move@bob' })   // parked
    ws.bufferedAmount = 0
    pool.send({ t: 'relay', n: 2 }, { coalesceKey: 'move@bob' })   // sent now, supersedes the parked n=1
    await new Promise(r => setTimeout(r, 400))                      // give the drain timer a chance to misbehave
    expect(ws.frames('relay').map(f => f.n)).toEqual([2])
    pool.close()
  })

  it('the offline buffer keeps one frame per coalesceKey (latest wins) but every un-keyed frame', async () => {
    const pool = new XpaceNodePool()
    // never connected — everything buffers
    for (let n = 1; n <= 20; n++) pool.send({ t: 'relay', n }, { coalesceKey: 'move@bob' })
    pool.send({ t: 'hello', a: 1 })
    pool.send({ t: 'hb' })

    const connecting = pool.connect('ws://fake')
    const ws = FakeWS.instances.at(-1)
    ws.simulateOpen()
    await connecting

    expect(ws.frames('relay').map(f => f.n)).toEqual([20])   // 19 stale ticks never sent
    expect(ws.frames('hello')).toHaveLength(1)
    expect(ws.frames('hb')).toHaveLength(1)
    pool.close()
  })
})
