import { describe, it, expect, afterEach } from 'vitest'
import { PeerMesh } from '../src/mesh/PeerMesh.js'

/**
 * join() on an ALREADY-JOINED mesh.
 *
 * Before the guard, join() overwrote #pool and #heartbeatTimer
 * unconditionally. The previous interval became unreachable, kept firing
 * every 30 seconds, and threw `Cannot read properties of null (reading
 * 'send')` from inside the timer once leave() had nulled the pool — while
 * the old socket was dropped silently, leaving #peers full of connections
 * whose signalling path was gone.
 *
 * It matters because sharing one connection between surfaces is the point:
 * hand a live mesh to a second surface and the natural thing for it to do
 * is call join(). That must not destroy the mesh it was handed.
 */
class FakeNodeServer {
  constructor () { this.clients = new Map() }
  handle (pool, msg) {
    if (msg.t === 'hello') { pool.peerId = msg.peerId; pool.roomId = msg.roomId; this.clients.set(msg.peerId, pool) }
    else if (msg.t === 'leave') this.clients.delete(pool.peerId)
  }
}

class FakePool {
  #handlers = new Map()
  #openCb = null
  static live = 0
  constructor (server) { this.server = server; this.closed = false; FakePool.live += 1 }
  onOpen (cb) { this.#openCb = cb }
  on (type, cb) { if (!this.#handlers.has(type)) this.#handlers.set(type, []); this.#handlers.get(type).push(cb) }
  send (msg) { this.server.handle(this, msg) }
  deliver (msg) { this.#handlers.get(msg.t)?.forEach(cb => cb(msg)) }
  async connect () { this.#openCb?.() }
  close () { this.closed = true; FakePool.live -= 1 }
}

/** Records every interval the code under test creates, and who cleared it. */
function intervalLedger () {
  const realSet = globalThis.setInterval
  const realClear = globalThis.clearInterval
  const created = []
  const cleared = new Set()
  globalThis.setInterval = (...args) => { const h = realSet(...args); created.push(h); return h }
  globalThis.clearInterval = (h) => { cleared.add(h); return realClear(h) }
  return {
    created,
    orphans: () => created.filter(h => !cleared.has(h)),
    restore () {
      for (const h of created) realClear(h)
      globalThis.setInterval = realSet
      globalThis.clearInterval = realClear
    },
  }
}

let ledger = null
afterEach(() => { ledger?.restore(); ledger = null; FakePool.live = 0 })

describe('PeerMesh#join on an already-joined mesh', () => {
  it('re-joining the SAME room is a no-op: one socket, one heartbeat, no orphan', async () => {
    ledger = intervalLedger()
    const server = new FakeNodeServer()
    const mesh = new PeerMesh({ selfId: 'alice', transportPolicy: 'relay-only', poolFactory: () => new FakePool(server) })

    await mesh.join('ws://node', 'room-a')
    await mesh.join('ws://node', 'room-a')
    await mesh.join('ws://node', 'room-a')

    expect(ledger.created.length).toBe(1)
    expect(ledger.orphans().length).toBe(1) // the live one, not yet left
    expect(FakePool.live).toBe(1)
    expect(mesh.roomId).toBe('room-a')

    mesh.leave()
    expect(ledger.orphans().length).toBe(0)
    expect(FakePool.live).toBe(0)
  })

  it('moving to a DIFFERENT room still works, and leaves nothing behind', async () => {
    ledger = intervalLedger()
    const server = new FakeNodeServer()
    const mesh = new PeerMesh({ selfId: 'alice', transportPolicy: 'relay-only', poolFactory: () => new FakePool(server) })

    await mesh.join('ws://node', 'room-a')
    await mesh.join('ws://node', 'room-b')

    expect(mesh.roomId).toBe('room-b')
    expect(ledger.created.length).toBe(2)
    expect(ledger.orphans().length).toBe(1) // only room-b's, which is live
    expect(FakePool.live).toBe(1)           // room-a's socket was closed, not dropped

    mesh.leave()
    expect(ledger.orphans().length).toBe(0)
    expect(FakePool.live).toBe(0)
  })

  it('leave() then join() again is a clean re-connect', async () => {
    ledger = intervalLedger()
    const server = new FakeNodeServer()
    const mesh = new PeerMesh({ selfId: 'alice', transportPolicy: 'relay-only', poolFactory: () => new FakePool(server) })

    await mesh.join('ws://node', 'room-a')
    mesh.leave()
    await mesh.join('ws://node', 'room-a')

    expect(FakePool.live).toBe(1)
    expect(ledger.orphans().length).toBe(1)
    mesh.leave()
    expect(ledger.orphans().length).toBe(0)
  })
})
