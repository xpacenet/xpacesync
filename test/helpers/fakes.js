/**
 * Shared test doubles for the connection-sharing work.
 *
 * REAL enough to be evidence. The sender bookkeeping (addTrack / getSenders /
 * removeTrack / sender.replaceTrack) behaves as a browser's does, because
 * those four are exactly what the media half of this change operates on — a
 * mock that merely recorded calls could not tell in-place replacement apart
 * from accumulation, which is the measurement at issue. Everything else is
 * the minimum RTCPeer and PeerMesh actually touch.
 */

export class FakeSender {
  constructor (track, stream) { this.track = track; this.stream = stream }
  replaceTrack (track) { this.track = track; return Promise.resolve() }
}

export class FakePeerConnection {
  static instances = []
  static reset () { FakePeerConnection.instances = [] }
  /** Connections built but never closed — the leak check. */
  static live () { return FakePeerConnection.instances.filter(pc => !pc.closed) }

  constructor (config) {
    this.config = config
    this.signalingState = 'stable'
    this.connectionState = 'new'
    this.localDescription = null
    this.closed = false
    this._senders = []
    this.removedSenders = []
    this._dc = null
    this.ondatachannel = null
    this.onicecandidate = null
    this.onnegotiationneeded = null
    this.onsignalingstatechange = null
    this.ontrack = null
    this.onconnectionstatechange = null
    FakePeerConnection.instances.push(this)

    // The POLITE side of perfect negotiation never calls createDataChannel —
    // it waits for ondatachannel. Both sides must be able to actually open in
    // these tests, so if nothing created a channel by the time RTCPeer's
    // constructor has finished wiring its handlers, deliver one inbound.
    queueMicrotask(() => {
      if (this._dc || this.closed) return
      this.ondatachannel?.({ channel: this.#makeChannel() })
    })
  }

  #makeChannel () {
    const dc = {
      readyState: 'open',
      sent: [],
      send (data) { this.sent.push(data) },
      close () { this.readyState = 'closed' },
      onopen: null, onclose: null, onmessage: null,
    }
    this._dc = dc
    // onopen is assigned by RTCPeer#hookDC after this returns, hence the hop.
    queueMicrotask(() => { if (!this.closed) dc.onopen?.() })
    return dc
  }

  createDataChannel () { return this.#makeChannel() }

  /** Frames this connection actually put on the wire, parsed. */
  get wire () { return (this._dc?.sent ?? []).map(raw => JSON.parse(raw)) }

  /** Deliver an inbound frame as if the peer had sent it. */
  receive (payload) { this._dc?.onmessage?.({ data: JSON.stringify(payload) }) }

  /** Deliver an inbound media track as if the peer had added one. */
  receiveTrack (track, stream) { this.ontrack?.({ track, streams: stream ? [stream] : [] }) }

  fail () {
    this.connectionState = 'failed'
    this.onconnectionstatechange?.()
  }

  addTrack (track, stream) {
    // The browser throws on a genuine double-add; mirror it so no test can
    // pass by accident on a duplicate attach.
    if (this._senders.some(s => s.track === track)) {
      throw new Error('InvalidAccessError: a sender already exists for the track')
    }
    const sender = new FakeSender(track, stream)
    this._senders.push(sender)
    return sender
  }
  getSenders () { return [...this._senders] }
  removeTrack (sender) {
    const i = this._senders.indexOf(sender)
    if (i === -1) return
    this._senders.splice(i, 1)
    this.removedSenders.push(sender)
  }

  async setLocalDescription () { this.localDescription = { type: 'offer', sdp: 'fake-sdp' } }
  async setRemoteDescription () {}
  async addIceCandidate () {}
  close () { this.closed = true; if (this._dc) this._dc.readyState = 'closed' }
}

/**
 * The xpacenode bridge, in memory. Rooms are tracked per (peerId, roomId)
 * because the whole point of these tests is one person reachable through two
 * different rooms at once.
 */
export class FakeNodeServer {
  constructor () { this.rooms = new Map() }   // roomId → Map(peerId → pool)
  #room (roomId) {
    if (!this.rooms.has(roomId)) this.rooms.set(roomId, new Map())
    return this.rooms.get(roomId)
  }
  handle (pool, msg) {
    if (msg.t === 'hello') {
      pool.peerId = msg.peerId
      pool.roomId = msg.roomId
      const room = this.#room(msg.roomId)
      for (const [otherId, other] of room) {
        if (otherId === msg.peerId) continue
        other.deliver({ t: 'peer_join', roomId: msg.roomId, peerId: msg.peerId })
        pool.deliver({ t: 'peer_join', roomId: msg.roomId, peerId: otherId })
      }
      room.set(msg.peerId, pool)
    } else if (msg.t === 'leave') {
      this.#room(msg.roomId).delete(pool.peerId)
      for (const other of this.#room(msg.roomId).values()) {
        other.deliver({ t: 'peer_leave', roomId: msg.roomId, peerId: pool.peerId })
      }
    } else if (msg.t === 'signal' || msg.t === 'relay') {
      this.signals = this.signals ?? []
      if (msg.t === 'signal') this.signals.push({ from: pool.peerId, roomId: msg.roomId, to: msg.to })
      const target = this.#room(msg.roomId).get(msg.to)
      target?.deliver({ ...msg, from: pool.peerId })
    }
  }
  /** Put a bare peer in a room, so the mesh under test learns of them. */
  joinPeer (peerId, roomId) {
    const pool = new FakePool(this)
    pool.send({ t: 'hello', roomId, peerId })
    return pool
  }
  /** Take a peer out of a room. */
  partPeer (pool, roomId) { pool.send({ t: 'leave', roomId }) }
}

export class FakePool {
  #handlers = new Map()
  #openCb = null
  static live = 0
  static reset () { FakePool.live = 0 }
  constructor (server) { this.server = server; this.closed = false; FakePool.live += 1 }
  onOpen (cb) { this.#openCb = cb }
  on (type, cb) {
    if (!this.#handlers.has(type)) this.#handlers.set(type, [])
    this.#handlers.get(type).push(cb)
  }
  send (msg) { if (!this.closed) this.server.handle(this, msg) }
  deliver (msg) { this.#handlers.get(msg.t)?.forEach(cb => cb(msg)) }
  async connect () { this.#openCb?.() }
  close () { if (!this.closed) { this.closed = true; FakePool.live -= 1 } }
}

/** Records every interval created, and whether anybody cleared it. */
export function intervalLedger () {
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

let trackSeq = 0
export function fakeTrack (kind = 'audio') {
  return {
    kind,
    id: `${kind}-${++trackSeq}`,
    readyState: 'live',
    enabled: true,
    stop () { this.readyState = 'ended' },
  }
}
export function fakeStream (tracks) { return { getTracks: () => tracks } }

/** Let queued microtasks and timers-of-zero run. */
export const flush = () => new Promise(resolve => setTimeout(resolve, 0))

export function installWebRTC () {
  const prior = globalThis.RTCPeerConnection
  globalThis.RTCPeerConnection = FakePeerConnection
  FakePeerConnection.reset()
  FakePool.reset()
  return () => { globalThis.RTCPeerConnection = prior }
}
