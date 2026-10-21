import { describe, expect, it, vi } from 'vitest'
import { DirectPeerChannel } from '../src/channel/DirectPeerChannel.js'

class FakeMesh extends EventTarget {
  constructor () {
    super()
    this.selfId = 'alice'
    this.peerIds = []
    this.sent = []
    this.left = false
  }
  async join (nodeUrl, routeId) { this.joined = { nodeUrl, routeId } }
  send (peerId, payload) {
    this.sent.push({ peerId, payload })
    return this.peerIds.includes(peerId)
  }
  leave () { this.left = true }
  emit (type, detail) { this.dispatchEvent(new CustomEvent(type, { detail })) }
}

describe('DirectPeerChannel', () => {
  it('routes one typed protocol only to the expected peer', async () => {
    const mesh = new FakeMesh()
    const channel = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'private.payload.v1' })
    const messages = []
    channel.addEventListener('message', ({ detail }) => messages.push(detail))
    await channel.join('wss://node.example', 'opaque-route')

    mesh.emit('message', { from: 'mallory', payload: { type: 'private.payload.v1', payload: 'wrong peer' } })
    mesh.emit('message', { from: 'bob', payload: { type: 'different.protocol', payload: 'wrong protocol' } })
    mesh.emit('message', { from: 'bob', payload: { type: 'private.payload.v1', payload: { cipher: 'ok' } } })

    expect(messages).toEqual([{ from: 'bob', payload: { cipher: 'ok' }, meta: undefined }])
    expect(mesh.joined).toEqual({ nodeUrl: 'wss://node.example', routeId: 'opaque-route' })
  })

  it('reports route state and returns whether direct sending succeeded', async () => {
    const mesh = new FakeMesh()
    const channel = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'payment.intent.v1' })
    const states = []
    channel.addEventListener('state', ({ detail }) => states.push(detail.state))
    await channel.join('wss://node.example', 'route')
    expect(channel.send({ amount: '10' })).toBe(false)

    mesh.peerIds = ['bob']
    mesh.emit('peer:open', { peerId: 'bob' })
    expect(channel.connected).toBe(true)
    expect(channel.send({ amount: '10' })).toBe(true)
    expect(mesh.sent.at(-1)).toMatchObject({ peerId: 'bob', payload: { type: 'payment.intent.v1', payload: { amount: '10' } } })

    mesh.emit('peer:leave', { peerId: 'bob' })
    channel.close()
    channel.close()
    expect(states).toEqual(['connecting', 'offline', 'direct', 'offline', 'closed'])
    expect(mesh.left).toBe(true)
  })

  it('removes listeners when closed', () => {
    const mesh = new FakeMesh()
    const channel = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'work.delta.v1' })
    const message = vi.fn()
    channel.addEventListener('message', message)
    channel.close()
    mesh.emit('message', { from: 'bob', payload: { type: 'work.delta.v1', payload: {} } })
    expect(message).not.toHaveBeenCalled()
    expect(channel.send({})).toBe(false)
  })

  it('cleans up the mesh when joining fails', async () => {
    const mesh = new FakeMesh()
    mesh.join = async () => { throw new Error('node unavailable') }
    const channel = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'device.command.v1' })
    await expect(channel.join('wss://node.example', 'route')).rejects.toThrow('node unavailable')
    expect(mesh.left).toBe(true)
  })
})

describe('DirectPeerChannel on a BORROWED mesh (ownsMesh: false)', () => {
  // The point of the flag: several surfaces reach one person over ONE
  // connection, each filtering on its own message type, and none of them
  // able to pull the connection out from under the others.
  it('neither joins nor leaves a mesh it does not own', async () => {
    const mesh = new FakeMesh()
    mesh.peerIds = ['bob']
    const borrower = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'spaceinbox.envelope.v1', ownsMesh: false })
    const states = []
    borrower.addEventListener('state', ({ detail }) => states.push(detail.state))

    await borrower.join('wss://node.example', 'the-lenders-room')
    expect(mesh.joined).toBeUndefined()
    expect(states).toEqual(['direct'])

    borrower.close()
    expect(mesh.left).toBe(false)
    expect(borrower.ownsMesh).toBe(false)
  })

  it('still carries and filters its own typed traffic while borrowing', async () => {
    const mesh = new FakeMesh()
    mesh.peerIds = ['bob']
    const borrower = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'spaceinbox.envelope.v1', ownsMesh: false })
    const received = []
    borrower.addEventListener('message', ({ detail }) => received.push(detail.payload))
    await borrower.join('wss://node.example', 'the-lenders-room')

    expect(borrower.send({ cipher: 'hello' })).toBe(true)
    mesh.emit('message', { from: 'bob', payload: { type: 'other.surface.v1', payload: 'not mine' } })
    mesh.emit('message', { from: 'bob', payload: { type: 'spaceinbox.envelope.v1', payload: { cipher: 'mine' } } })
    expect(received).toEqual([{ cipher: 'mine' }])
  })

  it('two borrowers share one mesh, and one closing leaves the other working', async () => {
    const mesh = new FakeMesh()
    mesh.peerIds = ['bob']
    const a = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'a.v1', ownsMesh: false })
    const b = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'b.v1', ownsMesh: false })
    const seenByB = []
    b.addEventListener('message', ({ detail }) => seenByB.push(detail.payload))
    await a.join('wss://node.example', 'room')
    await b.join('wss://node.example', 'room')

    a.close()
    expect(mesh.left).toBe(false)
    mesh.emit('message', { from: 'bob', payload: { type: 'b.v1', payload: 'still here' } })
    expect(seenByB).toEqual(['still here'])
  })

  it('a channel that OWNS its mesh still tears it down -- the default is unchanged', async () => {
    const mesh = new FakeMesh()
    const owner = new DirectPeerChannel(mesh, { peerId: 'bob', type: 'x.v1' })
    await owner.join('wss://node.example', 'its-own-room')
    expect(mesh.joined).toEqual({ nodeUrl: 'wss://node.example', routeId: 'its-own-room' })
    owner.close()
    expect(mesh.left).toBe(true)
    expect(owner.ownsMesh).toBe(true)
  })
})
