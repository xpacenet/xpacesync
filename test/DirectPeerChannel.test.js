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
