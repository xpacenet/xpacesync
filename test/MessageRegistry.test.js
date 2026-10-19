import { describe, it, expect } from 'vitest'
import { MessageRegistry } from '../src/registry/MessageRegistry.js'

describe('MessageRegistry', () => {
  it('returns the mesh/no-persist/no-queue default for an unregistered type', () => {
    const registry = new MessageRegistry()
    expect(registry.get('unknown')).toEqual({ persist: false, transport: 'mesh', queueOnFail: false, relay: false, coalesce: false })
    expect(registry.has('unknown')).toBe(false)
  })

  it('remembers a registered strategy exactly', () => {
    const registry = new MessageRegistry()
    registry.register('chat', { persist: true })
    expect(registry.get('chat')).toEqual({ persist: true, transport: 'mesh', queueOnFail: false, relay: false, coalesce: false })
    expect(registry.has('chat')).toBe(true)
  })

  it('lets a new type declare a different transport without touching existing ones', () => {
    const registry = new MessageRegistry()
    registry.register('chat', { persist: true })
    registry.register('photo', { persist: false, transport: 'contentStore' })

    expect(registry.get('chat')).toEqual({ persist: true, transport: 'mesh', queueOnFail: false, relay: false, coalesce: false })
    expect(registry.get('photo')).toEqual({ persist: false, transport: 'contentStore', queueOnFail: false, relay: false, coalesce: false })
    expect(registry.types().sort()).toEqual(['chat', 'photo'])
  })

  it('queueOnFail defaults to false and can be turned on independently of persist/transport', () => {
    const registry = new MessageRegistry()
    registry.register('chat', { persist: true, queueOnFail: true })
    registry.register('move', { persist: false })

    expect(registry.get('chat').queueOnFail).toBe(true)
    expect(registry.get('move').queueOnFail).toBe(false)
  })

  it('relay defaults to false and is an independent per-type opt-in (the server-relay floor)', () => {
    const registry = new MessageRegistry()
    registry.register('tick', { relay: true })
    registry.register('chat', { persist: true })

    expect(registry.get('tick').relay).toBe(true)
    expect(registry.get('chat').relay).toBe(false)
    expect(registry.get('never-registered').relay).toBe(false)
  })

  it('coalesce defaults to false and is an independent per-type opt-in (latest-only relay delivery)', () => {
    const registry = new MessageRegistry()
    registry.register('tick', { relay: true, coalesce: true })
    registry.register('signal', { relay: true })

    expect(registry.get('tick').coalesce).toBe(true)
    expect(registry.get('signal').coalesce).toBe(false)
    expect(registry.get('never-registered').coalesce).toBe(false)
  })

  it('register() is idempotent-overwrite, not additive', () => {
    const registry = new MessageRegistry()
    registry.register('chat', { persist: true })
    registry.register('chat', { persist: false })
    expect(registry.get('chat').persist).toBe(false)
  })
})
