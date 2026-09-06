import { describe, it, expect } from 'vitest'
import { MessageRegistry } from '../src/registry/MessageRegistry.js'

describe('MessageRegistry', () => {
  it('returns the mesh/no-persist default for an unregistered type', () => {
    const registry = new MessageRegistry()
    expect(registry.get('unknown')).toEqual({ persist: false, transport: 'mesh' })
    expect(registry.has('unknown')).toBe(false)
  })

  it('remembers a registered strategy exactly', () => {
    const registry = new MessageRegistry()
    registry.register('chat', { persist: true })
    expect(registry.get('chat')).toEqual({ persist: true, transport: 'mesh' })
    expect(registry.has('chat')).toBe(true)
  })

  it('lets a new type declare a different transport without touching existing ones', () => {
    const registry = new MessageRegistry()
    registry.register('chat', { persist: true })
    registry.register('photo', { persist: false, transport: 'contentStore' })

    expect(registry.get('chat')).toEqual({ persist: true, transport: 'mesh' })
    expect(registry.get('photo')).toEqual({ persist: false, transport: 'contentStore' })
    expect(registry.types().sort()).toEqual(['chat', 'photo'])
  })

  it('register() is idempotent-overwrite, not additive', () => {
    const registry = new MessageRegistry()
    registry.register('chat', { persist: true })
    registry.register('chat', { persist: false })
    expect(registry.get('chat').persist).toBe(false)
  })
})
