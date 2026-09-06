import { describe, it, expect, vi } from 'vitest'
import { ContentCache } from '../src/cache/ContentCache.js'

describe('ContentCache', () => {
  it('resolves a miss via the resolver and serves the next get() from cache', async () => {
    const resolver = vi.fn(async cid => `bytes-for-${cid}`)
    const cache    = new ContentCache(resolver)

    const first  = await cache.get('cid-1')
    const second = await cache.get('cid-1')

    expect(first).toBe('bytes-for-cid-1')
    expect(second).toBe('bytes-for-cid-1')
    expect(resolver).toHaveBeenCalledTimes(1)   // second get() was a cache hit, not a re-fetch
  })

  it('has() reports presence without triggering a fetch', async () => {
    const resolver = vi.fn(async cid => `bytes-for-${cid}`)
    const cache    = new ContentCache(resolver)

    expect(await cache.has('cid-2')).toBe(false)
    expect(resolver).not.toHaveBeenCalled()

    await cache.get('cid-2')
    expect(await cache.has('cid-2')).toBe(true)
  })

  it('evicts the least-recently-used entry once past maxEntries', async () => {
    const resolver = vi.fn(async cid => `bytes-for-${cid}`)
    const cache    = new ContentCache(resolver, { maxEntries: 2 })

    await cache.get('a')
    await cache.get('b')
    await cache.get('a')   // touch 'a' so 'b' becomes the least-recently-used
    await cache.get('c')   // pushes past maxEntries — 'b' should be evicted, not 'a'

    expect(await cache.has('a')).toBe(true)
    expect(await cache.has('b')).toBe(false)
    expect(await cache.has('c')).toBe(true)
  })

  it('a cleared/evicted entry is transparently re-resolved, never a hard failure', async () => {
    const resolver = vi.fn(async cid => `bytes-for-${cid}`)
    const cache    = new ContentCache(resolver, { maxEntries: 1 })

    await cache.get('x')
    await cache.get('y')   // evicts 'x'
    expect(await cache.has('x')).toBe(false)

    const refetched = await cache.get('x')
    expect(refetched).toBe('bytes-for-x')
    expect(resolver).toHaveBeenCalledWith('x')
    expect(resolver).toHaveBeenCalledTimes(3)   // x, y, x again
  })
})
