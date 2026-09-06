import { describe, it, expect } from 'vitest'
import { MessageLog } from '../src/persistence/MessageLog.js'

describe('MessageLog', () => {
  it('replays appended messages in insertion order', async () => {
    const log = new MessageLog()
    await log.append('room-1', 'chat', { text: 'hi' })
    await log.append('room-1', 'chat', { text: 'there' })

    const history = await log.replay('room-1', 'chat')
    expect(history).toEqual([{ text: 'hi' }, { text: 'there' }])
  })

  it('this is the fix for the reported bug: history survives being reconstructed fresh', async () => {
    // Simulates a full page discard/reload: nothing in memory, only what
    // was durably logged before the reload survives.
    const roomId = 'room-reload'
    {
      const log = new MessageLog()
      await log.append(roomId, 'chat', { text: 'before reload' })
    }
    // A brand-new MessageLog instance, same IndexedDB — this is what a
    // fresh page load looks like.
    const reloaded = new MessageLog()
    const history  = await reloaded.replay(roomId, 'chat')
    expect(history).toEqual([{ text: 'before reload' }])
  })

  it('keeps rooms and types independent', async () => {
    const log = new MessageLog()
    await log.append('room-a', 'chat', { text: 'a-chat' })
    await log.append('room-b', 'chat', { text: 'b-chat' })
    await log.append('room-a', 'photo', { cid: 'bafy...' })

    expect(await log.replay('room-a', 'chat')).toEqual([{ text: 'a-chat' }])
    expect(await log.replay('room-b', 'chat')).toEqual([{ text: 'b-chat' }])
    expect(await log.replay('room-a', 'photo')).toEqual([{ cid: 'bafy...' }])
  })

  it('since filters out entries appended before the given timestamp', async () => {
    const log = new MessageLog()
    await log.append('room-1', 'chat', { text: 'old' })
    const cutoff = Date.now()
    await new Promise(r => setTimeout(r, 5))
    await log.append('room-1', 'chat', { text: 'new' })

    const recent = await log.replay('room-1', 'chat', { since: cutoff })
    expect(recent).toEqual([{ text: 'new' }])
  })

  it('clearRoom removes all types for that room but leaves others intact', async () => {
    const log = new MessageLog()
    await log.append('room-clear-a', 'chat', { text: 'a' })
    await log.append('room-clear-a', 'photo', { cid: 'x' })
    await log.append('room-clear-b', 'chat', { text: 'b' })

    await log.clearRoom('room-clear-a')

    expect(await log.replay('room-clear-a', 'chat')).toEqual([])
    expect(await log.replay('room-clear-a', 'photo')).toEqual([])
    expect(await log.replay('room-clear-b', 'chat')).toEqual([{ text: 'b' }])
  })
})
