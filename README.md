# xpacesync

The generalized realtime layer described in
[`spaxerail/docs/REALTIME_LAYER.md`](../spaxerail/docs/REALTIME_LAYER.md),
extracted out of `spacework` so any app on the xpacenet swarm can reuse it —
`spacework` today, `spacevilla`'s chat and the announced social app next.

This package answers one question per message type: **how should this be
carried, and does it need to survive a reload?** — and answers it once, by
declaration, not by growing a bigger switch statement every time a new type
shows up.

## The four pieces

```
        ┌──────────────────┐
        │  RealtimeChannel │  ← what apps actually use: send(type, payload) / on(type, cb)
        └──────┬───────────┘
               │ consults
        ┌──────┴───────────┐        ┌─────────────┐       ┌───────────────┐
        │  MessageRegistry │        │ MessageLog  │       │ ContentCache  │
        │  type → strategy │        │ (durable)   │       │ (disposable)  │
        └──────────────────┘        └─────────────┘       └───────────────┘
               │
        ┌──────┴───────────┐
        │     PeerMesh      │  ← room membership + live WebRTC connections
        └──────┬───────────┘
               │
     ┌─────────┴─────────┐
     │  XpaceNodePool     │  ← signaling over one xpacenode
     │  RTCPeer           │  ← one data channel + media tracks
     └────────────────────┘
```

- **`PeerMesh`** — connects to an xpacenode, joins a room, keeps a live
  WebRTC connection to every peer in it, and rebuilds any connection that
  goes stale (phone sleep, tab suspend, network blip). This is the
  generalized core of `spacework`'s `RemoteSync` v3, with every
  SpaceWork-specific concept (avatar, position, presence snapshots)
  removed. It emits raw `{ from, payload }` messages and knows nothing
  about what a message means.

  It also picks up STUN/TURN servers automatically from whichever
  xpacenode it connects to (sent in the node's own `welcome` message) — no
  app needs to pass its own `iceServers` anymore. Pass an explicit
  `iceServers` array to the constructor if you need to override that (e.g.
  a fixed test environment); an explicit value always wins and a node's own
  offer never replaces it. With no override and no node-provided servers
  (a node with no coturn configured), PeerMesh falls back to STUN-only
  public defaults — no TURN relay, so it won't help peers that need one.

- **`MessageRegistry`** — the abstraction from `REALTIME_LAYER.md` made
  literal: `registry.register('chat', { persist: true })`. A new message
  type is one call, not a new case in a library file.

- **`MessageLog`** — durable, IndexedDB-backed history for types marked
  `persist: true`. This is the actual fix for the original bug report:
  `spacework`'s chat had a live reconnect path but nowhere to remember a
  message once received, so a full page discard (the common case on
  mobile — most browsers discard a backgrounded tab rather than suspend
  it) lost history no amount of reconnect-logic fixing could recover.

- **`ContentCache`** — deliberately *not* the same thing as `MessageLog`.
  Everything in it is bytes fetchable again from `contentStore`/the swarm
  by CID, so losing an entry is a performance regression, never data loss.
  This is the local half of the "browser cache that fetches through it"
  piece `xpacenode`'s own `ROADMAP.md` already named as Milestone 2 —
  given a real seam here instead of invented twice.

- **`RealtimeChannel`** — the piece an app actually imports. Wraps a
  `PeerMesh` with the registry, `MessageLog`, and (by the consumer's own
  wiring) `ContentCache`, and exposes `send(type, payload)` / `on(type,
  cb)` / `history(type)`.

## Example

```js
import { PeerMesh, RealtimeChannel, MessageRegistry } from '@xpacenet/xpacesync'

const mesh = new PeerMesh({ selfId: myIdentity.peerId })
await mesh.join('wss://your-xpacenode/xpacenode-ws', roomId)

const registry = new MessageRegistry()
registry.register('chat', { persist: true })   // survives a reload
registry.register('move', { persist: false })  // ephemeral, high-frequency

const channel = new RealtimeChannel(mesh, { registry })

channel.on('chat', ({ from, payload }) => showMessage(from, payload.text))
channel.send('chat', { text: 'hello room' })

// After a reload, before any peer has re-sent anything:
const history = await channel.history('chat')
```

## What this is not

Per `REALTIME_LAYER.md`: this ships Milestone 1 (generalized text + media
with real persistence) and the seams for what comes later (a
`contentStore`-backed transport for media, `ContentCache` for its bytes).
It does not ship streaming media tracks, blockchain-anchored message types,
or compute-routed AI requests — those are named as deliberately undesigned
future milestones, not built here ahead of a real consumer.

## Status

Extracted and unit-tested (`npm test`) — `MessageRegistry`, `MessageLog`,
`ContentCache`, and `RealtimeChannel`'s wiring are covered without needing
a live WebRTC connection. `PeerMesh` itself is the same peer-lifecycle code
already verified live in `spacework` (5-peer browser testing, this
session) — it has not yet been re-verified live *as this standalone
package* consumed by `spacework`. That's the next step, not a gap in this
commit.
