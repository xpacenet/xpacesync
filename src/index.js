export { PeerMesh }        from './mesh/PeerMesh.js'
export { RealtimeChannel } from './channel/RealtimeChannel.js'
export { MessageRegistry } from './registry/MessageRegistry.js'
export { MessageLog }      from './persistence/MessageLog.js'
export { ContentCache }    from './cache/ContentCache.js'
export { OutboundQueue }   from './queue/OutboundQueue.js'

// Lower-level transport primitives — exported for consumers that need to
// build something other than a RealtimeChannel directly on top of the mesh
// (e.g. a raw media-streaming layer, per REALTIME_LAYER.md Milestone 3+).
export { XpaceNodePool } from './transport/XpaceNodePool.js'
export { RTCPeer }       from './transport/RTCPeer.js'
