export { PeerMesh }          from './mesh/PeerMesh.js'
// One connection per peer per device, shared by every surface that needs it.
// Hand the same registry to two meshes and they stop opening two connections
// to one person. See mesh/PeerLink.js for the ownership and lifetime rules.
export { PeerLinkRegistry }  from './mesh/PeerLinkRegistry.js'
export { PeerLink, PeerLease, LANE_TAG } from './mesh/PeerLink.js'
export { RealtimeChannel }   from './channel/RealtimeChannel.js'
export { DirectPeerChannel } from './channel/DirectPeerChannel.js'
export { CallChannel }       from './channel/CallChannel.js'
export { MessageRegistry }   from './registry/MessageRegistry.js'
export { MessageLog }        from './persistence/MessageLog.js'
export { ContentCache }      from './cache/ContentCache.js'
export { OutboundQueue }     from './queue/OutboundQueue.js'

// Lower-level transport primitives — exported for consumers that need to
// build something other than a RealtimeChannel directly on top of the mesh
// (e.g. a raw media-streaming layer, per REALTIME_LAYER.md Milestone 3+).
export { XpaceNodePool } from './transport/XpaceNodePool.js'
export { RTCPeer }       from './transport/RTCPeer.js'
