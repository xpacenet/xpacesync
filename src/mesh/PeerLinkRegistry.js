import { RTCPeer }  from '../transport/RTCPeer.js'
import { PeerLink } from './PeerLink.js'

/**
 * PeerLinkRegistry — at most ONE live connection per peer, per device.
 *
 * This is the object that makes "no duplicates" structural rather than a
 * rule people have to remember. It is a table keyed by peerId, so a second
 * connection to a person is not forbidden by convention: there is nowhere to
 * put one.
 *
 * SHARING IS WHICH REGISTRY YOU PASS. Two meshes handed the same registry
 * share their connections; two meshes with their own registries do not.
 *
 *   const links = new PeerLinkRegistry({ selfId })
 *   const world  = new PeerMesh({ selfId, links })
 *   const thread = new PeerMesh({ selfId, links })
 *   // Both reach the same person over ONE connection.
 *
 * PeerMesh builds a PRIVATE registry when none is passed, and a private
 * registry holds exactly one lease per peer — which is byte for byte the
 * behaviour this library had before any of this existed. Sharing is opt-in
 * at the composition root and invisible everywhere else: no surface's code
 * changes, and no surface can tell whether it is alone on a connection.
 * That is the same principle DirectPeerChannel's `ownsMesh` states one layer
 * up — ownership is a parameter, not a fork — applied to the connection
 * itself rather than to the mesh.
 *
 * NOT to be confused with MessageRegistry, which is a table of message TYPES
 * and their delivery strategies. This is a table of CONNECTIONS. They share
 * a suffix and nothing else.
 */
export class PeerLinkRegistry {
  #selfId
  #links = new Map()  // peerId → PeerLink
  #peerFactory
  #onWarn

  /**
   * @param {object} [opts]
   * @param {string} [opts.selfId] This device's stable identity. Required in
   *   practice (the constructor throws without one); optional in the type only
   *   because the parameter object itself defaults to `{}`, the same
   *   convention PeerMesh uses for its own selfId. Decides
   *   perfect-negotiation politeness (`selfId < peerId`), so every mesh
   *   sharing a registry MUST agree on it — a mismatch would have the two
   *   surfaces disagree about who yields on glare, on one connection. The
   *   registry refuses the mismatch rather than letting it become an
   *   intermittent negotiation failure.
   * @param {(isPolite: boolean, iceServers: Array<object>) => object} [opts.peerFactory]
   *   Injectable transport factory, same posture as PeerMesh's poolFactory.
   * @param {(msg: string, ...rest: any[]) => void} [opts.onWarn]
   */
  constructor ({ selfId, peerFactory, onWarn } = {}) {
    if (!selfId) throw new Error('PeerLinkRegistry requires selfId')
    this.#selfId      = selfId
    this.#peerFactory = peerFactory ?? ((isPolite, iceServers) => new RTCPeer(isPolite, iceServers))
    this.#onWarn      = onWarn
  }

  get selfId () { return this.#selfId }
  /** How many live connections exist right now. The number "no duplicates" is about. */
  get size ()   { return this.#links.size }

  /** The live link to `peerId`, or null. Diagnostics and tests. */
  get (peerId)  { return this.#links.get(peerId) ?? null }

  /** Peer ids with a live connection. */
  get peerIds () { return [...this.#links.keys()] }

  /**
   * Get a lease on the connection to `peerId`, building one only if there
   * isn't one already.
   *
   * The whole D.3 fix is that this method has no second code path. A surface
   * calls it whether it is the first to want this person or the fifth; it
   * gets back a lease either way and cannot tell which happened. "Adopt an
   * existing connection" is not an API a consumer has to find and call
   * correctly — it is what acquire() does when a connection already exists.
   *
   * A STALE link is replaced, and replaced for everyone. `stale` means it
   * opened once and is not open now (the phone-went-to-sleep case). Since
   * there is only one connection, one surface noticing it died is the same
   * event as all of them noticing: the dead link is killed, which notifies
   * every attached surface so each runs its own teardown, and a fresh
   * connection is built. The alternative — letting the noticing surface
   * build a second link while others keep holding the corpse — would
   * reintroduce the duplicate this class exists to prevent, and leave the
   * other surfaces permanently attached to a connection that will never
   * carry anything again.
   *
   * @param {string} peerId
   * @param {object} [opts]
   * @param {string|null} [opts.lane] Logical channel for this surface's
   *   traffic. Omit for the shared/legacy lane — see PeerLink#routeMessage.
   * @param {(payload: object) => boolean} [opts.sendSignal] How THIS surface
   *   puts a signalling frame on the wire; returns false when it cannot, so
   *   the link can fail over to another surface's socket.
   * @param {Array<object>} [opts.iceServers] Used only if a connection is
   *   actually built. An existing connection keeps the ICE configuration it
   *   was born with, which is a property of RTCPeerConnection, not a choice
   *   made here.
   * @returns {import('./PeerLink.js').PeerLease}
   */
  acquire (peerId, { lane = null, sendSignal, iceServers = [] } = {}) {
    if (!peerId) throw new Error('PeerLinkRegistry.acquire requires peerId')

    let link = this.#links.get(peerId)
    if (link && !link.usable) {
      const dead = link
      dead.kill()           // cascades: every attached surface is told, then releases
      // Re-read rather than deleting blindly. kill() notifies the attached
      // surfaces SYNCHRONOUSLY, and a surface's own teardown is allowed to
      // ask for a connection again from inside that notification — in which
      // case the table already holds a NEW, live link by the time control
      // returns here. Deleting unconditionally would orphan that one (its
      // holders keep working leases on a link nothing can find) and then
      // build a second connection beside it: the exact duplicate this class
      // exists to make unrepresentable.
      link = this.#links.get(peerId) ?? null
      if (link === dead) { this.#links.delete(peerId); link = null }
    }

    if (!link) {
      const isPolite = this.#selfId < peerId
      const peer = this.#peerFactory(isPolite, iceServers)
      const created = new PeerLink({
        peerId,
        peer,
        onWarn: this.#onWarn,
        // Only ever evict the link this closure was made for: by the time a
        // release lands, the entry may already be a NEWER link for the same
        // peer (a rebuild), and deleting that would drop a live connection
        // out of the table while surfaces still hold leases on it.
        onEmpty: () => { if (this.#links.get(peerId) === created) this.#links.delete(peerId) },
      })
      this.#links.set(peerId, created)
      link = created
    }

    return link.acquire({ lane, sendSignal })
  }

  /**
   * Assert that a mesh sharing this registry speaks for the same device.
   * @param {string} selfId
   */
  assertSelf (selfId) {
    if (selfId !== this.#selfId) {
      throw new Error(
        `PeerLinkRegistry is for selfId "${this.#selfId}" but was given "${selfId}". ` +
        'A shared registry represents ONE device identity: politeness and intro frames ' +
        'both derive from it, so two identities on one connection cannot agree.',
      )
    }
  }

  /**
   * Kill every connection. For a composition root shutting down, not for a
   * surface leaving — a surface releases its leases and lets the refcount
   * decide, which is the entire point.
   */
  closeAll () {
    for (const link of [...this.#links.values()]) link.kill()
    this.#links.clear()
  }
}
