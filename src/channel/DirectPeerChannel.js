/**
 * DirectPeerChannel — a typed, one-peer route over PeerMesh.
 *
 * This is intentionally product-neutral. It knows only a peer identifier and
 * an application-defined message type; authentication, encryption, storage,
 * retries, permissions, and user-facing state remain the consumer's policy.
 * That makes the same primitive suitable for private messages, payments,
 * work sessions, device control, or any future xpacenet application without
 * teaching the network layer those product concepts.
 *
 * OWNERSHIP IS A PARAMETER, NOT A FORK. A channel that MINTED its mesh must
 * tear it down on close(); a channel handed a mesh that something else is
 * still using must not. Both are legitimate, and before `ownsMesh` existed
 * only the first was expressible — so CallChannel had to become a separate
 * class for no other reason, as its own header says: "DirectPeerChannel is
 * deliberately NOT reused here even though the shape matches:
 * DirectPeerChannel.close() calls mesh.leave(), which is correct for a
 * channel that owns its mesh but wrong here."
 *
 * That cost a consumer real connections. spacehub exposes a conversation's
 * mesh with a comment promising a later capability can "reuse this exact
 * connection instead of opening a second one to the same peer" — true of
 * CallChannel, false of this class, so the one seam that existed for
 * sharing could not be used for sharing: borrowing a mesh through it left
 * the room on the lender's behalf and closed every connection the lender
 * had. Measured in spacehub's tests/comms-connection-census.test.mjs
 * (PART D.1), where standing next to someone while messaging them was found
 * to open three connections to that one person.
 *
 * `ownsMesh` defaults to TRUE, which is exactly the previous behaviour, so
 * no existing consumer changes. Pass false to borrow:
 *
 *   new DirectPeerChannel(someonesLiveMesh, { peerId, type, ownsMesh: false })
 *
 * A borrowing channel never joins and never leaves; it only subscribes. The
 * lender stays responsible for the room, and several borrowers can ride one
 * connection at once because each filters on its own message `type` — which
 * is what "one connection, many surfaces" means in practice.
 */
export class DirectPeerChannel extends EventTarget {
  #mesh
  #peerId
  #type
  #ownsMesh
  #closed = false

  /**
   * @param {import('../mesh/PeerMesh.js').PeerMesh} mesh
   * @param {{peerId: string, type: string, ownsMesh?: boolean}} opts
   *   `ownsMesh` (default true): whether closing this channel may tear the
   *   mesh down. False for a mesh another surface owns.
   */
  constructor (mesh, { peerId, type, ownsMesh = true }) {
    super()
    if (!mesh) throw new Error('DirectPeerChannel requires a mesh')
    if (!peerId) throw new Error('DirectPeerChannel requires peerId')
    if (!type) throw new Error('DirectPeerChannel requires type')
    this.#mesh = mesh
    this.#peerId = peerId
    this.#type = type
    this.#ownsMesh = ownsMesh !== false
    this.#mesh.addEventListener('peer:open', this.#handleOpen)
    this.#mesh.addEventListener('peer:leave', this.#handleLeave)
    this.#mesh.addEventListener('message', this.#handleMessage)
  }

  get peerId () { return this.#peerId }
  get type () { return this.#type }
  get connected () { return !this.#closed && this.#mesh.peerIds.includes(this.#peerId) }
  /** False when this channel is riding a mesh something else owns. */
  get ownsMesh () { return this.#ownsMesh }

  /**
   * Connect the underlying mesh and join one application-defined route.
   *
   * On a BORROWED mesh (`ownsMesh: false`) this neither joins nor leaves —
   * the lender already did, and re-joining on its behalf would replace its
   * socket. It still reports the current state, so a borrower's wiring is
   * identical either way.
   */
  async join (nodeUrl, routeId) {
    if (this.#closed) throw new Error('DirectPeerChannel is closed')
    if (!this.#ownsMesh) {
      this.#emitState(this.connected ? 'direct' : 'offline')
      return
    }
    this.#emitState('connecting')
    try {
      await this.#mesh.join(nodeUrl, routeId)
      if (this.connected) this.#emitState('direct')
      else this.#emitState('offline')
    } catch (error) {
      this.#mesh.leave()
      this.#emitState('offline')
      throw error
    }
  }

  /**
   * Send a JSON-serializable payload only to the configured peer.
   * @returns {boolean} whether an open direct data channel carried it.
   */
  send (payload) {
    if (this.#closed) return false
    return this.#mesh.send(this.#peerId, {
      type: this.#type,
      payload,
      meta: { from: this.#mesh.selfId, ts: Date.now() },
    })
  }

  close () {
    if (this.#closed) return
    this.#closed = true
    this.#mesh.removeEventListener('peer:open', this.#handleOpen)
    this.#mesh.removeEventListener('peer:leave', this.#handleLeave)
    this.#mesh.removeEventListener('message', this.#handleMessage)
    // A borrowed mesh is left alone: unsubscribing is the whole teardown.
    // Leaving it would close connections other surfaces are still using,
    // which is the defect this flag exists to make impossible.
    if (this.#ownsMesh) this.#mesh.leave()
    this.#emitState('closed')
  }

  #handleOpen = ({ detail }) => {
    if (!this.#closed && detail.peerId === this.#peerId) this.#emitState('direct')
  }

  #handleLeave = ({ detail }) => {
    if (!this.#closed && detail.peerId === this.#peerId) this.#emitState('offline')
  }

  #handleMessage = ({ detail }) => {
    const { from, payload: message } = detail
    if (this.#closed || from !== this.#peerId || message?.type !== this.#type) return
    this.dispatchEvent(new CustomEvent('message', {
      detail: { from, payload: message.payload, meta: message.meta },
    }))
  }

  #emitState (state) {
    this.dispatchEvent(new CustomEvent('state', { detail: { state, peerId: this.#peerId } }))
  }
}
