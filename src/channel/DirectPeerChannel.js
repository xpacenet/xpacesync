/**
 * DirectPeerChannel — a typed, one-peer route over PeerMesh.
 *
 * This is intentionally product-neutral. It knows only a peer identifier and
 * an application-defined message type; authentication, encryption, storage,
 * retries, permissions, and user-facing state remain the consumer's policy.
 * That makes the same primitive suitable for private messages, payments,
 * work sessions, device control, or any future xpacenet application without
 * teaching the network layer those product concepts.
 */
export class DirectPeerChannel extends EventTarget {
  #mesh
  #peerId
  #type
  #closed = false

  /**
   * @param {import('../mesh/PeerMesh.js').PeerMesh} mesh
   * @param {{peerId: string, type: string}} opts
   */
  constructor (mesh, { peerId, type }) {
    super()
    if (!mesh) throw new Error('DirectPeerChannel requires a mesh')
    if (!peerId) throw new Error('DirectPeerChannel requires peerId')
    if (!type) throw new Error('DirectPeerChannel requires type')
    this.#mesh = mesh
    this.#peerId = peerId
    this.#type = type
    this.#mesh.addEventListener('peer:open', this.#handleOpen)
    this.#mesh.addEventListener('peer:leave', this.#handleLeave)
    this.#mesh.addEventListener('message', this.#handleMessage)
  }

  get peerId () { return this.#peerId }
  get type () { return this.#type }
  get connected () { return !this.#closed && this.#mesh.peerIds.includes(this.#peerId) }

  /** Connect the underlying mesh and join one application-defined route. */
  async join (nodeUrl, routeId) {
    if (this.#closed) throw new Error('DirectPeerChannel is closed')
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
    this.#mesh.leave()
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
