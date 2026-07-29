'use strict';

const { FINGER_COUNT, add, inInterval, validateId } = require('./ring');

class ChordNode {
  constructor({ id, host = '127.0.0.1', port = 5000, requestTimeout = 3000 }) {
    this.id = validateId(id);
    this.host = host;
    this.port = Number(port);
    if (!Number.isInteger(this.port) || this.port < 1 || this.port > 65535) {
      throw new Error('A porta deve ser um inteiro entre 1 e 65535');
    }
    this.requestTimeout = requestTimeout;
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();
    this.joined = false;
  }

  get reference() {
    return { id: this.id, host: this.host, port: this.port };
  }

  buildEmptyFingerTable() {
    return Array.from({ length: FINGER_COUNT }, (_, index) => ({
      index: index + 1,
      start: add(this.id, 2 ** index),
      node: null
    }));
  }

  get successor() {
    return this.fingers[0].node;
  }

  set successor(node) {
    this.fingers[0].node = node;
  }

  createRing() {
    this.predecessor = this.reference;
    for (const finger of this.fingers) finger.node = this.reference;
    this.joined = true;
  }

  async join(bootstrap) {
    if (this.joined) throw new Error('Este nó já pertence a uma rede Chord');
    if (!bootstrap) {
      this.createRing();
      return this.state();
    }

    const contact = normalizeReference(bootstrap);
    if (contact.id === this.id) throw new Error('O nó de entrada não pode ter o mesmo id');

    // Localiza a posição do novo nó no anel usando o nó de entrada.
    const successor = await this.rpc(contact, '/rpc/find-successor', {
      method: 'POST',
      body: { id: this.id }
    });
    if (successor.id === this.id) throw new Error(`O id ${this.id} já está em uso`);

    const predecessorResult = await this.rpc(successor, '/rpc/predecessor');
    const predecessor = predecessorResult.node || successor;

    this.successor = successor;
    this.predecessor = predecessor;

    // Faz o novo nó entrar entre predecessor e sucessor.
    await this.rpc(successor, '/rpc/predecessor', {
      method: 'PUT',
      body: { node: this.reference }
    });
    if (predecessor.id !== successor.id) {
      await this.rpc(predecessor, '/rpc/successor', {
        method: 'PUT',
        body: { node: this.reference }
      });
    } else {
      // A rede possuía apenas um nó.
      await this.rpc(successor, '/rpc/successor', {
        method: 'PUT',
        body: { node: this.reference }
      });
    }

    this.joined = true;

    await this.refreshFingerTable();

    // A entrada altera também as fingers dos nós que já estavam no anel.
    await this.rpc(this.successor, '/rpc/refresh-fingers', {
      method: 'POST',
      body: { originId: this.id, hops: 0 }
    });
    return this.state();
  }

  async refreshFingerTable() {
    for (const finger of this.fingers) {
      finger.node = await this.findSuccessor(finger.start);
    }
  }

  async refreshRingFingerTables(originId, hops = 0) {
    validateId(originId);
    if (this.id === Number(originId)) return { ok: true };
    if (hops >= 32) throw new Error('Limite de nós excedido ao atualizar finger tables');

    await this.refreshFingerTable();
    return this.rpc(this.successor, '/rpc/refresh-fingers', {
      method: 'POST',
      body: { originId: Number(originId), hops: hops + 1 }
    });
  }

  async findSuccessor(rawId, hops = 0) {
    const id = validateId(rawId);
    if (!this.joined || !this.successor) throw new Error('O nó ainda não entrou em uma rede');
    if (this.successor.id === this.id) return this.reference;
    if (id === this.id) return this.reference;

    if (inInterval(id, this.id, this.successor.id, false, true)) {
      return this.successor;
    }

    if (hops >= 32) throw new Error('Limite de saltos excedido ao procurar sucessor');
    let next = this.closestPrecedingFinger(id);
    // Uma finger table ainda desatualizada não deve interromper a busca:
    // caminhar pelo sucessor sempre encontra a posição correta no anel.
    if (next.id === this.id) next = this.successor;

    return this.rpc(next, '/rpc/find-successor', {
      method: 'POST',
      body: { id, hops: hops + 1 }
    });
  }

  closestPrecedingFinger(id) {
    for (let i = this.fingers.length - 1; i >= 0; i -= 1) {
      const candidate = this.fingers[i].node;
      if (candidate && candidate.id !== this.id
        && inInterval(candidate.id, this.id, id, false, false)) {
        return candidate;
      }
    }
    return this.reference;
  }

  async rpc(node, path, { method = 'GET', body } = {}) {
    const target = normalizeReference(node);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.requestTimeout);
    try {
      const response = await fetch(`http://${target.host}:${target.port}${path}`, {
        method,
        headers: body ? { 'content-type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || `Erro HTTP ${response.status}`);
      return data;
    } finally {
      clearTimeout(timer);
    }
  }

  state() {
    return {
      node: this.reference,
      joined: this.joined,
      predecessor: this.predecessor,
      successor: this.successor,
      fingerTable: this.fingers
    };
  }
}

function normalizeReference(node) {
  if (!node || typeof node !== 'object') throw new Error('Referência de nó inválida');
  return {
    id: validateId(node.id),
    host: String(node.host || '127.0.0.1'),
    port: Number(node.port || 5000)
  };
}

module.exports = { ChordNode, normalizeReference };
