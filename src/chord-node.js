'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { FINGER_COUNT, add, hashKey, inInterval, validateId } = require('./ring');

const CATALOG_NAME = 'catalogo.txt';
const META_DIR_NAME = '.chord-meta';
const DEFAULT_REPLICATION_FACTOR = 3;

class ChordNode {
  constructor({ id, host = '127.0.0.1', port = 5000, requestTimeout = 3000,
    storageDirectory, replicationFactor = DEFAULT_REPLICATION_FACTOR } = {}) {
    this.id = validateId(id);
    this.host = String(host || '').trim();
    if (!this.host || this.host === '0.0.0.0' || this.host === '::') {
      throw new Error('Informe o IP ou hostname pelo qual os outros nós acessam esta máquina');
    }
    this.port = Number(port);
    if (!Number.isInteger(this.port) || this.port < 1 || this.port > 65535) {
      throw new Error('A porta deve ser um inteiro entre 1 e 65535');
    }
    this.requestTimeout = requestTimeout;
    this.storageDirectory = storageDirectory || path.join(
      process.cwd(), 'data', `node-${this.id}-${this.port}`);
    // Número total de cópias do arquivo na rede (dono + réplicas nos sucessores).
    this.replicationFactor = Math.max(1, Number(replicationFactor) || 1);
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();
    this.joined = false;
  }

  get reference() {
    return { id: this.id, host: this.host, port: this.port };
  }

  get metaDirectory() {
    return path.join(this.storageDirectory, META_DIR_NAME);
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
    // A cada atualização da finger table, garante que os arquivos deste nó
    // continuam replicados nos sucessores corretos e que as réplicas locais
    // ainda fazem sentido na topologia atual.
    try {
      await this.verifyReplicas();
    } catch (error) {
      console.error(`[node ${this.id}] falha ao verificar réplicas: ${error.message}`);
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

  /**
   * Percorre a cadeia de sucessores (fingers[0] encadeado) a partir de
   * `fromNode` (por padrão, este próprio nó) e devolve até `n` nós,
   * sem incluir `fromNode`. Usado tanto para decidir para onde replicar
   * quanto para verificar se este nó ainda deveria guardar uma réplica.
   */
  async getSuccessorList(n, fromNode = this.reference) {
    const list = [];
    if (n <= 0) return list;

    let current;
    if (fromNode.id === this.id) {
      current = this.successor;
    } else {
      const result = await this.rpc(fromNode, '/rpc/successor');
      current = result.node;
    }

    const seen = new Set();
    while (current && current.id !== fromNode.id && !seen.has(current.id) && list.length < n) {
      list.push(current);
      seen.add(current.id);
      let next;
      try {
        next = (await this.rpc(current, '/rpc/successor')).node;
      } catch (error) {
        break; // nó inacessível: devolve a lista parcial obtida até aqui.
      }
      current = next;
    }
    return list;
  }

  /** Insere bytes na rede e devolve a posição do hash e o nó responsável. */
  async put(fileName, content, { updateCatalog = true } = {}) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const hashId = hashKey(name);
    const owner = await this.findSuccessor(hashId);

    if (owner.id === this.id) {
      await this.acceptOwnership(name, bytes);
    } else {
      await this.rpc(owner, '/rpc/files', {
        method: 'PUT',
        body: { name, content: bytes.toString('base64') }
      });
    }

    if (updateCatalog && name !== CATALOG_NAME) await this.addToCatalog(name);
    return { name, hashId, node: owner, size: bytes.length };
  }

  /** Busca os bytes de um arquivo a partir de qualquer nó da rede. */
  async get(fileName) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const hashId = hashKey(name);
    const owner = await this.findSuccessor(hashId);
    let content;

    try {
      if (owner.id === this.id) {
        content = await this.readLocal(name);
      } else {
        const result = await this.rpc(owner, `/rpc/files?name=${encodeURIComponent(name)}`);
        content = Buffer.from(result.content, 'base64');
      }
    } catch (error) {
      if (error.code === 'ENOENT') throw error;
      // Dono indisponível no momento: tenta servir a partir de uma réplica conhecida.
      const fallback = await this.readFromReplica(name, owner);
      if (!fallback) throw error;
      content = fallback;
    }
    return { name, hashId, node: owner, size: content.length, content };
  }

  /** Grava localmente como dono e propaga cópias para os sucessores. */
  async acceptOwnership(fileName, bytes) {
    await this.storeLocal(fileName, bytes, { role: 'owner', ownerId: this.id });
    await this.replicateToSuccessors(fileName, bytes, this.id);
  }

  /** Empurra o conteúdo para os (replicationFactor - 1) sucessores mais próximos. */
  async replicateToSuccessors(fileName, bytes, ownerId) {
    const name = validateFileName(fileName);
    const count = Math.max(0, this.replicationFactor - 1);
    if (count === 0) return [];

    const targets = await this.getSuccessorList(count);
    const results = [];
    for (const target of targets) {
      if (target.id === ownerId) continue;
      try {
        await this.rpc(target, '/rpc/replicas', {
          method: 'PUT',
          body: { name, ownerId, content: bytes.toString('base64') }
        });
        results.push({ node: target, ok: true });
      } catch (error) {
        // Nó indisponível agora: será reparado no próximo verifyReplicas().
        results.push({ node: target, ok: false, error: error.message });
      }
    }
    return results;
  }

  /** Tenta ler o conteúdo a partir de uma das réplicas do dono informado. */
  async readFromReplica(fileName, owner) {
    const name = validateFileName(fileName);
    const count = Math.max(0, this.replicationFactor - 1);
    if (count === 0) return null;

    let targets;
    try {
      targets = await this.getSuccessorList(count, owner);
    } catch (error) {
      return null;
    }

    for (const target of targets) {
      try {
        if (target.id === this.id) {
          return await this.readLocal(name);
        }
        const result = await this.rpc(target, `/rpc/replicas?name=${encodeURIComponent(name)}`);
        return Buffer.from(result.content, 'base64');
      } catch (error) {
        continue; // tenta a próxima réplica candidata
      }
    }
    return null;
  }

  /**
   * Percorre os arquivos guardados localmente e:
   *  - para os arquivos dos quais este nó é dono, garante que os sucessores
   *    atuais têm cópia (repara buracos deixados por entradas/saídas de nós);
   *  - para as réplicas guardadas por conta de outro dono, confirma que o
   *    dono e a posição deste nó na cadeia de sucessores continuam válidos,
   *    ressincronizando ou descartando a réplica quando necessário.
   */
  async verifyReplicas() {
    if (!this.joined) return;
    const entries = await this.listLocalMeta();
    for (const entry of entries) {
      try {
        if (entry.role === 'owner') {
          await this.verifyOwnedFile(entry.name);
        } else if (entry.role === 'replica') {
          await this.verifyReplicaFile(entry.name);
        }
      } catch (error) {
        console.error(`[node ${this.id}] falha ao verificar "${entry.name}": ${error.message}`);
      }
    }
  }

  async verifyOwnedFile(fileName) {
    const name = validateFileName(fileName);
    const meta = await this.readMeta(name);
    if (!meta || meta.role !== 'owner') return;

    const targets = await this.getSuccessorList(Math.max(0, this.replicationFactor - 1));
    let content = null;

    for (const target of targets) {
      let needsPush = true;
      try {
        const remote = await this.rpc(
          target, `/rpc/replicas?name=${encodeURIComponent(name)}&metaOnly=1`);
        needsPush = remote.checksum !== meta.checksum;
      } catch (error) {
        needsPush = true; // sem réplica ainda, ou nó momentaneamente inacessível
      }

      if (!needsPush) continue;

      try {
        if (!content) content = await this.readLocal(name);
        await this.rpc(target, '/rpc/replicas', {
          method: 'PUT',
          body: { name, ownerId: this.id, content: content.toString('base64') }
        });
      } catch (error) {
        // Nó indisponível: tenta novamente no próximo ciclo de verificação.
      }
    }
  }

  async verifyReplicaFile(fileName) {
    const name = validateFileName(fileName);
    const meta = await this.readMeta(name);
    if (!meta || meta.role !== 'replica') return;

    const hashId = hashKey(name);
    let owner;
    try {
      owner = await this.findSuccessor(hashId);
    } catch (error) {
      return; // não foi possível determinar o dono agora; tenta no próximo ciclo.
    }

    if (owner.id === this.id) {
      // A topologia mudou: este nó passou a ser o dono do arquivo.
      try {
        const content = await this.readLocal(name);
        await this.storeLocal(name, content, { role: 'owner', ownerId: this.id });
        await this.replicateToSuccessors(name, content, this.id);
      } catch (error) {
        // Conteúdo local ausente/corrompido: nada a promover.
      }
      return;
    }

    const count = Math.max(0, this.replicationFactor - 1);
    let targets = [];
    try {
      targets = count > 0 ? await this.getSuccessorList(count, owner) : [];
    } catch (error) {
      return; // dono inacessível no momento: mantém a réplica por ora.
    }

    const stillReplica = targets.some((candidate) => candidate.id === this.id);
    if (!stillReplica) {
      await this.removeLocal(name).catch(() => {});
      return;
    }

    try {
      const ownerInfo = await this.rpc(
        owner, `/rpc/replicas?name=${encodeURIComponent(name)}&metaOnly=1`);
      if (ownerInfo.checksum && ownerInfo.checksum !== meta.checksum) {
        const fresh = await this.rpc(owner, `/rpc/files?name=${encodeURIComponent(name)}`);
        const content = Buffer.from(fresh.content, 'base64');
        await this.storeLocal(name, content, { role: 'replica', ownerId: owner.id });
      }
    } catch (error) {
      // Dono inacessível ou arquivo ainda não propagado: tenta no próximo ciclo.
    }
  }

  async addToCatalog(fileName) {
    let names = [];
    try {
      const catalog = await this.get(CATALOG_NAME);
      names = catalog.content.toString('utf8').split(/\r?\n/).filter(Boolean);
    } catch (error) {
      if (error.code !== 'ENOENT' && !/não encontrado/i.test(error.message)) throw error;
    }
    if (!names.includes(fileName)) names.push(fileName);
    names.sort((a, b) => a.localeCompare(b, 'pt-BR'));
    await this.put(CATALOG_NAME, Buffer.from(`${names.join('\n')}\n`), {
      updateCatalog: false
    });
  }

  /** Grava bytes localmente, junto do metadado de papel/dono/checksum. */
  async storeLocal(fileName, content, { role = 'owner', ownerId = this.id } = {}) {
    const name = validateFileName(fileName);
    await fs.mkdir(this.storageDirectory, { recursive: true });
    await fs.writeFile(path.join(this.storageDirectory, name), content);
    await this.writeMeta(name, {
      role,
      ownerId: Number(ownerId),
      checksum: checksumOf(content),
      updatedAt: new Date().toISOString()
    });
  }

  async readLocal(fileName) {
    const name = validateFileName(fileName);
    try {
      return await fs.readFile(path.join(this.storageDirectory, name));
    } catch (error) {
      if (error.code === 'ENOENT') {
        const notFound = new Error(`Arquivo "${name}" não encontrado na rede`);
        notFound.code = 'ENOENT';
        throw notFound;
      }
      throw error;
    }
  }

  async removeLocal(fileName) {
    const name = validateFileName(fileName);
    await fs.rm(path.join(this.storageDirectory, name), { force: true });
    await this.deleteMeta(name);
  }

  /** Metadado (papel, dono, checksum) de um arquivo guardado localmente, com ou sem conteúdo. */
  async getLocalFileInfo(fileName, { includeContent = true } = {}) {
    const name = validateFileName(fileName);
    const meta = await this.readMeta(name);
    if (!meta) {
      const notFound = new Error(`Arquivo "${name}" não encontrado localmente`);
      notFound.code = 'ENOENT';
      throw notFound;
    }
    const result = {
      name,
      role: meta.role,
      ownerId: meta.ownerId,
      checksum: meta.checksum,
      updatedAt: meta.updatedAt
    };
    if (includeContent) {
      result.content = (await this.readLocal(name)).toString('base64');
    }
    return result;
  }

  metaPathFor(name) {
    return path.join(this.metaDirectory, `${name}.json`);
  }

  async writeMeta(name, meta) {
    await fs.mkdir(this.metaDirectory, { recursive: true });
    await fs.writeFile(this.metaPathFor(name), JSON.stringify(meta, null, 2));
  }

  async readMeta(name) {
    try {
      const raw = await fs.readFile(this.metaPathFor(name), 'utf8');
      return JSON.parse(raw);
    } catch (error) {
      if (error.code === 'ENOENT') return null;
      throw error;
    }
  }

  async deleteMeta(name) {
    await fs.rm(this.metaPathFor(name), { force: true });
  }

  /** Lista os metadados de todos os arquivos (donos e réplicas) guardados localmente. */
  async listLocalMeta() {
    let files;
    try {
      files = await fs.readdir(this.metaDirectory);
    } catch (error) {
      if (error.code === 'ENOENT') return [];
      throw error;
    }
    const entries = [];
    for (const file of files) {
      if (!file.endsWith('.json')) continue;
      const name = file.slice(0, -'.json'.length);
      const meta = await this.readMeta(name);
      if (meta) entries.push({ name, ...meta });
    }
    return entries;
  }

  assertJoined() {
    if (!this.joined) throw new Error('O nó ainda não entrou em uma rede');
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

  async state() {
    const files = await this.listLocalMeta().catch(() => []);
    return {
      node: this.reference,
      joined: this.joined,
      predecessor: this.predecessor,
      successor: this.successor,
      fingerTable: this.fingers,
      replicationFactor: this.replicationFactor,
      files: files
        .map(({ name, role, ownerId, checksum, updatedAt }) => (
          { name, role, ownerId, checksum, updatedAt }))
        .sort((a, b) => a.name.localeCompare(b.name, 'pt-BR'))
    };
  }
}

function checksumOf(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

function validateFileName(fileName) {
  if (typeof fileName !== 'string' || !fileName.trim()) {
    throw new Error('O nome do arquivo é obrigatório');
  }
  const name = fileName.trim();
  if (name === '.' || name === '..' || path.basename(name) !== name
    || name.includes('/') || name.includes('\\') || name.includes('\0')) {
    throw new Error('Nome de arquivo inválido');
  }
  return name;
}

function normalizeReference(node) {
  if (!node || typeof node !== 'object') throw new Error('Referência de nó inválida');
  return {
    id: validateId(node.id),
    host: String(node.host || '127.0.0.1'),
    port: Number(node.port || 5000)
  };
}

module.exports = { ChordNode, normalizeReference, validateFileName, CATALOG_NAME };
