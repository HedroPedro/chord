'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { add, inInterval } = require('../src/ring');
const { ChordNode } = require('../src/chord-node');

test('aritmética circular usa ids públicos de 1 a 32', () => {
  assert.equal(add(31, 1), 32);
  assert.equal(add(32, 1), 1);
  assert.equal(add(30, 4), 2);
});

test('intervalos circulares atravessam o fim do anel', () => {
  assert.equal(inInterval(32, 30, 3, false, true), true);
  assert.equal(inInterval(2, 30, 3, false, true), true);
  assert.equal(inInterval(20, 30, 3, false, true), false);
});

test('primeiro nó cria anel e preenche cinco fingers', async () => {
  const node = new ChordNode({ id: 8 });
  await node.join(null);
  assert.equal(node.fingers.length, 5);
  assert.equal(node.predecessor.id, 8);
  assert.ok(node.fingers.every((finger) => finger.node.id === 8));
  assert.deepEqual(node.fingers.map((finger) => finger.start), [9, 10, 12, 16, 24]);
});

test('cada nó aceita uma porta própria e rejeita portas inválidas', () => {
  assert.equal(new ChordNode({ id: 2, port: 5001 }).port, 5001);
  assert.throws(() => new ChordNode({ id: 2, port: 70000 }), /porta/);
});
