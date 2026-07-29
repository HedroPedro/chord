'use strict';

const elements = {
  connectionDot: document.querySelector('#connection-dot'),
  connectionLabel: document.querySelector('#connection-label'),
  nodeTitle: document.querySelector('#node-title'),
  nodeAddress: document.querySelector('#node-address'),
  currentId: document.querySelector('#current-id'),
  joinedLabel: document.querySelector('#joined-label'),
  predecessorId: document.querySelector('#predecessor-id'),
  predecessorAddress: document.querySelector('#predecessor-address'),
  successorId: document.querySelector('#successor-id'),
  successorAddress: document.querySelector('#successor-address'),
  fingerBody: document.querySelector('#finger-body'),
  ring: document.querySelector('#ring'),
  lastUpdate: document.querySelector('#last-update'),
  joinPanel: document.querySelector('#join-panel'),
  joinForm: document.querySelector('#join-form'),
  joinButton: document.querySelector('#join-button'),
  joinMessage: document.querySelector('#join-message'),
  bootstrapFields: document.querySelector('#bootstrap-fields'),
  globalError: document.querySelector('#global-error'),
  refreshButton: document.querySelector('#refresh-button')
};

let refreshing = false;

function address(node) {
  return node ? `${node.host}:${node.port}` : 'Não definido';
}

function renderNode(targetId, targetAddress, node) {
  targetId.textContent = node?.id ?? '—';
  targetAddress.textContent = address(node);
}

function renderRing(state) {
  const routeIds = new Set(state.fingerTable.flatMap((finger) =>
    finger.node ? [finger.node.id] : []));
  if (state.predecessor) routeIds.add(state.predecessor.id);
  if (state.successor) routeIds.add(state.successor.id);

  elements.ring.replaceChildren(...Array.from({ length: 32 }, (_, index) => {
    const id = index + 1;
    const angle = ((id - 1) / 32) * 360 - 90;
    const marker = document.createElement('span');
    marker.className = `ring-id${routeIds.has(id) ? ' route' : ''}${id === state.node.id ? ' self' : ''}`;
    marker.textContent = id;
    marker.title = id === state.node.id ? `Nó atual: ${id}` : `ID ${id}`;
    marker.style.transform = `rotate(${angle}deg) translateX(var(--ring-radius)) rotate(${-angle}deg)`;
    return marker;
  }));
}

function renderTable(fingers) {
  elements.fingerBody.replaceChildren(...fingers.map((finger) => {
    const row = document.createElement('tr');
    const values = [
      finger.index,
      finger.start,
      finger.node?.id ?? '—',
      address(finger.node)
    ];
    values.forEach((value, index) => {
      const cell = document.createElement('td');
      if (index === 2 && finger.node) {
        const badge = document.createElement('span');
        badge.className = 'node-badge';
        badge.textContent = value;
        cell.append(badge);
      } else {
        cell.textContent = value;
      }
      row.append(cell);
    });
    return row;
  }));
}

function render(state) {
  elements.nodeTitle.textContent = state.node.id;
  elements.currentId.textContent = state.node.id;
  elements.nodeAddress.textContent = `http://${address(state.node)}`;
  elements.joinedLabel.textContent = state.joined ? 'No anel' : 'Fora do anel';
  elements.joinedLabel.classList.toggle('active', state.joined);
  elements.joinPanel.hidden = state.joined;
  renderNode(elements.predecessorId, elements.predecessorAddress, state.predecessor);
  renderNode(elements.successorId, elements.successorAddress, state.successor);
  renderTable(state.fingerTable);
  renderRing(state);
  elements.lastUpdate.textContent = `Atualizado às ${new Date().toLocaleTimeString('pt-BR')}`;
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try {
    const response = await fetch('/api/state', { cache: 'no-store' });
    if (!response.ok) throw new Error(`Falha HTTP ${response.status}`);
    render(await response.json());
    elements.connectionDot.classList.add('online');
    elements.connectionLabel.textContent = 'Nó acessível';
    elements.globalError.hidden = true;
  } catch (error) {
    elements.connectionDot.classList.remove('online');
    elements.connectionLabel.textContent = 'Sem conexão';
    elements.globalError.textContent = `Não foi possível consultar o nó: ${error.message}`;
    elements.globalError.hidden = false;
  } finally {
    refreshing = false;
  }
}

document.querySelectorAll('input[name="mode"]').forEach((input) => {
  input.addEventListener('change', () => {
    const existing = input.value === 'existing' && input.checked;
    if (!input.checked) return;
    elements.bootstrapFields.hidden = !existing;
    elements.joinButton.textContent = existing ? 'Entrar no anel' : 'Criar anel';
  });
});

elements.joinForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  elements.joinButton.disabled = true;
  elements.joinMessage.className = 'form-message';
  elements.joinMessage.textContent = 'Conectando…';
  const existing = document.querySelector('input[name="mode"]:checked').value === 'existing';
  const bootstrap = existing ? {
    id: Number(document.querySelector('#bootstrap-id').value),
    host: document.querySelector('#bootstrap-host').value.trim(),
    port: Number(document.querySelector('#bootstrap-port').value)
  } : null;

  try {
    const response = await fetch('/join', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ bootstrap })
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'Não foi possível entrar no anel');
    elements.joinMessage.textContent = 'Nó conectado com sucesso.';
    render(result);
  } catch (error) {
    elements.joinMessage.className = 'form-message error';
    elements.joinMessage.textContent = error.message;
  } finally {
    elements.joinButton.disabled = false;
  }
});

elements.refreshButton.addEventListener('click', refresh);
refresh();
setInterval(refresh, 5000);
