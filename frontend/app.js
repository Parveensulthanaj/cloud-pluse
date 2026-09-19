/**
 * CloudPulse SRE Dashboard Application Logic
 * Consumes real-time Server-Sent Events (SSE), updates DOM elements,
 * and manages interactive Chaos Fault Injection.
 */

let eventSource = null;
let activeIncidentsCache = [];
let remediationActionsCache = [];

function initSSE() {
  const statusEl = document.getElementById('sse-status');

  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource('/api/stream');

  eventSource.onopen = () => {
    statusEl.textContent = 'STREAM CONNECTED';
    statusEl.parentElement.classList.remove('disconnected');
    statusEl.parentElement.classList.add('live-pulse');
  };

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      renderDashboard(data);
    } catch (err) {
      console.error('Error parsing SSE payload:', err);
    }
  };

  eventSource.onerror = (err) => {
    statusEl.textContent = 'RECONNECTING...';
    statusEl.parentElement.classList.remove('live-pulse');
    console.warn('SSE stream interrupted, browser will auto-retry...', err);
  };
}

function renderDashboard(data) {
  renderKPIs(data);
  renderTopology(data.nodes || []);
  renderIncidents(data.active_incidents || []);
  renderRemediations(data.recent_actions || []);
  renderChaosBanner(data.chaos || {});
}

function renderKPIs(data) {
  const busStats = data.bus_stats || { metrics: {} };
  const nodes = data.nodes || [];
  const incidents = data.active_incidents || [];
  const actions = data.recent_actions || [];

  // Total Bus Events
  const totalPublished = busStats.metrics ? busStats.metrics.published || 0 : 0;
  document.getElementById('kpi-events-published').textContent = totalPublished.toLocaleString();

  // Dead Letter Queue
  const dlqSize = busStats.dlq_size || 0;
  document.getElementById('kpi-dlq-count').textContent = dlqSize;

  // Active Incidents
  const incEl = document.getElementById('kpi-active-incidents');
  incEl.textContent = incidents.length;
  document.getElementById('kpi-incidents-sub').textContent =
    incidents.length === 0 ? '0 Pending Anomaly' : `${incidents.length} Critical Issue(s)`;

  // System Health %
  const healthyCount = nodes.filter(n => n.status === 'HEALTHY').length;
  const totalNodes = nodes.length || 1;
  const healthPercent = Math.round((healthyCount / totalNodes) * 100);
  const healthEl = document.getElementById('kpi-system-health');
  healthEl.textContent = `${healthPercent}%`;
  healthEl.className = `kpi-value ${healthPercent === 100 ? 'text-emerald' : healthPercent > 70 ? 'text-amber' : 'text-rose'}`;
  document.getElementById('kpi-health-sub').textContent =
    healthPercent === 100 ? 'All microservices green' : `${nodes.length - healthyCount} degraded/healing`;

  // Remediations
  document.getElementById('kpi-remediations').textContent = actions.length;
}

function renderTopology(nodes) {
  const container = document.getElementById('services-container');
  document.getElementById('topology-count').textContent = `${nodes.length} Nodes Monitored`;

  if (!nodes || nodes.length === 0) return;

  container.innerHTML = nodes.map(node => {
    const latColor = node.current_latency_ms > 120 ? 'text-rose' : node.current_latency_ms > 60 ? 'text-amber' : 'text-emerald';
    const errColor = node.current_error_rate > 5.0 ? 'text-rose' : 'text-muted';
    const cpuColor = node.current_cpu > 80 ? 'text-rose' : node.current_cpu > 60 ? 'text-amber' : 'text-cyan';

    return `
      <div class="service-card state-${node.status}">
        <div class="card-top">
          <div>
            <div class="service-title">${escapeHtml(node.name)}</div>
            <div class="service-sub">${escapeHtml(node.service_id)} • ${node.tier}</div>
          </div>
          <span class="service-status-pill pill-${node.status}">
            ${node.status}
          </span>
        </div>

        <div class="metrics-row">
          <div class="metric-col">
            <span class="metric-label">P95 LATENCY</span>
            <span class="metric-val ${latColor}">${node.current_latency_ms} ms</span>
          </div>
          <div class="metric-col">
            <span class="metric-label">ERROR RATE</span>
            <span class="metric-val ${errColor}">${node.current_error_rate}%</span>
          </div>
          <div class="metric-col">
            <span class="metric-label">CPU LOAD</span>
            <span class="metric-val ${cpuColor}">${node.current_cpu}%</span>
          </div>
        </div>

        <div class="node-badges">
          <span class="badge-tag">Replicas: <strong>${node.replicas}</strong></span>
          ${node.circuit_breaker_open ? '<span class="badge-tag text-rose">CIRCUIT OPEN (50% SHED)</span>' : '<span class="badge-tag">Circuit Closed</span>'}
        </div>
      </div>
    `;
  }).join('');
}

function renderIncidents(incidents) {
  const container = document.getElementById('incidents-container');
  const countEl = document.getElementById('active-incident-count');
  countEl.textContent = `${incidents.length} Open`;

  if (incidents.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">✓</div>
        <div class="empty-title">All Systems Nominal</div>
        <div class="empty-sub">Dynamic Z-score & EWMA baseline filters show no statistical anomalies.</div>
      </div>
    `;
    return;
  }

  container.innerHTML = incidents.map(inc => {
    const timeAgo = Math.max(0, Math.round((Date.now() / 1000) - inc.detected_at));
    return `
      <div class="incident-item">
        <div class="incident-top">
          <strong class="incident-title">${escapeHtml(inc.title)}</strong>
          <span class="badge ${inc.severity === 'CRITICAL' ? 'badge-danger' : 'badge-warning'}">
            ${inc.severity} (${timeAgo}s ago)
          </span>
        </div>
        <div class="incident-root">
          <strong>Root Cause Correlation:</strong> ${escapeHtml(inc.root_cause)}
        </div>
        <div class="node-badges">
          <span class="badge-tag">Target: ${inc.service_id}</span>
          <span class="badge-tag">Trigger: ${inc.trigger_metric} = ${inc.trigger_value}</span>
          <span class="badge-tag text-violet">${inc.status}</span>
        </div>
      </div>
    `;
  }).join('');
}

function renderRemediations(actions) {
  const container = document.getElementById('remediations-container');
  if (!actions || actions.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <div class="empty-title">Autonomous Controller Standing By</div>
        <div class="empty-sub">Policies configured: HPA Auto-Scaling, Circuit Breaker Tripping, Container Restarts.</div>
      </div>
    `;
    return;
  }

  container.innerHTML = actions.map(act => {
    const timeAgo = Math.max(0, Math.round((Date.now() / 1000) - act.executed_at));
    return `
      <div class="remediation-item">
        <div class="remediation-type">
          <span>⚡ ${act.action_type} &rarr; ${act.service_id}</span>
          <span class="text-dim">${timeAgo}s ago</span>
        </div>
        <div class="remediation-text">
          ${escapeHtml(act.details)}
        </div>
      </div>
    `;
  }).join('');
}

function renderChaosBanner(chaos) {
  const banner = document.getElementById('chaos-status-bar');
  const nameEl = document.getElementById('chaos-active-name');
  const keys = Object.keys(chaos);

  if (keys.length > 0) {
    const service = keys[0];
    const details = chaos[service];
    nameEl.textContent = `${details.fault_type} on ${service}`;
    banner.classList.remove('hidden');
  } else {
    banner.classList.add('hidden');
  }
}

async function triggerChaos(targetService, faultType) {
  try {
    const resp = await fetch('/api/chaos/inject', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        target_service: targetService,
        fault_type: faultType,
        duration_seconds: 20,
        intensity: 1.0,
      }),
    });
    const result = await resp.json();
    console.log('Injected chaos fault:', result);
  } catch (err) {
    console.error('Failed to trigger chaos fault:', err);
  }
}

async function clearChaos() {
  try {
    await fetch('/api/chaos/clear', { method: 'POST' });
    console.log('Cleared all chaos faults.');
  } catch (err) {
    console.error('Failed to clear chaos:', err);
  }
}

function escapeHtml(str) {
  if (!str) return '';
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

// Initialize on page load
window.addEventListener('DOMContentLoaded', () => {
  initSSE();
});
