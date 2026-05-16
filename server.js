/**
 * Zyvon Server v1.0.0
 * Free, open-source IoT dashboard server (Blynk alternative)
 * WebSocket paths:
 *   /device    → ESP32/ESP8266 device connections
 *   /dashboard → Web dashboard connections
 */

const express = require('express');
const http    = require('http');
const WebSocket = require('ws');
const path    = require('path');
const crypto  = require('crypto');

const app    = express();
const server = http.createServer(app);

const wssDevice    = new WebSocket.Server({ noServer: true });
const wssDashboard = new WebSocket.Server({ noServer: true });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── In-memory state ────────────────────────────────────────────────────────
// devices Map: token → { id, token, name, ws, pins:{}, online, lastSeen }
const devices          = new Map();
const dashboardClients = new Set();

// ─── Helpers ────────────────────────────────────────────────────────────────
function getDeviceList() {
  return Array.from(devices.values()).map(d => ({
    id: d.id, token: d.token, name: d.name,
    online: d.online, lastSeen: d.lastSeen, pins: d.pins
  }));
}

function findDeviceById(id) {
  for (const d of devices.values()) if (d.id === id) return d;
  return null;
}

function broadcastToDashboards(msg) {
  const str = JSON.stringify(msg);
  for (const ws of dashboardClients)
    if (ws.readyState === WebSocket.OPEN) ws.send(str);
}

function broadcastDeviceList() {
  broadcastToDashboards({ type: 'devices', devices: getDeviceList() });
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify(obj));
}

// ─── WebSocket Upgrade Routing ───────────────────────────────────────────────
server.on('upgrade', (req, socket, head) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (url.pathname === '/device') {
      wssDevice.handleUpgrade(req, socket, head,
        ws => wssDevice.emit('connection', ws, req));
    } else if (url.pathname === '/dashboard') {
      wssDashboard.handleUpgrade(req, socket, head,
        ws => wssDashboard.emit('connection', ws, req));
    } else {
      socket.destroy();
    }
  } catch { socket.destroy(); }
});

// ─── Device Connections ──────────────────────────────────────────────────────
wssDevice.on('connection', (ws) => {
  let token = null;
  let pingTimer = null;
  const authTimeout = setTimeout(() => { if (!token) ws.close(); }, 15000);

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === 'auth') {
        clearTimeout(authTimeout);
        token = String(msg.token).toUpperCase().trim();
        const name = msg.name || `Device_${token.substring(0, 6)}`;

        if (!devices.has(token)) {
          devices.set(token, {
            id: crypto.randomUUID(), token, name,
            ws, pins: {}, online: true,
            lastSeen: new Date().toISOString()
          });
        } else {
          const d = devices.get(token);
          d.ws = ws; d.online = true;
          d.lastSeen = new Date().toISOString();
        }

        const d = devices.get(token);
        send(ws, { type: 'auth', status: 'ok', id: d.id });

        // Restore pin state to device
        for (const [pin, value] of Object.entries(d.pins))
          send(ws, { type: 'vw', pin: Number(pin), value });

        broadcastDeviceList();

        // Keepalive ping every 25s
        pingTimer = setInterval(() => send(ws, { type: 'ping' }), 25000);

      } else if (msg.type === 'vw' && token) {
        const d = devices.get(token);
        if (!d) return;
        d.pins[msg.pin] = String(msg.value);
        d.lastSeen = new Date().toISOString();
        broadcastToDashboards({ type: 'vw', deviceId: d.id, pin: msg.pin, value: String(msg.value) });

      } else if (msg.type === 'pong' && token) {
        if (devices.has(token)) devices.get(token).lastSeen = new Date().toISOString();
      }
    } catch { /* ignore malformed */ }
  });

  ws.on('close', () => {
    clearInterval(pingTimer);
    if (token && devices.has(token)) {
      const d = devices.get(token);
      d.online = false; d.ws = null;
      broadcastDeviceList();
    }
  });

  ws.on('error', () => ws.terminate());
});

// ─── Dashboard Connections ───────────────────────────────────────────────────
wssDashboard.on('connection', (ws) => {
  dashboardClients.add(ws);
  send(ws, { type: 'devices', devices: getDeviceList() });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === 'vw') {
        // Dashboard → device pin write
        const d = findDeviceById(msg.deviceId);
        if (!d) return;
        d.pins[msg.pin] = String(msg.value);
        if (d.online && d.ws) send(d.ws, { type: 'vw', pin: msg.pin, value: msg.value });
        // Echo to all dashboards (stay in sync)
        broadcastToDashboards({ type: 'vw', deviceId: msg.deviceId, pin: msg.pin, value: String(msg.value) });

      } else if (msg.type === 'requestPins') {
        const d = findDeviceById(msg.deviceId);
        if (d) send(ws, { type: 'pinState', deviceId: d.id, pins: d.pins });
      }
    } catch { }
  });

  ws.on('close', () => dashboardClients.delete(ws));
  ws.on('error', () => ws.terminate());
});

// ─── REST API ────────────────────────────────────────────────────────────────
// List all devices
app.get('/api/devices', (_req, res) => res.json(getDeviceList()));

// Create a new device token
app.post('/api/device/create', (req, res) => {
  const token = crypto.randomBytes(8).toString('hex').toUpperCase();
  const name  = req.body?.name || `Device_${token.substring(0, 6)}`;
  devices.set(token, { id: crypto.randomUUID(), token, name, ws: null, pins: {}, online: false, lastSeen: null });
  broadcastDeviceList();
  res.json({ success: true, token, name });
});

// Delete a device
app.delete('/api/device/:token', (req, res) => {
  const token = req.params.token.toUpperCase();
  if (!devices.has(token)) return res.status(404).json({ error: 'Device not found' });
  const d = devices.get(token);
  if (d.ws) d.ws.close();
  devices.delete(token);
  broadcastDeviceList();
  res.json({ success: true });
});

// Write a pin via REST (webhook/automation)
app.post('/api/device/:token/pin/:pin', (req, res) => {
  const token = req.params.token.toUpperCase();
  const pin   = Number(req.params.pin);
  const value = String(req.body?.value ?? 0);
  if (!devices.has(token)) return res.status(404).json({ error: 'Not found' });
  const d = devices.get(token);
  d.pins[pin] = value;
  if (d.online && d.ws) send(d.ws, { type: 'vw', pin, value });
  broadcastToDashboards({ type: 'vw', deviceId: d.id, pin, value });
  res.json({ success: true });
});

// ─── Start ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Zyvon server on port ${PORT}`));
