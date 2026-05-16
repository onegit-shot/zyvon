/**
 * Zyvon Server v2.0.0
 * WebSocket paths:
 *   /device    → ESP32/ESP8266 connections  (authenticated by device token)
 *   /dashboard → Web dashboard connections  (authenticated by session token)
 *
 * Environment variables (set in Render dashboard):
 *   DASHBOARD_PASSWORD  → your chosen login password (required)
 *   PORT                → set automatically by Render
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

// ── Auth ──────────────────────────────────────────────────
const DASHBOARD_PASSWORD = process.env.DASHBOARD_PASSWORD || 'zyvon123';
const sessions = new Set();

function makeToken() { const t = crypto.randomBytes(24).toString('hex'); sessions.add(t); return t; }
function validSession(t) { return typeof t === 'string' && sessions.has(t); }

function requireAuth(req, res, next) {
  const a = req.headers['authorization'] || '';
  const t = a.startsWith('Bearer ') ? a.slice(7) : null;
  if (!validSession(t)) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

app.post('/api/login', (req, res) => {
  const { password } = req.body || {};
  if (!password || password !== DASHBOARD_PASSWORD)
    return res.status(401).json({ error: 'Wrong password' });
  res.json({ token: makeToken() });
});

app.use(express.static(path.join(__dirname, 'public')));

// ── State ─────────────────────────────────────────────────
const devices          = new Map();
const dashboardClients = new Set();

function getDeviceList() {
  return Array.from(devices.values()).map(d => ({
    id: d.id, token: d.token, name: d.name,
    online: d.online, lastSeen: d.lastSeen, pins: d.pins
  }));
}
function findDeviceById(id) { for (const d of devices.values()) if (d.id === id) return d; return null; }
function broadcastToDashboards(msg) {
  const s = JSON.stringify(msg);
  for (const ws of dashboardClients) if (ws.readyState === WebSocket.OPEN) ws.send(s);
}
function broadcastDeviceList() { broadcastToDashboards({ type: 'devices', devices: getDeviceList() }); }
function send(ws, obj) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }

// ── WebSocket upgrade routing ─────────────────────────────
server.on('upgrade', (req, socket, head) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (url.pathname === '/device')
      wssDevice.handleUpgrade(req, socket, head, ws => wssDevice.emit('connection', ws, req));
    else if (url.pathname === '/dashboard')
      wssDashboard.handleUpgrade(req, socket, head, ws => wssDashboard.emit('connection', ws, req));
    else socket.destroy();
  } catch { socket.destroy(); }
});

// ── Device WebSocket ──────────────────────────────────────
wssDevice.on('connection', (ws) => {
  let token = null, pingTimer = null;
  const authTimeout = setTimeout(() => { if (!token) ws.close(); }, 15000);

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'auth') {
        clearTimeout(authTimeout);
        token = String(msg.token).toUpperCase().trim();
        const name = msg.name || `Device_${token.substring(0, 6)}`;
        if (!devices.has(token))
          devices.set(token, { id: crypto.randomUUID(), token, name, ws, pins: {}, online: true, lastSeen: new Date().toISOString() });
        else { const d = devices.get(token); d.ws = ws; d.online = true; d.lastSeen = new Date().toISOString(); }
        const d = devices.get(token);
        send(ws, { type: 'auth', status: 'ok', id: d.id });
        for (const [pin, value] of Object.entries(d.pins)) send(ws, { type: 'vw', pin: Number(pin), value });
        broadcastDeviceList();
        pingTimer = setInterval(() => send(ws, { type: 'ping' }), 25000);
      } else if (msg.type === 'vw' && token) {
        const d = devices.get(token); if (!d) return;
        d.pins[msg.pin] = String(msg.value); d.lastSeen = new Date().toISOString();
        broadcastToDashboards({ type: 'vw', deviceId: d.id, pin: msg.pin, value: String(msg.value) });
      } else if (msg.type === 'pong' && token) {
        if (devices.has(token)) devices.get(token).lastSeen = new Date().toISOString();
      }
    } catch {}
  });

  ws.on('close', () => {
    clearInterval(pingTimer);
    if (token && devices.has(token)) { const d = devices.get(token); d.online = false; d.ws = null; broadcastDeviceList(); }
  });
  ws.on('error', () => ws.terminate());
});

// ── Dashboard WebSocket (requires session token) ──────────
wssDashboard.on('connection', (ws) => {
  let authed = false;
  const authTimeout = setTimeout(() => { if (!authed) ws.close(1008, 'Auth required'); }, 8000);

  ws.once('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'dashAuth' && validSession(msg.token)) {
        clearTimeout(authTimeout);
        authed = true;
        dashboardClients.add(ws);
        send(ws, { type: 'authOk' });
        send(ws, { type: 'devices', devices: getDeviceList() });

        ws.on('message', (raw2) => {
          try {
            const m = JSON.parse(raw2.toString());
            if (m.type === 'vw') {
              const d = findDeviceById(m.deviceId); if (!d) return;
              d.pins[m.pin] = String(m.value);
              if (d.online && d.ws) send(d.ws, { type: 'vw', pin: m.pin, value: m.value });
              broadcastToDashboards({ type: 'vw', deviceId: m.deviceId, pin: m.pin, value: String(m.value) });
            } else if (m.type === 'requestPins') {
              const d = findDeviceById(m.deviceId);
              if (d) send(ws, { type: 'pinState', deviceId: d.id, pins: d.pins });
            }
          } catch {}
        });
        ws.on('close', () => dashboardClients.delete(ws));
        ws.on('error', () => ws.terminate());
      } else {
        ws.close(1008, 'Invalid token');
      }
    } catch { ws.close(1008, 'Bad auth'); }
  });
});

// ── Protected REST API ────────────────────────────────────
app.get('/api/devices', requireAuth, (_req, res) => res.json(getDeviceList()));

app.post('/api/device/create', requireAuth, (req, res) => {
  const token = crypto.randomBytes(8).toString('hex').toUpperCase();
  const name  = req.body?.name || `Device_${token.substring(0, 6)}`;
  devices.set(token, { id: crypto.randomUUID(), token, name, ws: null, pins: {}, online: false, lastSeen: null });
  broadcastDeviceList();
  res.json({ success: true, token, name });
});

app.delete('/api/device/:token', requireAuth, (req, res) => {
  const token = req.params.token.toUpperCase();
  if (!devices.has(token)) return res.status(404).json({ error: 'Not found' });
  const d = devices.get(token);
  if (d.ws) d.ws.close();
  devices.delete(token);
  broadcastDeviceList();
  res.json({ success: true });
});

app.post('/api/device/:token/pin/:pin', requireAuth, (req, res) => {
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

// ── Start ─────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Zyvon server on port ${PORT}`);
  if (!process.env.DASHBOARD_PASSWORD)
    console.warn('⚠️  DASHBOARD_PASSWORD not set — using default "zyvon123". Set it in Render!');
});
