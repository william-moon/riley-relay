// Riley relay — public "front desk" on Render. Free, no ngrok, no MyCloudAlly.
// Twilio talks to this over HTTPS/WSS; Riley (on the always-on box) dials OUT to /agent and
// holds it open. Per call, this relay asks the box to open a fresh /link socket, then pipes
// the Twilio <-> box audio frames verbatim. All traffic rides 443 (the only port the box can egress).
//
// Env: PORT (Render-provided), RILEY_RELAY_TOKEN (shared secret; box must present it).
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.RILEY_RELAY_TOKEN || '';

let agentSock = null;                 // the box's persistent control socket
const pendingLinks = new Map();       // linkId -> Twilio ws waiting for the box to connect back

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, agent: !!agentSock, pending: pendingLinks.size }));
  }
  if (u.pathname === '/twiml') {
    // Twilio fetches this on inbound/outbound answer -> connect a Media Stream back to us.
    const host = req.headers['x-forwarded-host'] || req.headers.host;
    const call = u.searchParams.get('call') || '';
    res.writeHead(200, { 'content-type': 'text/xml' });
    return res.end(`<?xml version="1.0" encoding="UTF-8"?>
<Response><Connect><Stream url="wss://${host}/media?call=${call}"/></Connect></Response>`);
  }
  res.writeHead(404); res.end('riley-relay');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, 'http://x');
  // /agent and /link are the box side — require the shared token. /media is Twilio (no token).
  if (u.pathname === '/agent' || u.pathname === '/link') {
    if (!TOKEN || u.searchParams.get('token') !== TOKEN) { socket.destroy(); return; }
  } else if (u.pathname !== '/media') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => handle(ws, u));
});

function handle(ws, u) {
  if (u.pathname === '/agent') {
    agentSock = ws;
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('close', () => { if (agentSock === ws) agentSock = null; });
    ws.on('message', () => {});        // control channel: box may send heartbeats
    return;
  }
  if (u.pathname === '/link') {
    const linkId = u.searchParams.get('link');
    const twilioWs = pendingLinks.get(linkId);
    pendingLinks.delete(linkId);
    if (!twilioWs || twilioWs.readyState !== twilioWs.OPEN) { ws.close(); return; }
    pipe(twilioWs, ws);                 // box <-> Twilio, verbatim
    return;
  }
  if (u.pathname === '/media') {
    // Twilio just connected for a call. Ask the box to dial a matching /link socket.
    if (!agentSock || agentSock.readyState !== agentSock.OPEN) { ws.close(); return; }
    const linkId = crypto.randomBytes(9).toString('hex');
    const call = u.searchParams.get('call') || '';
    // Twilio's "start" (with streamSid) can arrive before the box's /link attaches. Buffer every
    // frame until the pipe is live, then flush in order — so no leading audio/metadata is lost.
    ws._prebuf = [];
    ws._preHandler = (data, isBinary) => ws._prebuf.push([data, isBinary]);
    ws.on('message', ws._preHandler);
    pendingLinks.set(linkId, ws);
    try { agentSock.send(JSON.stringify({ type: 'open', link: linkId, call })); } catch { ws.close(); }
    // if the box never connects back, clean up
    setTimeout(() => {
      if (pendingLinks.get(linkId) === ws) { pendingLinks.delete(linkId); try { ws.close(); } catch {} }
    }, 15000);
    return;
  }
  ws.close();
}

// Verbatim two-way pipe between two websockets; closing one closes the other.
function pipe(a, b) {
  // a is the Twilio side, which may have buffered leading frames while waiting for the box.
  if (a._preHandler) { a.removeListener('message', a._preHandler); a._preHandler = null; }
  if (a._prebuf) {
    for (const [data, isBinary] of a._prebuf) {
      if (b.readyState === b.OPEN) b.send(data, { binary: isBinary });
    }
    a._prebuf = null;
  }
  const fwd = (from, to) => {
    from.on('message', (data, isBinary) => {
      if (to.readyState === to.OPEN) to.send(data, { binary: isBinary });
    });
    from.on('close', () => { try { to.close(); } catch {} });
    from.on('error', () => { try { to.close(); } catch {} });
  };
  fwd(a, b); fwd(b, a);
}

// Keep the box's control socket alive (and keep Render from idling us).
setInterval(() => {
  if (agentSock) {
    if (agentSock.isAlive === false) { try { agentSock.terminate(); } catch {}; agentSock = null; return; }
    agentSock.isAlive = false;
    try { agentSock.ping(); } catch {}
  }
}, 25000);

server.listen(PORT, () => console.log('riley-relay listening on', PORT));
