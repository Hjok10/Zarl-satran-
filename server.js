// Zarli Satranc - kendi sunucun. Bagimlilik yok, sadece Node 16+ gerekir.
// Hem oyunu (index.html) sunar hem de oyunun konustugu kucuk MQTT-WebSocket aracisini (/mqtt) calistirir.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const GAME = path.join(__dirname, 'index.html');

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    fs.readFile(GAME, (e, d) => {
      if (e) { res.writeHead(500); res.end('index.html bulunamadi'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(d);
    });
  } else if (u === '/saglik') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end('yok'); }
});

// ---- WebSocket ----
const clients = new Set();
const retained = new Map();
server.on('upgrade', (req, socket) => {
  if (req.url.split('?')[0] !== '/mqtt' || !req.headers['sec-websocket-key']) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  const proto = String(req.headers['sec-websocket-protocol'] || '').split(',')[0].trim();
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + (proto ? '\r\nSec-WebSocket-Protocol: ' + proto : '') + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, subs: new Set(), will: null, clean: false, wsbuf: Buffer.alloc(0), mqbuf: Buffer.alloc(0), frag: [], dead: false };
  clients.add(c);
  socket.on('data', d => { c.wsbuf = Buffer.concat([c.wsbuf, d]); if (c.wsbuf.length > 1 << 20) return end(c); try { wsParse(c); } catch (e) { end(c); } });
  socket.on('close', () => end(c));
  socket.on('error', () => end(c));
});

function wsSend(c, payload, op = 2) {
  if (c.dead) return;
  let h; const n = payload.length;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) h = Buffer.from([0x80 | op, 126, n >> 8, n & 255]);
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(Math.floor(n / 4294967296), 2); h.writeUInt32BE(n % 4294967296, 6); }
  try { c.socket.write(Buffer.concat([h, payload])); } catch (e) {}
}
function wsParse(c) {
  for (;;) {
    const b = c.wsbuf; if (b.length < 2) return;
    const fin = b[0] & 0x80, op = b[0] & 15, masked = b[1] & 0x80; let len = b[1] & 127, p = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); p = 4; }
    else if (len === 127) { if (b.length < 10) return; len = b.readUInt32BE(2) * 4294967296 + b.readUInt32BE(6); p = 10; }
    if (len > 1 << 20) return end(c);
    const need = p + (masked ? 4 : 0) + len; if (b.length < need) return;
    let data = b.slice(p + (masked ? 4 : 0), need);
    if (masked) { const m = b.slice(p, p + 4); data = Buffer.from(data); for (let i = 0; i < data.length; i++) data[i] ^= m[i & 3]; }
    c.wsbuf = b.slice(need);
    if (op === 8) { wsSend(c, Buffer.alloc(0), 8); return end(c); }
    if (op === 9) { wsSend(c, data, 10); continue; }
    if (op === 10) continue;
    c.frag.push(data);
    if (fin) { const msg = Buffer.concat(c.frag); c.frag = []; mqttFeed(c, msg); }
  }
}

// ---- MQTT 3.1.1 (CONNECT, SUBSCRIBE, PUBLISH, PINGREQ, DISCONNECT, will, retained; QoS 0) ----
const rstr = (b, i) => { const n = (b[i] << 8) | b[i + 1]; return [b.slice(i + 2, i + 2 + n).toString('utf8'), i + 2 + n]; };
function enc(type, flags, body) {
  let n = body.length; const lb = [];
  do { let d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 128; lb.push(d); } while (n > 0);
  return Buffer.concat([Buffer.from([(type << 4) | flags]), Buffer.from(lb), body]);
}
function pubPkt(topic, payload, retain) {
  const tb = Buffer.from(topic);
  return enc(3, retain ? 1 : 0, Buffer.concat([Buffer.from([tb.length >> 8, tb.length & 255]), tb, payload]));
}
function deliver(topic, payload, except) {
  const pkt = pubPkt(topic, payload, false);
  for (const c of clients) if (c !== except && c.subs.has(topic)) wsSend(c, pkt);
}
function mqttFeed(c, data) {
  c.mqbuf = Buffer.concat([c.mqbuf, data]);
  for (;;) {
    const b = c.mqbuf; if (b.length < 2) return;
    let mul = 1, len = 0, i = 1, dg;
    do { if (i >= b.length) return; dg = b[i++]; len += (dg & 127) * mul; mul *= 128; if (mul > 268435456) return end(c); } while (dg & 128);
    if (b.length < i + len) return;
    const body = b.slice(i, i + len), type = b[0] >> 4, flags = b[0] & 15;
    c.mqbuf = b.slice(i + len);
    mqttPacket(c, type, flags, body);
    if (c.dead) return;
  }
}
function mqttPacket(c, type, flags, body) {
  if (type === 1) {
    let p = 0, s; [s, p] = rstr(body, 0); p++; const cf = body[p++]; p += 2; [s, p] = rstr(body, p);
    if (cf & 0x04) { let wt; [wt, p] = rstr(body, p); const wl = (body[p] << 8) | body[p + 1]; p += 2; c.will = { topic: wt, payload: body.slice(p, p + wl), retain: !!(cf & 0x20) }; }
    wsSend(c, Buffer.from([0x20, 2, 0, 0]));
  } else if (type === 8) {
    let p = 2; const codes = [];
    while (p < body.length) {
      let t; [t, p] = rstr(body, p); p++;
      if (c.subs.size >= 50) { codes.push(0x80); continue; }
      c.subs.add(t); codes.push(0);
      const r = retained.get(t); if (r) wsSend(c, pubPkt(t, r, true));
    }
    wsSend(c, enc(9, 0, Buffer.concat([Buffer.from([body[0], body[1]]), Buffer.from(codes)])));
  } else if (type === 10) {
    let p = 2; while (p < body.length) { let t; [t, p] = rstr(body, p); p++; c.subs.delete(t); }
    wsSend(c, Buffer.from([0xB0, 2, body[0], body[1]]));
  } else if (type === 3) {
    const [t, p] = rstr(body, 0); const qos = (flags >> 1) & 3; const off = p + (qos ? 2 : 0); const payload = body.slice(off);
    publish(t, payload, !!(flags & 1), c);
  } else if (type === 12) wsSend(c, Buffer.from([0xD0, 0]));
  else if (type === 14) { c.clean = true; end(c); }
}
function publish(t, payload, retain, from) {
  if (!t || t.length > 200 || payload.length > 1 << 19) return;
  if (retain) {
    if (payload.length) { if (retained.size < 5000) retained.set(t, payload); } else retained.delete(t);
  }
  deliver(t, payload, null);
}
function end(c) {
  if (c.dead) return; c.dead = true; clients.delete(c);
  try { c.socket.destroy(); } catch (e) {}
  if (!c.clean && c.will) publish(c.will.topic, c.will.payload, c.will.retain, c);
}
// eski retained lobi kayitlarini temizle (30 dk)
setInterval(() => {
  for (const [t, v] of retained) {
    try { const j = JSON.parse(v.toString()); const ts = j && j.d && j.d.ts; if (ts && Date.now() - ts > 30 * 60 * 1000) retained.delete(t); } catch (e) {}
  }
}, 60000).unref();

server.listen(PORT, () => console.log('Zarli Satranc sunucusu hazir: http://localhost:' + PORT));
