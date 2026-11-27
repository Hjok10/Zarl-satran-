// Zarli Satranc - kendi sunucun. Bagimlilik yok, Node 14+ yeterli.
// Oyunu (index.html) sunar, hesap/profil API'sini (/api/...) ve oyuncular arasi MQTT-WebSocket yolunu (/mqtt) calistirir.
const http = require('http'), https = require('https'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const GAME = path.join(__dirname, 'index.html');
const ADMIN_USER = process.env.ADMIN_USER || 'Hjok';
const ADMIN_PASS = process.env.ADMIN_PASS || '1234';
const ADMIN_PASS_FROM_ENV = !!process.env.ADMIN_PASS;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data', 'users.json');
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const UP_TOK = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const NAME_RE = /^[A-Za-z0-9_\u00C0-\u024F\u0400-\u04FF\u4E00-\u9FFF]{3,16}$/;
const MAX_GAMES = 30;
const LEVELS = ['easy', 'medium', 'hard', 'expert'];
const STAT_KEYS = LEVELS.concat(['online']);

// ---------------------------------------------------------------- depolama
function httpJson(urlStr, headers, bodyObj) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const mod = u.protocol === 'http:' ? http : https;
    const body = Buffer.from(JSON.stringify(bodyObj));
    const req = mod.request({ method: 'POST', hostname: u.hostname, port: u.port || (u.protocol === 'http:' ? 80 : 443), path: u.pathname + u.search,
      headers: Object.assign({ 'Content-Type': 'application/json', 'Content-Length': body.length }, headers) }, res => {
      const chunks = [];
      res.on('data', d => chunks.push(d));
      res.on('end', () => {
        const txt = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode >= 300) return reject(new Error('HTTP ' + res.statusCode + ' ' + txt.slice(0, 200)));
        try { resolve(JSON.parse(txt)); } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => req.destroy(new Error('timeout')));
    req.end(body);
  });
}
function makeFileStore() {
  let timer = null, getAll = null;
  return {
    name: 'dosya (' + DATA_FILE + ')',
    persistent: false,
    async load() {
      try { const j = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); return Array.isArray(j.users) ? j.users : []; } catch (e) { return []; }
    },
    save(user, all) {
      getAll = all;
      if (timer) return;
      timer = setTimeout(() => {
        timer = null;
        try {
          fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
          const tmp = DATA_FILE + '.tmp';
          fs.writeFileSync(tmp, JSON.stringify({ users: getAll() }));
          fs.renameSync(tmp, DATA_FILE);
        } catch (e) { console.error('Kayit hatasi:', e.message); }
      }, 300);
    },
    async flush(all) {
      if (timer) { clearTimeout(timer); timer = null; }
      try {
        fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
        fs.writeFileSync(DATA_FILE + '.tmp', JSON.stringify({ users: all() }));
        fs.renameSync(DATA_FILE + '.tmp', DATA_FILE);
      } catch (e) {}
    }
  };
}
function makeUpstashStore() {
  const hdr = { Authorization: 'Bearer ' + UP_TOK };
  const cmd = args => httpJson(UP_URL, hdr, args).then(r => { if (r && r.error) throw new Error(r.error); return r.result; });
  const timers = {};
  return {
    name: 'Upstash Redis',
    persistent: true,
    async load() {
      const keys = (await cmd(['KEYS', 'zsu:*'])) || [];
      const out = [];
      for (let i = 0; i < keys.length; i += 40) {
        const part = keys.slice(i, i + 40);
        const res = await httpJson(UP_URL + '/pipeline', hdr, part.map(k => ['GET', k]));
        for (const r of res) { try { if (r && r.result) out.push(JSON.parse(r.result)); } catch (e) {} }
      }
      return out;
    },
    save(user) {
      const k = 'zsu:' + user.lname;
      if (timers[k]) return;
      timers[k] = setTimeout(() => {
        delete timers[k];
        cmd(['SET', k, JSON.stringify(user)]).catch(e => console.error('Upstash kayit hatasi:', e.message));
      }, 1500);
    },
    async flush() {}
  };
}
const store = (UP_URL && UP_TOK) ? makeUpstashStore() : makeFileStore();

// ---------------------------------------------------------------- kullanicilar
const users = new Map();      // lname -> doc
const sessIdx = new Map();    // sha256(token) -> lname
const online = new Map();     // lname -> acik soket sayisi
const allUsers = () => Array.from(users.values());
const persist = u => store.save(u, allUsers);
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
function freshStats() { const r = {}; for (const k of STAT_KEYS) r[k] = [0, 0, 0]; return { r, streak: 0, best: 0 }; }
function scrypt(pass, salt) {
  return new Promise((resolve, reject) => crypto.scrypt(String(pass), salt, 64, (e, k) => e ? reject(e) : resolve(k.toString('hex'))));
}
function safeEq(a, b) {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}
async function makeUser(name, pass, role) {
  const salt = crypto.randomBytes(16).toString('hex');
  return { name, lname: name.toLowerCase(), salt, hash: await scrypt(pass, salt), role: role || 'user', banned: false, created: Date.now(), last: 0,
    stats: freshStats(), games: [], prefs: null, sessions: [] };
}
function indexSessions(u) {
  const now = Date.now();
  u.sessions = (u.sessions || []).filter(s => s && typeof s.h === 'string' && s.exp > now);
  for (const s of u.sessions) sessIdx.set(s.h, u.lname);
}
async function initUsers() {
  const list = await store.load();
  for (const u of list) {
    if (!u || !u.lname || !u.hash) continue;
    if (!u.stats || !u.stats.r) u.stats = freshStats();
    for (const k of STAT_KEYS) if (!Array.isArray(u.stats.r[k])) u.stats.r[k] = [0, 0, 0];
    if (!Array.isArray(u.games)) u.games = [];
    users.set(u.lname, u); indexSessions(u);
  }
  const ln = ADMIN_USER.toLowerCase();
  let a = users.get(ln);
  if (!a) { a = await makeUser(ADMIN_USER, ADMIN_PASS, 'admin'); users.set(ln, a); persist(a); }
  else {
    let ch = false;
    if (a.role !== 'admin' || a.banned) { a.role = 'admin'; a.banned = false; ch = true; }
    if (ADMIN_PASS_FROM_ENV && !safeEq(await scrypt(ADMIN_PASS, a.salt), a.hash)) { a.salt = crypto.randomBytes(16).toString('hex'); a.hash = await scrypt(ADMIN_PASS, a.salt); a.sessions = []; ch = true; }
    if (ch) persist(a);
  }
}
function newSession(u, remember) {
  const token = crypto.randomBytes(32).toString('hex');
  const exp = Date.now() + (remember ? 30 : 1) * 24 * 3600 * 1000;
  u.sessions = (u.sessions || []).filter(s => s.exp > Date.now()).slice(-7);
  u.sessions.push({ h: sha(token), exp });
  sessIdx.set(sha(token), u.lname);
  u.last = Date.now();
  persist(u);
  return token;
}
function userByToken(token) {
  if (!token || typeof token !== 'string' || token.length !== 64) return null;
  const h = sha(token), ln = sessIdx.get(h);
  if (!ln) return null;
  const u = users.get(ln);
  if (!u || u.banned) { sessIdx.delete(h); return null; }
  const s = (u.sessions || []).find(x => x.h === h);
  if (!s || s.exp < Date.now()) { sessIdx.delete(h); return null; }
  return u;
}
function dropSession(token) {
  const h = sha(token), ln = sessIdx.get(h);
  sessIdx.delete(h);
  const u = ln && users.get(ln);
  if (u) { u.sessions = (u.sessions || []).filter(s => s.h !== h); persist(u); }
}
const gameSummary = g => ({ id: g.id, t: g.t, mode: g.mode, level: g.level, opp: g.opp, color: g.color, out: g.out, res: g.res, n: (g.log || []).length });
const publicMe = u => ({ name: u.name, role: u.role, stats: u.stats, prefs: u.prefs, games: u.games.slice().reverse().map(gameSummary) });

// ---------------------------------------------------------------- sinirlama
const hits = new Map();
function limited(key, max, windowMs, peek) {
  const now = Date.now();
  let h = hits.get(key);
  if (!h || h.reset < now) { h = { n: 0, reset: now + windowMs }; hits.set(key, h); }
  if (!peek) h.n++;
  return h.n > max;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (v.reset < now) hits.delete(k); }, 60000).unref();
const clientIp = req => String((req.headers['x-forwarded-for'] || '').split(',')[0] || req.socket.remoteAddress || '').trim();

// ---------------------------------------------------------------- HTTP
function send(res, code, obj, extra) {
  const body = Buffer.from(JSON.stringify(obj));
  res.writeHead(code, Object.assign({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Content-Length': body.length }, extra || {}));
  res.end(body);
}
function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    let n = 0; const chunks = [];
    req.on('data', d => { n += d.length; if (n > limit) { reject(new Error('big')); req.destroy(); } else chunks.push(d); });
    req.on('end', () => { try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); } catch (e) { reject(new Error('json')); } });
    req.on('error', reject);
  });
}
const bearer = req => { const m = /^Bearer ([0-9a-f]{64})$/.exec(req.headers.authorization || ''); return m ? m[1] : ''; };
const num = (x, lo, hi) => Number.isInteger(x) && x >= lo && x <= hi;

function cleanLog(a) {
  if (!Array.isArray(a)) return null;
  const out = [];
  for (const e of a.slice(0, 600)) {
    if (!Array.isArray(e) || e.length !== 11) return null;
    const ok = num(e[0], 0, 1) && num(e[1], 0, 1) && num(e[2], 0, 1) && num(e[3], 0, 7) && num(e[4], 0, 2) && num(e[5], 0, 5) &&
      num(e[6], 0, 63) && num(e[7], 0, 63) && num(e[8], 0, 1) && num(e[9], 0, 1) && num(e[10], 0, 4);
    if (!ok) return null;
    out.push(e.slice());
  }
  return out;
}
function applyOutcome(u, key, out) {
  const r = u.stats.r[key]; if (!r) return;
  if (out === 'win') { r[0]++; u.stats.streak++; if (u.stats.streak > u.stats.best) u.stats.best = u.stats.streak; }
  else if (out === 'lose') { r[1]++; u.stats.streak = 0; }
  else r[2]++;
}
const credErr = (res, code, status) => send(res, status || 400, { error: code });

async function api(req, res, url) {
  const route = req.method + ' ' + url.pathname;
  if (route === 'GET /api/ping') return send(res, 200, { ok: true, auth: true, persistent: store.persistent });

  if (route === 'POST /api/register' || route === 'POST /api/login') {
    const ip = clientIp(req);
    const reg = route === 'POST /api/register';
    if (limited((reg ? 'reg:' : 'login:') + ip, reg ? 12 : 10, reg ? 3600000 : 600000, !reg)) return credErr(res, 'rate', 429);
    let b; try { b = await readBody(req, 4096); } catch (e) { return credErr(res, 'bad'); }
    const name = String(b.name == null ? '' : b.name).trim(), pass = String(b.password == null ? '' : b.password);
    if (reg) {
      if (!NAME_RE.test(name)) return credErr(res, 'name');
      if (pass.length < 6 || pass.length > 64) return credErr(res, 'pass');
      if (users.has(name.toLowerCase())) return credErr(res, 'taken', 409);
      const u = await makeUser(name, pass, 'user');
      users.set(u.lname, u);
      const token = newSession(u, !!b.remember);
      return send(res, 200, { token, me: publicMe(u) });
    }
    const u = users.get(name.toLowerCase());
    const salt = u ? u.salt : '00000000000000000000000000000000';
    const h = await scrypt(pass, salt);
    if (!u || !safeEq(h, u.hash)) { limited('login:' + ip, 10, 600000); return credErr(res, 'creds', 401); }
    if (u.banned) return credErr(res, 'banned', 403);
    hits.delete('login:' + ip);
    const token = newSession(u, !!b.remember);
    return send(res, 200, { token, me: publicMe(u) });
  }

  const token = bearer(req), me = userByToken(token);
  if (!me) return credErr(res, 'auth', 401);

  if (route === 'GET /api/me') return send(res, 200, { me: publicMe(me) });
  if (route === 'POST /api/logout') { dropSession(token); return send(res, 200, { ok: true }); }

  if (route === 'POST /api/prefs') {
    let b; try { b = await readBody(req, 2048); } catch (e) { return credErr(res, 'bad'); }
    const okLang = ['tr', 'en', 'zh'], okB = ['green', 'wood', 'blue', 'slate'], okP = ['classic', 'bold', 'gold'];
    if (!okLang.includes(b.lang) || !okB.includes(b.board) || !okP.includes(b.pieces)) return credErr(res, 'bad');
    me.prefs = { lang: b.lang, board: b.board, pieces: b.pieces };
    persist(me);
    return send(res, 200, { ok: true });
  }
  if (route === 'POST /api/stats/reset') { me.stats = freshStats(); persist(me); return send(res, 200, { stats: me.stats }); }

  if (route === 'POST /api/game') {
    if (limited('game:' + me.lname, 40, 60000)) return credErr(res, 'rate', 429);
    let b; try { b = await readBody(req, 65536); } catch (e) { return credErr(res, 'bad'); }
    const log = cleanLog(b.log);
    const res0 = b.res || {};
    const types = ['mate', 'stalemate', 'material', 'fifty', 'left'];
    if (!/^[0-9a-f]{12}$/.test(String(b.id)) || !['cpu', 'online'].includes(b.mode) || !log || !types.includes(res0.type) ||
        !['win', 'lose', 'draw'].includes(b.out) || !['w', 'b'].includes(b.color) ||
        (b.mode === 'cpu' && !LEVELS.includes(b.level))) return credErr(res, 'bad');
    if (me.games.some(g => g.id === b.id)) return send(res, 200, { stats: me.stats, dup: true });
    const g = { id: b.id, t: Date.now(), mode: b.mode, level: b.mode === 'cpu' ? b.level : '', opp: b.mode === 'online' ? String(b.opp || '').replace(/[\u0000-\u001f]/g, '').slice(0, 16) : '',
      color: b.color, out: b.out, res: { type: res0.type, winner: res0.winner === 'w' ? 'w' : res0.winner === 'b' ? 'b' : '' }, log };
    me.games.push(g);
    if (me.games.length > MAX_GAMES) me.games = me.games.slice(-MAX_GAMES);
    applyOutcome(me, b.mode === 'cpu' ? b.level : 'online', b.out);
    persist(me);
    return send(res, 200, { stats: me.stats });
  }
  const gm = /^GET \/api\/game\/([0-9a-f]{12})$/.exec(route);
  if (gm) {
    const g = me.games.find(x => x.id === gm[1]);
    return g ? send(res, 200, { game: g }) : credErr(res, 'none', 404);
  }

  if (route.indexOf('/api/admin/') >= 0) {
    if (me.role !== 'admin') return credErr(res, 'forbidden', 403);
    if (route === 'GET /api/admin/users') {
      const list = allUsers().map(u => ({ name: u.name, role: u.role, banned: !!u.banned, created: u.created, last: u.last || 0,
        games: STAT_KEYS.reduce((n, k) => n + u.stats.r[k].reduce((a, c) => a + c, 0), 0), online: (online.get(u.lname) || 0) > 0 }));
      list.sort((a, c) => (c.created - a.created));
      return send(res, 200, { users: list, persistent: store.persistent, store: store.name });
    }
    if (route === 'POST /api/admin/ban') {
      let b; try { b = await readBody(req, 1024); } catch (e) { return credErr(res, 'bad'); }
      const u = users.get(String(b.name || '').toLowerCase());
      if (!u) return credErr(res, 'none', 404);
      if (u.role === 'admin') return credErr(res, 'forbidden', 403);
      u.banned = true;
      for (const s of u.sessions || []) sessIdx.delete(s.h);
      u.sessions = [];
      persist(u);
      kick(u.lname);
      return send(res, 200, { ok: true });
    }
    if (route === 'POST /api/admin/reset') {
      let b; try { b = await readBody(req, 1024); } catch (e) { return credErr(res, 'bad'); }
      const u = users.get(String(b.name || '').toLowerCase());
      if (!u) return credErr(res, 'none', 404);
      if (u.role === 'admin') return credErr(res, 'forbidden', 403);
      const al = 'abcdefghjkmnpqrstuvwxyz23456789', rb = crypto.randomBytes(8);
      let temp = ''; for (let i = 0; i < 8; i++) temp += al[rb[i] % al.length];
      u.salt = crypto.randomBytes(16).toString('hex'); u.hash = await scrypt(temp, u.salt);
      for (const s2 of u.sessions || []) sessIdx.delete(s2.h);
      u.sessions = []; persist(u); kick(u.lname);
      return send(res, 200, { password: temp });
    }
    if (route === 'GET /api/admin/backup') {
      const data = JSON.stringify({ v: 1, users: allUsers().map(u => Object.assign({}, u, { sessions: [] })) });
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Content-Disposition': 'attachment; filename="zarli-satranc-yedek.json"' });
      return res.end(data);
    }
    if (route === 'POST /api/admin/restore') {
      let b; try { b = await readBody(req, 8 * 1024 * 1024); } catch (e) { return credErr(res, 'bad'); }
      if (!b || !Array.isArray(b.users)) return credErr(res, 'bad');
      let n = 0;
      for (const u of b.users) {
        if (!u || typeof u.name !== 'string' || !NAME_RE.test(u.name) || typeof u.salt !== 'string' || typeof u.hash !== 'string') continue;
        const ln = u.name.toLowerCase();
        const cur = users.get(ln);
        if (cur && cur.role === 'admin') continue;
        const doc = { name: u.name, lname: ln, salt: u.salt, hash: u.hash, role: 'user', banned: !!u.banned, created: Number(u.created) || Date.now(), last: Number(u.last) || 0,
          stats: freshStats(), games: [], prefs: null, sessions: [] };
        if (u.stats && u.stats.r) { for (const k of STAT_KEYS) { const a = u.stats.r[k]; if (Array.isArray(a)) for (let i = 0; i < 3; i++) doc.stats.r[k][i] = num(a[i], 0, 1e7) ? a[i] : 0; } doc.stats.streak = num(u.stats.streak, 0, 1e7) ? u.stats.streak : 0; doc.stats.best = num(u.stats.best, 0, 1e7) ? u.stats.best : 0; }
        if (Array.isArray(u.games)) for (const g of u.games.slice(-MAX_GAMES)) {
          const log = cleanLog(g && g.log);
          if (log && /^[0-9a-f]{12}$/.test(String(g.id)) && ['win', 'lose', 'draw'].includes(g.out)) doc.games.push({ id: g.id, t: Number(g.t) || 0, mode: g.mode === 'online' ? 'online' : 'cpu', level: LEVELS.includes(g.level) ? g.level : '', opp: String(g.opp || '').slice(0, 16), color: g.color === 'b' ? 'b' : 'w', out: g.out, res: { type: String(g.res && g.res.type || 'mate'), winner: g.res && g.res.winner === 'b' ? 'b' : g.res && g.res.winner === 'w' ? 'w' : '' }, log });
        }
        if (u.prefs && typeof u.prefs === 'object') doc.prefs = { lang: ['tr', 'en', 'zh'].includes(u.prefs.lang) ? u.prefs.lang : 'tr', board: ['green', 'wood', 'blue', 'slate'].includes(u.prefs.board) ? u.prefs.board : 'green', pieces: ['classic', 'bold', 'gold'].includes(u.prefs.pieces) ? u.prefs.pieces : 'classic' };
        if (cur) for (const s of cur.sessions || []) sessIdx.delete(s.h);
        users.set(ln, doc); persist(doc); n++;
        if (doc.banned) kick(ln);
      }
      return send(res, 200, { restored: n });
    }
  }
  return credErr(res, 'none', 404);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const u = url.pathname;
  if (u.indexOf('/api/') === 0) {
    api(req, res, url).catch(e => { console.error('API hatasi:', e && e.message); if (!res.headersSent) send(res, 500, { error: 'server' }); });
  } else if (u === '/' || u === '/index.html') {
    fs.readFile(GAME, (e, d) => {
      if (e) { res.writeHead(500); res.end('index.html bulunamadi'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer' });
      res.end(d);
    });
  } else if (u === '/saglik') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end('yok'); }
});

// ---------------------------------------------------------------- WebSocket
const clients = new Set();
const retained = new Map();
function kick(lname) { for (const c of Array.from(clients)) if (c.user === lname) end(c); }
server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname !== '/mqtt' || !req.headers['sec-websocket-key']) { socket.destroy(); return; }
  const me = userByToken(url.searchParams.get('t') || '');
  if (!me) { try { socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); } catch (e) {} socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  const proto = String(req.headers['sec-websocket-protocol'] || '').split(',')[0].trim();
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + (proto ? '\r\nSec-WebSocket-Protocol: ' + proto : '') + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, user: me.lname, subs: new Set(), will: null, clean: false, wsbuf: Buffer.alloc(0), mqbuf: Buffer.alloc(0), frag: [], dead: false };
  clients.add(c);
  online.set(c.user, (online.get(c.user) || 0) + 1);
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
  const n = (online.get(c.user) || 1) - 1; if (n > 0) online.set(c.user, n); else online.delete(c.user);
  try { c.socket.destroy(); } catch (e) {}
  if (!c.clean && c.will) publish(c.will.topic, c.will.payload, c.will.retain, c);
}
// eski retained lobi kayitlarini temizle (30 dk)
setInterval(() => {
  for (const [t, v] of retained) {
    try { const j = JSON.parse(v.toString()); const ts = j && j.d && j.d.ts; if (ts && Date.now() - ts > 30 * 60 * 1000) retained.delete(t); } catch (e) {}
  }
}, 60000).unref();

process.on('SIGTERM', () => { store.flush(allUsers).then(() => process.exit(0), () => process.exit(0)); });
initUsers().then(() => {
  server.listen(PORT, () => {
    console.log('Zarli Satranc sunucusu hazir: http://localhost:' + PORT);
    console.log('Depolama: ' + store.name + ', uye sayisi: ' + users.size);
    if (!store.persistent) console.log('UYARI: Kalici depolama yok. Barindirma sitesi dosyalari sifirlarsa hesaplar silinir (OKU.txt: Upstash).');
    if (!ADMIN_PASS_FROM_ENV) console.log('UYARI: Yonetici sifresi varsayilan. ADMIN_PASS ortam degiskeniyle degistir.');
  });
}).catch(e => { console.error('Baslatma hatasi:', e.message); process.exit(1); });
