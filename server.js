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
const START_PTS = 100;
const seasonKey = () => new Date().toISOString().slice(0, 7);
const dayKey = (off) => new Date(Date.now() - (off || 0) * 86400000).toISOString().slice(0, 10);
const MIN_LOG = process.env.ZS_MIN_LOG !== undefined ? Number(process.env.ZS_MIN_LOG) : 8;
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
    remove(lname, all) { getAll = all; this.save(null, all); },
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
      const keys = ((await cmd(['SMEMBERS', 'zsu-index'])) || []).map(n => 'zsu:' + n);
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
        httpJson(UP_URL + '/pipeline', hdr, [['SET', k, JSON.stringify(user)], ['SADD', 'zsu-index', user.lname]])
          .then(r => { for (const x of r) if (x && x.error) throw new Error(x.error); })
          .catch(e => console.error('Upstash kayit hatasi:', e.message));
      }, 1500);
    },
    remove(lname) {
      delete timers['zsu:' + lname];
      httpJson(UP_URL + '/pipeline', hdr, [['DEL', 'zsu:' + lname], ['SREM', 'zsu-index', lname]]).catch(e => console.error('Upstash silme hatasi:', e.message));
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
    stats: freshStats(), games: [], prefs: null, sessions: [], pts: START_PTS, qw: 0, ql: 0, qd: 0, peak: START_PTS, sk: seasonKey(), sbase: START_PTS, sg: 0, sb: [], dq: { d: '', streak: 0 }, pz: { last: '', streak: 0, best: 0, total: 0 } };
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
    if (!Number.isInteger(u.pts) || u.pts < 0) u.pts = START_PTS;
    for (const k of ['qw', 'ql', 'qd', 'sg']) if (!Number.isInteger(u[k]) || u[k] < 0) u[k] = 0;
    if (!Number.isInteger(u.peak) || u.peak < u.pts) u.peak = u.pts;
    if (typeof u.sk !== 'string') { u.sk = seasonKey(); u.sbase = u.pts; }
    if (!Number.isInteger(u.sbase)) u.sbase = u.pts;
    if (!Array.isArray(u.sb)) u.sb = [];
    if (!u.dq || typeof u.dq !== 'object') u.dq = { d: '', streak: 0 };
    if (!u.pz || typeof u.pz !== 'object') u.pz = { last: '', streak: 0, best: 0, total: 0 };
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
function achievements(u) {
  const a = [], tot = STAT_KEYS.reduce((n, k) => n + u.stats.r[k].reduce((x, c) => x + c, 0), 0);
  const wins = STAT_KEYS.reduce((n, k) => n + u.stats.r[k][0], 0);
  if (wins >= 1) a.push('first_win');
  if (tot >= 10) a.push('games10');
  if (u.stats.best >= 5) a.push('streak5');
  if (u.stats.r.hard[0] >= 1) a.push('beat_hard');
  if (u.stats.r.expert[0] >= 1) a.push('beat_expert');
  if (u.qw >= 1) a.push('quick_win');
  if (u.qw >= 10) a.push('quick10');
  if (u.peak >= 200) a.push('pts200');
  if (u.peak >= 400) a.push('pts400');
  const z = u.pz || {};
  if (z.total >= 1) a.push('pz_first');
  if (z.best >= 7) a.push('pz_week');
  if (z.total >= 30) a.push('pz_30');
  return a;
}
function dailyInfo(u) {
  const d = u.dq || {};
  return { done: d.d === dayKey(0), streak: (d.d === dayKey(0) || d.d === dayKey(1)) ? d.streak || 0 : 0 };
}
const publicMe = u => ({ name: u.name, role: u.role, stats: u.stats, prefs: u.prefs, pts: u.pts, qw: u.qw, ql: u.ql, qd: u.qd, ach: achievements(u), sb: u.sb || [], daily: dailyInfo(u), pz: pzInfo(u), season: { key: seasonKey(), gain: u.pts - (u.sk === seasonKey() ? u.sbase : u.pts) }, games: u.games.slice().reverse().map(gameSummary) });

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


// ---------------------------------------------------------------- oyun motoru (mac dogrulama)
let E = null;
try {
  const html = fs.readFileSync(GAME, 'utf8');
  const a = html.indexOf('// ENGINE START'), b = html.indexOf('// ENGINE END');
  if (a > 0 && b > a) E = new Function(html.slice(a, b) + '\nreturn { newState, legalMoves, applyMove, cloneS, attacked, insufficient, playable, spendAfter };')();
} catch (e) { console.error('Motor yuklenemedi, maclar sadece kayit uzunluguna gore denetlenecek:', e.message); }
const PRC = ['', 'q', 'r', 'b', 'n'];
function replayLog(log) {
  if (!E) return null;
  let s = E.newState();
  for (const e of log) {
    const color = e[0] ? 'b' : 'w';
    if (e[1]) { s = E.cloneS(s); s.ep = -1; continue; }
    s.turn = color;
    const m = E.legalMoves(s, color).find(x => x.from === e[6] && x.to === e[7]);
    if (!m) return null;
    const ns = E.applyMove(s, m, PRC[e[10]] || undefined);
    ns.turn = color; s = ns;
  }
  return s;
}
function inCheckOf(s, color) {
  const k = s.board.findIndex(p => p && p[0] === color && p[1] === 'k');
  return k >= 0 && E.attacked(s.board, k, color === 'w' ? 'b' : 'w');
}
// Rapor edilen sonucu hamle kaydiyla karsilastirir. true = tutarli, false = sahte/bozuk.
function verifyReport(color, out, type, winner, log) {
  if (!E || !log) return true;
  if (out === 'win' && winner !== color) return false;
  if (out === 'lose' && winner !== (color === 'w' ? 'b' : 'w')) return false;
  const s = replayLog(log);
  if (!s) return false;
  if (type === 'mate') {
    const loser = winner === 'w' ? 'b' : 'w';
    return E.legalMoves(s, loser).length === 0 && inCheckOf(s, loser);
  }
  if (type === 'stalemate') return out === 'draw' && E.legalMoves(s, 'w').length + E.legalMoves(s, 'b').length >= 0 && [s.turn === 'w' ? 'b' : 'w'].every(c => E.legalMoves(s, c).length === 0 && !inCheckOf(s, c));
  if (type === 'material') return out === 'draw' && E.insufficient(s.board);
  if (type === 'fifty') return out === 'draw' && s.half >= 100;
  return true; // left, resign, time, agreed: kayit yalnizca gecerli hamlelerden olusmali
}

// ---------------------------------------------------------------- gunluk bulmaca
let PUZ = [];
try {
  const html = fs.readFileSync(GAME, 'utf8');
  const m = /<script type="application\/json" id="pzData">([\s\S]*?)<\/script>/.exec(html);
  if (m) PUZ = JSON.parse(m[1]);
} catch (e) { console.error('Bulmaca verisi okunamadi:', e.message); }
const PZ_LET = { P: 'wp', N: 'wn', B: 'wb', R: 'wr', Q: 'wq', K: 'wk', p: 'bp', n: 'bn', b: 'bb', r: 'br', q: 'bq', k: 'bk' };
const dayTR = off => new Date(Date.now() + 3 * 3600000 - (off || 0) * 86400000).toISOString().slice(0, 10);
const pzIdx = () => PUZ.length ? Math.floor((Date.now() + 3 * 3600000) / 86400000) % PUZ.length : 0;
function pzInfo(u) {
  const z = u.pz || {};
  return { idx: pzIdx(), n: PUZ.length, done: z.last === dayTR(0), streak: (z.last === dayTR(0) || z.last === dayTR(1)) ? z.streak || 0 : 0, total: z.total || 0, best: z.best || 0 };
}
function verifyPuzzle(p, moves) {
  if (!E || !p || !Array.isArray(moves) || moves.length < 1 || moves.length > 40) return false;
  const color = p.t, o = color === 'w' ? 'b' : 'w';
  let s = { board: Array.from(p.b).map(ch => ch === '.' ? null : PZ_LET[ch]), turn: color,
    castle: { wk: p.c.indexOf('K') >= 0, wq: p.c.indexOf('Q') >= 0, bk: p.c.indexOf('k') >= 0, bq: p.c.indexOf('q') >= 0 }, ep: p.e, half: 0 };
  let left = p.r, sp = {};
  for (const mv of moves) {
    if (!Array.isArray(mv) || !num(mv[0], 0, 63) || !num(mv[1], 0, 63)) return false;
    const m = E.playable(s, color, left, sp).find(x => x.from === mv[0] && x.to === mv[1]);
    if (!m) return false;
    const pr = ['q', 'r', 'b', 'n'].indexOf(mv[2]) >= 0 ? mv[2] : undefined;
    const ns = E.applyMove(s, m, pr);
    ns.turn = color; left -= m.cost; sp = E.spendAfter(sp, m); s = ns;
  }
  if (left > 0 && E.playable(s, color, left, sp).length) return false;
  const s2 = E.cloneS(s); s2.turn = o;
  return E.legalMoves(s2, o).length === 0 && E.attacked(s2.board, s2.board.indexOf(o + 'k'), color);
}
function rollSeason() {
  const cur = seasonKey();
  const old = allUsers().filter(u => u.sk && u.sk !== cur);
  if (!old.length) return;
  const byKey = {};
  for (const u of old) (byKey[u.sk] = byKey[u.sk] || []).push(u);
  for (const k of Object.keys(byKey)) {
    byKey[k].filter(u => !u.banned && u.sg > 0 && u.pts - u.sbase > 0).sort((a, b) => (b.pts - b.sbase) - (a.pts - a.sbase)).slice(0, 3)
      .forEach((u, i) => { u.sb.push({ k, r: i + 1 }); });
  }
  for (const u of old) { u.sk = cur; u.sbase = u.pts; u.sg = 0; persist(u); }
}
setInterval(rollSeason, 600000).unref();

// ---------------------------------------------------------------- hizli oyun ve puan
const queue = new Map();      // lname -> son sorgu zamani
const pendingMatch = new Map(); // lname -> eslesme bilgisi (henuz teslim edilmedi)
const matches = new Map();    // mid -> eslesme kaydi
const pairLog = [];           // {k, t} puanli biten maclar (ayni cift sinirlamasi)
const QUEUE_TTL = 9000, SOLO_WAIT = Number(process.env.ZS_SOLO_WAIT) || 45000, MATCH_TTL = 6 * 3600 * 1000, PAIR_MAX = Number(process.env.ZS_PAIR_MAX) || 3, MIN_AGE = Number(process.env.ZS_MIN_AGE) || 20000;
const hex = n => crypto.randomBytes(n).toString('hex');
function newCode() {
  for (let i = 0; i < 20; i++) { const c = String(100000 + crypto.randomInt(900000)); if (!Array.from(matches.values()).some(m => m.code === c && !m.done)) return c; }
  return String(100000 + crypto.randomInt(900000));
}
function tryPair(me) {
  let best = null;
  for (const [ln, t] of queue) if (ln !== me.lname && Date.now() - t < QUEUE_TTL && (!best || t < queue.get(best))) best = ln;
  if (!best) return null;
  queue.delete(best); queue.delete(me.lname);
  const m = { id: hex(6), code: newCode(), host: best, guest: me.lname, t: Date.now(), rep: {}, repAt: {}, n: {}, bad: false, done: false, res: null };
  matches.set(m.id, m);
  pendingMatch.set(best, m);
  return m;
}
const matchFor = (m, ln) => ({ mid: m.id, code: m.code, host: m.host === ln, opp: users.get(m.host === ln ? m.guest : m.host).name });
function eloDelta(ra, rb, sa) { const e = 1 / (1 + Math.pow(10, (rb - ra) / 400)); return 32 * (sa - e); }
function settle(m, force) {
  if (m.done) return;
  const ra = m.rep[m.host], rb = m.rep[m.guest];
  let a = null;
  if (m.bad) { m.done = true; m.res = 'dispute'; return; }
  if (ra && rb) {
    if ((ra === 'win' && rb === 'lose') || (ra === 'lose' && rb === 'win') || (ra === 'draw' && rb === 'draw')) a = ra;
    else { m.done = true; m.res = 'dispute'; return; }
  } else if (force) {
    const who = ra ? m.host : rb ? m.guest : null, r = ra || rb;
    if (!who || r === 'draw') { m.done = true; m.res = 'none'; return; }
    const at = m.repAt[who];
    if (at - m.t < MIN_AGE) { m.done = true; m.res = 'short'; return; }
    a = who === m.host ? r : (r === 'win' ? 'lose' : 'win');
  } else return;
  const logN = Math.max(m.n[m.host] || 0, m.n[m.guest] || 0);
  if (logN < MIN_LOG) { m.done = true; m.res = 'short'; return; }
  m.done = true;
  const H = users.get(m.host), Gs = users.get(m.guest);
  if (!H || !Gs) { m.res = 'none'; return; }
  const key = [m.host, m.guest].sort().join('|'), now = Date.now();
  while (pairLog.length && now - pairLog[0].t > 24 * 3600 * 1000) pairLog.shift();
  if (pairLog.filter(x => x.k === key).length >= PAIR_MAX) { m.res = 'limit'; return; }
  pairLog.push({ k: key, t: now });
  const sh = a === 'win' ? 1 : a === 'lose' ? 0 : 0.5;
  let dh = Math.round(eloDelta(H.pts, Gs.pts, sh)), dg = Math.round(eloDelta(Gs.pts, H.pts, 1 - sh));
  if (sh === 1) { dh = Math.max(dh, 1); } else if (sh === 0) { dg = Math.max(dg, 1); }
  const oh = H.pts, og = Gs.pts;
  H.pts = Math.max(0, H.pts + dh); Gs.pts = Math.max(0, Gs.pts + dg);
  if (sh === 1) { H.qw++; Gs.ql++; } else if (sh === 0) { H.ql++; Gs.qw++; } else { H.qd++; Gs.qd++; }
  m.res = 'ok'; m.delta = {}; m.delta[m.host] = H.pts - oh; m.delta[m.guest] = Gs.pts - og;
  m.pts = {}; m.pts[m.host] = H.pts; m.pts[m.guest] = Gs.pts;
  for (const [u, won] of [[H, a === 'win'], [Gs, a === 'lose']]) {
    u.sg++; if (u.pts > u.peak) u.peak = u.pts;
    if (won) {
      const d = u.dq || (u.dq = { d: '', streak: 0 }), today = dayKey(0);
      if (d.d !== today) { d.streak = d.d === dayKey(1) ? (d.streak || 0) + 1 : 1; d.d = today; }
    }
  }
  persist(H); persist(Gs);
}
setInterval(() => {
  const now = Date.now();
  for (const [k, t] of queue) if (now - t > QUEUE_TTL * 2) queue.delete(k);
  for (const [k, m] of pendingMatch) if (now - m.t > 60000) pendingMatch.delete(k);
  for (const [id, m] of matches) {
    if (!m.done) {
      const times = Object.values(m.repAt);
      if (times.length === 1 && now - times[0] > SOLO_WAIT) settle(m, true);
      else if (!times.length && now - m.t > 30 * 60 * 1000) { m.done = true; m.res = 'none'; }
    }
    if (now - m.t > MATCH_TTL) matches.delete(id);
  }
}, 5000).unref();
function matchStatus(m, ln) {
  if (!m.done) return { done: false };
  return { done: true, res: m.res, delta: m.delta ? m.delta[ln] : 0, pts: m.pts ? m.pts[ln] : users.get(ln).pts };
}
function leaderboard(me, season) {
  const cur = seasonKey(), gain = u => u.pts - (u.sk === cur ? u.sbase : u.pts), sg = u => u.sk === cur ? u.sg : 0;
  const list = allUsers().filter(u => !u.banned && (season ? sg(u) > 0 : (u.qw + u.ql + u.qd) > 0))
    .sort(season ? (a, b) => gain(b) - gain(a) || b.pts - a.pts : (a, b) => b.pts - a.pts || b.qw - a.qw || a.created - b.created);
  const rows = list.slice(0, 50).map((u, i) => ({ rank: i + 1, name: u.name, pts: season ? gain(u) : u.pts, w: u.qw, l: u.ql, d: u.qd, me: u.lname === me.lname, top: (u.sb || []).slice(-1)[0] || null }));
  const idx = list.findIndex(u => u.lname === me.lname);
  return { rows, total: list.length, season: season ? cur : '', mine: { rank: idx < 0 ? 0 : idx + 1, pts: season ? gain(me) : me.pts, w: me.qw, l: me.ql, d: me.qd } };
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

  if (route === 'GET /api/leaderboard') return send(res, 200, leaderboard(me, url.searchParams.get('season') === '1'));
  if (route === 'POST /api/quick/join') {
    if (limited('qj:' + me.lname, 30, 60000)) return credErr(res, 'rate', 429);
    const pend = pendingMatch.get(me.lname);
    if (pend) { pendingMatch.delete(me.lname); return send(res, 200, { match: matchFor(pend, me.lname) }); }
    const m = tryPair(me);
    if (m) return send(res, 200, { match: matchFor(m, me.lname) });
    queue.set(me.lname, Date.now());
    return send(res, 200, { waiting: true, n: queue.size });
  }
  if (route === 'GET /api/quick/status') {
    const pend = pendingMatch.get(me.lname);
    if (pend) { pendingMatch.delete(me.lname); queue.delete(me.lname); return send(res, 200, { match: matchFor(pend, me.lname) }); }
    if (!queue.has(me.lname)) return send(res, 200, { waiting: false });
    queue.set(me.lname, Date.now());
    const m = tryPair(me);
    if (m) return send(res, 200, { match: matchFor(m, me.lname) });
    queue.set(me.lname, Date.now());
    return send(res, 200, { waiting: true, n: queue.size });
  }
  if (route === 'POST /api/quick/leave') { queue.delete(me.lname); pendingMatch.delete(me.lname); return send(res, 200, { ok: true }); }
  if (route === 'POST /api/match/result') {
    let b; try { b = await readBody(req, 65536); } catch (e) { return credErr(res, 'bad'); }
    const m = matches.get(String(b.mid));
    if (!m || (m.host !== me.lname && m.guest !== me.lname) || !['win', 'lose', 'draw'].includes(b.out)) return credErr(res, 'bad');
    if (!m.rep[me.lname]) {
      const log = cleanLog(b.log), r0 = b.res || {};
      m.n[me.lname] = log ? log.length : 0;
      if (b.out !== 'lose' || log) {
        if (b.out !== 'lose' && !log) m.bad = true;
        else if (log && !verifyReport(b.color === 'b' ? 'b' : 'w', b.out, String(r0.type || ''), r0.winner === 'b' ? 'b' : r0.winner === 'w' ? 'w' : '', log)) m.bad = true;
      }
      m.rep[me.lname] = b.out; m.repAt[me.lname] = Date.now();
    }
    settle(m, false);
    return send(res, 200, matchStatus(m, me.lname));
  }
  const ms = /^GET \/api\/match\/([0-9a-f]{12})$/.exec(route);
  if (ms) {
    const m = matches.get(ms[1]);
    if (!m || (m.host !== me.lname && m.guest !== me.lname)) return credErr(res, 'none', 404);
    return send(res, 200, matchStatus(m, me.lname));
  }

  if (route === 'POST /api/prefs') {
    let b; try { b = await readBody(req, 2048); } catch (e) { return credErr(res, 'bad'); }
    const okLang = ['tr', 'en', 'zh'], okB = ['green', 'wood', 'blue', 'slate'], okP = ['classic', 'bold', 'gold'];
    if (!okLang.includes(b.lang) || !okB.includes(b.board) || !okP.includes(b.pieces)) return credErr(res, 'bad');
    me.prefs = { lang: b.lang, board: b.board, pieces: b.pieces };
    persist(me);
    return send(res, 200, { ok: true });
  }
  if (route === 'POST /api/puzzle/solve') {
    if (limited('pz:' + me.lname, 60, 60000)) return credErr(res, 'rate', 429);
    let b; try { b = await readBody(req, 4096); } catch (e) { return credErr(res, 'bad'); }
    if (!PUZ.length || !E) return credErr(res, 'none', 404);
    const ok = verifyPuzzle(PUZ[pzIdx()], b.moves);
    if (ok) {
      const z = me.pz || (me.pz = { last: '', streak: 0, best: 0, total: 0 }), today = dayTR(0);
      if (z.last !== today) { z.streak = z.last === dayTR(1) ? (z.streak || 0) + 1 : 1; z.last = today; z.total = (z.total || 0) + 1; if (z.streak > (z.best || 0)) z.best = z.streak; persist(me); }
    }
    return send(res, 200, { ok, pz: pzInfo(me) });
  }
  if (route === 'POST /api/password') {
    if (limited('pw:' + me.lname, 10, 600000)) return credErr(res, 'rate', 429);
    let b; try { b = await readBody(req, 1024); } catch (e) { return credErr(res, 'bad'); }
    const np = String(b.next == null ? '' : b.next);
    if (np.length < 6 || np.length > 64) return credErr(res, 'pass');
    if (!safeEq(await scrypt(String(b.old == null ? '' : b.old), me.salt), me.hash)) return credErr(res, 'creds', 401);
    me.salt = crypto.randomBytes(16).toString('hex'); me.hash = await scrypt(np, me.salt);
    const keep = sha(token);
    for (const s2 of me.sessions || []) if (s2.h !== keep) sessIdx.delete(s2.h);
    me.sessions = (me.sessions || []).filter(s2 => s2.h === keep);
    persist(me);
    return send(res, 200, { ok: true });
  }
  if (route === 'POST /api/account/delete') {
    if (limited('del:' + me.lname, 5, 600000)) return credErr(res, 'rate', 429);
    let b; try { b = await readBody(req, 1024); } catch (e) { return credErr(res, 'bad'); }
    if (me.role === 'admin') return credErr(res, 'forbidden', 403);
    if (!safeEq(await scrypt(String(b.password == null ? '' : b.password), me.salt), me.hash)) return credErr(res, 'creds', 401);
    for (const s2 of me.sessions || []) sessIdx.delete(s2.h);
    users.delete(me.lname); queue.delete(me.lname); pendingMatch.delete(me.lname);
    store.remove(me.lname, allUsers);
    kick(me.lname);
    return send(res, 200, { ok: true });
  }
  if (route === 'POST /api/stats/reset') { me.stats = freshStats(); persist(me); return send(res, 200, { stats: me.stats }); }

  if (route === 'POST /api/game') {
    if (limited('game:' + me.lname, 40, 60000)) return credErr(res, 'rate', 429);
    let b; try { b = await readBody(req, 65536); } catch (e) { return credErr(res, 'bad'); }
    const log = cleanLog(b.log);
    const res0 = b.res || {};
    const types = ['mate', 'stalemate', 'material', 'fifty', 'left', 'resign', 'time', 'agreed'];
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
          stats: freshStats(), games: [], prefs: null, sessions: [], pts: num(u.pts, 0, 1e7) ? u.pts : START_PTS, qw: num(u.qw, 0, 1e7) ? u.qw : 0, ql: num(u.ql, 0, 1e7) ? u.ql : 0, qd: num(u.qd, 0, 1e7) ? u.qd : 0, peak: num(u.peak, 0, 1e7) ? u.peak : (num(u.pts, 0, 1e7) ? u.pts : START_PTS), sk: seasonKey(), sbase: num(u.pts, 0, 1e7) ? u.pts : START_PTS, pz: (u.pz && typeof u.pz === 'object') ? { last: typeof u.pz.last === 'string' ? u.pz.last.slice(0, 10) : '', streak: num(u.pz.streak, 0, 1e5) ? u.pz.streak : 0, best: num(u.pz.best, 0, 1e5) ? u.pz.best : 0, total: num(u.pz.total, 0, 1e6) ? u.pz.total : 0 } : { last: '', streak: 0, best: 0, total: 0 }, sg: 0, sb: Array.isArray(u.sb) ? u.sb.filter(b => b && typeof b.k === 'string' && num(b.r, 1, 3)).slice(0, 60).map(b => ({ k: b.k.slice(0, 7), r: b.r })) : [], dq: { d: '', streak: 0 } };
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


// ---------------------------------------------------------------- PWA (ana ekrana ekleme)
const ICON_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><rect width="512" height="512" rx="104" fill="#18211f"/><g fill="#6c8d83"><rect x="64" y="64" width="96" height="96"/><rect x="256" y="64" width="96" height="96"/><rect x="160" y="160" width="96" height="96"/><rect x="352" y="160" width="96" height="96"/><rect x="64" y="256" width="96" height="96"/><rect x="256" y="256" width="96" height="96"/><rect x="160" y="352" width="96" height="96"/><rect x="352" y="352" width="96" height="96"/></g><g transform="rotate(-12 256 256)"><rect x="136" y="136" width="240" height="240" rx="44" fill="#d9901c"/><g fill="#1d1405"><circle cx="190" cy="190" r="22"/><circle cx="322" cy="190" r="22"/><circle cx="256" cy="256" r="22"/><circle cx="190" cy="322" r="22"/><circle cx="322" cy="322" r="22"/></g></g></svg>';
const ICON192 = Buffer.from('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', 'base64');
const ICON512 = Buffer.from('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', 'base64');
const MANIFEST = JSON.stringify({ name: 'Zarli Satranc', short_name: 'Zarli Satranc', start_url: '/', scope: '/', display: 'standalone', orientation: 'any',
  background_color: '#18211f', theme_color: '#18211f', lang: 'tr',
  icons: [{ src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' }, { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' }, { src: '/icon.svg', sizes: 'any', type: 'image/svg+xml', purpose: 'any' }] });
const SW = "const C='zs-v1';self.addEventListener('install',e=>{self.skipWaiting();});self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!==C).map(x=>caches.delete(x)))).then(()=>self.clients.claim()));});" +
  "self.addEventListener('fetch',e=>{const u=new URL(e.request.url);if(e.request.method!=='GET'||u.origin!==location.origin||u.pathname.indexOf('/api/')===0||u.pathname==='/mqtt')return;" +
  "e.respondWith(fetch(e.request).then(r=>{if(r.ok&&(e.request.mode==='navigate'||u.pathname==='/'||u.pathname.indexOf('/icon')===0)){const c=r.clone();caches.open(C).then(x=>x.put(e.request,c));}return r;}).catch(()=>caches.match(e.request).then(r=>r||caches.match('/'))));});";
const STATIC = {
  '/manifest.webmanifest': ['application/manifest+json', Buffer.from(MANIFEST)],
  '/sw.js': ['application/javascript; charset=utf-8', Buffer.from(SW)],
  '/icon.svg': ['image/svg+xml', Buffer.from(ICON_SVG)],
  '/icon-192.png': ['image/png', ICON192],
  '/icon-512.png': ['image/png', ICON512]
};

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
  } else if (STATIC[u]) {
    res.writeHead(200, { 'Content-Type': STATIC[u][0], 'Cache-Control': u === '/sw.js' ? 'no-cache' : 'public, max-age=86400', 'Content-Length': STATIC[u][1].length, 'X-Content-Type-Options': 'nosniff' });
    res.end(STATIC[u][1]);
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
async function checkStore() {
  if (!(UP_URL && UP_TOK)) return;
  try { await httpJson(UP_URL + '/pipeline', { Authorization: 'Bearer ' + UP_TOK }, [['SET', 'zsu-check', '1']]).then(r => { if (r[0] && r[0].error) throw new Error(r[0].error); }); }
  catch (e) { throw new Error('Upstash yazma denemesi basarisiz (Read-Only jeton kullaniyor olabilirsin; UPSTASH_REDIS_REST_TOKEN kullan): ' + e.message); }
}
checkStore().then(initUsers).then(() => { rollSeason();
  server.listen(PORT, () => {
    console.log('Zarli Satranc sunucusu hazir: http://localhost:' + PORT);
    console.log('Depolama: ' + store.name + ', uye sayisi: ' + users.size);
    if (!store.persistent) console.log('UYARI: Kalici depolama yok. Barindirma sitesi dosyalari sifirlarsa hesaplar silinir (OKU.txt: Upstash).');
    if (!ADMIN_PASS_FROM_ENV) console.log('UYARI: Yonetici sifresi varsayilan. ADMIN_PASS ortam degiskeniyle degistir.');
  });
}).catch(e => { console.error('Baslatma hatasi:', e.message); process.exit(1); });
