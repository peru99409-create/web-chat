/*
 * ウェブチャット用の中継サーバー（Cloudflare Workers + Durable Objects）
 *
 * ntfy.sh の代わりに、メッセージの受け渡しだけをします。
 * - 部屋ごとに Durable Object（Room）が1つ。入っている人の WebSocket をつないで、届いたメッセージを全員に配ります。
 * - 本文は、ブラウザ側で部屋名から作った鍵で暗号化済みです。ここでは中身を読みません。
 * - 保管するのは、次の3つだけです（SQLite、無料枠で使えます）。
 *     c  : チャット（15分・60件まで。つなぎ直したときの取りこぼし用）
 *     bd : 伝言板（12時間・200件まで）
 *     att: 大きいアイコン（暗号化済み・6時間・20件まで）
 * - IPアドレスは保存しません。連投を止めるための数え上げに、メモリの中だけで使います（つながっている間だけ）。
 * - 流量の上限（画面側の制限を改造した相手や、スクリプトから来る相手を止めるためのもの）：
 *     接続ごと／IPごと／部屋ぜんたい／1通の大きさ／1つのIPの同時接続数／伝言板の書き込み。
 *     上限を何度も超える接続は、切ります。
 *
 * 呼び方（ブラウザ側 index.html の NTFY 定数に Worker のURLを入れると使われます）
 *   GET  /<topic>/ws                 WebSocket。?since=<秒> で、その時刻以降のチャットを送り直す
 *   POST /<topic>                    1件送る（退室の合図など、WebSocketが使えないとき用）
 *   PUT  /<topic>                    大きいアイコンの預かり（Filename・Message ヘッダ）
 *   GET  /<topic>/json?since=12h&tags=bd   伝言板の履歴
 *   GET  /<topic>/att/<id>           預かったアイコン
 */
import { DurableObject } from 'cloudflare:workers';

const TOPIC = /^wcr-[0-9a-f]{40}$/;
const UID = /^[A-Z0-9]{1,6}$/;
const MAX_ENV = 9000;          // 封筒（JSON文字列）の最大文字数
const MAX_ATT = 300000;        // 大きいアイコン（暗号化後）の最大バイト数
const MAX_CONN = 120;          // 1部屋のWebSocket上限
const BURST = 30, REFILL = 6;  // 1接続あたり：最大30通まで溜まり、1秒に6通ずつ回復
const IP_CONN = 8;             // 1つのIPが、1部屋に同時につなげるWebSocketの数
const IP_BURST = 40, IP_PER = 100;   // IPごと（その部屋の中で）：最大40通まで溜まり、0.1秒に1通ずつ回復
const ROOM_BURST = 100, ROOM_REFILL = 30; // 部屋ぜんたい：最大100通まで溜まり、1秒に30通ずつ回復
const STRIKES_MAX = 30;        // 上限超え・不正な送信がこれだけ溜まった接続は切る
const EDGE_BURST = 100, EDGE_PER = 100;  // 入口（Worker）でのIPごとの上限。1秒に10回ずつ回復（目安。Workerが分かれていると別々に数えます）
const ERR429 = '{"event":"error","code":429}';
const KEEP = { c: [15 * 60e3, 60], bd: [12 * 36e5, 200] };
const ATT_TTL = 6 * 36e5, ATT_MAX = 20;
const DEFAULT_ORIGINS = ['https://web-chat-9ne.pages.dev'];

const edge = new Map();
function edgeOk(ip) {
  const n = Date.now();
  let b = edge.get(ip);
  if (!b) {
    b = { v: EDGE_BURST, t: n };
    edge.set(ip, b);
    if (edge.size > 2000) edge.delete(edge.keys().next().value);
  }
  b.v = Math.min(EDGE_BURST, b.v + (n - b.t) / EDGE_PER);
  b.t = n;
  if (b.v < 1) return false;
  b.v -= 1;
  return true;
}

const allowedOrigins = env =>
  DEFAULT_ORIGINS.concat(String(env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean));

const cors = origin => ({
  'Access-Control-Allow-Origin': origin,
  'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Tags, Filename, Message',
  'Access-Control-Max-Age': '86400',
  'Vary': 'Origin',
});

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const origin = req.headers.get('Origin') || '';
    const okOrigin = allowedOrigins(env).includes(origin);
    const h = okOrigin ? cors(origin) : {};

    if (url.pathname === '/' || url.pathname === '') {
      return new Response('web-chat relay', { status: 200, headers: { 'Content-Type': 'text/plain' } });
    }
    if (req.method === 'OPTIONS') return new Response(null, { status: okOrigin ? 204 : 403, headers: h });
    if (!okOrigin) return new Response('forbidden', { status: 403 });

    const m = /^\/(wcr-[0-9a-f]{40})(?:\/.*)?$/.exec(url.pathname);
    if (!m || !TOPIC.test(m[1])) return new Response('not found', { status: 404, headers: h });

    /* 部屋（Durable Object）を起こす前に、大きすぎる送信と、多すぎる要求を断る */
    const ip = req.headers.get('CF-Connecting-IP') || '';
    const clen = +req.headers.get('Content-Length') || 0;
    if (req.method === 'POST' && clen > MAX_ENV * 4) return new Response('{"error":"large"}', { status: 413, headers: h });
    if (req.method === 'PUT' && clen > MAX_ATT) return new Response('{"error":"large"}', { status: 413, headers: h });
    if (!edgeOk(ip)) return new Response('{"error":"rate"}', { status: 429, headers: h });

    const stub = env.ROOMS.get(env.ROOMS.idFromName(m[1]));
    const fwd = new Headers(req.headers);
    fwd.set('X-Client-Ip', ip);
    const res = await stub.fetch(new Request(req, { headers: fwd }));
    if (res.status === 101) return res; // WebSocket
    const out = new Response(res.body, res);
    for (const k in h) out.headers.set(k, h[k]);
    return out;
  },
};

const rid = () => crypto.randomUUID().replace(/-/g, '').slice(0, 16);
const json = (o, status = 200) =>
  new Response(JSON.stringify(o), { status, headers: { 'Content-Type': 'application/json' } });

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.ipb = new Map();
    this.rb = { v: ROOM_BURST, t: Date.now() };
    this.lastClean = 0;
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
    ctx.blockConcurrencyWhile(async () => {
      this.sql.exec('CREATE TABLE IF NOT EXISTS msgs(seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT, t INTEGER, kind TEXT, body TEXT)');
      this.sql.exec('CREATE TABLE IF NOT EXISTS att(id TEXT PRIMARY KEY, t INTEGER, data BLOB)');
    });
  }

  /* IPごとの流量制限（メモリの中だけ）。cap個まで溜まり、perミリ秒に1個回復 */
  ipOk(ip, cap, per) {
    const n = Date.now();
    let b = this.ipb.get(ip);
    if (!b) {
      b = { v: cap, t: n };
      this.ipb.set(ip, b);
      if (this.ipb.size > 300) this.ipb.delete(this.ipb.keys().next().value);
    }
    b.v = Math.min(cap, b.v + (n - b.t) / per);
    b.t = n;
    if (b.v < 1) return false;
    b.v -= 1;
    return true;
  }

  /* 部屋ぜんたいの流量制限。接続やIPをいくつ使っても、この部屋が受けるのは、1秒に30通ずつ（最大100通の溜め）まで */
  roomOk() {
    const n = Date.now(), b = this.rb;
    b.v = Math.min(ROOM_BURST, b.v + (n - b.t) * ROOM_REFILL / 1000);
    b.t = n;
    if (b.v < 1) return false;
    b.v -= 1;
    return true;
  }

  async fetch(req) {
    const url = new URL(req.url);
    const m = /^\/(wcr-[0-9a-f]{40})(?:\/(.*))?$/.exec(url.pathname);
    if (!m) return new Response('not found', { status: 404 });
    const topic = m[1], sub = m[2] || '';
    const ip = req.headers.get('X-Client-Ip') || '-';

    if (sub === 'ws' && req.headers.get('Upgrade') === 'websocket') return this.openWs(url, topic, ip);
    if (req.method === 'POST' && sub === '') return this.httpPost(req, topic, ip);
    if (req.method === 'PUT' && sub === '') return this.putAtt(req, url, topic, ip);
    if (req.method === 'GET' && sub === 'json') return this.history(url, topic, ip);
    if (req.method === 'GET' && sub.startsWith('att/')) return this.getAtt(sub.slice(4), ip);
    return new Response('not found', { status: 404 });
  }

  /* ---- WebSocket ---- */
  openWs(url, topic, ip) {
    if (!this.ipOk('ws|' + ip, 10, 3000)) return new Response('too many', { status: 429 });
    const socks = this.ctx.getWebSockets();
    if (socks.length >= MAX_CONN) return new Response('busy', { status: 503 });
    let same = 0;
    for (const w of socks) {
      let at = null;
      try { at = w.deserializeAttachment(); } catch (e) {}
      if (at && at.ip === ip) same++;
    }
    if (same >= IP_CONN) return new Response('too many', { status: 429 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ v: BURST, t: Date.now(), p: topic, ip, s: 0 });
    const since = Math.floor(+url.searchParams.get('since') || 0);
    if (since > 0) {
      const from = Math.max(since * 1000, Date.now() - KEEP.c[0]);
      for (const r of this.sql.exec("SELECT id,t,body FROM msgs WHERE kind='c' AND t>? ORDER BY seq LIMIT 60", from)) {
        try { server.send(JSON.stringify(this.wrap(topic, r.id, r.t, r.body, 'c'))); } catch (e) { break; }
      }
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    const now = Date.now();
    const a = ws.deserializeAttachment() || { v: BURST, t: now, p: '', ip: '-', s: 0 };
    if (!TOPIC.test(a.p || '')) return;
    /* 終わりに状態を保存。違反が溜まった接続は切る */
    const fin = notify => {
      if (notify) { try { ws.send(ERR429); } catch (e) {} }
      if ((a.s || 0) >= STRIKES_MAX) { try { ws.close(1008, 'too many requests'); } catch (e) {} return; }
      try { ws.serializeAttachment(a); } catch (e) {}
    };
    if (typeof msg !== 'string' || msg.length > MAX_ENV + 200) { a.s = (a.s || 0) + 5; return fin(false); }
    const v = Math.min(BURST, a.v + (now - a.t) * REFILL / 1000);
    a.t = now;
    if (v < 1) { a.v = v; a.s = (a.s || 0) + 1; return fin(true); }
    a.v = v - 1;
    if (!this.ipOk('m|' + a.ip, IP_BURST, IP_PER)) { a.s = (a.s || 0) + 1; return fin(true); }
    let o;
    try { o = JSON.parse(msg); } catch (e) { a.s = (a.s || 0) + 2; return fin(false); }
    if (!o || typeof o.m !== 'string') { a.s = (a.s || 0) + 2; return fin(false); }
    const id = this.accept(a.p, o.m, o.g, a.ip);
    if (id === 0) return fin(true);                       // 部屋ぜんたい・伝言板の上限
    a.s = id ? Math.max(0, (a.s || 0) - 0.2) : (a.s || 0) + 2; // 正しい送信は、違反を少しずつ減らす
    fin(false);
  }

  webSocketClose(ws, code) {
    try { ws.close(code >= 1000 && code < 5000 && code !== 1005 && code !== 1006 ? code : 1000, 'closed'); } catch (e) {}
  }
  webSocketError(ws) {
    try { ws.close(1011, 'error'); } catch (e) {}
  }

  /* 封筒を検査して、保管して、全員に配る。mはJSON文字列 {u,n,b} */
  /* 戻り値：id（成功）／null（形が正しくない）／0（上限で止めた） */
  accept(topic, m, tag, ip) {
    if (m.length > MAX_ENV) return null;
    let e;
    try { e = JSON.parse(m); } catch (x) { return null; }
    if (!e || typeof e.b !== 'string' || e.b.length > MAX_ENV - 200 || typeof e.u !== 'string' || !UID.test(e.u)) return null;
    if (e.n != null && (typeof e.n !== 'string' || e.n.length > 60)) return null;
    const kind = tag === 'bd' || tag === 'c' ? tag : '';
    if (!this.roomOk()) return 0;
    if (kind === 'bd' && !this.ipOk('bd|' + ip, 5, 4000)) return 0; // 伝言板：1つのIPから、最大5件まで溜まり、4秒に1件ずつ回復
    const id = rid(), t = Date.now();
    if (kind) {
      this.sql.exec('INSERT INTO msgs(id,t,kind,body) VALUES(?,?,?,?)', id, t, kind, m);
      this.clean(t);
    }
    this.broadcast(this.wrap(topic, id, t, m, kind));
    return id;
  }

  wrap(topic, id, t, body, kind) {
    return { id, time: Math.floor(t / 1000), event: 'message', topic, message: body, tags: kind ? [kind] : [] };
  }

  broadcast(obj) {
    const s = JSON.stringify(obj);
    for (const ws of this.ctx.getWebSockets()) {
      try { ws.send(s); } catch (e) {}
    }
  }

  /* 古いものを消す（1分に1回まで） */
  clean(now) {
    if (now - this.lastClean < 60000) return;
    this.lastClean = now;
    for (const k of Object.keys(KEEP)) {
      const [ttl, max] = KEEP[k];
      this.sql.exec('DELETE FROM msgs WHERE kind=? AND t<?', k, now - ttl);
      this.sql.exec('DELETE FROM msgs WHERE kind=? AND seq<=(SELECT seq FROM msgs WHERE kind=? ORDER BY seq DESC LIMIT 1 OFFSET ?)', k, k, max);
    }
    this.sql.exec('DELETE FROM att WHERE t<?', now - ATT_TTL);
    this.sql.exec('DELETE FROM att WHERE id IN (SELECT id FROM att ORDER BY t DESC LIMIT -1 OFFSET ?)', ATT_MAX);
  }

  /* ---- HTTP: 1件送る ---- */
  async httpPost(req, topic, ip) {
    if (!this.ipOk('p|' + ip, 40, 1000)) return json({ error: 'rate' }, 429);
    const len = +req.headers.get('Content-Length') || 0;
    if (len > MAX_ENV * 4) return json({ error: 'large' }, 413);
    const text = await req.text();
    if (text.length > MAX_ENV) return json({ error: 'large' }, 413);
    const id = this.accept(topic, text, req.headers.get('Tags') || '', ip);
    if (id === 0) return json({ error: 'rate' }, 429);
    return id ? json({ id, vf: '' }) : json({ error: 'bad' }, 400);
  }

  /* ---- HTTP: 履歴（伝言板） ---- */
  history(url, topic, ip) {
    if (!this.ipOk('h|' + ip, 20, 2000)) return json({ error: 'rate' }, 429);
    const tag = (url.searchParams.get('tags') || 'bd').split(',')[0];
    const kind = tag === 'c' ? 'c' : 'bd';
    const ttl = KEEP[kind][0];
    const sinceP = url.searchParams.get('since') || '';
    let from = Date.now() - ttl;
    const hm = /^(\d+)h$/.exec(sinceP);
    if (hm) from = Math.max(from, Date.now() - (+hm[1]) * 36e5);
    const lines = [];
    for (const r of this.sql.exec('SELECT id,t,body FROM msgs WHERE kind=? AND t>? ORDER BY seq LIMIT 200', kind, from)) {
      lines.push(JSON.stringify(this.wrap(topic, r.id, r.t, r.body, kind)));
    }
    return new Response(lines.join('\n'), { headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' } });
  }

  /* ---- HTTP: 大きいアイコンの預かり ---- */
  async putAtt(req, url, topic, ip) {
    if (!this.ipOk('a|' + ip, 6, 20000)) return json({ error: 'rate' }, 429);
    const len = +req.headers.get('Content-Length') || 0;
    if (len > MAX_ATT) return json({ error: 'large' }, 413);
    const buf = await req.arrayBuffer();
    if (buf.byteLength < 28 || buf.byteLength > MAX_ATT) return json({ error: 'size' }, 413);
    const msg = (req.headers.get('Message') || '').slice(0, 2000);
    if (!/^e1\.[A-Za-z0-9+/=]+$/.test(msg)) return json({ error: 'bad' }, 400);
    const id = rid(), t = Date.now();
    this.sql.exec('INSERT INTO att(id,t,data) VALUES(?,?,?)', id, t, buf);
    this.clean(t);
    this.broadcast({
      id, time: Math.floor(t / 1000), event: 'message', topic, message: msg, tags: [],
      attachment: { name: 'a.bin', url: url.origin + '/' + topic + '/att/' + id, size: buf.byteLength },
    });
    return json({ id });
  }

  getAtt(id, ip) {
    if (!this.ipOk('g|' + ip, 40, 1000)) return json({ error: 'rate' }, 429);
    if (!/^[0-9a-f]{16}$/.test(id)) return new Response('not found', { status: 404 });
    const r = [...this.sql.exec('SELECT data FROM att WHERE id=?', id)][0];
    if (!r) return new Response('not found', { status: 404 });
    return new Response(r.data, { headers: { 'Content-Type': 'application/octet-stream', 'Cache-Control': 'no-store' } });
  }
}
