/* ウェブチャット中継 Worker（Cloudflare Workers + Durable Objects）
   ntfy.sh の代わりに、部屋（topic）ごとに1つの Durable Object でメッセージを中継します。

   公開API（index.html から使う）
     GET  /{topic}/ws?since=秒   WebSocket。届いたメッセージを、ntfy と同じ形のJSONで流す
     POST /{topic}?tags=bd       メッセージを送る（本文＝index.html が作る封筒のJSON文字列）
     POST /{topic}/att?u=ID&m=.. 大きいアイコン（暗号化済みのバイト列）を一時的に預ける
     GET  /{topic}/a/{id}        預かったアイコンを返す
     GET  /{topic}/json?since=12h&tags=bd  伝言板の履歴を返す（1行1JSON）

   topic は 'wcr-' + 16進40文字 だけを受け付けます（部屋名そのものは届きません）。 */
import { DurableObject } from 'cloudflare:workers';

const TOPIC = /^wcr-[0-9a-f]{40}$/;
const ATT_ID = /^[0-9a-f]{12}$/;
const MAX_MSG = 9000;            // 1通の最大文字数（index.html 側の上限は約3,900）
const MAX_ATT = 400000;          // 大きいアイコンの最大バイト数
const ATT_TTL = 3 * 3600;        // アイコンの預かり時間（秒）
const BOARD_TTL = 12 * 3600;     // 伝言板の保管時間（秒）
const RING_TTL = 120;            // 再接続用に覚えておく時間（秒）
const RING_MAX = 40;             // 再接続用に覚えておく件数
const RING_BYTES = 300000;       // 再接続用に覚えておく合計の大きさ
const MAX_SOCKETS = 100;         // 1つの部屋につなげる数

const rid = () => crypto.randomUUID().replace(/-/g, '').slice(0, 12);
const nowS = () => Math.floor(Date.now() / 1000);

function originOk(origin, env) {
  if (!origin) return true; // ブラウザ以外（curlなど）。必要なら false にして締められます
  const list = String(env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
  return list.some(o => {
    if (o === origin) return true;
    // 先頭が https://*. の指定は、サブドメインも許可する（プレビュー用）
    const m = /^(https?:\/\/)\*\.(.+)$/.exec(o);
    return !!m && origin.startsWith(m[1]) && origin.endsWith('.' + m[2]);
  });
}

function cors(origin) {
  const h = { Vary: 'Origin' };
  if (origin) {
    h['Access-Control-Allow-Origin'] = origin;
    h['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS';
    h['Access-Control-Allow-Headers'] = 'Content-Type';
    h['Access-Control-Max-Age'] = '86400';
  }
  return h;
}

const reply = (body, status, origin, extra) =>
  new Response(body, { status, headers: Object.assign({ 'Cache-Control': 'no-store' }, cors(origin), extra || {}) });

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin') || '';
    if (!originOk(origin, env)) return new Response('forbidden', { status: 403 });
    if (request.method === 'OPTIONS') return reply(null, 204, origin);

    const topic = url.pathname.split('/').filter(Boolean)[0] || '';
    if (!TOPIC.test(topic)) return reply('not found', 404, origin);

    const stub = env.ROOM.get(env.ROOM.idFromName(topic));
    const res = await stub.fetch(request);
    if (res.status === 101) return res; // WebSocket はそのまま返す
    const out = new Response(res.body, res);
    for (const [k, v] of Object.entries(cors(origin))) out.headers.set(k, v);
    return out;
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ring = [];
    this.board = [];
    // 'ping' には、DOを起こさずに 'pong' を返す（接続の生存確認用・費用がかからない）
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
    ctx.blockConcurrencyWhile(async () => {
      this.ring = (await ctx.storage.get('ring')) || [];
      this.board = (await ctx.storage.get('board')) || [];
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const seg = url.pathname.split('/').filter(Boolean);
    const topic = seg[0];
    const sub = seg[1] || '';
    const m = request.method;

    if (m === 'GET' && sub === 'ws') return this.openSocket(request, url);
    if (m === 'POST' && !sub) return this.postMessage(request, url, topic);
    if (m === 'POST' && sub === 'att') return this.postAttachment(request, url, topic);
    if (m === 'GET' && sub === 'a' && ATT_ID.test(seg[2] || '')) return this.getAttachment(seg[2]);
    if (m === 'GET' && sub === 'json') return this.history(url);
    return new Response('not found', { status: 404 });
  }

  /* ---------- WebSocket ---------- */
  openSocket(request, url) {
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('expected websocket', { status: 426 });
    if (this.ctx.getWebSockets().length >= MAX_SOCKETS) return new Response('room is full', { status: 503 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server); // Hibernation API：静かなあいだは、DOがメモリから降りて費用がかからない
    // 切れていた間の分を送り直す（受け取る側が、同じidを無視する）
    const since = Math.min(300, Math.max(0, +url.searchParams.get('since') || 0));
    if (since > 0) {
      const cut = nowS() - since;
      for (const e of this.ring) if (e.time >= cut) { try { server.send(JSON.stringify(e)); } catch (_) {} }
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage() { /* 送信は POST で行うので、受け取った内容は使いません（ping は自動で返答済み） */ }
  webSocketClose(ws, code) { try { ws.close(code && code !== 1005 && code !== 1006 ? code : 1000, 'bye'); } catch (_) {} }
  webSocketError() {}

  broadcast(json) {
    for (const ws of this.ctx.getWebSockets()) { try { ws.send(json); } catch (_) {} }
  }

  /* ---------- 送信 ---------- */
  async postMessage(request, url, topic) {
    const body = await request.text();
    if (!body || body.length > MAX_MSG) return new Response('bad size', { status: 413 });
    const tags = (url.searchParams.get('tags') || '').split(',').map(s => s.trim()).filter(s => /^[a-z]{1,8}$/.test(s)).slice(0, 3);
    const ev = { id: rid(), time: nowS(), event: 'message', topic, message: body };
    if (tags.length) ev.tags = tags;
    this.broadcast(JSON.stringify(ev));
    await this.remember(ev, tags.includes('bd'));
    return Response.json({ id: ev.id, time: ev.time, event: 'message' });
  }

  async remember(ev, isBoard) {
    const t = nowS();
    this.ring.push(ev);
    this.ring = this.ring.filter(e => e.time >= t - RING_TTL).slice(-RING_MAX);
    let size = 0;
    for (let i = this.ring.length - 1; i >= 0; i--) {
      size += (this.ring[i].message || '').length + 200;
      if (size > RING_BYTES) { this.ring = this.ring.slice(i + 1); break; }
    }
    const put = { ring: this.ring };
    if (isBoard) {
      this.board.push(ev);
      this.board = this.board.filter(e => e.time >= t - BOARD_TTL).slice(-150);
      put.board = this.board;
    }
    await this.ctx.storage.put(put);
  }

  /* ---------- 大きいアイコン ---------- */
  async postAttachment(request, url, topic) {
    const buf = await request.arrayBuffer();
    if (buf.byteLength < 28 || buf.byteLength > MAX_ATT) return new Response('bad size', { status: 413 });
    const msg = url.searchParams.get('m') || '';
    const u = url.searchParams.get('u') || '';
    if (msg.length > 600 || !/^[A-Z0-9]{1,6}$/.test(u)) return new Response('bad request', { status: 400 });

    const t = nowS();
    let list = (await this.ctx.storage.get('atts')) || [];
    const drop = [];
    list = list.filter(a => {
      const keep = a.exp > t && a.u !== u; // 期限切れと、同じ人の古いものは捨てる
      if (!keep) drop.push('att:' + a.id);
      return keep;
    });
    const id = rid();
    list.push({ id, u, exp: t + ATT_TTL });
    while (list.length > 20) drop.push('att:' + list.shift().id);
    if (drop.length) await this.ctx.storage.delete(drop);
    await this.ctx.storage.put({ ['att:' + id]: buf, atts: list });

    const ev = {
      id: rid(), time: t, event: 'message', topic, message: msg,
      attachment: { name: 'a.bin', size: buf.byteLength, url: new URL('/' + topic + '/a/' + id, url.origin).href },
    };
    this.broadcast(JSON.stringify(ev));
    return Response.json({ id: ev.id, time: ev.time, event: 'message' });
  }

  async getAttachment(id) {
    const list = (await this.ctx.storage.get('atts')) || [];
    const a = list.find(x => x.id === id);
    if (!a || a.exp <= nowS()) return new Response('gone', { status: 404 });
    const buf = await this.ctx.storage.get('att:' + id);
    if (!buf) return new Response('gone', { status: 404 });
    return new Response(buf, { headers: { 'Content-Type': 'application/octet-stream', 'Cache-Control': 'private, max-age=600' } });
  }

  /* ---------- 履歴（伝言板） ---------- */
  history(url) {
    const m = /^(\d{1,3})([smh])$/.exec(url.searchParams.get('since') || '12h');
    const span = m ? (+m[1]) * { s: 1, m: 60, h: 3600 }[m[2]] : BOARD_TTL;
    const cut = nowS() - Math.min(span, BOARD_TTL);
    const wantBoard = (url.searchParams.get('tags') || '').split(',').includes('bd');
    const src = wantBoard ? this.board : this.ring;
    const lines = src.filter(e => e.time >= cut).map(e => JSON.stringify(e)).join('\n');
    return new Response(lines, { headers: { 'Content-Type': 'application/x-ndjson', 'Cache-Control': 'no-store' } });
  }
}
