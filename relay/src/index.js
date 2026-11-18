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
