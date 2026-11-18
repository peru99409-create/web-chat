# ウェブチャットの中継サーバー（Cloudflare）

ntfy.sh の代わりに、メッセージの受け渡しをするサーバーです。Cloudflare の無料プランで動きます。

## 公開の手順（GitHubとつないで自動で公開）

1. GitHub のリポジトリ `peru99409-create/web-chat` に、この `relay` フォルダ（`wrangler.toml` と `src/index.js`）を追加する。
2. Cloudflare の管理画面 → 「Workers & Pages」→「作成」→「Workers」→「Import a repository（リポジトリをインポート）」を選ぶ。
3. `peru99409-create/web-chat` を選ぶ。
4. 設定で次のようにする。
   - プロジェクト名（Worker名）: `wc-relay`
   - ルートディレクトリ（Root directory）: `/relay`
   - デプロイコマンド: `npx wrangler deploy`（そのままでOK）
5. 「デプロイ」を押す。終わると `https://wc-relay.〇〇.workers.dev` というURLが出る。
6. `index.html` の `const RELAY_URL='';` の中に、そのURLを入れて、GitHub に反映する。

```
const RELAY_URL='https://wc-relay.〇〇.workers.dev/';
```

`RELAY_URL` が空のままなら、これまでどおり ntfy.sh を使います。

## 確認

ブラウザで、Workerのトップ（`https://wc-relay.〇〇.workers.dev/`）を開いて `web-chat relay` と出れば動いています。

## 無料枠について

- Durable Objects は、無料プランでも SQLite 版なら使えます。
- WebSocket でつないでいる間は、メッセージを送る回数だけが数えられます（受け取りは数えられません）。
- 1日の上限を超えると送れなくなります。数字は変わるので、Cloudflare の料金ページで確認してください。

## 他のサイトから使うとき

`wrangler.toml` の `ALLOWED_ORIGINS` に、ウェブチャットを開くサイトのURLを入れます（カンマで区切って複数）。ここにないサイトからの接続は断ります。独自ドメインにしたときは、`index.html` の CSP（`connect-src`）にも、そのドメインを足してください。
