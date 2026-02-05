# pxt-telemetry

MakeCode 用 xAPI テレメトリ受信サーバ。
学習活動を xAPI 1.0.3 Statement 形式で記録します。

## ログ保存先

```
logs/xapi/YYYY/MM/DD.jsonl
```

日付ベースの JSONL ファイルに xAPI Statement を追記します。

## 起動（ローカル）

```bash
npm install
npm start
# → http://localhost:3000/healthz で ok を確認
```

## API エンドポイント

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/xapi/statements | xAPI Statement を受信・保存 |
| GET | /api/xapi/statements | 日付範囲で Statement を取得 |
| GET | /api/xapi/statements/:id | ID で Statement を取得 |
| GET | /api/xapi/stats | ストレージ統計情報 |
| GET | /healthz | ヘルスチェック |

## 環境変数

| 変数 | デフォルト | 説明 |
|------|-----------|------|
| PORT | 3000 | 待受ポート |
| LOG_DIR | ./logs | ログベースディレクトリ |
| TOKEN | please_change_me | 認証トークン |
| XAPI_LOG_DIR | {LOG_DIR}/xapi | xAPI ログディレクトリ |
| XAPI_MAX_FILE_SIZE | 104857600 | ファイルローテーション閾値 (100MB) |
| CORS_ORIGINS | http://localhost:*,http://127.0.0.1:* | 許可オリジン |

## 認証

リクエストヘッダに `X-Telemetry-Token` を含める必要があります。

```bash
curl -X POST http://localhost:3000/api/xapi/statements \
  -H "Content-Type: application/json" \
  -H "X-Telemetry-Token: please_change_me" \
  -d '{"id":"...", "actor":{...}, "verb":{...}, ...}'
```
