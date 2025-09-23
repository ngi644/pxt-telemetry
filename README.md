# pxt-telemetry

PXT/VR 用の軽量テレメトリ受信サーバ。  
ローカルでは `/var/log/pxt/tracking.log` に JSON Lines で追記します。  
将来は Docker コンテナ化し、Cloud Run にそのまま載せ替え可。

## 📦 起動（ローカル）

```bash
npm install
npm run dev
# → http://localhost:3000/healthz で ok を確認
