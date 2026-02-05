// server.js
// PXT xAPI テレメトリ受信サーバ
// ・xAPI 1.0.3 Statement形式で学習活動を記録
// ・ヘッダ x-telemetry-token で簡易認証
// ・ストレージ: STORAGE_BACKEND=local (default) or gcs

import fs from "fs";
import path from "path";
import express from "express";
import cors from "cors";
import xapiRoutes from "./routes/xapi.js";
import { configure as configureStorage, backendName } from "./storage/index.js";

// ===== 設定 =====
const cfg = loadConfig();

// ===== アプリ =====
const app = express();

// ===== CORS =====
// 許可するオリジン（複数可）を環境変数で指定（無指定はローカル開発用に http://localhost:* を許可）
const ALLOW_ORIGINS = (process.env.CORS_ORIGINS || "http://localhost:*,http://127.0.0.1:*")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

// ワイルドカード+Credentialsを避けるため、Originを見てマッチしたものだけ返す方式
const corsOptionsDelegate = (req, callback) => {
  const reqOrigin = req.header("origin");
  let corsOptions = { origin: false };

  if (reqOrigin) {
    const ok = ALLOW_ORIGINS.some(pat => {
      if (pat.endsWith(":*")) {
        // 例: http://localhost:* を許可
        const base = pat.slice(0, -2);
        return reqOrigin.startsWith(base);
      }
      return reqOrigin === pat;
    });
    if (ok) {
      corsOptions = {
        origin: reqOrigin,
        // 認証情報を使わないなら false のままでOK（fetch keepalive 可）
        credentials: false,
        methods: ["GET", "POST", "OPTIONS"],
        allowedHeaders: [
          "Content-Type",
          "X-Telemetry-Token",
          "Content-Encoding"
        ],
        maxAge: 86400 // プリフライトを1日キャッシュ
      };
    }
  }
  callback(null, corsOptions);
};

// すべてのルートにCORSを適用（プリフライト含む）
app.use(cors(corsOptionsDelegate));
app.options("*", cors(corsOptionsDelegate)); // 明示的にプリフライトを処理

// ===== xAPI Storage Configuration =====
const xapiLogDir = process.env.XAPI_LOG_DIR || path.join(cfg.logDir, "xapi");

if (backendName === "gcs") {
  // GCS backend configuration
  configureStorage({
    bucket: process.env.GCS_BUCKET || "pxt-xapi-logs",
    prefix: process.env.GCS_PREFIX || "xapi",
    projectId: process.env.GCS_PROJECT_ID,
  });
} else {
  // Local filesystem backend configuration
  ensureDir(xapiLogDir);
  configureStorage({
    baseDir: xapiLogDir,
    maxFileSize: parseInt(process.env.XAPI_MAX_FILE_SIZE || "104857600", 10),
  });
}

// ===== xAPI Routes (with authentication) =====
app.use("/api/xapi", (req, res, next) => {
  // Token authentication for xAPI endpoints
  const token = req.header("x-telemetry-token");
  if (!cfg.token || token !== cfg.token) {
    return res.sendStatus(401);
  }
  next();
}, xapiRoutes);

// GET /healthz: ヘルスチェック
app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// 起動
const port = Number(process.env.PORT || cfg.port || 3000);
app.listen(port, () => {
  console.log(`[xapi] listening on :${port}`);
  console.log(`[xapi] storage backend: ${backendName}`);
  if (backendName === "gcs") {
    console.log(`[xapi] GCS bucket: ${process.env.GCS_BUCKET || "pxt-xapi-logs"}`);
  } else {
    console.log(`[xapi] log dir: ${xapiLogDir}`);
  }
});

// ===== ヘルパ =====
function loadConfig() {
  // 既定値
  const base = {
    port: 3000,
    logDir: process.env.LOG_DIR || "./logs",
    token: process.env.TOKEN || "please_change_me",
    bodyLimit: process.env.BODY_LIMIT || "10mb",
  };
  return base;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
