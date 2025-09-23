// server.js
// PXT/VR テレメトリ受信サーバ（NDJSON 追記）
// ・単発 JSON, {events:[...]} バッチ, NDJSON に対応
// ・ヘッダ x-telemetry-token で簡易認証
// ・props.code はインライン保存（サイズ上限・ハッシュ付与・任意の切り詰め）
// ・ログ: LOG_DIR/tracking.log に 1イベント=1行

import fs from "fs";
import path from "path";
import express from "express";
import zlib from "zlib";
import crypto from "crypto";
import cors from "cors";

// ===== 設定 =====
const cfg = loadConfig();
ensureDir(cfg.logDir);

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
        methods: ["POST", "OPTIONS"],
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


// POST /api/telemetry: JSON/NDJSON/gzip を受ける
app.post(
  "/api/telemetry",
  express.raw({ type: "*/*", limit: cfg.bodyLimit }),
  (req, res) => {
    try {
      // 認証
      const token = req.header("x-telemetry-token");
      if (!cfg.token || token !== cfg.token) return res.sendStatus(401);

      // gzip 展開
      let buf = req.body || Buffer.from([]);
      const enc = (req.header("content-encoding") || "").toLowerCase();
      if (enc === "gzip" && buf.length) buf = zlib.gunzipSync(buf);

      const text = buf.toString("utf8").trim();
      if (!text) return res.sendStatus(400);

      // JSON か NDJSON かを判定
      let events;
      try {
        const payload = JSON.parse(text);
        events = Array.isArray(payload?.events) ? payload.events : [payload];
      } catch {
        // NDJSON 行ごと
        const lines = text.split(/\r?\n/).filter(Boolean);
        events = lines.map((l) => JSON.parse(l));
      }

      appendEvents(events, req);
      return res.sendStatus(204);
    } catch (e) {
      console.error("[telemetry] error:", e);
      return res.sendStatus(500);
    }
  }
);

// GET /healthz: ヘルスチェック
app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// 起動
const port = Number(process.env.PORT || cfg.port || 3000);
app.listen(port, () => {
  console.log(`[telemetry] listening on :${port}`);
  console.log(`[telemetry] log file: ${logFilePath()}`);
});

// ===== ヘルパ =====
function loadConfig() {
  // 既定値
  const base = {
    port: 3000,
    logDir: process.env.LOG_DIR || "/var/log/pxt",
    token: process.env.TOKEN || "please_change_me",
    bodyLimit: process.env.BODY_LIMIT || "2mb",
    inlineCode: (process.env.INLINE_CODE || "true").toLowerCase() === "true",
    maxCodeBytes: parseInt(process.env.MAX_CODE_BYTES || "524288", 10), // 512KB
    truncateInline:
      (process.env.TRUNCATE_CODE_INLINE || "false").toLowerCase() === "true",
    truncateKeepBytes: parseInt(process.env.TRUNCATE_KEEP_BYTES || "131072", 10) // 128KB
  };
  return base;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function logFilePath() {
  return path.join(cfg.logDir, "tracking.log");
}

function sha256Hex(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function guardInlineCode(obj) {
  if (!obj || typeof obj !== "object") return { ok: true, obj, hash: null, size: 0, truncated: false };

  const buf = Buffer.from(JSON.stringify(obj), "utf8");
  const hash = sha256Hex(buf);
  const size = buf.length;

  if (size <= cfg.maxCodeBytes) {
    return { ok: true, obj, hash, size, truncated: false };
  }
  if (!cfg.truncateInline) {
    return {
      ok: false,
      reason: `code too large (${size} bytes > ${cfg.maxCodeBytes})`,
      hash,
      size
    };
  }

  // 切り詰め（files[].content の合計サイズを制限）
  try {
    const copy = JSON.parse(buf.toString("utf8"));
    if (Array.isArray(copy.files)) {
      let budget = cfg.truncateKeepBytes;
      for (const f of copy.files) {
        if (typeof f?.content === "string") {
          const s = Buffer.from(f.content, "utf8");
          if (s.length > budget) {
            f.content = s.subarray(0, Math.max(0, budget)).toString("utf8");
            budget = 0;
          } else {
            budget -= s.length;
          }
          if (budget <= 0) break;
        }
      }
    }
    const truncatedBuf = Buffer.from(JSON.stringify(copy), "utf8");
    return {
      ok: true,
      obj: copy,
      hash,
      size: truncatedBuf.length,
      truncated: true
    };
  } catch {
    return { ok: false, reason: "truncate failed", hash, size };
  }
}

function normalizeEvent(ev, srcIp) {
  const out = { ...ev };

  // 最低限の型
  if (!out.ts) out.ts = new Date().toISOString();
  if (!out.source) out.source = "pxt";
  if (!out.props) out.props = {};

  // props.code をインラインで保持
  if (cfg.inlineCode && out.props.code) {
    const res = guardInlineCode(out.props.code);
    if (!res.ok) {
      out.props.code_error = {
        reason: res.reason,
        size: res.size,
        max: cfg.maxCodeBytes,
        code_hash: res.hash
      };
      delete out.props.code;
    } else {
      out.props.code_hash = res.hash;
      out.props.code_size = res.size;
      out.props.code_truncated = !!res.truncated;
      out.props.code = res.obj;
    }
  }

  // 受信メタ
  out.ingest_time = new Date().toISOString();
  out.src_ip = srcIp;

  return out;
}

function appendEvents(events, req) {
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").toString();
  const lines = [];
  for (const ev of events) {
    const norm = normalizeEvent(ev, ip);
    lines.push(JSON.stringify(norm));
  }
  fs.appendFileSync(logFilePath(), lines.join("\n") + "\n", "utf8");
}
