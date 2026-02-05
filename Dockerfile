# Dockerfile - Cloud Run 用
FROM node:20-slim

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .

# Cloud Run 既定ポート
ENV PORT=8080

# GCS ストレージバックエンド（Cloud Run 推奨）
# GCS_BUCKET は Cloud Run のサービス設定で指定
ENV STORAGE_BACKEND=gcs

# リクエストサイズ上限
ENV BODY_LIMIT=10mb

# 非 root ユーザーで実行（セキュリティ）
USER node

CMD ["node", "server.js"]
