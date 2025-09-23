# Dockerfile - Cloud Run / 任意のコンテナ環境用
FROM node:20-slim

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .

# Cloud Run 既定ポート
ENV PORT=8080
# Cloud Run ではローカルファイルの永続化は想定しないが、
# デバッグ時には /var/log/pxt に出すことも可（mountやsidecarで収集）
ENV LOG_DIR=/var/log/pxt
ENV BODY_LIMIT=2mb

CMD ["node", "server.js"]
