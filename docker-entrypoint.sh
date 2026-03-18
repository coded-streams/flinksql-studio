#!/bin/sh
# docker-entrypoint.sh — Str:::lab Studio v1.2.4
# Works in Docker Compose, Kubernetes, and local environments.
set -e

echo "Str:::lab Studio starting..."
echo "  Gateway:    ${FLINK_GATEWAY_HOST}:${FLINK_GATEWAY_PORT}"
echo "  JobManager: ${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

# ── DNS resolver ──────────────────────────────────────────────────────────
# Read nameserver from /etc/resolv.conf — correct for all environments:
#   Docker Compose : 127.0.0.11
#   Kubernetes     : cluster DNS IP from pod's resolv.conf
#   Local          : host DNS
# MUST be exported so envsubst substitutes ${RESOLVER} in the nginx template.
RESOLVER=$(awk 'NF==2 && $1=="nameserver" {print $2; exit}' /etc/resolv.conf)
RESOLVER=${RESOLVER:-127.0.0.11}
export RESOLVER
echo "  DNS resolver: ${RESOLVER}"

# ── UDF JAR storage directory ─────────────────────────────────────────────
# nginx WebDAV PUT saves JARs here. Must be owned by the nginx worker user
# so the worker process can write uploaded files. Without chown, PUT returns
# HTTP 500 (Permission denied).
mkdir -p /var/www/udf-jars
chown -R nginx:nginx /var/www/udf-jars
chmod 755 /var/www/udf-jars
echo "  JAR storage: /var/www/udf-jars (owned by nginx)"

# ── Write nginx config ────────────────────────────────────────────────────
# List all substituted vars explicitly to prevent envsubst from replacing
# nginx runtime variables like $host, $uri, $remote_addr, $flink_gateway etc.
envsubst '${FLINK_GATEWAY_HOST} ${FLINK_GATEWAY_PORT} ${JOBMANAGER_HOST} ${JOBMANAGER_PORT} ${RESOLVER}' \
    < /etc/nginx/templates/default.conf.template \
    > /etc/nginx/conf.d/default.conf

echo "nginx config written. Starting nginx..."
exec "$@"