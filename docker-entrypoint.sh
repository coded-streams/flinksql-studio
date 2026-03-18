#!/bin/sh
# docker-entrypoint.sh — substitutes env vars into nginx config at runtime
set -e

echo "Str:::lab Studio starting..."
echo "  Gateway:    ${FLINK_GATEWAY_HOST}:${FLINK_GATEWAY_PORT}"
echo "  JobManager: ${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

# Create the UDF JAR directory served by nginx WebDAV
# This is the directory where uploaded JARs are stored and served from
mkdir -p /var/www/udf-jars
chmod 755 /var/www/udf-jars

# envsubst replaces all ${VAR} placeholders in the nginx config template
envsubst '${FLINK_GATEWAY_HOST} ${FLINK_GATEWAY_PORT} ${JOBMANAGER_HOST} ${JOBMANAGER_PORT}' \
    < /etc/nginx/templates/default.conf.template \
    > /etc/nginx/conf.d/default.conf

echo "nginx config written. Starting nginx..."
exec "$@"