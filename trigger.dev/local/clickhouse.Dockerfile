# Bakes the upstream config overrides into the image instead of bind-mounting them
# (works on Docker hosts without host file sharing).
ARG CLICKHOUSE_IMAGE_TAG=26.2
FROM clickhouse/clickhouse-server:${CLICKHOUSE_IMAGE_TAG}
COPY data-paths.xml /etc/clickhouse-server/config.d/data-paths.xml
COPY override.xml /etc/clickhouse-server/config.d/override.xml
COPY users-override.xml /etc/clickhouse-server/users.d/override.xml
