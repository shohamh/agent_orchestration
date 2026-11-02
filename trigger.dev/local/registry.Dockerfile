# Bakes the htpasswd into the image instead of bind-mounting it, so the stack also works on
# Docker hosts without host file sharing. Rebuild after rotating the password:
#   docker compose up -d --build registry
ARG REGISTRY_IMAGE_TAG=2
FROM registry:${REGISTRY_IMAGE_TAG}
COPY auth.htpasswd /auth/htpasswd
