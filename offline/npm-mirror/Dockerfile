ARG VERDACCIO_IMAGE_TAG=6
FROM verdaccio/verdaccio:${VERDACCIO_IMAGE_TAG}
ARG MODE=offline
COPY config.${MODE}.yaml /verdaccio/conf/config.yaml
