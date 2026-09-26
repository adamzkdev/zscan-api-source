# Runtime image from binaries cross-compiled on the host (scripts/docker-local.sh).
# Needs no network during the build, for machines where Docker's build network is restricted.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY docker/bin/zscan-indexer docker/bin/zscan-api docker/bin/zscan-verify /usr/local/bin/
