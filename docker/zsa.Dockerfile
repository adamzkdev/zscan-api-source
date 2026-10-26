# ZStreet's ZSA Lab backend (zsa-svc, from zapp-chrome/zsa-engine), cross-compiled on the host:
#   cargo zigbuild --release --features svc --bin zsa-svc --target x86_64-unknown-linux-gnu.2.36
# then copied to docker/bin/zsa-svc. The sponsor seed is mounted at run time, never baked in.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY docker/bin/zsa-svc /usr/local/bin/zsa-svc
ENV ZSA_ADDR=0.0.0.0:8095 \
    ZSA_DATA_DIR=/data \
    ZSA_SEED_FILE=/run/secrets/zsa-sponsor.seed \
    MALLOC_ARENA_MAX=2
EXPOSE 8095
CMD ["zsa-svc"]
