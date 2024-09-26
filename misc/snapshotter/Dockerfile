FROM alpine:3.17.0 AS base

FROM base AS nydus-sourcer
ARG TARGETPLATFORM
ARG NYDUS_VER=v2.2.5

RUN apk add -q --no-cache curl && \
  apk add -q --no-cache --upgrade grep && \
  curl -fsSL -O https://github.com/dragonflyoss/nydus/releases/download/$NYDUS_VER/nydus-static-$NYDUS_VER-linux-amd64.tgz && \
  echo $NYDUS_VER > /.nydus_version && \
  tar xzf nydus-static-$NYDUS_VER-linux-amd64.tgz && \
  rm nydus-static-$NYDUS_VER-linux-amd64.tgz && \
  mv nydus-static/* / \
  && rm -rf /nydus-overlayfs

FROM base AS kubectl-sourcer
ARG TARGETPLATFORM

RUN apk add -q --no-cache curl && \
  curl -fsSL -o /usr/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/"$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)"/bin/"$TARGETPLATFORM"/kubectl && \
  chmod +x /usr/bin/kubectl

FROM base
ARG DESTINATION=/opt/nydus-artifacts
ARG CONFIG_DESTINATION=${DESTINATION}/etc/nydus
ARG BINARY_DESTINATION=${DESTINATION}/usr/local/bin
ARG SCRIPT_DESTINATION=${DESTINATION}/opt/nydus

WORKDIR /root/
RUN apk add -q --no-cache libc6-compat bash python3 py3-pip && \
  pip3 install --no-cache-dir toml && \
  apk del py3-pip && \
  rm -rf /var/cache/apk/* /root/.cache

VOLUME /var/lib/containerd/io.containerd.snapshotter.v1.nydus
VOLUME /run/containerd-nydus

COPY --from=nydus-sourcer /.nydus_version /.nydus_version
COPY --from=kubectl-sourcer /usr/bin/kubectl /usr/bin/kubectl

RUN mkdir -p ${CONFIG_DESTINATION} ${BINARY_DESTINATION} ${SCRIPT_DESTINATION} /var/lib/containerd/io.containerd.snapshotter.v1.nydus/cache /tmp/blobs/
COPY --from=nydus-sourcer /nydus* ${BINARY_DESTINATION}/
COPY --chmod=755 containerd-nydus-grpc nydus-overlayfs ${BINARY_DESTINATION}/
COPY --chmod=755 snapshotter.sh ${SCRIPT_DESTINATION}/snapshotter.sh
COPY nydusd-config.fusedev.json ${CONFIG_DESTINATION}/nydusd-fusedev.json
COPY nydusd-config-localfs.json ${CONFIG_DESTINATION}/nydusd-localfs.json
COPY nydusd-config.fscache.json ${CONFIG_DESTINATION}/nydusd-fscache.json
COPY config.toml ${CONFIG_DESTINATION}/config.toml
COPY nydus-snapshotter.service ${DESTINATION}/etc/systemd/system/nydus-snapshotter.service
COPY --chmod=755 update_containerd_config /usr/local/bin/update_containerd_config
