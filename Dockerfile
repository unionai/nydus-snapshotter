FROM ghcr.io/containerd/nydus-snapshotter:v0.10.0

RUN apk add go delve make git
RUN git clone https://github.com/unionai/nydus-snapshotter.git && \
    cd nydus-snapshotter && \
    git checkout v0.10.0-gar-patch
# TOD make Nydus work with S3, need to modify these three files: daemonconfig.go, fuse.go, and s3.go
RUN cd nydus-snapshotter && make debug
RUN cd nydus-snapshotter && install -D -m 755 bin/containerd-nydus-grpc /usr/local/bin/containerd-nydus-grpc

COPY ./nydusd /usr/local/bin/nydusd
