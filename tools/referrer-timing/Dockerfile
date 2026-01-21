# Image for running the referrer timing test harness with cloud CLI tools
FROM google/cloud-sdk:slim

# Install AWS CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
    awscli \
    && rm -rf /var/lib/apt/lists/*

COPY referrer-timing-linux /referrer-timing

ENTRYPOINT ["/referrer-timing"]
