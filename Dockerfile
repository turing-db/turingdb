FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY docker/turing_install/ /usr/local/

# Optional: set a default binary
# ENTRYPOINT ["/usr/local/bin/turingdb"]
