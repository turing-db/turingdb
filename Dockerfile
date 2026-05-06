# Stage 1: build the turingdb-visualizer frontend bundle
FROM node:22-slim AS visualizer-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /vis
RUN git clone --depth 1 https://github.com/turing-db/turingdb-visualizer.git . \
 && npm install \
 && npm run build

# Stage 2: runtime image
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        nodejs \
        npm \
    && rm -rf /var/lib/apt/lists/* \
 && npm install -g serve \
 && npm cache clean --force

# Install uv (standalone binary) to manage the Python 3.14 runtime the wheel targets.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python
RUN uv python install 3.14

COPY wheel/*.whl /tmp/wheel/
RUN uv venv --python 3.14 /opt/turingdb-venv \
 && uv pip install --python /opt/turingdb-venv/bin/python /tmp/wheel/*.whl \
 && rm -rf /tmp/wheel

ENV PATH=/opt/turingdb-venv/bin:$PATH

COPY --from=visualizer-builder /vis/dist /opt/turingdb-visualizer

ENV TURINGDB_VIS_DIR=/opt/turingdb-visualizer
ENV TURINGDB_VIS_PORT=3000
ENV TURINGDB_VIS_URL=http://127.0.0.1:3000

COPY run_visualizer.sh /usr/local/bin/run_visualizer.sh
RUN chmod +x /usr/local/bin/run_visualizer.sh

ENTRYPOINT ["/usr/local/bin/run_visualizer.sh"]
CMD ["turingdb", "--help"]
