FROM python:3.14-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install scikit-build-core so pip can build turingdb
RUN pip install --upgrade pip scikit-build-core wheel setuptools

# Install turingdb 
RUN pip install --upgrade turingdb

