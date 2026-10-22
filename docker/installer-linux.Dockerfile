FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        build-essential \
        ca-certificates \
        curl \
        expect \
        git \
        python3 \
        python3-dev \
        python3-pip \
        python3-venv \
        sudo \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
