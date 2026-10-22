FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        git \
        nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip \
    && python -m pip install setuptools wheel setuptools-scm pytest pytest-asyncio coverage playwright \
    && python -m playwright install --with-deps chromium \
    && python -c "from pathlib import Path; from playwright.sync_api import sync_playwright; p = sync_playwright().start(); Path('/usr/local/bin/chromium').symlink_to(p.chromium.executable_path); p.stop()"

RUN python -m venv /opt/nullion-browser-use \
    && /opt/nullion-browser-use/bin/python -m pip install browser-use==0.13.10 playwright==1.59.0 \
    && /opt/nullion-browser-use/bin/python -m pip check \
    && /opt/nullion-browser-use/bin/python -m playwright install chromium
ENV NULLION_BROWSER_USE_TEST_PYTHON=/opt/nullion-browser-use/bin/python

WORKDIR /tmp/nullion-src
