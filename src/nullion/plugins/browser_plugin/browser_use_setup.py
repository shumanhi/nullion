"""Install the visual browser worker without changing the application environment.

This file is also executable directly by installers before configuration exists.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


def worker_python(home: Path) -> Path:
    return home / "browser-use-venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def install_worker(home: Path) -> Path:
    home = home.expanduser().resolve()
    python = worker_python(home)
    if not python.is_file():
        subprocess.run([sys.executable, "-m", "venv", str(python.parent.parent)], check=True, timeout=120)
    subprocess.run([str(python), "-m", "ensurepip", "--upgrade"], check=True, timeout=120)
    requirements = Path(__file__).with_name("requirements-browser-use.txt")
    subprocess.run([str(python), "-m", "pip", "install", "--disable-pip-version-check", "--quiet", "--upgrade", "-r", str(requirements)], check=True, timeout=1200)
    subprocess.run([str(python), "-m", "pip", "check"], check=True, timeout=60)
    subprocess.run([str(python), "-m", "playwright", "install", "chromium"], check=True, timeout=600)
    subprocess.run([str(python), "-c", "import browser_use; from playwright.sync_api import sync_playwright; from pathlib import Path; p=sync_playwright().start(); assert Path(p.chromium.executable_path).is_file(); p.stop()"], check=True, timeout=60)
    return python


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, required=True)
    args = parser.parse_args()
    print("Visual browser worker ready:", install_worker(args.home))


if __name__ == "__main__":
    main()
