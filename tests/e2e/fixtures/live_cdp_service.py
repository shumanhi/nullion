"""Disposable headed stage CDP browser; --headless is an explicit test option."""
import argparse
import asyncio
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parents[3]/'scripts'))
from live_stage_tcp_forwarder import forward_connection


def stop_process(process):
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill(); process.wait(timeout=5)


def start_display():
    if os.environ.get('DISPLAY'):
        return None
    executable = shutil.which('Xvfb')
    if not executable:
        raise RuntimeError('Headed browser checks require Xvfb; use the E2E Docker image or explicitly select --headless')
    display = ':99'
    child = subprocess.Popen([executable, display, '-screen', '0', '1280x960x24', '-nolisten', 'tcp'])
    try:
        for _ in range(50):
            if child.poll() is not None:
                raise RuntimeError('Virtual display exited before startup')
            if Path('/tmp/.X11-unix/X99').exists():
                os.environ['DISPLAY'] = display
                return child
            time.sleep(.1)
        raise TimeoutError('Virtual display did not become ready')
    except BaseException:
        stop_process(child)
        raise


async def serve(*, headless=False):
    display_child = None
    child = None
    try:
        if not headless:
            display_child = start_display()
        async with async_playwright() as playwright:
            executable = playwright.chromium.executable_path
        args = [executable, '--no-sandbox', '--disable-dev-shm-usage',
            '--remote-debugging-port=19225', '--user-data-dir=/tmp/stage-live-browser',
            '--no-first-run', 'about:blank']
        if headless:
            args.insert(1, '--headless=new')
        child = subprocess.Popen(args)
        stopped = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in [signal.SIGTERM, signal.SIGINT]:
            loop.add_signal_handler(sig, stopped.set)
        async def connect(reader, writer):
            await forward_connection(reader, writer, host='127.0.0.1', port=19225)
        server = await asyncio.start_server(connect, '0.0.0.0', 19224)
        async with server:
            await stopped.wait()
    finally:
        stop_process(child)
        stop_process(display_child)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--headless', action='store_true')
    asyncio.run(serve(headless=parser.parse_args().headless))
