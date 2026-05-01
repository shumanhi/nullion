"""Helpers for restoring the local desktop shell after restarts."""
from __future__ import annotations

import os
import subprocess
import sys


def schedule_desktop_reload(*, port: int = 8742, delay_seconds: float = 2.0) -> None:
    """Restart the tray icon and reopen a fresh native Web UI window shortly.

    Restart callers usually exit their current process soon after returning a
    response, so the desktop work runs in a detached child after a short delay.
    """
    subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import sys,time;"
                "time.sleep(float(sys.argv[1]));"
                "from nullion.cli import _open_desktop_entrypoint;"
                "_open_desktop_entrypoint(port=int(sys.argv[2]), force_reload=True)"
            ),
            str(max(delay_seconds, 0.0)),
            str(port),
        ],
        cwd=os.getcwd(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
