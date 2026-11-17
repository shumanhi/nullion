"""Exact v0.11.0 parent probe (030e3c8), retained for upgrade compatibility."""
import logging
import os
from pathlib import Path
import subprocess
import sys
log = logging.getLogger(__name__)
def _src_dir():
    return Path(os.environ["NULLION_SRC_DIR"])

def fresh_health_check(
    port: int = 8742,
    timeout: float = 10.0,
    *,
    require_web: bool = True,
    require_messaging: bool = True,
    env_path: str | Path | None = None,
    checkpoint_path: str | Path | None = None,
    _warnings_out: list[str] | None = None,
) -> bool:
    """Run the installed updater's health check in a fresh Python process.

    After ``git pull`` and ``pip install -e .``, the current updater process is
    still executing the pre-update Python module.  Calling ``health_check()``
    directly would therefore use stale code.  This wrapper asks the freshly
    installed package to import and run its own health check.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "from nullion.updater import health_check\n"
                "port = int(sys.argv[1])\n"
                "timeout = float(sys.argv[2])\n"
                "require_web = sys.argv[3] == '1'\n"
                "require_messaging = sys.argv[4] == '1'\n"
                "env_path = sys.argv[5] or None\n"
                "checkpoint_path = sys.argv[6] or None\n"
                "warnings = []\n"
                "ok = health_check(\n"
                "    port=port,\n"
                "    timeout=timeout,\n"
                "    require_web=require_web,\n"
                "    require_messaging=require_messaging,\n"
                "    env_path=env_path,\n"
                "    checkpoint_path=checkpoint_path,\n"
                "    _warnings_out=warnings,\n"
                ")\n"
                "for w in warnings: print('PROVIDER_WARNING:', w)\n"
                "raise SystemExit(0 if ok else 1)\n"
            ),
            str(port),
            str(timeout),
            "1" if require_web else "0",
            "1" if require_messaging else "0",
            "" if env_path is None else str(env_path),
            "" if checkpoint_path is None else str(checkpoint_path),
        ],
        capture_output=True,
        text=True,
        timeout=45,
        cwd=_src_dir(),
    )
    if _warnings_out is not None:
        for line in (result.stdout or "").splitlines():
            if line.startswith("PROVIDER_WARNING:"):
                _warnings_out.append(line.removeprefix("PROVIDER_WARNING:").strip())
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        if detail:
            log.warning("Fresh health check failed: %s", detail)
        return False
    return True
