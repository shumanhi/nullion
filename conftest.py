from __future__ import annotations

import os
from pathlib import Path
import sys


APP_ROOT = Path(
    os.environ.get(
        "NULLION_APP_REPO",
        Path(__file__).resolve().parent,
    )
).resolve()

SRC_ROOT = APP_ROOT / "src"
if SRC_ROOT.exists():
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(0, str(APP_ROOT))
