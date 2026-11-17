"""The old updater must accept the new release within its original deadline."""
import importlib.util
import os
from pathlib import Path
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def test_v0110_parent_accepts_two_slow_required_bootstraps(tmp_path, monkeypatch):
    from nullion import updater
    source = Path(updater.__file__).parent
    package = tmp_path / 'nullion'
    package.mkdir()
    (package / '__init__.py').write_text(f'__path__.append({str(source)!r})\n')
    # Only external application construction is faked. Both real subprocess
    # boundaries, the installed health implementation and the old parent run.
    marker = tmp_path / 'web'
    (package / 'web_app.py').write_text(
        'import time\nfrom pathlib import Path\nfrom types import SimpleNamespace\n'
        '_STARTUP_WARNINGS = []\ndef _build_runtime():\n'
        f'    time.sleep(24)\n    Path({str(marker)!r}).write_text("checked")\n'
        '    return SimpleNamespace(model_client=True), None, None\n')
    marker2 = tmp_path / 'telegram'
    (package / 'telegram_entrypoint.py').write_text(
        'import time\nfrom pathlib import Path\n'
        'def build_runtime_service_from_settings(**kwargs):\n'
        f'    time.sleep(24)\n    Path({str(marker2)!r}).write_text("checked")\n')
    (package / 'config.py').write_text(
        'from types import SimpleNamespace as S\n'
        'def load_settings(**kwargs):\n'
        '    return S(telegram=S(bot_token="fixture"), slack=S(enabled=False), discord=S(enabled=False))\n')
    (package / 'messaging_runtime.py').write_text('def build_messaging_runtime_service_from_settings(**kwargs): pass\n')
    monkeypatch.setenv('PYTHONPATH', str(tmp_path))
    monkeypatch.setenv('NULLION_SRC_DIR', str(tmp_path))
    fixture = Path(__file__).parent / 'fixtures/updater_v0110_fresh_health.py'
    spec = importlib.util.spec_from_file_location('old_release_probe', fixture)
    old = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(old)
    start = time.monotonic()
    assert old.fresh_health_check(require_web=False, checkpoint_path=tmp_path / 'runtime.db')
    elapsed = time.monotonic() - start
    assert marker.exists() and marker2.exists()
    assert 24 <= elapsed < 43, elapsed
    print({'old_parent_health_seconds': elapsed, 'old_deadline_seconds': 45})
