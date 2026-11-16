"""Real Docker source preparation keeps historical evidence separate from output."""
import os
import errno
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get('NULLION_LIVE_STAGE_E2E') != '1'
    or os.environ.get('NULLION_E2E_CONTAINER') != '1',
    reason='Explicit live-runner preparation check; no model calls')


def test_candidate_references_read_only_history_without_copying_it():
    candidate = Path(os.environ['NULLION_APP_REPO']) / '.e2e-artifacts'
    original = Path('/candidate/.e2e-artifacts')
    assert candidate.is_symlink() and candidate.resolve() == original.resolve()
    historical = next((p for p in candidate.rglob('*') if p.is_file()), None)
    if historical is not None:
        with historical.open('rb') as stream:
            prefix = stream.read(32)
        with (original / historical.relative_to(candidate)).open('rb') as stream:
            assert stream.read(32) == prefix
    attempted = candidate / ('owned-read-only-probe-' + uuid4().hex)
    try:
        with pytest.raises(OSError) as rejected:
            attempted.write_text('This mount must reject output writes.')
        assert rejected.value.errno in {errno.EROFS, errno.EACCES, errno.EPERM}
    finally:
        if attempted.exists():
            attempted.unlink()
    output = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR']) / 'owned-output-probe'
    output.write_text('Writable output remains separate from historical input.')
    assert output.read_text().startswith('Writable output')
    output.unlink()
