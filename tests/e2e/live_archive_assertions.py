"""Read the exact native ZIP input independently of its extraction choices."""
from collections import Counter
import hashlib
from pathlib import Path, PurePosixPath
import zipfile


def archive_real_inventory(extraction, database):
    source = Path(extraction['archive_path'])
    if not source.is_file():
        # The live Docker runner binds the protected stage home read-only at
        # the runtime DB's parent. Native receipts retain their host paths.
        parts = source.parts
        assert 'workspaces' in parts, source
        source = Path(database).parent.joinpath(*parts[parts.index('workspaces'):])
    assert source.is_file(), ('Exact extracted ZIP input is unavailable', source)
    with zipfile.ZipFile(source) as archive:
        records = [row for row in archive.infolist() if not row.is_dir()]
    names = [row.filename for row in records if not any(
        part == '__MACOSX' or part.startswith('._') or part == '.DS_Store'
        for part in PurePosixPath(row.filename).parts)]
    return {'record_count': len(names), 'names': names,
        'extension_counts': Counter(Path(name).suffix.casefold() for name in names),
        'metadata_count': len(records)-len(names), 'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
