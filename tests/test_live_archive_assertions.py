"""Metadata inclusion during extraction does not change requested real files."""
import zipfile

import pytest


@pytest.mark.parametrize('mapped', [False, True])
@pytest.mark.parametrize('entry_count', [2, 5])
def test_real_inventory_counts_exact_zip_members_independently_of_extraction_metadata(tmp_path, mapped, entry_count):
    from tests.e2e.live_archive_assertions import archive_real_inventory
    source = tmp_path/'workspaces'/'admin'/'artifacts'/'source.zip'
    source.parent.mkdir(parents=True)
    with zipfile.ZipFile(source, 'w') as archive:
        for name in ['report.csv','nested/image.png','__MACOSX/._report.csv','nested/._image.png','.DS_Store']:
            archive.writestr(name,b'fixture')
    receipt = str(source) if not mapped else '/native/stage/workspaces/admin/artifacts/source.zip'
    result = archive_real_inventory({'archive_path':receipt,'entry_count':entry_count},str(tmp_path/'runtime.db'))
    assert result['record_count']==2 and result['metadata_count']==3
    assert result['names']==['report.csv','nested/image.png']
    assert result['extension_counts']=={'.csv':1,'.png':1}
    assert len(result['source_sha256'])==64
