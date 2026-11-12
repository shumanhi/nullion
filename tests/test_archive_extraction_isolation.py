"""Repeated/concurrent extractions never merge with prior files or overwrite them."""
import gzip,io,json,tarfile,zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest
from nullion.tools import _build_archive_extract_handler,ToolInvocation

@pytest.mark.parametrize('kind',['zip','tar.gz','gz'])
def test_repeat_and_parallel_extractions_have_owned_directories(tmp_path,kind):
    archive=tmp_path/('input.'+kind);data=b'Actual current data'
    if kind=='zip':
        with zipfile.ZipFile(archive,'w') as f:f.writestr('data.txt',data)
    elif kind=='tar.gz':
        with tarfile.open(archive,'w:gz') as f:
            info=tarfile.TarInfo('data.txt');info.size=len(data);f.addfile(info,io.BytesIO(data))
    else:
        with gzip.open(archive,'wb') as f:f.write(data)
    destination=tmp_path/'extracted';destination.mkdir();(destination/'data.txt').write_text('Prior version');(destination/'old.txt').write_text('Unrelated')
    handler=_build_archive_extract_handler(tmp_path,include_principal_workspace=False)
    def run(i):return handler(ToolInvocation(str(i),'archive_extract','fixture:user',{'path':str(archive),'output_dir':str(destination)}))
    with ThreadPoolExecutor(max_workers=3) as executor:results=list(executor.map(run,range(3)))
    roots=[]
    for result in results:
        assert result.status=='completed',result.error
        root=Path(result.output['output_dir']);roots.append(root)
        files=[p for p in root.rglob('*') if p.is_file()];assert len(files)==1 and files[0].read_bytes()==data
        assert result.output['requested_output_dir']==str(destination) and result.output['isolated_destination'] is True
        rows=json.loads(Path(result.output['manifest_json_path']).read_text())['rows'];assert len(rows)==1
        assert result.output['entry_count']==1
    assert len(set(roots))==3 and destination not in roots
    assert (destination/'data.txt').read_text()=='Prior version' and (destination/'old.txt').read_text()=='Unrelated'

def test_existing_destination_only_grant_does_not_authorize_sibling(tmp_path):
    allowed=tmp_path/'allowed';allowed.mkdir();archive=allowed/'input.zip'
    with zipfile.ZipFile(archive,'w') as f:f.writestr('data.txt','Actual')
    destination=tmp_path/'private'/'exact';destination.mkdir(parents=True);(destination/'personal.txt').write_text('Preserve')
    result=_build_archive_extract_handler(allowed,include_principal_workspace=False)(ToolInvocation('grant','archive_extract','fixture:user',{'path':str(archive),'output_dir':str(destination)},trusted_filesystem_selectors=(str(destination),)))
    assert result.status=='failed' and result.output['reason']=='output_dir_already_exists',result
    assert list(destination.parent.iterdir())==[destination] and (destination/'personal.txt').read_text()=='Preserve'
