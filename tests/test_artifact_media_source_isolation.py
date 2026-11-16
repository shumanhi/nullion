"""Presentation derivatives cannot become input files on a later inventory."""
import hashlib
from pathlib import Path

import pytest


@pytest.mark.parametrize('workspace', [False, True])
def test_optimized_raster_is_cached_outside_source_and_hidden_from_search(tmp_path,monkeypatch,workspace):
    import random
    from PIL import Image
    from nullion.tools import optimized_embedded_image_path,_build_file_search_handler,ToolInvocation
    home=tmp_path/'home';base=tmp_path/'workspaces'
    monkeypatch.setenv('NULLION_HOME',str(home));monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(base))
    root=base/'workspace_admin'/'artifacts'/'extracted' if workspace else tmp_path/'approved'/'extracted'
    root.mkdir(parents=True)
    source=root/'photo.png'
    Image.frombytes('RGB',(120,100),random.Random(37).randbytes(120*100*3)).save(source)
    before=hashlib.sha256(source.read_bytes()).hexdigest()
    cached=optimized_embedded_image_path(source)
    assert cached!=source and cached.is_file()
    assert not cached.is_relative_to(root)
    assert {p.relative_to(root) for p in root.rglob('*') if p.is_file()}=={Path('photo.png')}
    assert optimized_embedded_image_path(source)==cached
    assert hashlib.sha256(source.read_bytes()).hexdigest()==before
    result=_build_file_search_handler(tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False)(
        ToolInvocation('search','file_search','operator',{'pattern':'photo','root':str(tmp_path)}))
    assert result.status=='completed' and result.output['matches']==[str(source)]
