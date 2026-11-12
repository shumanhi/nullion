"""Keep meaningful filenames without sharing a mutable export path."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion import artifacts


@pytest.mark.parametrize('prefix', ['MEDIA:', 'ARTIFACT:', 'MEDIA '])
@pytest.mark.parametrize('quoted', [False, True])
def test_shared_attachment_protocol_preserves_whole_unicode_path(monkeypatch, tmp_path, prefix, quoted):
    import json
    path=tmp_path/'日本語 folder'/'Quarterly report.csv';path.parent.mkdir();path.write_text('Value\n31\n')
    value=json.dumps(str(path),ensure_ascii=False) if quoted else str(path)
    directive=artifacts.parse_media_directive_line(prefix+value)
    assert directive.path==path
    caption,files=artifacts.split_media_reply_attachments('完了\n'+prefix+value,
        is_safe_attachment_path=lambda candidate:artifacts.path_is_within(candidate,tmp_path))
    assert files==(path,) and caption=='完了'


@pytest.mark.parametrize('prompt', ['Actualizar /files/比較 final.csv.', '比較 final.csv を添付。', 'أرفق 比較 final.csv'])
def test_native_filename_identity_is_not_shortened_to_an_ascii_suffix(prompt):
    from nullion.chat_operator import _requested_output_filenames
    assert _requested_output_filenames(prompt,known_filenames=['比較 final.csv'])==('比較 final.csv',)


def test_known_filename_must_match_a_whole_literal_not_a_different_filename():
    from nullion.chat_operator import _requested_output_filenames
    assert _requested_output_filenames('not_final.csv',known_filenames=['final.csv'])==('not_final.csv',)


def test_native_and_plain_filenames_keep_their_request_order():
    from nullion.chat_operator import _requested_output_filenames
    assert _requested_output_filenames('a.csv, 長い比較 final.csv, b.csv',
        known_filenames=['長い比較 final.csv','b.csv','a.csv'])==('a.csv','長い比較 final.csv','b.csv')


@pytest.mark.parametrize('prefix', ['MEDIA:', 'ARTIFACT:', 'MEDIA '])
@pytest.mark.parametrize('prompt', ['Entrega el resultado visual.', '画像の結果を添付してください。'])
def test_attachment_transport_path_does_not_select_an_output_filename(tmp_path, prefix, prompt):
    from nullion.chat_operator import (_filter_paths_to_requested_output_filenames_if_present,
        _filter_descriptors_to_requested_output_filenames_if_present)
    paths=[str(tmp_path/'内部 note.txt'),str(tmp_path/'final.svg')]
    reply='✓\n'+prefix+paths[0]
    assert _filter_paths_to_requested_output_filenames_if_present(prompt,paths,reply=reply)==paths
    descriptors=[SimpleNamespace(path=p) for p in paths]
    assert _filter_descriptors_to_requested_output_filenames_if_present(prompt,descriptors,reply=reply)==descriptors


@pytest.mark.parametrize('suffix', ['.csv', '.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.zip'])
def test_named_exports_keep_basename_and_independent_bytes(monkeypatch, tmp_path, suffix):
    root = tmp_path/'artifacts'
    monkeypatch.setattr(artifacts, 'artifact_root_for_principal', lambda _: root)
    filename = 'Résumé 乙' + suffix
    one = artifacts.artifact_path_for_generated_workspace_file(principal_id='web:exports', suffix=suffix, filename=filename)
    two = artifacts.artifact_path_for_generated_workspace_file(principal_id='web:exports', suffix=suffix, filename=filename)
    assert one.name == two.name == filename and one != two
    assert root in one.parents and root in two.parents
    one.parent.mkdir(parents=True,exist_ok=True);two.parent.mkdir(parents=True,exist_ok=True)
    one.write_bytes(b'first selected version');two.write_bytes(b'second selected version')
    assert one.read_bytes() == b'first selected version' and two.read_bytes() == b'second selected version'


@pytest.mark.parametrize('filename', ['', '.', '..', '../secret.csv', '/secret.csv', 'folder\\secret.csv', 'bad\0.csv', 'wrong.pdf'])
def test_named_export_cannot_escape_or_change_format(monkeypatch, tmp_path, filename):
    monkeypatch.setattr(artifacts, 'artifact_root_for_principal', lambda _: tmp_path/'artifacts')
    with pytest.raises(ValueError):
        artifacts.artifact_path_for_generated_workspace_file(principal_id='web:exports', suffix='.csv', filename=filename)


@pytest.mark.parametrize('suffix', ['.csv', '.pdf', '.docx', '.pptx', '.xlsx', '.html', '.png', '.zip'])
def test_delegated_relocation_preserves_name_and_separate_versions(monkeypatch, tmp_path, suffix):
    from nullion import deep_agent_runner
    root = tmp_path/'artifacts';root.mkdir()
    monkeypatch.setattr(artifacts, 'artifact_root_for_principal', lambda _: root)
    monkeypatch.setattr(deep_agent_runner, 'artifact_root_for_principal', lambda _: root)
    source = tmp_path/('比較' + suffix)
    first_bytes=(b'<!doctype html><html><body><h1>First selected bytes</h1><p>Verified report contents.</p></body></html>'
                 if suffix=='.html' else b'first selected bytes')
    second_bytes=(b'<!doctype html><html><body><h1>Second selected bytes</h1><p>Verified report contents.</p></body></html>'
                  if suffix=='.html' else b'second selected bytes')
    source.write_bytes(first_bytes)
    config = SimpleNamespace(task=SimpleNamespace(principal_id='web:exports'))
    first = deep_agent_runner._relocate_external_artifact_paths_for_task(config, [str(source)])
    source.write_bytes(second_bytes)
    second = deep_agent_runner._relocate_external_artifact_paths_for_task(config, [str(source)])
    assert len(first) == len(second) == 1 and first != second
    assert Path(first[0]).name == Path(second[0]).name == source.name
    assert Path(first[0]).read_bytes() == first_bytes
    assert Path(second[0]).read_bytes() == source.read_bytes() == second_bytes


def test_delegated_export_keeps_relative_assets_without_exporting_parent_contents(monkeypatch, tmp_path):
    from nullion import deep_agent_runner
    root=tmp_path/'artifacts';root.mkdir()
    monkeypatch.setattr(artifacts,'artifact_root_for_principal',lambda _:root)
    monkeypatch.setattr(deep_agent_runner,'artifact_root_for_principal',lambda _:root)
    site=tmp_path/'source';(site/'assets').mkdir(parents=True)
    page=site/'index.html';page.write_text('<!doctype html><html><body><h1>Chart</h1><img src="assets/plot.svg"></body></html>')
    asset=site/'assets/plot.svg';asset.write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    (site/'private.txt').write_text('unselected')
    config=SimpleNamespace(task=SimpleNamespace(principal_id='web:exports'))
    paths=deep_agent_runner._relocate_external_artifact_paths_for_task(config,[str(page),str(asset)])
    final=next(Path(path) for path in paths if path.endswith('.html'))
    assert final.name=='index.html' and final.read_bytes()==page.read_bytes()
    assert (final.parent/'assets/plot.svg').read_bytes()==asset.read_bytes()
    assert not (final.parent/'private.txt').exists()
