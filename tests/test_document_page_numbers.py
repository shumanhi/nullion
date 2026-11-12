"""Dynamic native page fields are explicit tool input and inspected file facts."""
from pathlib import Path
from zipfile import ZipFile

import pytest

from nullion.tools import ToolInvocation, _build_document_create_handler
from nullion.artifact_evidence import _native_layout_evidence


@pytest.mark.parametrize('enabled', [None, False, True])
def test_document_numbering_is_explicit_and_reviewer_reads_actual_fields(tmp_path, enabled):
    path = tmp_path/'document.docx'
    args = {'output_path': str(path), 'title': '記録', 'paragraphs': ['Page 1 — this body label is not a page field.']}
    if enabled is not None:
        args['page_numbers'] = enabled
    result = _build_document_create_handler(tmp_path)(ToolInvocation('doc', 'document_create', 'operator', args))
    assert result.status == 'completed', result
    assert _native_layout_evidence(path)['footer_page_number_field_count'] == int(enabled is True)
    assert ('dynamic_page_numbers' in result.output['layout_features']) is (enabled is True)
    if enabled:
        from xml.etree import ElementTree as ET
        with ZipFile(path) as package:
            fields = [node.get('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}instr', '')
                for name in package.namelist() if name.startswith('word/footer') and name.endswith('.xml')
                for node in ET.fromstring(package.read(name)).iter() if node.tag.endswith('}fldSimple')]
        assert any('PAGE' in value.split() for value in fields)


@pytest.mark.parametrize('value,suffix', [('yes', '.docx'), (1, '.docx'), (True, '.html'), (True, '.pdf')])
def test_unsupported_numbering_input_does_not_silently_write_unnumbered_file(tmp_path, value, suffix):
    path = tmp_path/('document'+suffix)
    result = _build_document_create_handler(tmp_path)(ToolInvocation('doc', 'document_create', 'operator',
        {'output_path': str(path), 'title': 'Informe', 'paragraphs': ['37'], 'page_numbers': value}))
    assert result.status == 'failed' and not path.exists(), result
    from nullion.tool_repair_contract import tool_repair_kind
    from nullion.agent_orchestrator import _should_block_failed_tool_during_recovery
    assert tool_repair_kind(result) == 'correct_arguments'
    assert not _should_block_failed_tool_during_recovery(result)
