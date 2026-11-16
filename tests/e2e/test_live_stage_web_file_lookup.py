"""Live Web proves scoped absence is an answer, not a generation request."""
import os
from uuid import uuid4
import pytest
from tests.e2e.test_live_stage_web_matrix import live_web,_submit,catalogue

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in stage Web')

def test_live_web_missing_filename_finishes_without_creating_replacement(live_web,record_property):
    filename='no_such_document_'+uuid4().hex+'.pdf'
    response,turn,elapsed=_submit(live_web,f'Busca el archivo existente {filename} en mi espacio de trabajo. Si no existe, dilo brevemente. No crees ni adjuntes archivos.')
    results=turn.get('tool_results',[])
    searches=[r for r in results if r['tool_name']=='file_search' and r['status']=='completed']
    assert searches and any(r['output'].get('matches')==[] for r in searches),results
    assert turn.get('response_fulfilled') is True and not response.get('artifacts'),turn
    assert not any(r['status']=='completed' and r['tool_name'] in {'pdf_create','file_write','pdf_edit'} for r in results),results
    assert filename in response['text'],response
    assert elapsed<75000,elapsed
    record_property('filename_lookup_latency_ms',elapsed)
