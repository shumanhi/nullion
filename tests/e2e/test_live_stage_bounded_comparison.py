"""Real Web/provider coverage of a useful, scoped recommendation."""
import os
from pathlib import Path
import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1'
    or not os.environ.get('NULLION_LIVE_BOUNDED_COMPARISON_SOURCE'),reason='Authorized controlled stage source required')


def test_live_scoped_recommendation_resolves_real_contenders_without_exhaustive_listing_reads(live_web):
    page,frames,errors,database,conversation,base=live_web
    origin=os.environ['NULLION_LIVE_BOUNDED_COMPARISON_SOURCE'].rstrip('/')
    response,turn,elapsed=_submit(live_web,
        f'Recomiéndame una opción económica de {origin}/comparison/ y compárala con Reference B. '
        'Verifica el precio total y por gramo de las opciones seleccionadas. Quiero una recomendación práctica '
        'entre las opciones comprobadas, con enlaces directos y el alcance claro; no una auditoría de todo el catálogo. '
        'Examina cualquier alternativa prometedora que pueda cambiar la recomendación. No compres ni crees archivos.',timeout=240)
    assert '0.07' in response['text'] and '0.75' in response['text'],response
    assert origin+'/sample/2' in response['text'] and origin+'/sample/1' in response['text']
    assert 'Other relevant sources remain unchecked' not in response['text']
    # The producer owns completion. Verify its native records and published
    # answer instead of requiring the retired secondary grading event.
    assert turn.get('response_fulfilled') is True,turn
    reads=[row for result in turn.get('tool_results',[]) if result.get('status')=='completed'
        and result.get('tool_name')=='browser_read_pages'
        for row in result.get('output',{}).get('pages',[]) if row.get('status')=='completed']
    selected={row.get('page_url') for row in reads if row.get('read_kind')=='detail'}
    assert {origin+'/sample/1',origin+'/sample/2'}<=selected,reads
    discovered={item.get('url') for result in turn.get('tool_results',[])
        if result.get('status')=='completed' and result.get('tool_name') in {'browser_read_pages','browser_extract_items'}
        for item in result.get('output',{}).get('items',[]) if item.get('url')}
    for result in turn.get('tool_results',[]):
        if result.get('status')!='completed':continue
        output=result.get('output',{})
        records=[output.get('record',{}),*[row.get('record',{}) for row in output.get('details',[])]]
        for record in records:
            if str(record.get('url','')).rstrip('/')==origin+'/comparison':
                discovered.update(link['url'] for link in record.get('links',[]) if link.get('url'))
    assert discovered and (selected & discovered) < discovered, 'A bounded comparison must not read the entire catalog'
    assert elapsed < 90000, f'Bounded comparison exceeded 90 seconds: {elapsed/1000:.1f}s'
    evidence=__import__('json').dumps([r['output'] for r in turn.get('tool_results',[])
        if r.get('status')=='completed' and r.get('tool_name')=='browser_read_pages'],ensure_ascii=False)
    assert all(value in evidence for value in ('$7.00','$75.00','$0.07','$0.75'))
    assert not errors and not response['artifacts']
