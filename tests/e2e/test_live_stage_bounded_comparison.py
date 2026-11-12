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
    reviews=catalogue._conversation_event_payloads(database,conversation,event_type='conversation.completion_review')
    assert reviews[-1]['disposition']=='complete' and not reviews[-1]['search_coverage_errors'],reviews
    assert reviews[-1]['comparison_mode']=='bounded',reviews[-1]
    coverage=reviews[-1]['search_coverage'];assert coverage['comparison_scope']['scope_disclosed']
    assert coverage['comparison_scope']['ranking_complete'] and coverage['coverage_sufficient']
    assert not errors and not response['artifacts']
