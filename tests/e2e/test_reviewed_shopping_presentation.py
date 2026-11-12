"""Catalog-shaped complete and partial replies through the real product boundary."""
import os
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container product boundary')


@pytest.mark.parametrize('surface',['web_api','web_ui','telegram'])
@pytest.mark.parametrize('outcome',['complete','partial'])
@pytest.mark.parametrize('prompt',['木材と硬い面向けの強い接着剤を価格で比較。簡潔に。',
    'Compara adhesivos fuertes para madera y superficies duras por precio. Responde de forma concisa.'])
def test_catalog_details_do_not_displace_a_compact_comparison(tmp_path,monkeypatch,surface,outcome,prompt):
    from tests.e2e.test_reviewed_presentation_correction_boundary import test_reviewer_corrects_composition_once_without_reopening_completed_source_work as replay
    replay(tmp_path,monkeypatch,surface,prompt,outcome,comparison=True)
