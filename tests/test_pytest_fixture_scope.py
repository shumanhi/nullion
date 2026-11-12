import json
import pytest


def test_selected_e2e_cases_can_resolve_their_actual_scoped_fixture(request):
    fm=request._fixturemanager
    targets=[item for item in request.session.items if 'sourced_report_model_reply' in item.fixturenames]
    if not targets:
        pytest.skip('Mixed E2E fixture scope is checked only when those cases are selected')
    defs=fm._arg2fixturedefs.get('sourced_report_model_reply',())
    assert all(fm.getfixturedefs('sourced_report_model_reply',item) for item in targets), json.dumps({'defs':[{'baseid':d.baseid,'file':d.func.__code__.co_filename} for d in defs],
        'targets':[{'nodeid':item.nodeid,'parents':[n.nodeid for n in item.listchain()],
            'matched':bool(fm.getfixturedefs('sourced_report_model_reply',item))} for item in targets]},indent=2)
