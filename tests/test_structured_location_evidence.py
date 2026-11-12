import pytest
from nullion.agent_orchestrator import _completion_review_output_has_substantive_evidence

@pytest.mark.parametrize("location,expected", [("Providence", True), ({"name":"Providence", "latitude":41.8, "longitude":-71.4}, True), ({"latitude":41.8, "longitude":-71.4}, True), ({}, False), ({"status":"failed", "name":"Providence"}, False), ({"error":"not found", "name":"Providence"}, False), ({"status":"completed"}, False), ({"name":"Providence", "result_count":-1}, False), ({"name":"Providence", "kind":"unknown"}, False), ({"name":"Providence", "verified":False}, False), (22, False), (False, False)])
def test_location_schema_variants_do_not_invalidate_real_current_records(location, expected):
    assert _completion_review_output_has_substantive_evidence({"location":location, "current":{"temperature":36.3}, "source_url":"https://example.test/forecast"}) is expected
