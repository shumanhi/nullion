from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from nullion_e2e_manifest import E2E_COVERAGE_MANIFEST, manifest_counts, manifest_missing_rows


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run e2e coverage manifest checks",
)


def test_e2e_coverage_manifest_has_unique_complete_rows() -> None:
    seen: set[tuple[str, str]] = set()
    valid_statuses = {"covered", "partial", "planned"}
    for row in E2E_COVERAGE_MANIFEST:
        key = (row.area, row.name)
        assert key not in seen, f"duplicate e2e coverage row: {key}"
        seen.add(key)
        assert row.area
        assert row.name
        assert row.kind
        assert row.mock
        assert row.status in valid_statuses
        if row.status == "covered":
            assert row.tests, f"covered row must name at least one test: {key}"


def test_e2e_coverage_manifest_reports_current_gaps(tmp_path: Path) -> None:
    report_path = Path(os.environ.get("NULLION_E2E_COVERAGE_REPORT") or tmp_path / "nullion-e2e-coverage.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "area": row.area,
            "name": row.name,
            "kind": row.kind,
            "status": row.status,
            "mock": row.mock,
            "tests": list(row.tests),
            "notes": row.notes,
        }
        for row in E2E_COVERAGE_MANIFEST
    ]
    report = {
        "counts": manifest_counts(),
        "missing": [
            {
                "area": row.area,
                "name": row.name,
                "status": row.status,
                "mock": row.mock,
                "notes": row.notes,
            }
            for row in manifest_missing_rows()
        ],
        "rows": rows,
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    loaded = json.loads(report_path.read_text(encoding="utf-8"))
    assert loaded["counts"]["covered"] > loaded["counts"]["planned"]
    assert loaded["missing"] == []
