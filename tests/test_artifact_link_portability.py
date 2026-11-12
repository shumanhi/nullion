"""Artifact inspection must distinguish a portable relative image from a local URL."""
from pathlib import Path
import pytest
from nullion.artifact_evidence import artifact_structure_evidence

@pytest.mark.parametrize("reference,portable", [("image.png", True), ("nested/../image.png", True), ("../image.png", False), ("file:image.png", False), ("file:///tmp/image.png", False), ("file://localhost/tmp/image.png", False)])
def test_local_image_dependencies_are_not_mistaken_for_embedded_assets(tmp_path, reference, portable):
    image = tmp_path / "image.png"
    image.write_bytes(b"image reference only")
    page = tmp_path / "page.html"
    page.write_text('<html><body><img src="' + reference + '"></body></html>')
    evidence = artifact_structure_evidence(page)
    assert evidence["decoded_embedded_raster_image_count"] == 0
    assert evidence["linked_local_image_assets"] == [{"reference": reference, "available_in_artifact_directory": portable, "portable_reference": portable}]
