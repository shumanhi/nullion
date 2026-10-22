from nullion import version


def test_version_tag_normalizes_release_tag_prefix(monkeypatch) -> None:
    monkeypatch.setattr(version, "_git_version", lambda: "0.1.2")
    monkeypatch.setattr(version, "_package_metadata_version", lambda: "0.1.1")

    assert version.current_version() == "0.1.2"
    assert version.version_tag() == "v0.1.2"


def test_current_version_falls_back_to_package_metadata(monkeypatch) -> None:
    monkeypatch.setattr(version, "_git_version", lambda: "")
    monkeypatch.setattr(version, "_package_metadata_version", lambda: "0.1.1")

    assert version.current_version() == "0.1.1"
    assert version.version_tag() == "v0.1.1"


def test_normalize_version_strips_git_tag_prefix() -> None:
    assert version._normalize_version("v1.2.3") == "1.2.3"
    assert version._normalize_version("1.2.3") == "1.2.3"
