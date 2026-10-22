from __future__ import annotations

import subprocess
from pathlib import Path

from nullion import updater


def _git(cwd: Path, *args: str) -> str:
    env = dict(subprocess.os.environ)
    env.pop("GIT_DIR", None)
    env.pop("GIT_WORK_TREE", None)
    result = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    return result.stdout.strip()


def _commit_file(repo: Path, name: str, content: str, message: str) -> str:
    path = repo / name
    path.write_text(content)
    _git(repo, "add", name)
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "--short", "HEAD")


def _origin_main_hash(repo: Path) -> str:
    return _git(repo, "rev-parse", "--short", "origin/main")


def _latest_tag(repo: Path) -> str:
    _git(repo, "fetch", "--quiet", "--prune", "--prune-tags", "--force", "origin", "refs/tags/*:refs/tags/*")
    return _git(repo, "describe", "--tags", "--abbrev=0", "--match", "v[0-9]*", "origin/main")


def _repo_with_release_and_unreleased_head(tmp_path: Path) -> tuple[Path, str, str]:
    origin = tmp_path / "origin.git"
    repo = tmp_path / "repo"
    _git(tmp_path, "init", "--bare", "--initial-branch", "main", str(origin))
    _git(tmp_path, "init", "--initial-branch", "main", str(repo))
    _git(repo, "config", "user.name", "Nullion Tests")
    _git(repo, "config", "user.email", "tests@example.com")
    release_commit = _commit_file(repo, "README.md", "release\n", "release")
    _git(repo, "tag", "v0.1.0")
    head_commit = _commit_file(repo, "README.md", "unreleased\n", "unreleased")
    _git(repo, "remote", "add", "origin", str(origin))
    _git(repo, "push", "origin", "main", "--tags")
    return repo, release_commit, head_commit


def test_release_update_target_ignores_unreleased_origin_main_commits(tmp_path: Path) -> None:
    repo, release_commit, head_commit = _repo_with_release_and_unreleased_head(tmp_path)

    target = updater._update_target("release", cwd=repo)

    assert target.ref.startswith("v")
    assert target.label == target.ref
    assert target.commit
    assert target.commit != head_commit


def test_hash_update_target_tracks_origin_main(tmp_path: Path) -> None:
    repo, _, head_commit = _repo_with_release_and_unreleased_head(tmp_path)

    target = updater._update_target("hash", cwd=repo)

    assert target.ref == "origin/main"
    assert len(target.commit) > 0
    assert target.commit != ""
