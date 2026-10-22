"""Current-turn artifact selection shared by orchestration and delivery.

Files produced during repair are candidates, not an implicit delivery manifest.
Only explicit MEDIA directives or unambiguous producer receipts select outputs;
file names, user prose, and filename suffixes never imply revision ordering.
"""
from __future__ import annotations

from pathlib import Path
from collections.abc import Mapping


def identity(path: object) -> str:
    return str(Path(str(path)).expanduser().resolve())


def _output(result):
    value = result.get("output", {}) if isinstance(result, Mapping) else getattr(result, "output", {})
    return value if isinstance(value, Mapping) else {}


def _manifests(results):
    for result in results or ():
        value = _output(result).get("artifact_delivery_manifest")
        if isinstance(value, Mapping) and value.get("version") == 1 and isinstance(value.get("selected_paths"), list):
            yield value


def delivery_manifest(results):
    selected = []
    found = False
    for value in _manifests(results):
        found = True
        covered = {identity(p) for p in value.get("candidate_paths", value["selected_paths"]) if isinstance(p, str)}
        selected = [p for p in selected if identity(p) not in covered]
        selected.extend(p for p in value["selected_paths"] if isinstance(p, str))
    return tuple(dict.fromkeys(selected)) if found else None


def invalid_candidate_identities(results):
    invalid = set()
    for r in results or ():
        output = _output(r)
        status = r.get("status") if isinstance(r, Mapping) else getattr(r, "status", None)
        path = output.get("path") or output.get("artifact_path")
        if not isinstance(path, str):
            continue
        if status == "failed" and output.get("reason") in {"artifact_page_count_mismatch", "artifact_validation_failed"}:
            invalid.add(identity(path))
        elif status == "completed" and (output.get("artifact_path") or output.get("artifact_paths")):
            invalid.discard(identity(path))
    return invalid


def filter_delivery_paths(paths, results):
    paths = list(dict.fromkeys(str(p) for p in paths if str(p or "").strip()))
    invalid = invalid_candidate_identities(results)
    paths = [p for p in paths if identity(p) not in invalid]
    selected = delivery_manifest(results)
    decisions = {}
    for manifest in _manifests(results):
        covered = {identity(p) for p in manifest.get("candidate_paths", manifest["selected_paths"]) if isinstance(p, str)}
        allowed = {identity(p) for p in manifest["selected_paths"] if isinstance(p, str)}
        decisions.update({p: p in allowed for p in covered})
        decisions.update({p: True for p in allowed})
    paths = [p for p in paths if decisions.get(identity(p), True)]
    superseded = {identity(p) for r in results or () for p in _output(r).get("supersedes_artifact_paths", ())
                  if isinstance(p, str) and (r.get("status") if isinstance(r, Mapping) else getattr(r, "status", None)) == "completed"}
    explicitly_selected = {identity(p) for p in selected or ()}
    return [p for p in paths if identity(p) not in superseded or identity(p) in explicitly_selected]


def select_delivery_paths(paths, results, reply):
    """Return (selected, ambiguous). Preserve explicit multi-file deliveries."""
    from nullion.artifacts import parse_media_directive_line
    candidates = filter_delivery_paths(paths, results)
    invalid = invalid_candidate_identities(results)
    by_id = {identity(p): p for p in paths if identity(p) not in invalid}
    explicit = []
    for line in str(reply or "").splitlines():
        directive = parse_media_directive_line(line)
        if directive is not None and identity(directive.path) in by_id:
            explicit.append(by_id[identity(directive.path)])
    if explicit:
        return list(dict.fromkeys(explicit)), False
    # A prior runtime-owned selection remains authoritative for covered paths.
    # Any additional candidate needs a new selection, regardless of format:
    # different formats can also be drafts, conversions, or separate outputs.
    manifests = list(_manifests(results))
    covered = {identity(p) for manifest in manifests
               for p in manifest.get("candidate_paths", manifest["selected_paths"]) if isinstance(p, str)}
    if manifests and all(identity(p) in covered for p in candidates):
        return candidates, False
    return ([], True) if len(candidates) > 1 else (candidates, False)



def record_delivery_manifest(results, selected_paths, *, review_status=None, candidate_paths=None, failure_message=None):
    """Persist a scoped final decision; sibling tasks keep their own outputs."""
    from nullion.artifacts import artifact_paths_from_output_descriptors, output_has_artifact_descriptors
    results = list(results or ())
    covered = list(candidate_paths or ()) + list(selected_paths)
    for r in results:
        output = _output(r)
        if output_has_artifact_descriptors(output):
            covered.extend(artifact_paths_from_output_descriptors(output))
        else:
            path = output.get("artifact_path")
            if isinstance(path, str):
                covered.append(path)
            for key in ("artifact_paths", "artifacts"):
                values = output.get(key)
                if isinstance(values, (list, tuple)):
                    covered.extend(p for p in values if isinstance(p, str))
        prior = output.get("artifact_delivery_manifest")
        if isinstance(prior, Mapping):
            covered.extend(prior.get("candidate_paths", ()))
    for r in reversed(results):
        output = _output(r)
        if isinstance(output, dict):
            prior = output.get("artifact_delivery_manifest") or {}
            status = review_status
            if status is None and set(prior.get("selected_paths", ())) == set(selected_paths):
                status = prior.get("review_status")
            output["artifact_delivery_manifest"] = {"version": 1, "selected_paths": list(selected_paths),
                "candidate_paths": list(dict.fromkeys(covered)), "review_status": status,
                "failure_message": failure_message or (prior.get("failure_message") if not selected_paths else None)}
            return


def artifact_review_unavailable_reply(results):
    selected = delivery_manifest(results)
    if not selected:
        return None
    statuses = {}
    for manifest in _manifests(results):
        for p in manifest.get("candidate_paths", manifest["selected_paths"]):
            if isinstance(p, str):
                statuses[identity(p)] = manifest.get("review_status")
    if any(statuses.get(identity(p)) == "unavailable" for p in selected):
        noun = "file" if len(selected) == 1 else "files"
        return f"I created the attached {noun}, but the final content review could not be completed."
    return None


def artifact_terminal_failure_reply(results):
    if delivery_manifest(results):
        return None
    for manifest in reversed(list(_manifests(results))):
        if isinstance(manifest.get("failure_message"), str) and manifest["failure_message"].strip():
            return manifest["failure_message"]
    return None


def local_artifact_download_recovery(invocation, results):
    """Redirect mistaken downloads of already-created files without network access."""
    if invocation.tool_name != "file_download":
        return None
    from urllib.parse import urlparse, unquote
    from nullion.tools import ToolResult
    parsed = urlparse(str(invocation.arguments.get("url") or ""))
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        return None
    path = Path(unquote(parsed.path)).expanduser()
    known = set()
    for result in results or ():
        output = _output(result)
        status = result.get("status") if isinstance(result, Mapping) else getattr(result, "status", None)
        if status != "completed":
            continue
        for value in [output.get("artifact_path"), *(output.get("artifact_paths") or ())]:
            if isinstance(value, str):
                known.add(identity(value))
    if not path.is_absolute() or identity(path) not in known or not path.is_file():
        return None
    message = "This file is already available locally. Inspect it with file_read; no download is needed."
    return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {
        "reason": "local_artifact_already_available", "path": str(path), "suppress_activity": True,
        "recovery": {"kind": "inspect_local_artifact", "tool_name": "file_read", "arguments": {"path": str(path)}}}, message)
