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


def _artifact_source_aliases(results, candidate_identities):
    """Resolve provenance only to already eligible, completed deliverables."""
    aliases = {}
    for result in results or ():
        status = result.get("status") if isinstance(result, Mapping) else getattr(result, "status", None)
        if status != "completed":
            continue
        for descriptor in _output(result).get("artifact_descriptors") or ():
            if not isinstance(descriptor, Mapping) or descriptor.get("role") != "deliverable":
                continue
            source = descriptor.get("source_path")
            target = descriptor.get("path")
            if (isinstance(source, str) and source and isinstance(target, str)
                    and identity(target) in candidate_identities):
                aliases.setdefault(identity(source), set()).add(identity(target))
    return aliases


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


def artifact_candidate_versions(results):
    """Identify completed producer receipts without treating inspections as writes."""
    versions = {}
    for result in results or ():
        status = result.get("status") if isinstance(result, Mapping) else getattr(result, "status", None)
        invocation_id = result.get("invocation_id") if isinstance(result, Mapping) else getattr(result, "invocation_id", None)
        output = _output(result)
        receipt_id = output.get("artifact_producer_receipt_id")
        if isinstance(receipt_id, str) and receipt_id:
            invocation_id = receipt_id
        if status != "completed" or not isinstance(invocation_id, str) or not invocation_id:
            continue
        descriptors = output.get("artifact_descriptors")
        if isinstance(descriptors, (list, tuple)):
            paths = [row.get("path") for row in descriptors
                     if isinstance(row, Mapping) and row.get("role") == "deliverable"]
        else:
            many = output.get("artifact_paths")
            paths = [output.get("artifact_path"), *(many if isinstance(many, (list, tuple)) else ())]
        for path in paths:
            if isinstance(path, str) and path:
                versions[identity(path)] = invocation_id
    return versions


def _current_manifest_decisions(results):
    current = artifact_candidate_versions(results)
    decisions = {}
    for manifest in _manifests(results):
        covered = {identity(path) for path in manifest.get("candidate_paths", manifest["selected_paths"]) if isinstance(path, str)}
        allowed = {identity(path) for path in manifest["selected_paths"] if isinstance(path, str)}
        snapshot = manifest.get("candidate_versions")
        for path in covered | allowed:
            if (isinstance(snapshot, Mapping) and path in snapshot
                    and current.get(path) != snapshot[path]):
                # A newer producer requires a fresh choice and content review.
                decisions.pop(path, None)
            else:
                decisions[path] = path in allowed
    return decisions


def restored_delivery_paths(results):
    """Current validated sibling selections may restore a packaged member format."""
    results = tuple(results or ())
    current = artifact_candidate_versions(results)
    decisions = _current_manifest_decisions(results)
    restored = set()
    for manifest in _manifests(results):
        selected = {identity(path) for path in manifest["selected_paths"] if isinstance(path, str)}
        versions = manifest.get("candidate_versions") or {}
        for path in manifest.get("restored_format_paths") or ():
            if not isinstance(path, str):
                continue
            key = identity(path)
            if (key in selected and decisions.get(key) is True and versions.get(key)
                    and versions.get(key) == current.get(key)):
                restored.add(key)
    return frozenset(restored)


def reviewed_delivery_extensions(results):
    """Current completed manifest scopes own their independently requested formats."""
    results = tuple(results or ())
    current = artifact_candidate_versions(results)
    decisions = _current_manifest_decisions(results)
    covered, extensions, found = set(), [], False
    for manifest in reversed(list(_manifests(results))):
        selected = {identity(path) for path in manifest["selected_paths"] if isinstance(path, str)}
        shadowed = bool(selected.intersection(covered))
        covered.update(identity(path) for path in manifest.get("candidate_paths", manifest["selected_paths"]) if isinstance(path, str))
        value = manifest.get("required_artifact_extensions")
        versions = manifest.get("candidate_versions") or {}
        if (not shadowed and manifest.get("review_status") == "completed" and isinstance(value, list)
                and all(isinstance(extension, str) and extension.startswith(".") for extension in value)
                and selected and all(decisions.get(path) is True and versions.get(path)
                    and versions.get(path) == current.get(path) for path in selected)):
            extensions.extend(value)
            found = True
    return tuple(dict.fromkeys(extensions)) if found else None


def filter_delivery_paths(paths, results):
    paths = list(dict.fromkeys(str(p) for p in paths if str(p or "").strip()))
    invalid = invalid_candidate_identities(results)
    paths = [p for p in paths if identity(p) not in invalid]
    selected = delivery_manifest(results)
    decisions = _current_manifest_decisions(results)
    paths = [p for p in paths if decisions.get(identity(p), True)]
    superseded = set()
    for result in results or ():
        produced = set(artifact_candidate_versions([result]))
        if not produced:
            continue
        superseded.update(identity(path) for path in _output(result).get("supersedes_artifact_paths", ())
                          if isinstance(path, str) and identity(path) not in produced)
        # In-place edits and later writes are current producer versions, even
        # when that pathname was an input to an earlier replacement.
        superseded.difference_update(produced)
    explicitly_selected = {identity(p) for p in selected or ()}
    return [p for p in paths if identity(p) not in superseded or identity(p) in explicitly_selected]


def select_delivery_paths(paths, results, reply, *, required_extensions=()):
    """Return (selected, ambiguous). Preserve explicit multi-file deliveries."""
    from nullion.artifacts import parse_media_directive_line, artifact_format_extension
    candidates = filter_delivery_paths(paths, results)
    # Typed output formats separate requested deliverables from supporting
    # verification files. They never decide revision ordering within a format.
    formats = {artifact_format_extension("file" + str(extension).lower()) for extension in required_extensions}
    if formats:
        candidates = [path for path in candidates if artifact_format_extension(path) in formats]
    # A typed runtime choice owns the current versions. Later narration may
    # mention only a bundle or accidentally name a discarded working copy;
    # neither changes that choice. New or revised candidates require a fresh
    # structured selection before prose can choose their delivery.
    manifests = list(_manifests(results))
    covered = set(_current_manifest_decisions(results))
    if any(isinstance(manifest.get("candidate_versions"), Mapping) and manifest["candidate_versions"]
            for manifest in manifests):
        if all(identity(path) in covered for path in candidates):
            return candidates, False
        return [], True
    invalid = invalid_candidate_identities(results)
    by_id = {identity(p): p for p in paths if identity(p) not in invalid
             and (not formats or artifact_format_extension(p) in formats)}
    aliases = _artifact_source_aliases(results, by_id)
    explicit = []
    for line in str(reply or "").splitlines():
        directive = parse_media_directive_line(line)
        if directive is not None:
            key = identity(directive.path)
            if key in by_id:
                explicit.append(by_id[key])
            elif key in aliases:
                if len(aliases[key]) != 1:
                    return [], True
                explicit.append(by_id[next(iter(aliases[key]))])
    if explicit:
        return list(dict.fromkeys(explicit)), False
    if manifests and all(identity(path) in covered for path in candidates):
        return candidates, False
    # Distinct explicitly requested formats have no revision-order ambiguity
    # when each has exactly one completed native producer. Terminal review
    # outages may replace the caption, so prose cannot be their only selector.
    # Existing manifest exclusions and revised-candidate checks above still own
    # prior decisions; multiple candidates of one format still need selection.
    if len(formats) > 1 and len(candidates) == len(formats):
        by_format = {artifact_format_extension(path): path for path in candidates}
        produced = artifact_candidate_versions(results)
        if (set(by_format) == formats and len(by_format) == len(candidates)
                and all(identity(path) in produced for path in candidates)):
            return candidates, False
    return ([], True) if len(candidates) > 1 else (candidates, False)



def record_delivery_manifest(results, selected_paths, *, review_status=None, candidate_paths=None, failure_message=None,
                             review_error=None, selection_diagnostic=None, restored_format_paths=None, required_extensions=None):
    """Persist a scoped final decision; sibling tasks keep their own outputs."""
    from nullion.artifacts import artifact_paths_from_output_descriptors, output_has_artifact_descriptors
    results = list(results or ())
    # Stored conversation receipts can be reconstructed with transport-local
    # invocation ids. Keep the producer version stable across that boundary.
    for result in results:
        output = _output(result)
        status = result.get("status") if isinstance(result, Mapping) else getattr(result, "status", None)
        receipt_id = result.get("invocation_id") if isinstance(result, Mapping) else getattr(result, "invocation_id", None)
        if (isinstance(output, dict) and status == "completed" and isinstance(receipt_id, str) and receipt_id
                and (output.get("artifact_path") or output.get("artifact_paths") or output.get("artifact_descriptors"))):
            output.setdefault("artifact_producer_receipt_id", receipt_id)
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
            restored = (restored_format_paths if restored_format_paths is not None else
                prior.get("restored_format_paths", ()) if set(prior.get("selected_paths", ())) == set(selected_paths) else ())
            selected_ids = {identity(path) for path in selected_paths}
            output["artifact_delivery_manifest"] = {"version": 1, "selected_paths": list(selected_paths),
                "restored_format_paths": [path for path in restored if identity(path) in selected_ids],
                "candidate_versions": artifact_candidate_versions(results),
                "candidate_paths": list(dict.fromkeys(covered)), "review_status": status,
                "failure_message": failure_message or (prior.get("failure_message") if not selected_paths else None)}
            required = required_extensions
            if required is None and set(prior.get("selected_paths", ())) == set(selected_paths):
                required = prior.get("required_artifact_extensions")
            if required is not None:
                from nullion.artifacts import normalize_artifact_extensions
                output["artifact_delivery_manifest"]["required_artifact_extensions"] = list(normalize_artifact_extensions(required))
            if review_error is not None:
                output["artifact_delivery_manifest"]["review_error"] = str(review_error)[:200]
            elif status == "unavailable" and prior.get("review_error"):
                output["artifact_delivery_manifest"]["review_error"] = prior["review_error"]
            diagnostic = selection_diagnostic or prior.get("selection_diagnostic")
            if diagnostic is not None:
                output["artifact_delivery_manifest"]["selection_diagnostic"] = diagnostic
            return



def adopt_requested_filename_aliases(results, aliases):
    """Runtime naming copies inherit only current, selected manifest decisions.

    Called with receipts produced by the runtime alias materializer, never with
    model-supplied tool arguments. Preserve each original manifest's versions
    and review state so a later write still invalidates its approval.
    """
    import copy
    import hashlib
    results, aliases = tuple(results), tuple(aliases)
    current = artifact_candidate_versions(results)
    decisions = _current_manifest_decisions(results)
    replacements = {}
    for alias in aliases:
        output = _output(alias)
        source, target = output.get("source_path"), output.get("artifact_path")
        if (output.get("requested_filename_alias") is not True or alias.status != "completed"
                or not isinstance(source, str) or not isinstance(target, str)
                or decisions.get(identity(source)) is not True):
            continue
        try:
            if (Path(source).suffix.lower() != Path(target).suffix.lower()
                    or not Path(source).is_file() or not Path(target).is_file()):
                continue
            def digest(path):
                value = hashlib.sha256()
                with Path(path).open("rb") as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        value.update(chunk)
                return value.digest()
            if digest(source) != digest(target):
                continue
        except OSError:
            continue
        replacements[identity(source)] = (target, alias)
    for manifest in list(_manifests(results)):
        versions = manifest.get("candidate_versions") or {}
        active = {identity(path): replacements[identity(path)] for path in manifest["selected_paths"]
            if identity(path) in replacements and versions.get(identity(path)) == current.get(identity(path))}
        if not active:
            continue
        adopted = copy.deepcopy(manifest)
        adopted["selected_paths"] = [active[identity(path)][0] if identity(path) in active else path
                                     for path in manifest["selected_paths"]]
        adopted["candidate_paths"] = list(dict.fromkeys([*manifest.get("candidate_paths", manifest["selected_paths"]),
                                                       *(target for target, alias in active.values())]))
        adopted["candidate_versions"] = {**versions, **artifact_candidate_versions([alias for target, alias in active.values()])}
        adopted["restored_format_paths"] = [active[identity(path)][0] if identity(path) in active else path
                                            for path in manifest.get("restored_format_paths") or ()]
        # One new scoped decision follows the aliases without rewriting history.
        list(active.values())[-1][1].output["artifact_delivery_manifest"] = adopted


def artifact_review_unavailable(results) -> bool:
    """Read diagnostic state without selecting public content."""
    selected = delivery_manifest(results)
    if not selected:
        return False
    statuses = {}
    for manifest in _manifests(results):
        for p in manifest.get("candidate_paths", manifest["selected_paths"]):
            if isinstance(p, str):
                statuses[identity(p)] = manifest.get("review_status")
    if any(statuses.get(identity(p)) == "unavailable" for p in selected):
        return True
    return False


def artifact_terminal_failure_reply(results, *, user_message=None):
    from nullion.request_delivery_requirements import reviewed_request_delivery_requirements
    current = reviewed_request_delivery_requirements(results, user_message)
    if current is not None:
        if isinstance(current.get('partial_reply'), str) and current['partial_reply'].strip():
            return current['partial_reply']
        if not current['required_attachment_extensions']:
            return None
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
    requested_path = path
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
        targets = _artifact_source_aliases(results, known).get(identity(path), set())
        if path.is_absolute() and len(targets) > 1:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {
                "reason": "local_artifact_revision_selection_required", "suppress_activity": True,
                "recovery": {"kind": "select_generated_artifact", "candidate_paths": sorted(targets)}},
                "Several generated revisions share that original path. Select the intended generated artifact; no download ran.")
        if not path.is_absolute() or len(targets) != 1:
            return None
        path = Path(next(iter(targets)))
        if not path.is_file():
            return None
    message = "This file is already available locally. Inspect it with file_read; no download is needed."
    return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {
        "reason": "local_artifact_already_available", "path": str(requested_path), "suppress_activity": True,
        **({"resolved_artifact_path":str(path)} if path != requested_path else {}),
        "recovery": {"kind": "inspect_local_artifact", "tool_name": "file_read", "arguments": {"path": str(path)}}}, message)


def artifact_read_reference_recovery(invocation, results, *, registry=None):
    """Offer exact retained paths once before an unknown artifact read.

    This never resolves a guessed alias or reads a file. An intentional read of
    a different resource may be repeated and still uses normal boundary policy.
    """
    if invocation.tool_name != 'file_read':
        return None
    rows = list(results or ())
    reason = 'artifact_read_reference_selection_required'
    if any(_output(row).get('reason') == reason for row in rows):
        return None
    requested = invocation.arguments.get('path')
    if not isinstance(requested, str) or not Path(requested).is_absolute():
        return None
    paths = set()
    for row in rows:
        status = row.get('status') if isinstance(row, Mapping) else getattr(row, 'status', None)
        if status != 'completed':
            continue
        output = _output(row)
        values = [output.get('artifact_path')]
        if isinstance(output.get('artifact_paths'), (list, tuple)):
            values.extend(output['artifact_paths'])
        descriptors = output.get('artifact_descriptors')
        descriptors = descriptors if isinstance(descriptors, (list, tuple)) else ()
        values.extend(descriptor.get(key) for descriptor in descriptors
                      if isinstance(descriptor, Mapping) for key in ('path', 'source_path'))
        paths.update(value for value in values if isinstance(value, str) and Path(value).is_absolute())
    # Lexical normalization suffices for this advisory check. Do not probe an
    # unknown resource's existence or follow its symlinks before access policy.
    from os.path import normpath
    if not paths or normpath(requested) in {normpath(path) for path in paths}:
        return None
    candidates = sorted(path for path in paths if Path(path).suffix.casefold() == Path(requested).suffix.casefold())
    if not candidates:
        return None
    if registry is not None:
        roots = getattr(registry, 'filesystem_allowed_roots', None)
        from nullion.tools import _principal_workspace_file_roots
        allowed = [*(roots() if callable(roots) else ()), *_principal_workspace_file_roots(invocation.principal_id)]
        if any(Path(normpath(requested)).is_relative_to(Path(normpath(str(root)))) for root in allowed):
            return None
    from nullion.tools import ToolResult
    return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', {
        'reason': reason, 'executed': False, 'suppress_activity': True,
        'recovery': {'kind': 'select_retained_artifact', 'tool_name': 'file_read',
                     'retry_tool_name': 'file_read',
                     'candidate_paths': candidates, 'approval_bypass': False}},
        'That path is not one of the retained artifact paths. No file was read. '
        'If inspecting a retained artifact, copy its exact candidate path; do not abbreviate or invent it. '
        'If the user request requires a different file, retry that exact path to use its normal access policy.')


def artifact_deletion_recovery(invocation, results, *, registry):
    """Offer one non-destructive creation attempt before a deletion approval."""
    if invocation.tool_name != 'terminal_exec':
        return None
    flow = invocation.flow_context or {}
    if not flow.get('requested_artifact_extensions') or flow.get('requires_new_artifact_content') is not True:
        return None
    if any(_output(result).get('reason') == 'artifact_deletion_alternative_available' for result in results or ()):
        return None
    from nullion.tools import ToolResult, _terminal_delete_targets_for_invocation, _terminal_execution_cwd_from_registry
    targets = _terminal_delete_targets_for_invocation(invocation,
        execution_cwd=_terminal_execution_cwd_from_registry(registry))
    if not targets:
        return None
    message = (
        'This command includes deletion and would require confirmation. No command ran. '
        'If deletion is unnecessary to create the requested files, use unique workspace temporary paths '
        'and create the outputs without deleting existing paths. If deletion is essential to the user request, '
        'retry that action to request its normal approval. Do not claim a deletion or successful creation yet.'
    )
    return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', {
        'reason':'artifact_deletion_alternative_available', 'command_executed':False,
        'recovery':{'kind':'try_non_destructive_creation','retry_tool_name':'terminal_exec','approval_bypass':False},
        'suppress_activity':True}, message)
