"""Typed final outputs for scheduled runs, matched to native receipts and bytes."""
from __future__ import annotations

from collections.abc import Mapping

ROLES = ("file", "embedded_html", "linked_html", "source_image", "derived_image")
IMAGE_ROLES = frozenset({"source_image", "derived_image"})
DERIVED_IMAGE_TOOLS = frozenset({"image_render", "image_generate", "image_edit"})


def deliverables_schema():
    return {
        "type": "array", "maxItems": 32, "uniqueItems": True,
        "description": (
            "Final scheduled attachments only. Use separate entries for embedded and linked HTML "
            "and for sourced and derived images. Use an empty extension for an image in any format. "
            "Use required=false when an attachment is explicitly conditional on availability."
        ),
        "items": {
            "type": "object", "additionalProperties": False,
            "required": ["role", "extension"],
            "properties": {
                "role": {"enum": list(ROLES)},
                "extension": {"type": "string", "pattern": r"^(\.[a-zA-Z0-9]{1,16})?$"},
                "required": {"type": "boolean"},
            },
        },
    }


def normalize_deliverables(value):
    from nullion.attachment_format_graph import VALID_ATTACHMENT_EXTENSIONS
    from nullion.response_fulfillment_contract import IMAGE_ATTACHMENT_EXTENSIONS

    if not isinstance(value, list) or len(value) > 32:
        raise ValueError("Scheduled artifact deliverables must be a list of at most 32 entries")
    rows = {}
    for item in value:
        if not isinstance(item, Mapping) or set(item) - {"role", "extension", "required"}:
            raise ValueError("Invalid scheduled artifact deliverable fields")
        role, raw_extension, required = item.get("role"), item.get("extension"), item.get("required", True)
        if role not in ROLES or not isinstance(raw_extension, str) or type(required) is not bool:
            raise ValueError("Invalid scheduled artifact deliverable")
        extension = raw_extension.strip().lower()
        if extension == ".htm":
            extension = ".html"
        if role in IMAGE_ROLES:
            valid = not extension or extension in IMAGE_ATTACHMENT_EXTENSIONS
        elif role in {"embedded_html", "linked_html"}:
            valid = extension == ".html"
        else:
            valid = extension in VALID_ATTACHMENT_EXTENSIONS
        if not valid or (role, extension) in rows:
            raise ValueError("Invalid or duplicate scheduled artifact format and role")
        rows[role, extension] = {"role": role, "extension": extension, "required": required}
    return tuple(rows.values())


def extension_requirements(deliverables):
    return tuple(dict.fromkeys(row["extension"] for row in deliverables
                               if row["required"] and row["extension"]))


def scoped_deliverables(state):
    flow = state.get("tool_flow_context") or {}
    if not flow.get("scheduled_task_run"):
        return ()
    # An explicit saved contract, including an empty list, wins over run-time
    # proposals. Otherwise use the first validated fixed-scope plan with roles.
    if "scheduled_artifact_deliverables" in flow:
        return tuple(flow["scheduled_artifact_deliverables"])
    from nullion.tools import normalize_tool_status

    # The first declared role plan owns this run. A later tool-scope request
    # cannot remove an obligation that already authorized a producer.
    for result in state.get("tool_results") or ():
        output = result.output
        if (result.tool_name == "request_tool_scope" and normalize_tool_status(result.status) == "completed"
                and isinstance(output, Mapping) and output.get("artifact_deliverables")):
            return tuple(output["artifact_deliverables"])
    return ()


def source_image_paths(paths, results, registry):
    """Only admitted, completed source producers can establish source identity."""
    from nullion.artifact_lifecycle import artifact_candidate_versions, identity
    from nullion.artifacts import artifact_format_extension, artifact_paths_from_output_descriptors
    from nullion.response_fulfillment_contract import IMAGE_ATTACHMENT_EXTENSIONS

    identities = {identity(path) for path in paths}
    latest = artifact_candidate_versions(results)
    found = []
    for result in results:
        if result.status != "completed":
            continue
        metadata = result.response_requirements or {}
        if metadata.get("_native_scheduled_source_artifact") is not True:
            try:
                spec = registry.get_spec(result.tool_name)
            except (AttributeError, KeyError):
                continue
            if not spec.source_artifact_only:
                continue
        for path in artifact_paths_from_output_descriptors(result.output, roles={"source", "deliverable"}):
            target = identity(path)
            version = result.output.get("artifact_producer_receipt_id") or result.invocation_id
            if (artifact_format_extension(path) in IMAGE_ATTACHMENT_EXTENSIONS
                    and (not identities or target in identities) and latest.get(target, version) == version):
                found.append(path)
    return tuple(dict.fromkeys(found))


def match_deliverables(deliverables, paths, results, registry):
    """Return missing required slots and the verified paths assigned to slots."""
    required = [row for row in deliverables if row["required"]]
    if not required:
        return (), ()
    from nullion.artifact_evidence import verified_html_delivery_variant
    from nullion.artifact_lifecycle import artifact_candidate_versions, identity, invalid_candidate_identities
    from nullion.artifact_validation import validate_artifact_paths
    from nullion.artifacts import artifact_format_extension
    from nullion.response_fulfillment_contract import IMAGE_ATTACHMENT_EXTENSIONS

    selected = {identity(path) for path in paths} - invalid_candidate_identities(results)
    candidates = artifact_candidate_versions(results)
    derived = {identity(path) for path, version in artifact_candidate_versions(
        [row for row in results if row.tool_name in DERIVED_IMAGE_TOOLS]).items()
        if version == candidates.get(identity(path))}
    source = {identity(path) for path in source_image_paths(selected, results, registry)} - derived
    valid = [path for path in sorted(selected & (set(candidates) | source))
             if validate_artifact_paths([path]).ok]
    variants = {path: verified_html_delivery_variant(path) for path in valid
                if artifact_format_extension(path) in {".html", ".zip"}}

    def matches(row, path):
        role, extension = row["role"], row["extension"]
        actual = artifact_format_extension(path)
        if role == "linked_html":
            # A standalone attachment cannot preserve relative dependency paths
            # on the recipient's device. Verify the assets inside its bundle.
            return actual == ".zip" and variants.get(path) == "linked"
        if role == "embedded_html":
            return variants.get(path) == "embedded"
        if extension and extension != actual:
            return False
        if role in IMAGE_ROLES and actual not in IMAGE_ATTACHMENT_EXTENSIONS:
            return False
        return (path in source if role == "source_image"
                else path in derived if role == "derived_image" else path in candidates)

    by_path = {}

    def assign(index, visited):
        for path in valid:
            if path in visited or not matches(required[index], path):
                continue
            visited.add(path)
            earlier = by_path.get(path)
            if earlier is None or assign(earlier, visited):
                by_path[path] = index
                return True
        return False

    for index in range(len(required)):
        assign(index, set())
    matched = set(by_path.values())
    return tuple(row for index, row in enumerate(required) if index not in matched), tuple(by_path)


def missing_deliverables(deliverables, paths, results, registry):
    return match_deliverables(deliverables, paths, results, registry)[0]


def producer_names(missing, registry):
    from nullion.artifact_production import production_tool_names
    from nullion.tools import ToolSideEffectClass

    possible = set(production_tool_names(registry, extension_requirements(missing)))
    roles = {slot["role"] for slot in missing}
    if "derived_image" in roles:
        possible.update(DERIVED_IMAGE_TOOLS)
    if roles.intersection({"linked_html", "embedded_html"}):
        possible.add("file_write")
    if "linked_html" in roles:
        possible.add("archive_create")
    return {spec.name for spec in registry.list_specs() if spec.name in possible
            and spec.side_effect_class is ToolSideEffectClass.WRITE
            and not getattr(spec, "source_artifact_only", False)}
