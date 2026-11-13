"""Native output capability discovery, independent of request wording or surface."""
from __future__ import annotations


def production_tool_names(registry, extensions):
    """Return actual producers for a typed format, never source-only captures."""
    requested = set(extensions)
    return tuple(spec.name for spec in registry.list_specs()
        if not getattr(spec, "source_artifact_only", False)
        and getattr(spec.side_effect_class, "value", spec.side_effect_class) == "write"
        and "connector" not in getattr(spec, "capability_tags", ())
        and requested.intersection(getattr(spec, "produced_artifact_extensions", ())))
