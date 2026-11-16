"""Coalesce only adapter-declared, explicitly independent model read plans."""
from copy import deepcopy


def coalesce_independent_reads(content, registry, *, principal_id=None):
    if len(content) < 2:
        return content, []
    from jsonschema import Draft202012Validator
    from nullion.tools import ToolSideEffectClass
    def candidate(block):
        if not isinstance(block, dict) or block.get('type') != 'tool_use' or not block.get('id'):
            return None
        try:
            spec = registry.get_spec(block.get('name'))
        except (AttributeError, KeyError, TypeError):
            return None
        contract = getattr(spec, 'independent_batch', ())
        if (not isinstance(contract, tuple) or len(contract) != 2
                or spec.side_effect_class != ToolSideEffectClass.READ
                or spec.requires_approval or not isinstance(spec.input_schema, dict)):
            return None
        values, flag = contract
        arguments = block.get('input')
        if (not isinstance(arguments, dict) or arguments.get(flag) is not True
                or not isinstance(arguments.get(values), list) or not arguments[values]):
            return None
        required = getattr(spec, 'batch_item_required_fields', ())
        if required and any(not isinstance(item, dict) or any(not item.get(key) for key in required)
                            for item in arguments[values]):
            return None
        try:
            validator = Draft202012Validator(spec.input_schema)
            valid = validator.is_valid(arguments)
        except Exception:
            return None
        if not valid:
            return None
        preflight = getattr(spec, 'independent_batch_validator', None)
        if preflight is not None:
            try:
                if not preflight(arguments, principal_id):
                    return None
            except Exception:
                return None
        return spec, values, validator
    output, plans = [], []
    index = 0
    while index < len(content):
        block = content[index]; info = candidate(block)
        if info is None:
            output.append(block); index += 1; continue
        spec, values, validator = info
        merged = deepcopy(block)
        common = {k:v for k,v in block['input'].items() if k != values}
        ids = [block['id']]
        following = index + 1
        while following < len(content):
            other = content[following]; next_info = candidate(other)
            if (next_info is None or other.get('name') != spec.name or other['id'] in ids
                    or {k:v for k,v in other['input'].items() if k != values} != common):
                break
            combined = {**merged['input'], values:merged['input'][values]+other['input'][values]}
            if not validator.is_valid(combined):
                break
            merged['input'] = combined
            ids.append(other['id']); following += 1
        output.append(merged)
        if len(ids) > 1:
            plans.append({'tool_name':spec.name, 'model_tool_use_ids':ids,
                          'executed_tool_use_id':ids[0], 'item_count':len(merged['input'][values])})
        index = following
    return output, plans
