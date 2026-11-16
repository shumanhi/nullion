"""Native incomplete process observations never establish action success."""
from collections.abc import Mapping


def partial_capture_requirements(invocation_id, output):
    if (not isinstance(output, Mapping) or output.get('output_complete') is not False
            or output.get('command_executed') is not True
            or output.get('execution_outcome') not in {'timeout', 'nonzero_exit'}
            or not isinstance(output.get('stdout'), str) or not output['stdout'].strip()):
        return None
    code = output.get('timeout_seconds' if output['execution_outcome'] == 'timeout' else 'exit_code')
    if isinstance(code, bool) or not isinstance(code, (int, float)) or code == 0:
        return None
    return {'_native_partial_capture': {'version': 1, 'invocation_id': invocation_id,
        'captured_fields': ['stdout'], 'output_complete': False}}


def has_native_partial_capture(result):
    """Only a bound native receipt can preserve a writer's partial report."""
    if result.status != 'failed':
        return False
    requirements = getattr(result, 'response_requirements', None)
    binding = requirements.get('_native_partial_capture') if isinstance(requirements, Mapping) else None
    expected = partial_capture_requirements(result.invocation_id, result.output)
    return expected is not None and binding == expected['_native_partial_capture']
