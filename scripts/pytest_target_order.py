"""Keep explicit pytest selectors under one contiguous directory collector.

Recent pytest versions bind conftest fixtures to node identity. Interleaving
explicit files from parent and child directories can produce distinct nodes
with the same nodeid and make a registered fixture invisible. Group the initial
path-selector block using filesystem metadata; preserve option/value pairs,
selector suffixes, duplicate requests and order within each directory.
"""
from pathlib import Path


def order_pytest_selectors(arguments, *, cwd=None):
    values=list(arguments)
    boundary=next((i for i,value in enumerate(values) if value.startswith('-')),len(values))
    prefix=values[:boundary]
    base=Path(cwd) if cwd is not None else Path.cwd()
    contexts=[]
    for value in prefix:
        path=Path(value.split('::',1)[0])
        candidate=base/path
        if not candidate.exists():
            # Leave pyargs, invalid selectors and plugin inputs to pytest.
            return values
        context=path if candidate.is_dir() else path.parent
        contexts.append((len(context.parts),context.as_posix()))
    indexed=sorted(range(len(prefix)),key=lambda index:contexts[index])
    return [prefix[index] for index in indexed]+values[boundary:]


def pytest_arguments(arguments, *, environ=None):
    """Keep CLI argument boundaries across Docker without shell evaluation."""
    import json
    import os
    environment = os.environ if environ is None else environ
    encoded = environment.get('NULLION_E2E_PYTEST_ARGS_JSON')
    if not encoded:
        return list(arguments)
    values = json.loads(encoded)
    if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
        raise ValueError('NULLION_E2E_PYTEST_ARGS_JSON must contain an array of strings')
    return values


if __name__=='__main__':
    import sys
    for argument in order_pytest_selectors(pytest_arguments(sys.argv[1:])):
        sys.stdout.buffer.write(argument.encode()+b'\0')
