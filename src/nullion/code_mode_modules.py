"""Explicit, bounded, side-effect-free CPython helpers for the Monty session.

Native tools remain the only effect boundary. Never expose whole Python module
objects or resolve an arbitrary sandbox import through the host's import system.
"""
from __future__ import annotations

from importlib import import_module
from inspect import signature


_EXPORTS = {
    'urllib.parse': ('quote', 'quote_plus', 'quote_from_bytes', 'unquote',
        'unquote_plus', 'unquote_to_bytes', 'urlencode', 'parse_qs', 'parse_qsl', 'urljoin'),
    'html': ('escape', 'unescape'),
    'textwrap': ('dedent', 'wrap', 'fill', 'shorten'),
    'shlex': ('quote', 'join'),
}
MAX_HELPER_BYTES = 64_000
MAX_HELPER_CALLS = 128


def module_manifest():
    return {name:list(exports) for name,exports in _EXPORTS.items()}


def _check_size(value, remaining=MAX_HELPER_BYTES, depth=0):
    if remaining < 0 or depth > 16:
        raise ValueError('Host helper data exceeds the bounded input/output limit')
    if isinstance(value, (str, bytes)):
        remaining -= len(value.encode() if isinstance(value,str) else value)
    elif value is None or isinstance(value,(bool,int,float)):
        remaining -= len(str(value))
    elif isinstance(value,(tuple,list,dict)):
        for item in (value.items() if isinstance(value,dict) else value):
            remaining = _check_size(item,remaining-1,depth+1)
    else:
        raise TypeError('Host helpers accept and return ordinary Python data only')
    if remaining < 0:
        raise ValueError('Host helper data exceeds the bounded input/output limit')
    return remaining


def describe_module(name):
    if name not in _EXPORTS:
        return {'available':False,'host_modules':module_manifest(),
            'recovery':'Use an available helper or a scoped native tool for full Python/library work.'}
    module=import_module(name)
    return {'available':True,'name':name,'side_effects':False,
        'functions':{key:str(signature(getattr(module,key))) for key in _EXPORTS[name]},
        'limits':{'input_output_bytes':MAX_HELPER_BYTES,'text_indentation_chars':128}}


def host_modules(before_call):
    """Return module factories and bounded bindings for approved helper names.

    Unique export names also work directly. Shared names such as ``quote``
    require a module import, so URL quoting never silently becomes shell quoting.
    """
    from pydantic_monty import ExternalModule

    def bounded(module_name, function_name):
        function=getattr(import_module(module_name),function_name)
        def call(*args,**kwargs):
            before_call(module_name+'.'+function_name)
            _check_size((args,kwargs))
            if module_name=='textwrap' and any(len(kwargs.get(key,''))>128
                    for key in ('initial_indent','subsequent_indent')):
                raise ValueError('Text indentation is limited to 128 characters')
            result=function(*args,**kwargs)
            _check_size(result)
            return result
        return call

    bindings={module+'.'+key:bounded(module,key) for module,exports in _EXPORTS.items() for key in exports}
    owners={}
    for module,exports in _EXPORTS.items():
        for key in exports:
            owners.setdefault(key,[]).append(module)
    bindings.update({key:bindings[module_names[0]+'.'+key]
        for key,module_names in owners.items() if len(module_names)==1})

    def make_module(name):
        return ExternalModule({key:bindings[name+'.'+key] for key in _EXPORTS[name]})

    # ExternalModule values explicitly select attributes; nested values provide
    # package imports such as `from urllib.parse import quote` after restoration.
    modules={name:(lambda name=name:make_module(name)) for name in _EXPORTS if '.' not in name}
    modules['urllib']=lambda:ExternalModule({'parse':make_module('urllib.parse')})
    return modules,bindings
