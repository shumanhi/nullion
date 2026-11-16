"""Fail live acceptance if its desktop browser is an unrelated test service."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import sys
import subprocess
from urllib.parse import urlsplit
from urllib.request import urlopen


def effective_browser_config(config, runtime):
    """Settings credentials can override .env; validate the running selection."""
    selected = dict(config)
    for key, field in (('browser_backend','NULLION_BROWSER_BACKEND'),
                       ('browser_cdp_url','NULLION_BROWSER_CDP_URL'),
                       ('browser_preferred','NULLION_BROWSER_PREFERRED')):
        value=runtime.get(key)
        if isinstance(value,str) and value:
            selected[field]=value
    return selected


def validate_browser(config, *, system, owner=None, listening=False, allow_non_desktop=False):
    if system != 'Darwin' or allow_non_desktop:
        return
    backend=config.get('NULLION_BROWSER_BACKEND') or 'auto'
    if backend not in {'auto','cdp'} or str(config.get('NULLION_BROWSER_HEADLESS') or '').lower()=='true':
        raise ValueError('Live stage acceptance requires the selected desktop browser, not a headless test browser.')
    endpoint=urlsplit(config.get('NULLION_BROWSER_CDP_URL') or config.get('NULLION_BROWSER_USE_CDP_URL') or 'http://127.0.0.1:9222')
    if endpoint.hostname not in {'localhost','127.0.0.1','::1'}:
        raise ValueError('Live stage acceptance requires a local desktop browser connection.')
    if not listening and backend=='auto':
        return  # The real application may launch its managed desktop profile.
    if not owner or '.app/Contents/MacOS/' not in owner:
        raise ValueError('The stage browser endpoint is not owned by a native desktop browser. Restore its desktop connection before validation.')
    from nullion.plugins.browser_plugin.backends.auto_backend import _browser_kind_from_text
    preferred=config.get('NULLION_BROWSER_PREFERRED')
    kind=_browser_kind_from_text(owner)
    if kind is None:
        raise ValueError('The stage browser endpoint is owned by a different application, not a desktop browser.')
    if preferred and kind!=preferred:
        raise ValueError('The stage browser endpoint does not match the selected desktop browser.')


def main():
    from dotenv import dotenv_values
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage-home',type=Path,required=True)
    parser.add_argument('--base-url',default='http://127.0.0.1:8753')
    parser.add_argument('--expected-snapshot')
    parser.add_argument('action',choices=['check','snapshot','verify-unchanged'])
    args=parser.parse_args()
    config={k:v for k,v in dotenv_values(args.stage_home/'.env').items() if k.startswith('NULLION_BROWSER_')}
    try:
        with urlopen(args.base_url.rstrip('/')+'/api/config',timeout=5) as response:
            config=effective_browser_config(config,json.load(response))
    except (OSError,ValueError):
        if args.action!='verify-unchanged':raise
        print('Could not verify stage browser settings: the stage configuration check failed.',file=sys.stderr)
        return 1
    signature=hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest()
    if args.action=='verify-unchanged':
        if not args.expected_snapshot:parser.error('--expected-snapshot is required')
        if signature!=args.expected_snapshot:
            print('Stage browser settings changed during validation. Restore its selected desktop connection.',file=sys.stderr)
            return 1
        return 0
    if args.action=='snapshot':
        print(signature);return
    if platform.system()!='Darwin':
        return
    endpoint=urlsplit(config.get('NULLION_BROWSER_CDP_URL') or config.get('NULLION_BROWSER_USE_CDP_URL') or 'http://127.0.0.1:9222')
    owner=None;listening=False
    if endpoint.hostname in {'localhost','127.0.0.1','::1'}:
        try:
            with socket.create_connection((endpoint.hostname,endpoint.port or 9222),timeout=.3):listening=True
        except OSError:
            pass
        result=subprocess.run(['lsof','-nP',f'-iTCP:{endpoint.port or 9222}','-sTCP:LISTEN','-Fp'],capture_output=True,text=True,timeout=3)
        pid=next((line[1:] for line in result.stdout.splitlines() if line.startswith('p')),None)
        if pid:
            owner=subprocess.check_output(['ps','-p',pid,'-o','comm='],text=True,timeout=3).strip()
    validate_browser(config,system=platform.system(),owner=owner,listening=listening,
        allow_non_desktop=os.environ.get('NULLION_LIVE_ALLOW_NON_DESKTOP_BROWSER')=='1')


if __name__=='__main__':
    sys.exit(main() or 0)
