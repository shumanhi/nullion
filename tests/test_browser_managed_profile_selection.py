from types import SimpleNamespace

import pytest

from nullion.plugins.browser_plugin.backends import auto_backend as a


@pytest.mark.parametrize('selected', ['chrome', 'brave'])
def test_switching_back_reuses_exact_profile_even_when_saved_port_belongs_to_other_app(monkeypatch, tmp_path, selected):
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_BROWSER_PREFERRED', selected)
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://127.0.0.1:9223')
    monkeypatch.delenv('NULLION_BROWSER_USER_DATA_DIR', raising=False)
    monkeypatch.setattr(a.platform, 'system', lambda: 'Darwin')
    executable = '/Applications/Selected Browser.app/Contents/MacOS/' + selected
    profile = a._browser_profile_dir(executable)
    commands = '\n'.join([
        f'{executable} --remote-debugging-port=9444 --user-data-dir={profile}-different --no-first-run',
        f'{executable} --remote-debugging-port=9222 --user-data-dir={profile} --no-first-run',
        f'/Applications/Other.app/other --remote-debugging-port=9555 --user-data-dir={profile}',
    ])
    monkeypatch.setattr(a.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(stdout=commands))
    monkeypatch.setattr(a, '_is_cdp_reachable_at', lambda host, port: port in {9222, 9223, 9444, 9555})
    monkeypatch.setattr(a, '_running_cdp_matches_preference', lambda: False)
    monkeypatch.setattr(a.subprocess, 'Popen', lambda *args, **kwargs: pytest.fail('Must reuse the existing selected profile'))
    a._launch_chrome(executable)
    assert a._cdp_port() == 9222


@pytest.mark.parametrize('host', ['127.0.0.1', 'remote.example'])
def test_profile_selection_does_not_attach_other_profiles_or_scan_remote_hosts(monkeypatch, tmp_path, host):
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', f'http://{host}:9223')
    monkeypatch.delenv('NULLION_BROWSER_USER_DATA_DIR', raising=False)
    monkeypatch.setattr(a.platform, 'system', lambda: 'Darwin')
    executable = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    commands = f'{executable} --remote-debugging-port=9222 --user-data-dir={tmp_path}/different-profile'
    def processes(*args, **kwargs):
        assert host == '127.0.0.1', 'Remote configuration must remain attach-only'
        return SimpleNamespace(stdout=commands)
    monkeypatch.setattr(a.subprocess, 'run', processes)
    monkeypatch.setattr(a, '_is_cdp_reachable_at', lambda *args: pytest.fail('Unverified profile must not be probed'))
    assert a._managed_profile_cdp_port(executable) is None
