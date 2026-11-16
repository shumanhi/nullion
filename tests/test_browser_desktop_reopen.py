import pytest

from nullion.plugins.browser_plugin.backends import auto_backend as auto


@pytest.mark.parametrize('case',['unique','cookie_only','ambiguous','preferred_missing','explicit_missing','explicit_conflict','new_install'])
def test_browser_selection_uses_native_saved_identity_before_installed_app_order(tmp_path,monkeypatch,case):
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    for key in ['NULLION_BROWSER_PREFERRED','NULLION_BROWSER_PATH','NULLION_BROWSER_USER_DATA_DIR']:
        monkeypatch.delenv(key,raising=False)
    chrome=tmp_path/'Chrome';brave=tmp_path/'Brave';chrome.touch();brave.touch()
    monkeypatch.setattr(auto.platform,'system',lambda:'Linux')
    monkeypatch.setattr(auto,'_CHROME_CANDIDATES',{'Linux':[str(chrome),str(brave)]})
    if case in {'unique','ambiguous'}:
        profile=tmp_path/'home'/'browser-profiles'/'brave';profile.mkdir(parents=True);(profile/'Local State').write_text('{}')
    if case=='cookie_only':
        profile=tmp_path/'home'/'browser-profiles'/'brave'/'Default';profile.mkdir(parents=True);(profile/'Cookies').touch()
    if case=='ambiguous':
        profile=tmp_path/'home'/'browser-profiles'/'chrome';profile.mkdir();(profile/'Local State').write_text('{}')
    if case=='preferred_missing':
        monkeypatch.setenv('NULLION_BROWSER_PREFERRED','edge')
    if case=='explicit_missing':
        monkeypatch.setenv('NULLION_BROWSER_PATH',str(tmp_path/'missing'))
    if case=='explicit_conflict':
        monkeypatch.setenv('NULLION_BROWSER_PATH',str(chrome))
        monkeypatch.setenv('NULLION_BROWSER_PREFERRED','brave')
    result=auto._find_chrome()
    assert result==({'unique':str(brave),'cookie_only':str(brave),'ambiguous':None,'preferred_missing':None,
        'explicit_missing':None,'explicit_conflict':None,'new_install':str(chrome)}[case])


@pytest.mark.parametrize('case', ['remote', 'proxy', 'occupied', 'missing_profile', 'wrong_browser', 'headless'])
def test_reopen_cannot_replace_or_repoint_an_unavailable_connection(tmp_path, monkeypatch, case):
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_BROWSER_PREFERRED', 'brave')
    monkeypatch.setattr(auto, '_find_chrome', lambda: '/app/Chrome' if case=='wrong_browser' else '/app/Brave')
    monkeypatch.setattr(auto, '_browser_profile_dir', lambda _: str(tmp_path/'existing'))
    monkeypatch.setattr(auto, '_is_cdp_reachable_at', lambda *args: case=='occupied')
    monkeypatch.setattr(auto.subprocess, 'Popen', lambda *args, **kw: pytest.fail('Must not start another browser'))
    if case!='missing_profile': (tmp_path/'existing').mkdir()
    if case=='headless': monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    endpoint = 'http://remote.test:9222' if case=='remote' else 'https://127.0.0.1:19226' if case=='proxy' else 'http://127.0.0.1:19226'
    assert auto.reopen_configured_desktop_browser(endpoint) is False


def test_reopen_uses_exact_profile_and_port_without_environment_mutation(tmp_path, monkeypatch):
    import os
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://127.0.0.1:19226')
    monkeypatch.setenv('NULLION_BROWSER_PREFERRED', 'brave')
    profile=tmp_path/'existing';profile.mkdir();seen=[]
    monkeypatch.setattr(auto, '_find_chrome', lambda: '/app/Brave')
    monkeypatch.setattr(auto, '_browser_profile_dir', lambda _: str(profile))
    monkeypatch.setattr(auto, '_is_cdp_reachable_at', lambda *args: bool(seen))
    monkeypatch.setattr(auto.subprocess, 'Popen', lambda *args, **kw: seen.append((args, kw)))
    before=dict(os.environ)
    assert auto.reopen_configured_desktop_browser('http://127.0.0.1:19226')
    assert os.environ==before
    assert seen[0][0][0][:3]==['/app/Brave','--remote-debugging-port=19226',f'--user-data-dir={profile}']
    assert '--headless' not in seen[0][0][0]


def test_image_candidate_selection_receives_labeled_current_pixels(tmp_path, monkeypatch):
    from PIL import Image
    from nullion import agent_orchestrator as agent
    monkeypatch.setattr(agent, '_artifact_roots_for_agent_turn', lambda *args: [tmp_path])
    paths=[]
    for name,color in [('a.png','red'),('b.png','blue')]:
        path=tmp_path/name;Image.new('RGB',(50,50),color).save(path);paths.append(str(path))
    state={'artifact_selection_candidates':paths,'user_message':'画像を一つ選んでください。','tool_results':[]}
    request=agent._artifact_delivery_selection_request(state)
    content=request['messages'][0]['content']
    assert len([b for b in content if b.get('type')=='image'])==2
    assert any('candidate a0:' in b.get('text','') for b in content)
    assert any('candidate a1:' in b.get('text','') for b in content)
