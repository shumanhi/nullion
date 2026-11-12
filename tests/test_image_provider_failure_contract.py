"""A configured generator failure cannot become a successful placeholder image."""
import io
import urllib.error

import pytest

from nullion import providers
from nullion.tools import ToolInvocation, ToolRegistry, register_media_plugin


@pytest.mark.parametrize('backend', ['command', 'model'])
@pytest.mark.parametrize('status', [401, 402, 429, 503])
def test_configured_image_provider_rejection_retains_failure_without_placeholder(tmp_path, monkeypatch, backend, status):
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED', 'true')
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_COMMAND', 'fixture-command' if backend == 'command' else '')
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_PROVIDER', 'fixture-provider')
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_MODEL', 'fixture-model')
    def rejected(*args, **kwargs):
        cause = urllib.error.HTTPError('https://provider.invalid/generate', status, 'Provider rejected request', {}, io.BytesIO(b''))
        raise RuntimeError('Provider rejected image generation') from cause
    monkeypatch.setattr(providers, '_command_image_generate' if backend == 'command' else '_model_image_generate', rejected)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_media_plugin(registry, image_generator=providers._local_image_generate)
    output = tmp_path/'requested.png'
    result = registry.invoke(ToolInvocation('image', 'image_generate', 'web:operator',
        {'prompt':'風景','output_path':str(output)}))
    assert result.status == 'failed' and result.output['http_status_code'] == status
    assert result.output['retryable'] is (status == 429 or status >= 500)
    assert result.output['failure_scope'] == 'invocation'
    assert not output.exists() and not output.with_suffix('.svg').exists()
    assert not result.output.get('fallback_used')
