from types import SimpleNamespace

import pytest

from nullion.cron_delivery import _prepared_cron_artifact_delivery_satisfied
from nullion.report_completion import bind_report_outcome


@pytest.mark.parametrize('extension', ['.html', '.htm', '.txt', '.csv', '.pdf', '.png'])
@pytest.mark.parametrize('change', ['none', 'changed_bytes', 'different_request', 'missing_format', 'empty', 'no_receipt'])
def test_prepared_partial_file_truth_does_not_depend_on_overall_success(tmp_path, extension, change):
    path = tmp_path/('report'+extension)
    if extension == '.pdf':
        from pypdf import PdfWriter
        writer = PdfWriter(); writer.add_blank_page(width=180, height=240)
        with path.open('wb') as stream: writer.write(stream)
    elif extension == '.png':
        from PIL import Image
        Image.new('RGB', (16, 16), 'white').save(path)
    else:
        path.write_text('<!doctype html><html><body>Value 37</body></html>' if extension in {'.html', '.htm'} else 'Value,37')
    job = SimpleNamespace(task='依頼された資料', artifact_delivery_options={
        'requires_attachment_delivery': True, 'required_artifact_extensions': [extension]})
    result = {'artifacts':[str(path)], 'artifact_delivery_satisfied':False, 'response_fulfilled':False}
    receipt = bind_report_outcome(job.task, {'status':'unknown', 'remaining_work':[]}, [str(path)])
    if change == 'changed_bytes': path.write_text('Changed')
    elif change == 'different_request': job.task = 'Otra solicitud'
    elif change == 'missing_format': job.artifact_delivery_options['required_artifact_extensions'].append('.csv' if extension == '.pdf' else '.pdf')
    elif change == 'empty': path.write_bytes(b'')
    elif change == 'no_receipt': receipt = None
    assert _prepared_cron_artifact_delivery_satisfied(job, result, content_receipt=receipt) is (change == 'none')
    assert result['response_fulfilled'] is False
