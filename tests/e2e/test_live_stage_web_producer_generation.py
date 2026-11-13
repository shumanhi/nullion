"""Small opt-in live set for file delivery and adjacent ordinary workflows."""
import io
import os
from uuid import uuid4

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue
from tests.e2e.test_live_stage_web_producer_completion import _record_primary_boundary

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1',
    reason='Explicit real-provider stage Web acceptance only')


def test_live_web_primary_docx_selects_only_final_version(live_web):
    from tests.e2e.test_live_stage_web_extended import (
        test_live_web_only_final_document_survives_delivery_and_reload as replay)
    replay(live_web)
    _record_primary_boundary(live_web, 'primary-final-docx')


def test_live_web_primary_xlsx_has_all_twenty_rows(live_web):
    from openpyxl import load_workbook
    name = 'stage_inventory_' + uuid4().hex[:10] + '.xlsx'
    response, turn, elapsed = _submit(live_web,
        'Create one XLSX workbook named ' + name + ' with exactly 20 synthetic product rows. '
        'Columns: Product number, SKU, Short name, Category, Unit price, Notes. '
        'Use unique product numbers 1 through 20, unique SKUs and numeric unit prices. '
        'Attach only the final XLSX, with no CSV or JSON sidecars.', timeout=240)
    assert len(response['artifacts']) == 1, response
    artifact = response['artifacts'][0]
    assert artifact['name'] == name, artifact
    data, _ = catalogue._artifact_bytes(live_web[-1], artifact, timeout=30)
    workbook = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    try:
        rows = list(workbook.active.values)
    finally:
        workbook.close()
    assert rows[0] == ('Product number', 'SKU', 'Short name', 'Category', 'Unit price', 'Notes'), rows[:2]
    assert len(rows) == 21 and {row[0] for row in rows[1:]} == set(range(1, 21)), rows
    assert len({row[1] for row in rows[1:]}) == 20
    assert all(isinstance(row[4], (int, float)) for row in rows[1:]), rows
    assert any(row['status'] == 'completed' and row['tool_name'] in
        {'spreadsheet_create', 'terminal_exec', 'local_exec'} for row in turn['tool_results']), turn
    assert elapsed < 240000
    _record_primary_boundary(live_web, 'primary-twenty-row-xlsx')


def test_live_web_primary_csv_survives_dashboard_polling(live_web):
    from tests.e2e.test_live_stage_web_extended import (
        test_live_web_dashboard_polling_during_generation_keeps_final_file_and_history as replay)
    replay(live_web)
    _record_primary_boundary(live_web, 'primary-csv-status-polling')


def test_live_web_primary_file_lookup_followup_does_not_regenerate(live_web):
    from tests.e2e.test_live_stage_web_extended import (
        test_live_web_existing_filename_lookup_does_not_recreate_or_attach_input as replay)
    replay(live_web)
    _record_primary_boundary(live_web, 'primary-file-lookup-followup', expected_turns=2)


def test_live_web_primary_plain_chat_stays_fast_in_three_languages(live_web):
    from tests.e2e.test_live_stage_web_followups import (
        test_live_web_plain_turn_latency_across_languages as replay)
    replay(live_web)
    _record_primary_boundary(live_web, 'primary-multilingual-chat', expected_turns=3, allow_deferred=True)


def test_live_web_primary_inventory_does_not_start_scheduled_work(live_web):
    scenario = catalogue.SCENARIOS['scheduler_inventory_read_only']
    response, turn, elapsed = _submit(live_web, scenario.prompt, timeout=90)
    catalogue._assert_turn_tools(scenario, turn)
    assert not response['artifacts']
    assert any(row['tool_name'] == 'list_crons' and row['status'] == 'completed'
        for row in turn['tool_results']), turn
    assert not any(row['tool_name'] in {'run_cron', 'create_cron', 'update_cron', 'delete_cron', 'toggle_cron'}
        for row in turn['tool_results']), turn
    assert elapsed < 90000
    _record_primary_boundary(live_web, 'primary-scheduler-inventory')


def test_live_web_primary_weather_delivers_current_tool_result(live_web):
    scenario = catalogue.SCENARIOS['weather']
    response, turn, elapsed = _submit(live_web, scenario.prompt, timeout=90)
    catalogue._assert_turn_tools(scenario, turn)
    assert not response['artifacts']
    results = [row for row in turn['tool_results'] if row['tool_name'] == 'weather_forecast']
    assert results and any(row['status'] == 'completed' for row in results), turn
    assert elapsed < 90000
    _record_primary_boundary(live_web, 'primary-weather')


def test_live_web_primary_image_bytes_and_followup_keep_selected_subject(live_web, tmp_path):
    from PIL import Image, ImageDraw, ImageFont
    from playwright.sync_api import expect
    page = live_web[0]
    # Reproduce Telegram's old PNG hint on JPEG bytes at the shared input boundary.
    image = Image.new('RGB', (900, 300), 'white')
    ImageDraw.Draw(image).text((60, 80), 'ORION 47', fill='black', font=ImageFont.load_default(size=100))
    attachment = tmp_path / 'selected-source.png'
    image.save(attachment, format='JPEG')
    page.locator('#file-input').set_input_files(attachment)
    expect(page.locator('#attachments-bar')).to_contain_text(attachment.name)
    response, _, _ = _submit(live_web, 'Read the large code printed in the attached image. Reply with only that code.', timeout=90)
    assert 'ORION' in response['text'] and '47' in response['text'], response
    assert not response['artifacts'], response
    response, _, _ = _submit(live_web, 'What code was in the image I just sent? Reply with only that code.', timeout=90)
    assert 'ORION' in response['text'] and '47' in response['text'], response
    assert not response['artifacts'], response
    # Direct vision/plain replies may defer runtime import; the live history
    # boundary still requires exactly two persisted user and terminal replies.
    _record_primary_boundary(live_web, 'primary-selected-image-followup', expected_turns=2, allow_deferred=True)
