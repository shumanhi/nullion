"""Numeric prose must never become numbered choices during delivery."""
import pytest
from nullion.response_sanitizer import sanitize_user_visible_reply

REPORT = ('**Upcoming obligations**\n\n'
    '• Subscription — received Oct 13. Renewal is Nov 13, 2026; cancel by Nov 10. A matching event exists.\n'
    '• Account — received Oct 21. Payment is due Nov 14. No duplicate was created.\n\n'
    '**Calendar:** 0 new events created.')

@pytest.mark.parametrize('text', [REPORT,
    'Recibido el 13. Renovación el día 10. El pago vence el 21. No se creó un duplicado.',
    '受信日は13. 更新は10. 支払期限は21. 重複は作成していません。',
    'The temperatures were 1. They rose to 2. Those are observations.',
    'Measured sizes: 13. Length was 10. Width was 21. Units are millimeters.',
    'Received Oct\n13. Renewal is Nov 10. The payment was received Oct\n21. Amount is unchanged.',
    '13. This is a numbered reference, deliberately retained.',
    '13. First source.\n\nAnother section:\n21. Second source.'])
def test_numbers_in_prose_and_references_are_preserved(text):
    for source in ['agent', 'platform_delivery', 'cron_delivery']:
        rendered = sanitize_user_visible_reply(user_message='確認', reply=text, tool_results=[], source=source)
        assert rendered == text
        assert sanitize_user_visible_reply(user_message='確認',reply=rendered,tool_results=[],source=source) == rendered

@pytest.mark.parametrize('labels', [('First','Second','Third'),('Uno','Dos','Tres'),('選択肢甲','選択肢乙','選択肢丙')])
def test_real_collapsed_options_still_have_one_numeric_choice_per_line(labels):
    text = '選択： ' + ' '.join(f'{i}. {label}' for i,label in enumerate(labels,1))
    rendered = sanitize_user_visible_reply(user_message='確認',reply=text,tool_results=[])
    assert rendered == '選択：\n' + '\n'.join(f'{i}. {label}' for i,label in enumerate(labels,1))
