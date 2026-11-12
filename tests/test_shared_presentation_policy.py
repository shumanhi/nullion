"""A single policy and content contract across producers and delivery surfaces."""
from types import SimpleNamespace
import pytest

from nullion.chat_text import (UserMessage, ReplySection, ReplyFinding, ReplyBlock,
    render_user_message, structured_user_message, apply_presentation_preferences)
from nullion.preferences import Preferences, build_presentation_prompt, build_preferences_prompt, presentation_policy


@pytest.mark.parametrize('label', ['結果', 'Resultados', 'النتائج'])
@pytest.mark.parametrize('markdown', ['plain','light','full'])
def test_common_content_contract_applies_preferences_without_losing_findings_or_choices(label,markdown):
    prefs=Preferences(emoji_level='expressive',markdown_style=markdown,response_structure='numbered')
    message=UserMessage(title=label,summary='Overview',kind='update',
        findings=(ReplyFinding('One','43 → 45',' [Source](https://example.test/a)'),),
        sections=(ReplySection(label,('31 → 34','Value 25')),),
        choices=('Continue','Stop'),footer='Ends 21:45')
    text=render_user_message(message,preferences=prefs)
    assert text.startswith('## 🔔 ' if markdown=='full' else '🔔 ') and '43 → 45' in text and '31 → 34' in text
    assert '[Source](https://example.test/a)' in text and text.endswith('Ends 21:45')
    assert '\n1. Continue\n2. Stop' in text
    assert text.count('1. ')==1, 'Findings must not become another numbered pick-list'
    if markdown=='plain':assert '**' not in text and '## ' not in text
    elif markdown=='light':assert '**'+label+'**' in text
    else:assert '## '+label in text


def test_style_policy_is_shared_but_does_not_include_behavior_or_consent_instructions():
    prefs=Preferences(emoji_level='expressive',tone='formal',persona='PERSONA_ONLY',confirm_before_action=True)
    policy=build_presentation_prompt(prefs)
    assert 'emojis liberally' in policy and 'formal' in policy
    assert 'PERSONA_ONLY' not in policy and 'Always confirm' not in policy
    behavior=build_preferences_prompt(prefs,include_presentation=False)
    assert 'PERSONA_ONLY' in behavior and 'Always confirm' in behavior
    assert 'Shared presentation policy' not in behavior
    full=build_preferences_prompt(prefs)
    assert full.count('Shared presentation policy')==1


def test_style_changes_do_not_modify_code_links_or_literal_artifact_metadata():
    text='## 🚀 **Heading**\n\nDone ✅\n`print("🦊 **literal**")`\n```json\n{"x":"✅ **data**"}\n```\n[🔎 **label**](https://example.test/🦊/**opaque**)\nMEDIA:/tmp/🦊-exact-name.pdf\n53°F; 12℃; ⌘C; ☐ pending'
    result=apply_presentation_preferences(text,preferences=Preferences(emoji_level='none',markdown_style='plain'))
    assert result.startswith('Heading') and 'Done ✅' not in result
    for literal in ['`print("🦊 **literal**")`','```json\n{"x":"✅ **data**"}\n```',
        '[🔎 **label**](https://example.test/🦊/**opaque**)','MEDIA:/tmp/🦊-exact-name.pdf','53°F; 12℃; ⌘C; ☐ pending']:
        assert literal in result
    assert apply_presentation_preferences(result,preferences=Preferences(emoji_level='none',markdown_style='plain'))==result


def test_invalid_preference_values_fall_back_consistently_in_prompt_and_renderer():
    prefs=SimpleNamespace(emoji_level=[],markdown_style='unknown',response_structure=None)
    assert presentation_policy(prefs).emoji_level=='none'
    assert 'Use no emojis' in build_presentation_prompt(prefs)
    assert '✅' not in render_user_message(UserMessage(summary='Done ✅',kind='success'),preferences=prefs)


@pytest.mark.parametrize('structure', ['free','bullets','numbered','prose'])
def test_all_structured_producers_use_the_same_renderer(structure):
    prefs=Preferences(emoji_level='none',response_structure=structure,markdown_style='plain')
    content={'summary':'概要','sections':[{'heading':'比較','items':['A and B remain one finding','43 → 45']} ]}
    structured=structured_user_message(content,preferences=prefs)
    common=render_user_message(UserMessage(summary='概要',sections=(ReplySection('比較',('A and B remain one finding','43 → 45')),)),preferences=prefs)
    assert structured==common
    rows=render_user_message(UserMessage(blocks=(ReplyBlock('比較','heading'),ReplyBlock('A and B remain one finding','item'))),preferences=prefs)
    assert 'A and B remain one finding' in rows
    if structure=='prose':assert '• ' not in rows


def test_emoji_cleanup_preserves_list_indentation_and_authored_spacing():
    from nullion.chat_text import strip_emoji_characters
    text = '🚀 Done ✅\n- A\n  continued  value\n    nested\n\n☐ pending'
    assert strip_emoji_characters(text) == 'Done\n- A\n  continued  value\n    nested\n\n☐ pending'


def test_cleanup_keeps_spaces_next_to_each_protected_literal():
    source='Open: https://example.test/a\nA file: `report.txt`, `other.txt`.\n- `result.xlsx`'
    assert apply_presentation_preferences(source,preferences=Preferences(emoji_level='none'))==source


def test_plain_multiline_emphasis_and_typed_multiline_items_keep_their_content():
    prefs=Preferences(markdown_style='plain',emoji_level='none')
    assert apply_presentation_preferences('**No events\ncreated:** existing reminders.',preferences=prefs)=='No events\ncreated: existing reminders.'
    message=UserMessage(sections=(ReplySection('結果',('A\n43 → 45','B\n31 → 34')),))
    assert render_user_message(message,preferences=prefs)=='結果\n• A\n  43 → 45\n• B\n  31 → 34'


@pytest.mark.parametrize('heading',['⏰ **SCHEDULED TASK: Report**','**🌦 Forecast**','🚀 Updated'])
def test_adjacent_workflow_roles_do_not_stack_emoji_accents(heading):
    prefs=Preferences(emoji_level='expressive')
    assert apply_presentation_preferences(heading,preferences=prefs,message_kind='email')==heading
    assert apply_presentation_preferences(heading,preferences=prefs,message_kind='scheduled')==heading


def test_full_headings_keep_the_role_accent_inside_markdown_syntax():
    prefs=Preferences(emoji_level='expressive',markdown_style='full')
    value=render_user_message(UserMessage(title='結果',kind='update'),preferences=prefs)
    assert value=='## 🔔 結果'
    assert apply_presentation_preferences(value,preferences=prefs,message_kind='email')==value


def test_slack_rendering_translates_headings_without_changing_literal_code():
    from nullion.slack_app import _format_slack_reply
    literal='## literal heading\n**literal bold** [literal](https://example.test/code)'
    text='## 🔔 結果\n\n**Value**\n```text\n'+literal+'\n```\n`**inline**`'
    result=_format_slack_reply(text)
    assert result.startswith('*🔔 結果*\n\n*Value*')
    assert '```text\n'+literal+'\n```' in result and '`**inline**`' in result
