"""Shared chat text rendering helpers.

These helpers operate on assistant output after the model has produced a reply.
They avoid making routing decisions from message text; the goal is only to keep
known Markdown constructs readable across narrow chat surfaces.
"""

from __future__ import annotations

import re
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class ReplySection:
    heading: str = ''
    items: tuple[str, ...] = ()
    paragraphs: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReplyFinding:
    title: str
    detail: str = ''
    citations: str = ''


@dataclass(frozen=True)
class ReplyBlock:
    text: str
    kind: str = 'paragraph'  # paragraph | heading | item, selected by typed producer state


@dataclass(frozen=True)
class UserMessage:
    """Shared content contract; transport syntax and task routing live elsewhere."""
    title: str = ''
    summary: str = ''
    paragraphs: tuple[str, ...] = ()
    sections: tuple[ReplySection, ...] = ()
    findings: tuple[ReplyFinding, ...] = ()
    choices: tuple[str, ...] = ()
    footer: str = ''
    kind: str | None = None
    blocks: tuple[ReplyBlock, ...] = ()


# This describes user-facing content, never tool permissions or task routing.
REPORT_MESSAGE_FORMAT = 'nullion.report.v1'
OUTCOME_REVIEW_GUIDANCE = (
    'When the response schema includes supported_findings, classify them by draft_relation: '
    'a rewritten answer or full report is replacement; '
    'a narrow genuinely new fact is additional; a fact already present is already_covered. Never mark '
    'a reworded version of the whole draft as an addition. Material limitations are separate narrow '
    'rows, not another full report containing the same facts and the limitation. '
    'Act as a product-minded helper to the producer: start from what the user wants to accomplish and the agreed constraints, '
    'then help make the delivered result useful and actionable. You do not own delivery or permissions. '
    'Prefer authoring a grounded corrected answer in this same call over merely grading it or asking '
    'the producer to repeat work. Your critique and availability must not replace a usable result with '
    'a status paragraph. Request new execution only for a material gap in the original goal. '
    'Before reviewing coverage, distinguish the requested subjects and properties from contextual links, '
    'reference attachments and optional background. A reference identifies context; it does not independently '
    'require every property of that reference to be authenticated. Add a prerequisite only when its missing '
    'value prevents identifying the requested subject, delivering the requested result, or executing an '
    'authorized action safely. Do not make a blocked contextual page into a new mandatory research subject '
    'when the requested subjects are already identified and their requested properties have source evidence. '
    'Evidence collected along the way '
    'is not a new user requirement. Distinguish a missing essential from an optional unknown. An unknown '
    'optional attribute can be disclosed briefly; do not require another lookup merely to fill a comparison '
    'template. It is essential when explicitly requested, necessary to choose a sound result, or necessary '
    'for safety, consent or a requested action. Preserve those essentials and their proof. A recommendation '
    'needs the decision facts and available direct links that let the user act on it. Fix composition from '
    'retained evidence in the current review; do not send editorial defects back as new source work. '
    'When the draft already delivers that result correctly, retain it and return the required structured '
    'checks without reproducing the answer in supported_findings. A replacement is for a material correction, '
    'not a mandatory second edition of a correct draft. Keep evidence-binding explanations concise. '
    'Do not invent requirements to reproduce full internal labels or to describe workspace boundaries '
    'when the answer already identifies the observed items and checked scope accurately. Harmless '
    'shortening, whitespace normalization and optional explanatory detail do not justify retry. Require '
    'a correction only when the difference can mislead a user decision, change the requested target or '
    'scope, omit an explicit requirement, or affect safety or consent. '
)
REPORT_REVIEW_GUIDANCE = (
    OUTCOME_REVIEW_GUIDANCE +
    'Separate the requested deliverable from its accompanying chat message. When a detailed report, '
    'dashboard, document or export is attached, its requested sections, full source inventory, methods '
    'and supporting details belong in that artifact. The chat message is a short delivery summary: '
    'lead with the most useful findings or changes and any material unavailable result, then identify '
    'the attached deliverable. Do not mirror the file section by section, repeat its findings in an '
    'opening overview and again in bullets, or narrate commands, authentication checks, excluded data '
    'and graph-construction rules. Those are execution constraints, not chat findings. Required artifact '
    'contents remain mandatory in the file; this does not authorize omitting a finding from both '
    'places. Preserve an explicit request for the full report in chat as well as the attachment. '
    'Report presentation policy: required findings, uncertainties and requested provenance remain mandatory '
    'in their requested deliverable. A standalone chat report must contain them in chat. An attached '
    'report must contain them in the file; its accompanying summary must not be judged incomplete '
    'because it omits the file\'s full per-source detail. Keep only the decision-relevant highlights '
    'and material exceptions in that summary. Empty sections, sources and shared_details arrays are '
    'valid for a short artifact summary. Do not add a deliverable section that merely describes the '
    'file again, or an inventory of platforms, sources and negative searches already in the file. '
    'For a routine attached report, aim for about 50–100 words in chat: the headline metrics or changes, '
    'one short finding or material limitation, and the attachment. This is writing guidance, not a hard '
    'limit: expand when necessary for a real required action, essential exception, or explicit request '
    'for more chat detail. Full report coverage is carried by the attachment, not a duplicate catalog. '
    'When report_delivery_context.delivery_role is artifact_summary, compose that accompanying summary, '
    'not a standalone report: use a short paragraph or compact highlights, with sections and source '
    'inventories empty unless necessary for an explicit chat requirement or essential next action. '
    'Preserve every fact explicitly requested for the accompanying chat, including its trend or '
    'baseline/status note and material unavailable results. Saying that the file contains those '
    'facts is not a substitute for reporting that explicitly requested summary information. '
    'A supplied compact source label is sufficient identifying provenance unless the user explicitly '
    'requires exact full titles. expanded_label and extra_details are optional expansion, not missing '
    'work in compact mode. Do not require expanding them or repeating shared outcomes in each item. '
    'A shared limitation may appear once with explicit scope covering its affected items; do not '
    'demand duplicate per-item warnings. Correct source links establish their target; source-label '
    'numbering is presentation metadata, not missing source evidence. '
    'Do not invent an exact-layout requirement or reject otherwise completed work for a relevant '
    'optional detail. Honor explicit user format restrictions and flag incorrect or misleading content. '
    'A useful comparison is not an exhaustive specification or usage manual. Separate the user goal '
    'from additional details the assistant chose to collect; those details do not become new required '
    'work. State the recommendation once, then give compact per-option decision facts. Keep optional '
    'instructions, full compatibility catalogs and incidental metadata in extra_details. '
    'If the completed work has an overlong draft, return a corrected shared report in the current '
    'review rather than accepting the catalog unchanged or declaring new execution work. '
    'State each finding and aggregate total once: use the summary OR shared_details/footer for the '
    'same total, not both. Give dates and amounts once per item rather than restating the heading. '
    'Use a short identifying subject in a source label, retaining any requested sender and received '
    'date; keep the full notification wording in expanded_label unless the exact title is required. '
    'Compact means a decision report, not an audit transcript. Each item should read as a short outcome '
    'with only its necessary exception; do not fill every possible field just because it was collected. '
    'Provenance belongs only in sources, not again in required_details. A source label can identify the '
    'sender, short subject and received date once; the item already identifies its obligation. A common '
    'calendar/action result belongs once in shared_details, not in every item and the opening summary. '
    'Search chains, pagination, execution proofs and routine no-action explanations are optional '
    'extra_details rather than the final footer. A real coverage gap or uncertain outcome remains visible. '
    'Secondary account identifiers, search-method narration and timezone boilerplate belong in '
    'extra_details unless needed to identify the obligation, interpret the result or satisfy the '
    'request. Preserve material uncertainty in a concise clause, not an evidence-processing narrative. '
    'Report outcomes, not the reasoning used to establish them. Use one short outcome line per item; '
    'only an actionable exception needs another clause. For example, "Payment scheduled; posting '
    'unconfirmed" carries the limitation without explaining what a scheduling email does or does not '
    'prove. Do not repeat that explanation for every payment and again at the end. '
    'Report-wide explanations, methodology and routine reassurance belong in the root extra_details, '
    'which is optional detailed expansion. Keep the footer for a necessary final action or exception, '
    'not a second summary, evidence narrative or blanket disclaimer. A real unresolved amount, date, '
    'conflict or required action stays visible in summary or required_details. '
    'Judge source proof independently; this display policy never certifies facts or external actions.'
)
_TEXT = {'type': 'string'}
_STRINGS = {'type': 'array', 'items': _TEXT}
REPORT_MESSAGE_SCHEMA = {
    'type': 'object', 'additionalProperties': False,
    'required': ['summary', 'detail_level', 'sections', 'sources', 'shared_details'],
    'properties': {
        'title': _TEXT, 'title_is_required': {'type': 'boolean'}, 'summary': _TEXT,
        'footer': {**_TEXT, 'description': 'An essential final action or exception only; no repeated summary or explanation of evidence.'},
        'extra_details': {**_STRINGS, 'description': 'Optional report-wide explanations, methodology and supporting context. Shown only in detailed mode; never place a required finding, action or material uncertainty here.'},
        'detail_level': {'enum': ['default', 'compact', 'detailed']},
        'sections': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': False,
            'required': ['heading', 'priority', 'layout', 'items'], 'properties': {
                'heading': _TEXT, 'priority': {'enum': ['primary', 'secondary']},
                'layout': {'enum': ['list', 'inline']},
                'items': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': False,
                    'required': ['title', 'summary', 'required_details', 'extra_details', 'source_ids', 'shared_detail_ids'],
                    'properties': {'title': _TEXT, 'summary': _TEXT, 'required_details': {**_STRINGS,
                        'description': 'Necessary decision facts or material exceptions only. Put provenance in sources and repeated routine outcomes in shared_details, without repeating them here.'},
                        'extra_details': _STRINGS, 'source_ids': _STRINGS, 'shared_detail_ids': _STRINGS}}}}}},
        'sources': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': False,
            'required': ['id', 'label'], 'properties': {'id': _TEXT, 'label': _TEXT, 'expanded_label': _TEXT, 'url': _TEXT}}},
        'shared_details': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': False,
            'required': ['id', 'text'], 'properties': {'id': _TEXT, 'text': _TEXT}}},
    },
}

REPORT_MESSAGE_ENVELOPE_SCHEMA = {
    'type': 'object', 'additionalProperties': False, 'required': ['format', 'message'],
    'properties': {'format': {'const': REPORT_MESSAGE_FORMAT}, 'message': REPORT_MESSAGE_SCHEMA,
                   'text': {'type': 'string'}},
}


def report_delivery_context(*, artifact_delivery_required: bool = False) -> dict[str, str]:
    """Content scope from an existing typed delivery contract, never request prose."""
    return {'delivery_role': 'artifact_summary' if artifact_delivery_required else 'standalone_report'}


def report_presentation_instructions() -> str:
    """Same contract for original reports and tools-disabled recovery calls."""
    return (
        'For the final chat report, use the shared report message contract below. '
        + REPORT_REVIEW_GUIDANCE +
        'Default to compact, with actionable findings first and secondary/history/optional findings '
        'grouped compactly afterward. Use inline only for short groups; use lists for readable separate entries. '
        'Use one short overview sentence and one concise outcome line per item. Put each outcome in summary; keep every requested field, '
        'material uncertainty, discrepancy and action exception in required_details. extra_details '
        'are optional expansion only. Use detailed only for an explicitly requested detailed audit or '
        'the saved detailed response preference. Do not put required facts only in extra_details. '
        'Put a repeated common outcome once in shared_details, explicitly naming its scope, and '
        'reference its id from the affected items. Do not repeat it in each item summary or detail. '
        'Sources are user-facing provenance: put a short identifying label in label and the exact available '
        'public source URL in url. Keep direct links for recommended or compared options; a source name '
        'alone is not a clickable reference. Never invent a URL. Native records without a public URL remain labels. '
        'Include sender, subject and received date when requested or necessary to identify that record, '
        'using a short identifying subject unless the exact full title is requested; '
        'when source records already supply a compact label, reuse that label rather than its '
        'expanded_label. expanded_label may retain the full optional title, but never copy '
        'optional expanded titles into label or item details for compact output. '
        'Reuse source ids for the same source. '
        'Put report-wide reasoning in root extra_details, not shared_details or footer. '
        'Do not add search-process narration or repeated no-action explanations. Give aggregate '
        'action/calendar totals once. Preserve exact required titles, supported facts and numbered '
        'consent choices. This governs chat presentation, not artifact contents or fulfillment. '
        'Plain-text answers and file receipts remain valid when this structure is unsuitable. '
        'Set title_is_required=true only when the user explicitly requires that title as report content; '
        'otherwise it is a display label that an enclosing delivery may own. '
        'For an original final response, wrap the message as {"format":"' + REPORT_MESSAGE_FORMAT
        + '","message":<message>}; optional text is a usable plain-text fallback, not another full draft. '
        'For a recovery JSON decision, put <message> in report_message and leave report empty; '
        'all evidence and completion fields still apply. Message schema: '
        + json.dumps(REPORT_MESSAGE_SCHEMA, separators=(',', ':'))
    )


def _report_source_url(value):
    from urllib.parse import urlsplit, quote
    if not isinstance(value, str) or any(c.isspace() or ord(c) < 32 for c in value): return None
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username or parsed.password: return None
    except ValueError:
        return None
    return quote(value, safe="/:?#[]@!$&'*+,;=%~.-_")


def _report_source_label(source, detailed=False):
    label = source.get('label', '')
    url = _report_source_url(source.get('url'))
    if url and url not in label:
        escaped = label.replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]')
        label = f'[{escaped or "Source"}]({url})'
    expanded = source.get('expanded_label')
    if detailed and expanded and expanded != source.get('label'): label += ' — ' + expanded
    return label


def _report_bound_link_labels(visible, urls):
    """Keep exact cited record targets when a composition omitted their links.

    Callers supply only runtime-bound, finding-specific source URLs. Navigation
    inventories, labels and arbitrary prose never select these references.
    """
    targets = dict.fromkeys(url for raw in urls if (url := _report_source_url(raw)))
    return [f'[Source {i}]({url})' for i, url in enumerate(targets, 1) if url not in visible]


def _report_message_fallback(value: dict, *, include_extra_details: bool = False) -> str:
    """Preserve recognized public content on cosmetic protocol errors."""
    parts = [value.get('title'), value.get('summary')]
    for section in value.get('sections', ()) if isinstance(value.get('sections'), list) else ():
        if not isinstance(section, dict): continue
        parts.append(section.get('heading'))
        for item in section.get('items', ()) if isinstance(section.get('items'), list) else ():
            if not isinstance(item, dict): continue
            parts.extend([item.get('title'), item.get('summary')])
            for key in ('required_details', 'extra_details'):
                if isinstance(item.get(key), list): parts.extend(item[key])
    if isinstance(value.get('sources'), list):
        parts.extend(_report_source_label(row) for row in value['sources'] if isinstance(row, dict)
                     and isinstance(row.get('label'), str))
    if isinstance(value.get('shared_details'), list):
        parts.extend(row.get('text') for row in value['shared_details'] if isinstance(row, dict))
    if include_extra_details and isinstance(value.get('extra_details'), list):
        parts.extend(value['extra_details'])
    parts.append(value.get('footer'))
    return '\n\n'.join(part.strip() for part in parts if isinstance(part, str) and part.strip())


def render_report_message(value: dict, *, diagnostics=None, preferences=None, fallback: str = '', include_title: bool = True, bound_source_urls=()) -> str:
    """Compile a typed report into UserMessage, sharing the existing renderer.

    No truncation, prose classification, extra model pass, or fulfillment gate.
    Required content survives compact mode; shared facts and sources render once.
    """
    from jsonschema import validate, ValidationError
    prefs = _presentation_preferences(preferences)
    detailed = isinstance(value, dict) and (value.get('detail_level') == 'detailed' or (
        value.get('detail_level') == 'default' and prefs.response_length == 'detailed'))
    try:
        validate(value, REPORT_MESSAGE_SCHEMA)
        sources = {row['id']: row for row in value['sources']}
        common = {row['id']: row['text'] for row in value['shared_details']}
        if len(sources) != len(value['sources']) or len(common) != len(value['shared_details']):
            raise ValueError('Duplicate presentation references')
        for section in value['sections']:
            for item in section['items']:
                if not set(item['source_ids']).issubset(sources) or not set(item['shared_detail_ids']).issubset(common):
                    raise ValueError('Unknown presentation reference')
    except (ValidationError, ValueError, TypeError):
        if diagnostics is not None: diagnostics['report_presentation'] = 'content_fallback'
        safe = fallback.strip() or (_report_message_fallback(value, include_extra_details=detailed) if isinstance(value, dict) else '')
        links = _report_bound_link_labels(safe, bound_source_urls)
        return render_user_message(UserMessage(paragraphs=(safe,), footer=' · '.join(links)), preferences=preferences)
    used_sources = set(); sections = []
    for section in sorted(value['sections'], key=lambda row: row['priority'] != 'primary'):
        items = []
        for item in section['items']:
            title = item['title'].strip(); summary = item['summary'].strip()
            text = title + (' — ' if title and summary else '') + summary
            details = list(item['required_details']) + (item['extra_details'] if detailed else [])
            text = ' '.join(part.strip() for part in [text, *details] if part and part.strip())
            labels = []
            for ident in item['source_ids']:
                if ident in used_sources: continue
                source = sources[ident]
                labels.append(_report_source_label(source, detailed))
                used_sources.add(ident)
            if labels: text += ' (' + '; '.join(labels) + ')'
            items.append(text)
        inline = section['layout'] == 'inline' and not detailed
        sections.append(ReplySection(section['heading'], () if inline else tuple(items),
            ('; '.join(items),) if inline and items else ()))
    # Registry rows are explicit public findings, never merely scratch metadata.
    # Retain even unreferenced rows rather than silently deleting verified facts.
    common_text = [row['text'] for row in value['shared_details']]
    remaining_sources = [_report_source_label(row, detailed)
                         for row in value['sources'] if row['id'] not in used_sources]
    expansion = value.get('extra_details', []) if detailed else []
    footer = '\n'.join(part for part in [*common_text, *remaining_sources, value.get('footer', ''), *expansion] if part)
    visible = '\n'.join([value['summary'], footer, *(part for section in sections
        for part in (*section.items, *section.paragraphs))])
    links = _report_bound_link_labels(visible, bound_source_urls)
    if links: footer = '\n'.join(part for part in (footer, ' · '.join(links)) if part)
    if diagnostics is not None:
        diagnostics.update(report_presentation='structured', report_detail_level='detailed' if detailed else 'compact',
            report_source_count=len(sources), report_shared_detail_count=len(common), report_section_count=len(sections),
            report_extra_detail_count=len(value.get('extra_details', [])))
    return render_user_message(UserMessage(title=value.get('title', '') if include_title or value.get('title_is_required') is True else '', summary=value['summary'],
        sections=tuple(sections), footer=footer, kind='report' if include_title else None), preferences=prefs)


def render_report_envelope(text: str | None, *, diagnostics=None, preferences=None, include_title: bool = True) -> str | None:
    """Recognize only the explicit producer envelope, never infer intent."""
    if not isinstance(text, str) or not text.lstrip().startswith('{'): return text
    raw = text.lstrip()
    try: value, end = json.JSONDecoder().raw_decode(raw)
    except (ValueError, TypeError): return text
    if not isinstance(value, dict) or value.get('format') != REPORT_MESSAGE_FORMAT: return text
    # The explicit format selects this contract. Preserve trailing producer
    # text (including attachment descriptors) verbatim; it is not a reason to
    # expose the envelope's optional/internal fields or discard public text.
    suffix = raw[end:].strip()
    fallback = value.get('text') if isinstance(value.get('text'), str) else ''
    rendered = render_report_message(value.get('message'), diagnostics=diagnostics,
        preferences=preferences, fallback=fallback, include_title=include_title)
    return '\n'.join(part for part in (rendered, suffix) if part)


def _presentation_preferences(preferences=None):
    from nullion.preferences import load_preferences, presentation_policy
    if preferences is not None:
        return presentation_policy(preferences)
    return presentation_policy(load_preferences())


def render_user_message(message: UserMessage | str, *, preferences=None) -> str:
    """Render one message shape using the shared preferences, without model IO."""
    if isinstance(message, str):
        return apply_presentation_preferences(message, preferences=preferences)
    prefs = _presentation_preferences(preferences)
    markdown = getattr(prefs, 'markdown_style', 'light')
    structure = getattr(prefs, 'response_structure', 'free')
    bullet = '• ' if markdown == 'plain' else '- '
    def item_text(text, prefix):
        lines = text.splitlines()
        return prefix + lines[0] + ''.join('\n' + ' ' * len(prefix) + line if line else '\n' for line in lines[1:]) if lines else prefix.rstrip()

    def heading(text):
        if not text: return ''
        return text if markdown == 'plain' else '## ' + text if markdown == 'full' else '**' + text + '**'
    groups = [heading(message.title), message.summary, *message.paragraphs]
    section_groups = []
    for section in message.sections:
        items = list(section.items)
        if structure == 'prose':
            content = '\n\n'.join(items)
        elif structure == 'numbered' and not message.choices:
            content = '\n'.join(item_text(item, f'{i}. ') for i, item in enumerate(items, 1))
        else:
            content = '\n'.join(item_text(item, bullet) for item in items)
        section_groups.append('\n'.join(part for part in (heading(section.heading), content) if part))
        section_groups.extend(section.paragraphs)
    for index, finding in enumerate(message.findings, 1):
        title = finding.title if markdown == 'plain' else '**' + finding.title + '**'
        value = title + (' — ' + finding.detail if finding.detail else '') + finding.citations
        prefix = '' if structure == 'prose' else f'{index}. ' if structure == 'numbered' and not message.choices else bullet
        groups.append(item_text(value, prefix) if prefix else value)
    groups.extend(section_groups)
    if message.blocks:
        rendered=[]; previous_item=False; item_index=0
        for block in message.blocks:
            text=block.text.strip()
            if not text: continue
            item=block.kind=='item' and structure!='prose'
            if block.kind=='heading':
                text=heading(text); item_index=0
            elif item:
                item_index+=1
                prefix=f'{item_index}. ' if structure=='numbered' and not message.choices else bullet
                text=item_text(text, prefix)
            separator=('\n' if item and previous_item else '\n\n') if rendered else ''
            rendered.append(separator+text); previous_item=item
        groups.append(''.join(rendered))
    if message.choices:
        groups.append('\n'.join(f'{i}. {label}' for i, label in enumerate(message.choices, 1)))
    groups.append(message.footer)
    text = '\n\n'.join(part.strip() for part in groups if part and part.strip())
    return apply_presentation_preferences(text, preferences=prefs, message_kind=message.kind)


def complete_verified_findings(summary: str, findings: tuple[ReplyFinding, ...], *, preferences=None) -> str:
    """Preserve authored prose and fill omitted, producer-verified fact context."""
    from urllib.parse import urlsplit
    missing = []
    references = []
    represented = summary
    for finding in findings:
        title = finding.title.strip()
        url = finding.citations.strip()
        if url:
            try:
                parsed = urlsplit(url)
                if parsed.scheme not in {'http', 'https'} or not parsed.netloc or parsed.username or parsed.password:
                    url = ''
            except ValueError:
                url = ''
        if title and title not in represented:
            citation = ' ' + url if url and url not in represented else ''
            missing.append(ReplyFinding(title, citations=citation))
            represented += '\n' + title + citation
        elif url and url not in represented:
            references.append(url)
            represented += '\n' + url
    if not missing and not references:
        return summary
    return render_user_message(UserMessage(summary=summary, findings=tuple(missing),
        footer='\n'.join(references)), preferences=preferences)


def structured_user_message(value: dict, *, diagnostics=None, preferences=None) -> str:
    """Adapt model-selected sections to the common contract, preserving fallback."""
    if value.get('format') == REPORT_MESSAGE_FORMAT:
        return render_report_message(value.get('message'), diagnostics=diagnostics, preferences=preferences,
            fallback=value.get('text') if isinstance(value.get('text'), str) else '')
    summary = value.get('summary')
    summary = summary.strip() if isinstance(summary, str) else ''
    sections = value.get('sections')
    valid = (isinstance(sections, list) and bool(sections) and all(
        isinstance(section, dict) and isinstance(section.get('heading'), str) and section['heading'].strip()
        and isinstance(section.get('items'), list) and bool(section['items'])
        and all(isinstance(item, str) and item.strip() for item in section['items'])
        for section in sections))
    if diagnostics is not None:
        diagnostics['presentation'] = 'sections' if valid else 'summary_fallback'
        if valid: diagnostics['section_count'] = len(sections)
    return render_user_message(UserMessage(summary=summary, sections=tuple(
        ReplySection(section['heading'].strip(), tuple(item.strip() for item in section['items']))
        for section in sections) if valid else ()), preferences=preferences)


_ROLE_ICONS = {'email':'✉️', 'calendar':'📅', 'update':'🔔', 'report':'📊',
               'success':'✅', 'warning':'⚠️', 'error':'⚠️', 'scheduled':'⏰'}


def _decorative_character(char):
    point = ord(char)
    return (0x1F000 <= point <= 0x1FAFF or 0x2600 <= point <= 0x27BF
            or point in {0x231A, 0x231B, 0x23E9, 0x23EA, 0x23EB, 0x23EC, 0x23F0, 0x23F3, 0xFE0F, 0x200D})


def presentation_prefix(message_kind, *, preferences=None) -> str:
    policy = _presentation_preferences(preferences)
    icon = _ROLE_ICONS.get(message_kind) if policy.emoji_level != 'none' else None
    return icon + ' ' if icon else ''


def apply_presentation_preferences(text: str, *, preferences=None, message_kind=None) -> str:
    """Shared final style boundary. Never rewrite facts or make another model call."""
    from nullion.redaction import redact_text
    text = redact_text(text)
    prefs = _presentation_preferences(preferences)
    if getattr(prefs, 'markdown_style', 'light') == 'plain':
        def plain_prose(value):
            value = re.sub(r'(?m)^#{1,6} +', '', value)
            for pattern in (r'(?<!\w)(\*\*|__)(?=\S)([\s\S]+?)(?<=\S)\1(?!\w)',
                            r'(?<!\w)(\*|_)(?=\S)([\s\S]+?)(?<=\S)\1(?!\w)'):
                value = re.sub(pattern, lambda match: match.group(2), value)
            return value
        text = _transform_presentation_prose(text, plain_prose)
    if getattr(prefs, 'emoji_level', 'none') == 'none':
        return strip_emoji_characters(text)
    prefix = presentation_prefix(message_kind, preferences=prefs)
    # A producer may already have supplied a relevant accent. The final
    # boundary must not stack another workflow's role icon on that heading.
    visible_start = text.lstrip().lstrip('*_#').lstrip()
    accented = bool(visible_start and _decorative_character(visible_start[0]))
    if prefix and text and not accented:
        heading = re.match(r'^(#{1,6}[ \t]+)', text)
        if heading:
            return text[:heading.end()] + prefix + text[heading.end():]
        return prefix + text
    return text


_PRESENTATION_LITERALS = re.compile(r'(```[\s\S]*?```|`[^`\n]+`|\[[^\]\n]*\]\([^\s]+\)|https?://[^\s<>]+|(?m:^[ \t]*MEDIA:[^\n]*))')


def _transform_presentation_prose(text, transform):
    """Keep code and links literal across every preference transformation."""
    parts = _PRESENTATION_LITERALS.split(str(text))
    for i in range(0, len(parts), 2): parts[i] = transform(parts[i])
    return ''.join(parts)


def strip_emoji_characters(text: str) -> str:
    # This is output styling, never task classification or source interpretation.
    def strip(value):
        lines = []
        for line in value.splitlines(keepends=True):
            # Preserve authored indentation, including nested findings. Only the
            # padding left by a removed leading icon belongs to this cleanup.
            indent = line[:len(line) - len(line.lstrip(' \t'))]
            body = line[len(indent):]
            leading_icon = bool(body and _decorative_character(body[0]) and body[0] not in {'☐','☑','☒'})
            body = ''.join(char for char in body if char in {'☐','☑','☒'} or not _decorative_character(char))
            if leading_icon:
                body = body.lstrip(' \t')
            lines.append(indent + re.sub(r'[ \t]+(?=\n)', '', body))
        return ''.join(lines)
    return _transform_presentation_prose(text, strip).strip()


def transform_markdown_prose(text: str, transform) -> str:
    """Apply presentation cleanup without changing literal Markdown code."""
    parts = re.split(r"(```[\s\S]*?```|`[^`\n]+`)", text)
    if len(parts) == 1:
        return transform(text)
    for index in range(0, len(parts), 2):
        part = parts[index]
        if not part.strip():
            continue
        leading = part[:len(part) - len(part.lstrip())]
        trailing = part[len(part.rstrip()):]
        parts[index] = leading + transform(part).strip() + trailing
    return "".join(parts)


def _is_generic_column_label(label: str) -> bool:
    normalized = " ".join(str(label or "").strip().lower().split())
    if not normalized:
        return True
    if normalized == "column":
        return True
    if normalized.startswith("column ") and normalized.removeprefix("column ").strip().isdigit():
        return True
    return normalized in {"field", "item", "label", "name"}


def _clean_cell(text: str) -> str:
    return " ".join(str(text or "").strip().split())


def _split_table_row(line: str) -> list[str] | None:
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
        return None
    cells = [cell.strip() for cell in stripped[1:-1].split("|")]
    if len(cells) < 2:
        return None
    return cells


def _is_separator_row(cells: list[str]) -> bool:
    for cell in cells:
        normalized = cell.replace(":", "").replace("-", "").strip()
        if normalized:
            return False
    return any("-" in cell for cell in cells)


def _expand_compacted_table_lines(text: str) -> str:
    lines: list[str] = []
    in_fence = False
    for line in str(text).splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            lines.append(line)
            continue
        if in_fence:
            lines.append(line)
            continue
        if "|" not in line or "---" not in line:
            lines.append(line)
            continue
        expanded = re.sub(r"\|\s+(?=\|)", "|\n", line)
        lines.extend(part for part in expanded.splitlines() if part.strip() != "|")
    return "\n".join(lines)


def _render_table_as_list(header: list[str], rows: list[list[str]]) -> list[str]:
    rendered: list[str] = []
    multiple_rows = len(rows) > 1
    for row_index, row in enumerate(rows, start=1):
        cells = [_clean_cell(cell) for cell in row]
        if len(header) == 2 and len(cells) == 2 and cells[0] and cells[1]:
            rendered.append(f"- {cells[0]}: {cells[1]}")
            continue
        parts: list[tuple[str, str]] = []
        for cell_index in range(max(len(header), len(cells))):
            heading = header[cell_index] if cell_index < len(header) else f"Column {cell_index + 1}"
            value = cells[cell_index] if cell_index < len(cells) else ""
            if not value:
                continue
            label = _clean_cell(heading)
            if _is_generic_column_label(label):
                parts.append(("", value))
            else:
                parts.append((label, value))
        if parts:
            if rendered and rendered[-1] != "":
                rendered.append("")
            first_label, first_value = parts[0]
            prefix = f"{row_index}. " if multiple_rows else ""
            rendered.append(f"{prefix}{first_label + ': ' if first_label else ''}{first_value}")
            for label, value in parts[1:]:
                rendered.append(f"{label + ': ' if label else ''}{value}")
    return rendered


def make_markdown_tables_chat_readable(text: str) -> str:
    """Convert Markdown table blocks into wrapped list text for chat delivery."""
    lines = _expand_compacted_table_lines(str(text)).splitlines()
    output: list[str] = []
    index = 0
    in_fence = False
    while index < len(lines):
        line = lines[index]
        if line.strip().startswith("```"):
            in_fence = not in_fence
            output.append(line)
            index += 1
            continue
        if in_fence:
            output.append(line)
            index += 1
            continue

        header = _split_table_row(line)
        separator = _split_table_row(lines[index + 1]) if header is not None and index + 1 < len(lines) else None
        if header is None or separator is None or len(header) != len(separator) or not _is_separator_row(separator):
            output.append(line)
            index += 1
            continue

        rows: list[list[str]] = []
        index += 2
        while index < len(lines):
            row = _split_table_row(lines[index])
            if row is None:
                break
            rows.append(row)
            index += 1
        if rows:
            output.extend(_render_table_as_list(header, rows))
        else:
            output.append(line)
            output.append(lines[index - 1])
    return "\n".join(output)


__all__ = ['UserMessage','ReplySection','ReplyFinding','ReplyBlock','render_user_message',
           'REPORT_MESSAGE_SCHEMA','REPORT_MESSAGE_FORMAT','report_presentation_instructions',
           'render_report_message','render_report_envelope',
           'structured_user_message','presentation_prefix','apply_presentation_preferences','strip_emoji_characters',
           'transform_markdown_prose','make_markdown_tables_chat_readable']
