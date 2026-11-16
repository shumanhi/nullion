"""Shared primary-agent guidance, without a reviewer or execution policy."""
SOURCE_LINK_GUIDANCE = 'Include useful exact source/action URLs from retained evidence.'
RESEARCH_STOPPING_GUIDANCE = (
    'Research to resolve the current request, not to exhaust the available links. '
    'For a recommendation or bounded comparison, select a useful shortlist and verify its material '
    'requested facts; finish when those facts are supported. Broader discovery is necessary for an '
    'explicit exhaustive inventory or a broad negative claim, not for every positive recommendation. '
    'Before another search, identify the specific unresolved requested fact it would establish. '
    'Shortening research must preserve the requested dates, recency, availability and other selection '
    'constraints. Appearance on a current listing does not prove an item was recently added or released. '
    'Verify and report the relevant source dates for a recency-based selection; if those dates are '
    'unavailable, disclose that uncertainty instead of presenting the items as verified recent additions. '
    'Use a targeted read for that gap rather than rephrasing broad searches. '
    'Extra background, media and alternative options are optional unless the current request or an '
    'applicable durable preference requires them; their absence must not delay an otherwise complete answer. '
    'Check parameterized links against verified dates, location and selected options. Prefer a captured '
    'canonical information page for background details; omit mismatched action links rather than rewriting '
    'their parameters or researching merely to add a link. '
    'Reuse saved source data and exact links. Return only the relevant fields from Code Mode, not entire '
    'search pages or every candidate. An invalid retained-data pointer needs a corrected local inspection, '
    'not another external search. Preserve requested coverage and disclose material uncertainty; '
    'never replace required verification with an arbitrary page count.'
)
CONTINUATION_ACTION_GUIDANCE = (
    "Keep the selected request's action limits on follow-ups. Verify changes by reading state. "
    'Running jobs, sending messages or repeating effects needs current authorization. '
    'If unclear, inspect safely or ask a numbered clarification.'
)
SCHEDULER_VERIFICATION_GUIDANCE = (
    'A request for a fresh verification of the last action does not repeat that action or execute the affected job. '
    'After enabling or changing a schedule, verify its current configuration with list_crons; '
    'do not infer permission to run its task from the earlier enable/change receipt. '
    'Choose scheduler_action=run only when execution itself is authorized by the current user request, '
    'not merely because evidence_policy=fresh or the job is already identified.'
)
INPUT_RESOLUTION_GUIDANCE = (
    'Use supplied request and profile values before asking for information again. '
    'For a requested browser destination, verify the requested page state, not only navigation to its parent page. '
    'Before reporting a feedback or other form ready, follow the observed form link and use browser_snapshot '
    'to verify the actual URL and editable fields. Opening a management or contact landing page is not proof '
    'that its form is open. Preserve draft-only consent: do not fill or submit fields unless authorized. '
    'After changing a form, verify that its selected value means the requested parameter, '
    'including its type, precision and context; a shared text fragment is not a match. '
    'Correct a conflicting selection using the known constraints before interpreting results. '
    'If the available choices remain ambiguous or require genuinely missing user information, '
    'pause with a concise numbered clarification that retains the known values. '
    'Searching other sources does not repair an unresolved required input, and results under '
    'the wrong selected state do not establish availability for the requested state.'
)
ARTIFACT_PRODUCER_SELECTION_GUIDANCE = (
    'Use registered format producers for artifacts; they manage their dependencies and return native receipts. '
    'Use file_write for text formats and purpose-built producers for binary or visual formats. '
    'Request missing producer scope before considering terminal_exec. Use shell rendering only when the '
    'registered producers cannot satisfy the requested operation; do not assume Python packages are installed. '
    'For a necessary shell fallback, save long scripts or markup with file_write, then invoke the exact '
    'returned path with shell-appropriate argument quoting instead of nesting large scripts inside strings.'
)
ARTIFACT_PRODUCTION_GUIDANCE = (
    '- For text-and-data infographic images, prefer image_render when available. It uses the shared card layout '
    'and captures HTML to a high-resolution PNG locally. Supply concise, complete facts; avoid repetitive prose. '
    'Use browser_screenshot for source captures, not as the default authoring tool. '
    'Keep generation and source capture distinct in the selected scope and final answer.\n'
    '- ' + ARTIFACT_PRODUCER_SELECTION_GUIDANCE + ' '
    'file_write is not a required intermediate step for a dedicated format producer. '
    'Avoid hand-assembling binary formats.\n'
    '- Use the provided artifact_output_root for new deliverables and working subfolders. '
    'Use exact returned receipt paths in later steps; requested paths may be normalized.\n'
    '- For a file transformation, read or extract the exact current input file before writing. '
    'Chat summaries do not verify the input bytes or replace a requested extraction step.\n'
    '- When replacing a production method or converting support files, use '
    'request_tool_scope.excluded_artifact_extensions to remove abandoned/support-only formats '
    'from delivery. Preserve formats the user requires; deliver only the chosen final files.\n'
    '- For visual artifacts, measure and wrap text within each panel; size panels from the wrapped '
    'text height, preserve margins and separation, and reserve space for headings and footers. '
    'Inspect the actual final rendered pixels at readable size. Repair clipping or overlapping text '
    'in the existing output before delivery; file metadata or dimensions alone are not visual inspection.\n'
)
COMPACT_COMPARISON_GUIDANCE = (
    'Normalize units; keep quoted billing intervals distinct from actual usage, and derive effective rates '
    'from verified quantities.'
)
COMPARISON_GUIDANCE = (
    'Normalize units; calculate ratios and unit costs. Compare like specifications separately from alternatives; '
    'lowest package price may not be best value. Keep the quoted billing interval separate from actual '
    'usage duration. Calculate elapsed time from the verified start and end, not from a weekly or monthly '
    'price label. Preserve the source rate basis; label any calculated effective rate separately and use '
    'the actual verified usage quantity as its denominator.'
)
SOURCE_IDENTITY_GUIDANCE = (
    'Keep each item bound to its exact captured source URL. Use only supported items up to the requested count, '
    'copy their observed links, and state any unverified fields explicitly.'
)
