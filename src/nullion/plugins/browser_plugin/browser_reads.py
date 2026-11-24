"""Read-only page batches in the configured browser, without an inner model loop."""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import replace
from string import Template
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool


def text_links_script(selector=None):
    import json
    return """() => {
        const selector = SELECTOR;
        const root = selector ? document.querySelector(selector) : document.body;
        if (!root) return {document_title:document.title, links:[], headings:[], link_coverage:{scope_found:false}};
        const anchors = [...(root.matches('a[href]') ? [root] : []), ...root.querySelectorAll('a[href]')]
            .filter(node => node.getClientRects().length && getComputedStyle(node).visibility !== 'hidden'
                && ['http:', 'https:'].includes(node.protocol));
        const headings = [...(root.matches('h1,h2,h3') ? [root] : []), ...root.querySelectorAll('h1,h2,h3')]
            .filter(node => node.getClientRects().length && getComputedStyle(node).visibility !== 'hidden');
        const normalize = value => ' ' + value.normalize('NFKC').toLowerCase()
            .replace(/[^\\p{L}\\p{M}\\p{N}]+/gu, ' ').trim() + ' ';
        const title = normalize(document.title);
        const matchingHeadings = headings.filter(node => node.matches('h1')).map(node => node.innerText.trim())
            .filter(label => label.length <= 500 && normalize(label).trim().length >= 24 && title.includes(normalize(label)));
        return {url:location.href, document_title:document.title,
            primary_heading:matchingHeadings.length === 1 ? matchingHeadings[0] : null,
            headings:headings.slice(0,24).map(node => ({level:Number(node.tagName.slice(1)),text:(node.innerText || '').trim().slice(0,1000)})),
            heading_coverage:{scope_found:true,observed_count:headings.length,returned_count:Math.min(24,headings.length),truncated:headings.length>24},
            links:anchors.slice(0,200).map(node => ({text:(node.innerText || node.getAttribute('aria-label') || '').trim().slice(0,500), url:node.href})),
            link_coverage:{scope_found:true, observed_count:anchors.length, returned_count:Math.min(200,anchors.length), truncated:anchors.length>200}};
    }""".replace('SELECTOR', json.dumps(selector))


def observed_link_script(text: str) -> str:
    return Template(r"""() => {
        const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
        const expected = normalize($label);
        const rendered = node => {
            if (node.checkVisibility) {
                return node.checkVisibility({opacityProperty: true, visibilityProperty: true});
            }
            if (!node.getClientRects().length || getComputedStyle(node).visibility !== 'visible') return false;
            for (let ancestor = node; ancestor; ancestor = ancestor.parentElement) {
                const style = getComputedStyle(ancestor);
                if (Number(style.opacity) === 0 || style.contentVisibility === 'hidden') return false;
            }
            return true;
        };
        const links = [...new Set([...document.querySelectorAll('a[href]')]
            .filter(node => ['http:', 'https:'].includes(node.protocol)
                && !node.hasAttribute('download')
                && !node.closest('[inert], [aria-disabled="true" i]')
                && rendered(node)
                && normalize(node.innerText || node.getAttribute('aria-label')) === expected)
            .map(node => node.href))].slice(0, 2);
        return {url: location.href, links};
    }""").substitute(label=json.dumps(text))


class PageContextMatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    selector: str = Field(min_length=1, max_length=500,
        description="Observed unique selector for rendered context text, a selected option label, or a date/time control.")
    text: str = Field(min_length=1, max_length=140,
        description="Exact visible value expected for this request, such as its date, origin, or destination.")


class VerifyPageRecords(BaseModel):
    model_config = ConfigDict(extra="forbid")
    context: list[PageContextMatch] = Field(min_length=1, max_length=6,
        description="The visible values that together verify the requested context on this page.")
    row_selector: str = Field(min_length=1, max_length=500,
        description="Observed selector for the visible result rows, relative to the selected results region.")
    fields: dict[Literal["title", "price_text", "compact_text"], str] = Field(min_length=1, max_length=3,
        description="Required native fields and their observed selectors, relative to each result row. "
                    "Include title and price_text for priced options.")


class LinkSourceRecords(VerifyPageRecords):
    selector: str = Field(min_length=1, max_length=1000,
        description="Observed region on the source page containing the useful overview rows.")


def verified_items_script(extraction, *, selector, verification):
    """Read request context and its records in the same DOM evaluation."""
    from nullion.plugins.browser_plugin.browser_tools import _PRIVATE_CONTROL_SELECTOR

    plan = json.dumps({**verification.model_dump(), "scope_selector": selector})
    return Template(r"""() => {
        const plan = $plan;
        const visible = node => node.checkVisibility
            ? node.checkVisibility({opacityProperty: true, visibilityProperty: true})
            : node.getClientRects().length && getComputedStyle(node).visibility === 'visible';
        const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
        const dateTypes = new Set(['date', 'datetime-local', 'month', 'time', 'week']);
        const valueAt = (root, selector, limit, context = false) => {
            const matches = [...root.querySelectorAll(selector)].filter(visible);
            if (matches.length !== 1) return '';
            const element = matches[0];
            let displayed = '';
            if (element.matches($private_controls) || element.isContentEditable
                    || element.tagName === 'TEXTAREA') return '';
            if (element.tagName === 'INPUT') {
                if (!context || !dateTypes.has(element.type)) return '';
                displayed = element.value;
            } else if (element.tagName === 'SELECT') {
                if (!context || element.multiple || element.size > 1) return '';
                displayed = element.selectedOptions[0]?.label;
            } else {
                displayed = element.innerText;
            }
            const value = normalize(displayed);
            return value.length <= limit ? value : '';
        };
        const context = [];
        for (const check of plan.context) {
            const value = valueAt(document, check.selector, 140, true);
            if (!value || value !== normalize(check.text)) return {verified_context: null};
            context.push(value);
        }
        const rows = new Set();
        for (const scope of document.querySelectorAll(plan.scope_selector)) {
            if (!visible(scope)) continue;
            for (const row of scope.querySelectorAll(plan.row_selector)) {
                if (visible(row)) rows.add(row);
                if (rows.size >= 30) break;
            }
            if (rows.size >= 30) break;
        }
        const limits = {title: 180, price_text: 48, compact_text: 250};
        const records = [...rows].map(row => Object.fromEntries(
            Object.entries(plan.fields).map(([name, selector]) => [name, valueAt(row, selector, limits[name])])
        ));
        return {...$extraction, url:location.href, verified_context: context, verified_records: records};
    }""").substitute(plan=plan, extraction="(" + extraction + ")",
        private_controls=json.dumps(_PRIVATE_CONTROL_SELECTOR))



class PageRead(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str | None = Field(default=None, min_length=1)
    follow_link_text: str | None = Field(default=None, min_length=1, max_length=500,
        description="For read-only navigation from url, follow the unique visible HTTP link with this exact label. "
                    "Use only a link observed on the source page. Readiness and extraction apply to its destination.")
    retain_link_source: LinkSourceRecords | None = Field(default=None,
        description="Verify useful overview facts on the source page before following the link. "
                    "They remain available even if the destination cannot be read.")
    read_kind: Literal["items", "detail", "text"] = "items"
    selector: str | None = Field(default=None, min_length=1, max_length=1000)
    section_heading: str | None = Field(default=None, min_length=1, max_length=500)
    ready_selector: str | None = Field(default=None, min_length=1, max_length=500)
    ready_text: str | None = Field(default=None, min_length=1, max_length=200)
    expected_heading: str | None = Field(default=None, min_length=1, max_length=500,
        description="For detail reads, the visible product or record heading, or a distinctive phrase from it, observed "
                    "on the source page. Wait for one matching heading and scope the detail read to its record.")
    max_items: int = Field(default=30, ge=1, le=100)
    max_text_chars: int = Field(default=16000, ge=1000, le=50000)
    verify_records: VerifyPageRecords | None = Field(default=None,
        description="Verify context and retain structured results for delivery if another source or the model times out.")


class PageReadBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pages: list[PageRead] = Field(min_length=1, max_length=6)
    independent_pages: StrictBool = False
    session_id: str | None = None


def can_batch_read_plan(owner, arguments, principal_id):
    """Read-only preflight; invalid/blocked reads keep separate outcomes."""
    from nullion.plugins.browser_plugin.browser_tools import _principal_allows_private_host_navigation
    try:
        batch = PageReadBatch.model_validate(arguments)
        for page in batch.pages:
            if not page.url or page.follow_link_text:
                return False
            owner._policy.check_url(page.url,
                allow_private_host=_principal_allows_private_host_navigation(principal_id))
        return batch.independent_pages
    except Exception:
        return False


def compact_page_collection(output):
    """Lossless model projection: retain records/links, elide exact repetitions."""
    import json
    projected = dict(output)
    aliases = {}
    items = []
    for item in output.get('items', ()):
        if not isinstance(item, dict):
            items.append(item)
            continue
        record = {key:value for key,value in item.items() if value not in (None, '', [], {})}
        for alias, target in (('canonical_url','url'), ('link_text','title')):
            if alias in record and target in record and record[alias] == record[target]:
                del record[alias]
                aliases[alias] = target + ' when identical'
        items.append(record)
    if 'items' in output:
        projected['items'] = items
    if aliases:
        projected['field_aliases'] = {**output.get('field_aliases', {}), **aliases}
    documents = []
    for document in output.get('documents', ()):
        if not isinstance(document, dict) or not isinstance(document.get('links'), list):
            documents.append(document)
            continue
        seen, links = set(), []
        for link in document['links']:
            key = json.dumps(link, sort_keys=True, ensure_ascii=False)
            if key not in seen:
                seen.add(key)
                links.append(link)
        documents.append({**document, 'links':links, **({'link_projection':{
            'exact_duplicates_elided':len(document['links'])-len(links),
            'unique_links_retained':len(links)}} if len(links)!=len(document['links']) else {})})
    if 'documents' in output:
        projected['documents'] = documents
    return projected


class BrowserReadFirstPolicy:
    """Request-local native observations, shared by every caller of this plugin.

    History and client identity cannot opt a request into adaptive delegation.
    An opaque objective digest invalidates observations after an amendment.
    """

    def __init__(self):
        import threading
        self._lock = threading.Lock()
        self._observations = {}
        self._read_results = {}

    @staticmethod
    def key(invocation):
        context = invocation.flow_context or {}
        scope = (invocation.capsule_id or context.get("request_id")
                 or context.get("turn_id") or invocation.invocation_id)
        objective = str(context.get("user_request") or "")
        return (invocation.principal_id, str(scope), hashlib.sha256(objective.encode()).hexdigest())

    def remember(self, invocation, *urls):
        with self._lock:
            now = time.monotonic()
            self._observations = {key: value for key, value in self._observations.items()
                                  if now - value[0] < 600}
            key = self.key(invocation)
            observed = self._observations.get(key, (now, set()))[1]
            observed.update(url for url in urls if isinstance(url, str) and url)
            self._observations[key] = (now, observed)
            while len(self._observations) > 512:
                self._observations.pop(next(iter(self._observations)))

    def contains(self, invocation, url):
        with self._lock:
            remembered_at, urls = self._observations.get(self.key(invocation), (0, ()))
            return time.monotonic() - remembered_at < 600 and url in urls

    def remember_result(self, invocation, result):
        from copy import deepcopy
        with self._lock:
            now = time.monotonic()
            self._read_results = {key:value for key,value in self._read_results.items() if now-value[0]<600}
            for page in result.output.get('pages', ()):
                for url in (page.get('requested_url'), page.get('page_url')):
                    if url:
                        self._read_results[(self.key(invocation), url)] = (now, deepcopy(result))
            while len(self._read_results)>512:
                self._read_results.pop(next(iter(self._read_results)))

    def read_result(self, invocation, url):
        from copy import deepcopy
        with self._lock:
            item = self._read_results.get((self.key(invocation), url))
            return deepcopy(item[1]) if item and time.monotonic()-item[0]<600 else None

    def invalidate_reads(self, invocation):
        with self._lock:
            scope = self.key(invocation)
            self._read_results = {key:value for key,value in self._read_results.items() if key[0]!=scope}

    def clear(self, scope=None):
        with self._lock:
            if scope is None:
                self._observations.clear()
                self._read_results.clear()
            else:
                self._observations = {key: value for key, value in self._observations.items()
                                      if key[1] != str(scope)}
                self._read_results = {key:value for key,value in self._read_results.items() if key[0][1]!=str(scope)}


def browser_task_read_preflight(owner, invocation):
    """Return native page evidence before starting a nested browser agent.

    This is execution policy at the registered tool boundary, not a client
    prompt or another classifier. The primary agent decides what work remains
    after seeing the page. A subsequent invocation may delegate interaction.
    """
    from nullion.artifacts import normalize_artifact_extensions
    from nullion.plugins.browser_plugin.browser_tools import _fail
    context = invocation.flow_context or {}
    extensions = set()
    for name in ("artifact_extensions", "required_artifact_extensions", "requested_artifact_extensions"):
        values = context.get(name) or ()
        extensions.update(normalize_artifact_extensions((values,) if isinstance(values, str) else values))
    if ".png" in extensions and context.get('source_artifact_delivery_requested') is True:
        return None  # Explicit visual evidence already requires visual capture.
    url = invocation.arguments["url"]
    adaptive = invocation.arguments.get('execution_kind') == 'adaptive_interaction'
    urls = list(dict.fromkeys([url, *(invocation.arguments.get("read_urls") or [])]))
    if len(urls) > 1 and (adaptive or invocation.arguments.get("independent_pages") is not True):
        return _fail(invocation, "Multiple source reads require an explicit independent read plan.",
            {"reason":"invalid_tool_arguments","executed":False})
    if adaptive and owner._read_first_policy.contains(invocation, url):
        return None
    read = replace(invocation, tool_name="browser_read_pages", arguments={
        "pages": [{"url": source, "read_kind": "text"} for source in urls],
        "independent_pages": len(urls) > 1})
    # One cached page does not satisfy a newly declared multi-source objective.
    result = owner._read_first_policy.read_result(invocation, url) if len(urls) == 1 else None
    reused = result is not None
    if result is None:
        result = owner.browser_read_pages(read)
    source_invocation_id = result.invocation_id
    result.invocation_id = invocation.invocation_id
    # Failed reads are evidence too: after inspecting a native failure the
    # primary may choose another controller. Pending consent/cancellation must
    # never unlock adaptive execution.
    pages = result.output.get("pages", ())
    if not result.output.get("requires_user_choice") and not any(
            page.get("failure_reason") == "request_cancelled" for page in pages):
        owner._read_first_policy.remember(invocation, url)
    result.tool_name = invocation.tool_name
    if result.status != 'completed':
        # A native read may have run, but the requested delegated interaction
        # has not. Do not classify a preflight failure as an uncertain write.
        result.output.update(executed=False, execution_scope='delegated_interaction')
    result.output.update(completion_scope="tool_step", strategy={
        "kind": "native_read_preflight", "adaptive_browser_started": False,
        "read_reused":reused, "source_invocation_id":source_invocation_id,
        "continuation": {
            "tool_name": invocation.tool_name,
            "url": url,
            "execution_kind": "adaptive_interaction",
            "condition": "requested_evidence_requires_interaction",
            "guidance": "Only a native page read ran. Use these findings or lightweight readers for research. "
                "For a visible link to the requested results, use browser_read_pages with the captured URL, "
                "or follow_link_text when only its label was retained. "
                "If required evidence needs interactive controls or a visual state, call this tool with "
                "execution_kind=adaptive_interaction and an interaction_reason grounded in this observation, "
                "or use native controls. Changing the controller is distinct from repeating an empty read. "
                "Respect existing permissions and the user's action restrictions.",
        }})
    return result


def read_pages(owner, invocation):
    from nullion.plugins.browser_plugin.browser_tools import (
        _run, _ok, _fail, _browser_failure_output, _browser_extract_items_script,
        _browser_extract_detail_script, _principal_allows_private_host_navigation,
    )
    from nullion.tools import tool_execution_remaining_seconds, tool_execution_cancelled
    try:
        batch = PageReadBatch.model_validate(invocation.arguments)
        if len(batch.pages) > 1 and any(page.url is None for page in batch.pages):
            raise ValueError("Multiple reads require an explicit URL for every page")
        # Validate the entire batch before opening any tab. URLs and source text
        # are opaque references; no website or natural-language intent routing.
        for page in batch.pages:
            if page.expected_heading is not None and (page.read_kind != "detail" or not page.expected_heading.strip()):
                raise ValueError("Expected heading requires a detail read and a visible heading label.")
            if page.follow_link_text and (not page.url or len(batch.pages) != 1):
                raise ValueError("Following an observed link requires one source URL in a single-page read.")
            if page.retain_link_source and not page.follow_link_text:
                raise ValueError("Retaining a link source requires follow_link_text.")
            if page.verify_records and (page.read_kind != "items" or not page.selector
                    or not (page.ready_selector or page.ready_text)):
                raise ValueError("Record verification requires an item read, results selector and readiness condition.")
            for verification in (page.verify_records, page.retain_link_source):
                if verification is not None and ("title" not in verification.fields
                        or any(not 0 < len(value.strip()) <= 500 for value in verification.fields.values())):
                    raise ValueError("Record verification requires valid visible field selectors including title.")
            if page.url:
                owner._policy.check_url(page.url,
                    allow_private_host=_principal_allows_private_host_navigation(invocation.principal_id))
    except Exception as error:
        return _fail(invocation, str(error), {"reason": "invalid_tool_arguments"})
    parent_session = owner._session_id(invocation)
    owner._remember_session(invocation, parent_session)
    started = time.monotonic()
    completed_reads = set()
    verified_link_sources = []
    parallel_limit = 2 if batch.independent_pages else 1

    async def execute():
        semaphore = asyncio.Semaphore(parallel_limit)
        verified_reads = {}

        async def read(index, request):
            # The current tab is retained when no URL is supplied. Explicit
            # page reads use task-owned tabs, leaving the user's current tab.
            session = parent_session if request.url is None else parent_session + "-read-" + hashlib.sha256(
                f"{invocation.invocation_id}:{index}".encode()).hexdigest()[:12]
            owns_tab = session != parent_session
            async with semaphore:
                followed_link = None
                try:
                    if owns_tab:
                        # Register the exact owned tab before creation. A
                        # cancelled/failed close remains eligible for the
                        # existing scope cleanup; no global tab sweep is used.
                        with owner._cleanup_lock:
                            owner._sessions_by_scope.setdefault(owner._cleanup_scope(invocation), set()).add(session)
                        inherit = getattr(owner._backend, "share_session_context", None)
                        if inherit is not None:
                            import inspect
                            pending_context = inherit(parent_session, session)
                            if inspect.isawaitable(pending_context):
                                await pending_context
                        await owner._backend.navigate(session, request.url)
                        completed_reads.add("browser_navigate")
                    if request.follow_link_text:
                        allow_private = _principal_allows_private_host_navigation(invocation.principal_id)
                        source_url = await owner._backend.run_js(session, "() => location.href")
                        owner._policy.check_url(source_url, allow_private_host=allow_private)
                        link_script = observed_link_script(request.follow_link_text)
                        link_deadline = time.monotonic() + 5
                        observation = await owner._backend.run_js(session, link_script)
                        while (isinstance(observation, dict) and observation.get("url") == source_url
                               and observation.get("links") == [] and time.monotonic() < link_deadline):
                            await asyncio.sleep(0.25)
                            observation = await owner._backend.run_js(session, link_script)
                        if not isinstance(observation, dict) or observation.get("url") != source_url:
                            return {"status": "failed", "requested_url": request.url, "page_url": source_url,
                                "read_kind": request.read_kind, "failure_reason": "source_context_unverified"}
                        links = observation.get("links")
                        if verification := request.retain_link_source:
                            from nullion.browser_verified_records import verified_page_capture

                            script = verified_items_script("{}", selector=verification.selector, verification=verification)
                            source = await owner._backend.run_js(session, script)
                            if isinstance(source, dict) and source.get("url") == source_url:
                                captured = verified_page_capture(page_url=source_url, capture_time=time.time(),
                                    context=source.get("verified_context"), rows=source.get("verified_records") or (),
                                    fields=verification.fields)
                                if captured:
                                    verified_link_sources.append(captured)
                        if not isinstance(links, list) or len(links) != 1:
                            return {"status": "failed", "requested_url": request.url, "page_url": source_url,
                                "read_kind": request.read_kind,
                                "failure_reason": "source_link_ambiguous" if links else "source_link_unavailable"}
                        destination = links[0]
                        owner._policy.check_url(destination, allow_private_host=allow_private)
                        followed_link = {"source_url": source_url, "destination_url": destination}
                        await owner._backend.navigate(session, destination)
                    if request.ready_selector:
                        await owner._backend.wait_for(session, request.ready_selector, None, None, 5)
                        completed_reads.add("browser_wait_for")
                    if request.ready_text:
                        await owner._backend.wait_for(session, None, None, request.ready_text, 5)
                        completed_reads.add("browser_wait_for")
                    current_url = await owner._backend.run_js(session, "() => location.href")
                    owner._policy.check_url(current_url,
                        allow_private_host=_principal_allows_private_host_navigation(invocation.principal_id))
                    if request.read_kind == "items":
                        script = _browser_extract_items_script(max_items=request.max_items,
                            selector=request.selector, section_heading=request.section_heading)
                        if request.verify_records:
                            script = verified_items_script(script, selector=request.selector, verification=request.verify_records)
                        data = await owner._backend.run_js(session, script)
                        if request.verify_records and isinstance(data, dict) and data.get("verified_context") is None:
                            return {"status": "failed", "requested_url": request.url, "page_url": current_url,
                                    "read_kind": "items", "failure_reason": "source_context_unverified"}
                        if not isinstance(data, dict) or not isinstance(data.get("items"), list):
                            raise ValueError("Page item extraction returned an invalid result")
                        if (data.get("coverage") or {}).get("scope_found") is False:
                            return {"status": "failed", "page_url": current_url, "requested_url": request.url,
                                "failure_reason": "source_scope_missing", "coverage": data.get("coverage"),
                                "read_kind": request.read_kind}
                    elif request.read_kind == "detail":
                        script = _browser_extract_detail_script(selector=request.selector,
                            expected_heading=request.expected_heading, match_document_title=True)
                        heading_deadline = time.monotonic() + 5
                        data = await owner._backend.run_js(session, script)
                        while (request.expected_heading and isinstance(data, dict)
                               and data.get("reason") in {"expected_detail_heading_missing", "scope_not_found"}
                               and time.monotonic() < heading_deadline):
                            await asyncio.sleep(0.25)
                            data = await owner._backend.run_js(session, script)
                        if not isinstance(data, dict):
                            raise ValueError("Page detail extraction returned an invalid result")
                        if data.get("ok") is False:
                            if request.expected_heading:
                                return {"status": "failed", "requested_url": request.url, "page_url": current_url,
                                    "read_kind": "detail", "failure_reason": data.get("reason")}
                            # The same authorized DOM can still expose text. Keep
                            # this failure as provenance, not a completed detail.
                            detail_failure = {"ok": False, "reason": data.get("reason")}
                            links = await owner._backend.run_js(session, text_links_script(request.selector))
                            text = ("" if isinstance(links, dict) and (links.get("link_coverage") or {}).get("scope_found") is False
                                else await owner._backend.extract_text(session, request.selector))
                            data = {"selector": request.selector, "text": text[:request.max_text_chars],
                                "text_length": len(text), "truncated": len(text) > request.max_text_chars,
                                "detail_extraction": detail_failure}
                            if isinstance(links, dict):
                                data.update(links)
                            request = request.model_copy(update={"read_kind": "text"})
                    else:
                        links = await owner._backend.run_js(session, text_links_script(request.selector))
                        text = ("" if isinstance(links, dict) and (links.get("link_coverage") or {}).get("scope_found") is False
                            else await owner._backend.extract_text(session, request.selector))
                        data = {"selector": request.selector, "text": text[:request.max_text_chars], "text_length": len(text),
                            "truncated": len(text) > request.max_text_chars}
                        if isinstance(links, dict):
                            data.update(links)
                            if heading := links.get("primary_heading"):
                                detail = None
                                try:
                                    detail = await owner._backend.run_js(session, _browser_extract_detail_script(
                                        selector=request.selector, expected_heading=heading))
                                except Exception:
                                    pass  # The original text read is already captured.
                                if isinstance(detail, dict) and detail.get("ok") and detail.get("page", {}).get("url") == links.get("url"):
                                    record = detail.get("record", {})
                                    data["observed_title"] = record.get("title")
                                    data["observed_text"] = detail.get("observed_text")
                                    if record.get("price"):
                                        data["observed_price"] = record["price"]
                    if request.read_kind == "text" and (
                            (data.get("detail_extraction") and not str(data.get("text") or "").strip())
                            or (data.get("link_coverage") or {}).get("scope_found") is False):
                        return {"status": "failed", "requested_url": request.url, "page_url": current_url,
                            "read_kind": "text", "failure_reason": "source_scope_missing"
                                if (data.get("link_coverage") or {}).get("scope_found") is False else "source_empty"}
                    observed_url = data.get("url") or await owner._backend.run_js(session, "() => location.href")
                    owner._policy.check_url(observed_url,
                        allow_private_host=_principal_allows_private_host_navigation(invocation.principal_id))
                    completed_reads.add({"items": "browser_extract_items", "detail": "browser_extract_detail",
                                         "text": "browser_extract_text"}[request.read_kind])
                    result = {"status": "completed", "requested_url": request.url, "page_url": observed_url,
                        "read_kind": request.read_kind, "readiness": {"selector": request.ready_selector,
                            "text": request.ready_text,
                            "condition_verified": bool(request.ready_selector or request.ready_text
                                                       or (request.read_kind == "detail" and request.expected_heading)),
                            **({"heading": request.expected_heading} if request.read_kind == "detail"
                               and request.expected_heading else {})},
                        "data": data}
                    if followed_link:
                        result["followed_link"] = followed_link
                    if request.verify_records:
                        from nullion.browser_verified_records import verified_page_capture
                        result["verified_page"] = verified_page_capture(page_url=observed_url, capture_time=time.time(),
                            context=data.pop("verified_context"), rows=data.pop("verified_records"),
                            fields=request.verify_records.fields)
                        if result["verified_page"]:
                            # Closing a task-owned tab must not revoke a page
                            # result already captured before the batch deadline.
                            verified_reads[index] = result
                    return result
                except Exception as error:
                    return {"status": "failed", "requested_url": request.url, "read_kind": request.read_kind,
                        "error": str(error)[:500], **_browser_failure_output(error),
                        **({"followed_link": followed_link} if followed_link else {})}
                finally:
                    if owns_tab:
                        try:
                            await owner._backend.close_session(session)
                        except Exception:
                            pass
                        else:
                            owner._forget_session(session)
        tasks = [asyncio.create_task(read(index, page)) for index, page in enumerate(batch.pages)]
        remaining = tool_execution_remaining_seconds()
        budget = min(35.0, max(0.0, remaining - 5)) if remaining is not None else 35.0
        cancelled = False
        try:
            completed, pending = set(), set(tasks)
            deadline = time.monotonic() + budget
            while pending and time.monotonic() < deadline:
                if tool_execution_cancelled():
                    cancelled = True
                    break
                finished, pending = await asyncio.wait(pending, timeout=min(.1, max(0, deadline-time.monotonic())),
                    return_when=asyncio.FIRST_COMPLETED)
                completed.update(finished)
                if finished and invocation.progress_callback is not None and not tool_execution_cancelled():
                    observed = [task.result() for task in completed if not task.cancelled()]
                    from nullion.request_progress import publish_source_progress
                    publish_source_progress(invocation, unit='pages', total=len(batch.pages),
                        completed=sum(r.get('status') == 'completed' for r in observed),
                        failed=sum(r.get('status') != 'completed' for r in observed),
                        records=sum(len(r.get('data', {}).get('items', ())) for r in observed if r.get('status') == 'completed'))
            # Keep already observed pages if another source stalls. Cancellation
            # only closes the task-owned tabs, never the managed browser.
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=3)
            return [task.result() if task in completed and not task.cancelled() else verified_reads.get(index, {
                "status": "failed", "requested_url": request.url, "read_kind": request.read_kind,
                "failure_reason": "request_cancelled" if cancelled else "source_timeout", "failure_scope": "invocation"})
                for index, (task, request) in enumerate(zip(tasks, batch.pages))]
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

    results = _run(execute(), timeout_seconds=45)
    items, details, documents, pages = [], [], [], []
    verified_pages = list(verified_link_sources)
    for result in results:
        if verified := result.pop("verified_page", None):
            verified_pages.append(verified)
        data = result.pop("data", {})
        pages.append(result)
        if result["status"] != "completed":
            continue
        page_url = result["page_url"]
        owner._read_first_policy.remember(invocation, result.get("requested_url"), page_url)
        if result["read_kind"] == "items":
            result["coverage"] = data.get("coverage")
            result["item_count"] = len(data["items"])
            items.extend({**row, "source_page_url": page_url} for row in data["items"] if isinstance(row, dict))
        elif result["read_kind"] == "detail":
            record = data.get("record") or {}
            details.append({**data, "source_page_url": page_url, **{
                key: record[key] for key in ("title", "price", "availability", "description", "details") if key in record
            }})
        else:
            documents.append({**data, "url": page_url})
    declarations = []
    for key, records, identities in (("items", items, ["source_page_url", "source_index"]),
                                     ("details", details, ["source_page_url"]), ("documents", documents, ["url"])):
        if records:
            context_fields = ({'items':['title','price_text','numeric_snippets','url'],
                'details':['title','price','availability','observed_text','description','details'],
                'documents':['observed_title','observed_price','observed_text','text','links','url']})[key]
            declarations.append({"path": [key], "identity_fields": identities, "cardinality": "many",
                'context_fields':context_fields,
                **({'context_widths':{'observed_text':2000}} if key in {'details', 'documents'} else {})})
    completed = sum(page["status"] == "completed" for page in pages)
    from nullion.request_progress import publish_source_progress
    publish_source_progress(invocation, unit='pages', completed=completed,
        failed=len(pages)-completed, total=len(pages), records=len(items))
    output = {"result_kind": "browser_page_collection", "pages": pages, "items": items,
        "details": details, "documents": documents, "source_record_collections": declarations,
        "completed_pages": completed, "requested_pages": len(pages), "session_id": parent_session,
        "parallel_limit": parallel_limit, "elapsed_seconds": round(time.monotonic()-started, 3),
        **({"verified_pages": verified_pages} if verified_pages else {})}
    # A wholly failed reconnect offers the existing numbered recovery choices.
    # Partial batches keep their observed findings and per-page failures.
    if not completed:
        choice = next((page for page in pages if page.get("requires_user_choice")), None)
        if choice:
            output.update({key: value for key, value in choice.items()
                           if key not in {"status", "requested_url", "read_kind", "error"}})
        elif pages and all(page.get('failure_reason') in {'source_error','source_timeout'} for page in pages):
            output['failure_reason'] = 'source_timeout' if all(page.get('failure_reason')=='source_timeout' for page in pages) else 'source_error'
        failed = _fail(invocation, "The requested pages could not be read", output)
        owner._read_first_policy.remember_result(invocation, failed)
        return failed
    result = _ok(invocation, output)
    result.response_requirements = {"_native_completed_reads": {
        "tool_names": sorted(completed_reads), "read_only": True}}
    owner._read_first_policy.remember_result(invocation, result)
    return result
