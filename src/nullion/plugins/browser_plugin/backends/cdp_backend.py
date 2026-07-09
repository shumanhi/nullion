"""Browser plugin — CDP (attach to existing Chrome/Brave) backend.

Connects to a browser that was launched with --remote-debugging-port=9222.

Requires: pip install playwright  (uses Playwright's CDP support)

To launch Chrome with CDP enabled:
    /Applications/Google Chrome.app/Contents/MacOS/Google Chrome \
        --remote-debugging-port=9222 --no-first-run

Set NULLION_BROWSER_CDP_URL=http://127.0.0.1:9222
"""
from __future__ import annotations

import asyncio
import base64
import fnmatch
import json
import os
import re
from types import SimpleNamespace
from typing import Any
from urllib.request import Request, urlopen
from urllib.parse import urlparse, urlunparse

from nullion.plugins.browser_plugin.browser_config import DEFAULT_AGENT_BROWSER_SESSION_ID
from nullion.plugins.browser_plugin.browser_session import (
    BrowserScreenshotResult,
    auto_screenshot_uses_full_page,
)

try:
    from playwright.async_api import async_playwright, CDPSession, Browser, Page
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False


DEFAULT_CDP_URL = "http://127.0.0.1:9222"
_BLANK_PAGE_URLS = frozenset({"", "about:blank", "chrome://newtab/", "brave://newtab/"})
_TYPE_TEXT_TIMEOUT_MS = 3_000
_CLICK_TIMEOUT_MS = 5_000


def _require_playwright() -> None:
    if not _PLAYWRIGHT_AVAILABLE:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright"
        )


def normalized_cdp_url(raw_url: str | None = None) -> str:
    """Use IPv4 loopback for local CDP to avoid localhost resolving to ::1."""
    url = (raw_url or os.environ.get("NULLION_BROWSER_CDP_URL") or DEFAULT_CDP_URL).strip()
    if not url:
        return DEFAULT_CDP_URL
    parsed = urlparse(url)
    if parsed.hostname in {"localhost", "::1"}:
        port = parsed.port or 9222
        netloc = f"127.0.0.1:{port}"
        return urlunparse(parsed._replace(netloc=netloc))
    return url


def _page_url(page: object) -> str:
    try:
        return str(getattr(page, "url", "") or "").strip()
    except Exception:
        return ""


def _page_is_blank(page: object) -> bool:
    return _page_url(page).lower() in _BLANK_PAGE_URLS


async def _close_page_quietly(page: object) -> None:
    close = getattr(page, "close", None)
    if not callable(close):
        return
    try:
        result = close()
        if asyncio.iscoroutine(result):
            await result
    except Exception:
        pass


async def _prune_extra_blank_pages(pages: list[object]) -> list[object]:
    open_pages = [page for page in pages if not page.is_closed()]
    if len(open_pages) <= 1:
        return open_pages
    nonblank_pages = [page for page in open_pages if not _page_is_blank(page)]
    blank_pages = [page for page in open_pages if _page_is_blank(page)]
    keep_blank = [] if nonblank_pages else blank_pages[-1:]
    keep = [*nonblank_pages, *keep_blank]
    for page in blank_pages:
        if page in keep_blank:
            continue
        await _close_page_quietly(page)
    return keep


def _json_url(cdp_url: str, path: str) -> str:
    return f"{cdp_url.rstrip('/')}/{path.lstrip('/')}"


def _read_json_url(url: str) -> object:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _cdp_targets(cdp_url: str) -> list[dict[str, object]]:
    payload = _read_json_url(_json_url(cdp_url, "/json/list"))
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


async def _async_cdp_targets(cdp_url: str) -> list[dict[str, object]]:
    return await asyncio.to_thread(_cdp_targets, cdp_url)


async def _async_new_cdp_target(cdp_url: str) -> dict[str, object] | None:
    def create() -> dict[str, object] | None:
        request = Request(_json_url(cdp_url, "/json/new?about:blank"), method="PUT")
        try:
            with urlopen(request, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    return await asyncio.to_thread(create)


async def _async_page_targets_or_new(cdp_url: str) -> list[dict[str, object]]:
    targets = [
        target
        for target in await _async_cdp_targets(cdp_url)
        if str(target.get("type") or "") == "page"
    ]
    if targets:
        return targets
    target = await _async_new_cdp_target(cdp_url)
    if target and str(target.get("type") or "") == "page":
        return [target]
    return []


def _dom_type_script(selector: str, text: str) -> str:
    return (
        "(() => {"
        f" const el = document.querySelector({json.dumps(selector)});"
        f" const text = {json.dumps(text)};"
        " if (!el) throw new Error(`selector not found: ${selector}`);"
        " const style = window.getComputedStyle(el);"
        " const rect = el.getBoundingClientRect();"
        " const visible = style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;"
        " if (!visible) throw new Error('element not visible');"
        " if (el.disabled || el.getAttribute('aria-disabled') === 'true') throw new Error('element disabled');"
        " const tag = (el.tagName || '').toLowerCase();"
        " const type = (el.getAttribute('type') || '').toLowerCase();"
        " const editable = el.isContentEditable || tag === 'textarea' || tag === 'select' || el.getAttribute('role') === 'combobox' || el.getAttribute('contenteditable') === 'true' || (tag === 'input' && type !== 'hidden');"
        " if (!editable) throw new Error('element not editable');"
        " if (typeof el.focus === 'function') el.focus();"
        " const proto = el instanceof HTMLTextAreaElement"
        "   ? HTMLTextAreaElement.prototype"
        "   : el instanceof HTMLInputElement ? HTMLInputElement.prototype : null;"
        " const setter = proto && Object.getOwnPropertyDescriptor(proto, 'value')?.set;"
        " if (setter) setter.call(el, text);"
        " else if (el.isContentEditable) el.textContent = text;"
        " else el.value = text;"
        " el.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: text }));"
        " el.dispatchEvent(new Event('change', { bubbles: true }));"
        "})()"
    )


def _dom_type_element_function() -> str:
    return (
        "(el, text) => {"
        " if (!el) throw new Error('element not found');"
        " const style = window.getComputedStyle(el);"
        " const rect = el.getBoundingClientRect();"
        " const visible = style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;"
        " if (!visible) throw new Error('element not visible');"
        " if (el.disabled || el.getAttribute('aria-disabled') === 'true') throw new Error('element disabled');"
        " if (typeof el.scrollIntoView === 'function') el.scrollIntoView({block:'center', inline:'center'});"
        " if (typeof el.focus === 'function') el.focus({preventScroll: true});"
        " const tag = (el.tagName || '').toLowerCase();"
        " const type = (el.getAttribute('type') || '').toLowerCase();"
        " const editable = el.isContentEditable || tag === 'textarea' || tag === 'select' || el.getAttribute('role') === 'combobox' || el.getAttribute('contenteditable') === 'true' || (tag === 'input' && type !== 'hidden');"
        " if (!editable) {"
        "   const opts = {bubbles: true, cancelable: true, view: window};"
        "   for (const type of ['pointerdown','mousedown','pointerup','mouseup']) el.dispatchEvent(new MouseEvent(type, opts));"
        "   if (typeof el.click === 'function') el.click();"
        "   else el.dispatchEvent(new MouseEvent('click', opts));"
        "   return {ok: true, clicked: true, editable: false};"
        " }"
        " const before = 'value' in el ? String(el.value || '') : String(el.textContent || '');"
        " const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : el instanceof HTMLInputElement ? HTMLInputElement.prototype : null;"
        " const setter = proto && Object.getOwnPropertyDescriptor(proto, 'value')?.set;"
        " if (setter) setter.call(el, text);"
        " else if ('value' in el) el.value = text;"
        " else el.textContent = text;"
        " el.dispatchEvent(new InputEvent('input', {bubbles: true, inputType: 'insertText', data: text}));"
        " el.dispatchEvent(new Event('change', {bubbles: true}));"
        " const after = 'value' in el ? String(el.value || '') : String(el.textContent || '');"
        " return {ok: true, clicked: false, editable: true, before, after};"
        "}"
    )


def _dom_click_script(selector: str) -> str:
    return (
        "(() => {"
        f" const el = document.querySelector({json.dumps(selector)});"
        " if (!el) throw new Error(`selector not found: ${selector}`);"
        " if (typeof el.scrollIntoView === 'function') el.scrollIntoView({block:'center', inline:'center'});"
        " if (typeof el.focus === 'function') el.focus();"
        " const opts = { bubbles: true, cancelable: true, view: window };"
        " for (const type of ['pointerdown','mousedown','pointerup','mouseup']) el.dispatchEvent(new MouseEvent(type, opts));"
        " if (typeof el.click === 'function') el.click();"
        " else el.dispatchEvent(new MouseEvent('click', opts));"
        "})()"
    )


def _dom_click_target_script(target: dict[str, object]) -> str:
    selector = str(target.get("selector") or "").strip()
    if selector:
        return _dom_click_script(selector)
    expected = [
        str(target.get(key) or "").strip()
        for key in ("text", "label", "placeholder", "name")
        if str(target.get(key) or "").strip()
    ]
    role = str(target.get("role") or "").strip()
    return (
        "(() => {"
        f" const expected = {json.dumps(expected)};"
        f" const role = {json.dumps(role)};"
        " const normalize = (value) => String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();"
        " const wanted = expected.map(normalize).filter(Boolean);"
        " const visible = (el) => {"
        "   const style = window.getComputedStyle(el);"
        "   const rect = el.getBoundingClientRect();"
        "   return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;"
        " };"
        " const textOf = (el) => ["
        "   el.getAttribute('aria-label'), el.getAttribute('placeholder'), el.getAttribute('name'),"
        "   el.getAttribute('title'), 'value' in el ? el.value : '', el.innerText, el.textContent"
        " ].filter(Boolean).join(' ');"
        " const compatible = (value) => {"
        "   const normalized = normalize(value);"
        "   return normalized && wanted.some((item) => normalized === item || normalized.includes(item) || item.includes(normalized));"
        " };"
        " const roleMatches = (el) => !role || normalize(el.getAttribute('role')) === normalize(role) || normalize(el.tagName) === normalize(role);"
        " const candidates = Array.from(document.querySelectorAll('button,a,input,textarea,select,label,[role],[tabindex],li,div,span')).filter(visible);"
        " const match = candidates.find((el) => roleMatches(el) && compatible(textOf(el)));"
        " if (!match) throw new Error('target not found');"
        " const clickable = match.closest('button,a,[role=\"button\"],[role=\"menuitem\"],input,label,[tabindex]') || match;"
        " if (typeof clickable.scrollIntoView === 'function') clickable.scrollIntoView({block:'center', inline:'center'});"
        " if (typeof clickable.focus === 'function') clickable.focus();"
        " const opts = { bubbles: true, cancelable: true, view: window };"
        " for (const type of ['pointerdown','mousedown','pointerup','mouseup']) clickable.dispatchEvent(new MouseEvent(type, opts));"
        " if (typeof clickable.click === 'function') clickable.click();"
        " else clickable.dispatchEvent(new MouseEvent('click', opts));"
        " return true;"
        "})()"
    )


def _active_combobox_option_click_script(text: str) -> str:
    return (
        "(() => {"
        f" const needle = {json.dumps(text)}.trim().toLowerCase();"
        " if (!needle) return false;"
        " const normalize = (value) => (value || '').toLowerCase().replace(/\\b(west)\\b/g, 'w').replace(/\\b(east)\\b/g, 'e').replace(/\\b(north)\\b/g, 'n').replace(/\\b(south)\\b/g, 's').replace(/[^a-z0-9]+/g, ' ').trim();"
        " const needleNorm = normalize(needle);"
        " const needleTokens = needleNorm.split(' ').filter(Boolean);"
        " const needleNums = needleTokens.filter((token) => /\\d/.test(token));"
        " const needleWords = needleTokens.filter((token) => !/\\d/.test(token) && token.length >= 3);"
        " const compatible = (text) => {"
        "   const textNorm = normalize(text);"
        "   if (!textNorm) return false;"
        "   const textTokens = textNorm.split(' ').filter(Boolean);"
        "   if (needleNums.length > 0) return needleNums.every((token) => textTokens.includes(token)) && (!needleWords.length || needleWords.some((token) => textTokens.includes(token)));"
        "   if (needleWords.length > 1) return needleWords.every((token) => textTokens.includes(token));"
        "   if (needleWords.length === 1) return textTokens.includes(needleWords[0]);"
        "   return textNorm.includes(needleNorm);"
        " };"
        " const active = document.activeElement;"
        " const controlled = active && (active.getAttribute('aria-controls') || active.getAttribute('aria-owns'));"
        " if (!controlled) return false;"
        " const root = document.getElementById(controlled);"
        " if (!root) return false;"
        " const visible = (el) => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);"
        " const directText = (el) => Array.from(el.childNodes || []).filter((node) => node.nodeType === Node.TEXT_NODE).map((node) => node.textContent || '').join(' ').trim();"
        " const optionLike = Array.from(root.querySelectorAll('[role=\"option\"],button,[role=\"button\"],li,[data-testid*=\"option\" i],[data-baseweb*=\"menu\" i] div')).filter(visible);"
        " const leafLike = Array.from(root.querySelectorAll('*')).filter((el) => visible(el) && !Array.from(el.children || []).some(visible));"
        " const candidates = [...optionLike, ...leafLike];"
        " const seen = new Set();"
        " const match = candidates.find((el) => {"
        "   if (seen.has(el)) return false;"
        "   seen.add(el);"
        "   const text = (directText(el) || el.getAttribute('aria-label') || el.innerText || el.textContent || '').trim();"
        "   return text && compatible(text);"
        " });"
        " if (!match) return false;"
        " const clickable = match.closest('button,[role=\"option\"],[role=\"button\"],li,div') || match;"
        " if (typeof clickable.scrollIntoView === 'function') clickable.scrollIntoView({block:'center', inline:'center'});"
        " if (typeof clickable.focus === 'function') clickable.focus();"
        " const opts = { bubbles: true, cancelable: true, view: window };"
        " for (const type of ['pointerdown','mousedown','pointerup','mouseup']) clickable.dispatchEvent(new MouseEvent(type, opts));"
        " if (typeof clickable.click === 'function') clickable.click();"
        " else clickable.dispatchEvent(new MouseEvent('click', opts));"
        " return true;"
        "})()"
    )


def _semantic_selector_for_target(target: dict[str, object]) -> str:
    value = str(
        target.get("selector")
        or target.get("label")
        or target.get("placeholder")
        or target.get("text")
        or target.get("name")
        or ""
    ).strip()
    if not value:
        raise ValueError("Provide selector, label, placeholder, text, or role/name.")
    return value


def _first_locator(locator: Any) -> Any:
    first = getattr(locator, "first", None)
    if callable(first):
        return first()
    return first or locator


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return await value
    return value


def _base_locator_for_target(page: object, target: dict[str, object]):
    selector = str(target.get("selector") or "").strip()
    if selector and hasattr(page, "locator"):
        return page.locator(selector)
    label = str(target.get("label") or "").strip()
    if label and hasattr(page, "get_by_label"):
        return page.get_by_label(label)
    placeholder = str(target.get("placeholder") or "").strip()
    if placeholder and hasattr(page, "get_by_placeholder"):
        return page.get_by_placeholder(placeholder)
    role = str(target.get("role") or "").strip()
    name = str(target.get("name") or "").strip()
    if role and hasattr(page, "get_by_role"):
        kwargs = {"name": name} if name else {}
        return page.get_by_role(role, **kwargs)
    text = str(target.get("text") or "").strip()
    if text and hasattr(page, "get_by_text"):
        return page.get_by_text(text)
    if name and hasattr(page, "get_by_text"):
        return page.get_by_text(name)
    if hasattr(page, "locator"):
        return page.locator(_semantic_selector_for_target(target))
    return None


def _target_locator_variants(target: dict[str, object]) -> list[dict[str, object]]:
    variants = [target]
    selector = str(target.get("selector") or "").strip()
    if not selector:
        return variants
    semantic_keys = ("label", "placeholder", "text", "name")
    existing_semantic = {key: target.get(key) for key in semantic_keys if target.get(key)}
    if existing_semantic:
        semantic = {key: value for key, value in target.items() if key != "selector"}
        variants.append(semantic)
        return variants
    for key in semantic_keys:
        variants.append({key: selector})
    return variants


def _dom_type_field_by_target_function() -> str:
    return (
        "(payload) => {"
        " const target = payload && payload.target ? payload.target : {};"
        " const text = String(payload && payload.text != null ? payload.text : '');"
        " const normalize = (value) => String(value || '').toLowerCase().replace(/&/g, ' and ').replace(/[^a-z0-9]+/g, ' ').trim();"
        " const generic = new Set(['input','textarea','select','role combobox']);"
        " const rawTargets = Object.entries(target).flatMap(([key, value]) => {"
        "   const raw = String(value || '').trim();"
        "   if (!raw) return [];"
        "   if (key === 'selector' && /^\\s*(input|textarea|select|\\[role=['\\\"]?combobox['\\\"]?\\])\\s*$/i.test(raw)) return [];"
        "   return [raw];"
        " });"
        " const targets = rawTargets.map(normalize).filter((value) => value && !generic.has(value));"
        " if (!targets.length) return {ok:false, reason:'no_semantic_target'};"
        " const visible = (el) => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);"
        " const directText = (el) => Array.from(el.childNodes || []).filter((node) => node.nodeType === Node.TEXT_NODE).map((node) => node.textContent || '').join(' ').trim();"
        " const labelText = (el) => {"
        "   const parts = [];"
        "   const id = el.getAttribute('id');"
        "   if (id) document.querySelectorAll(`label[for=\"${CSS.escape(id)}\"]`).forEach((label) => parts.push(label.innerText || label.textContent || ''));"
        "   const wrapping = el.closest('label');"
        "   if (wrapping) parts.push(wrapping.innerText || wrapping.textContent || '');"
        "   const labelledBy = String(el.getAttribute('aria-labelledby') || '').split(/\\s+/).filter(Boolean);"
        "   labelledBy.forEach((ref) => { const node = document.getElementById(ref); if (node) parts.push(node.innerText || node.textContent || ''); });"
        "   const parent = el.parentElement;"
        "   if (parent) parts.push(directText(parent));"
        "   const previous = el.previousElementSibling;"
        "   if (previous) parts.push(previous.innerText || previous.textContent || '');"
        "   return parts.join(' ');"
        " };"
        " const candidates = Array.from(document.querySelectorAll('input, textarea, select, [contenteditable=\"true\"], [role=\"combobox\"]')).filter((el) => visible(el) && !el.disabled && el.getAttribute('aria-disabled') !== 'true');"
        " const score = (haystack, needle) => {"
        "   if (!haystack || !needle) return 0;"
        "   if (haystack.includes(needle) || needle.includes(haystack)) return 100 + needle.length;"
        "   const tokens = needle.split(' ').filter((token) => token.length > 1);"
        "   if (!tokens.length) return 0;"
        "   const matched = tokens.filter((token) => haystack.includes(token)).length;"
        "   if (matched === tokens.length) return 50 + matched;"
        "   return matched >= Math.min(2, tokens.length) ? matched : 0;"
        " };"
        " let best = null;"
        " for (const el of candidates) {"
        "   const haystack = normalize(["
        "     el.getAttribute('placeholder'), el.getAttribute('aria-label'), el.getAttribute('name'),"
        "     el.getAttribute('id'), el.getAttribute('title'), el.getAttribute('autocomplete'), labelText(el)"
        "   ].join(' '));"
        "   const candidateScore = Math.max(...targets.map((needle) => score(haystack, needle)));"
        "   if (candidateScore > 0 && (!best || candidateScore > best.score)) best = {el, score: candidateScore, haystack};"
        " }"
        " if (!best) return {ok:false, reason:'target_not_found'};"
        " const el = best.el;"
        " if (typeof el.scrollIntoView === 'function') el.scrollIntoView({block:'center', inline:'center'});"
        " if (typeof el.focus === 'function') el.focus({preventScroll:true});"
        " const before = 'value' in el ? String(el.value || '') : String(el.textContent || '');"
        " const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : el instanceof HTMLInputElement ? HTMLInputElement.prototype : null;"
        " const setter = proto && Object.getOwnPropertyDescriptor(proto, 'value')?.set;"
        " if (setter) setter.call(el, text);"
        " else if ('value' in el) el.value = text;"
        " else el.textContent = text;"
        " el.dispatchEvent(new InputEvent('input', {bubbles:true, inputType:'insertText', data:text}));"
        " el.dispatchEvent(new Event('change', {bubbles:true}));"
        " const after = 'value' in el ? String(el.value || '') : String(el.textContent || '');"
        " return {ok:true, before, after, matched: best.haystack, score: best.score};"
        "}"
    )


def _selector_visible_editable_function() -> str:
    return (
        "(selector) => {"
        " const el = document.querySelector(String(selector || ''));"
        " if (!el) return null;"
        " const style = window.getComputedStyle(el);"
        " const rect = el.getBoundingClientRect();"
        " const visible = style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;"
        " const tag = (el.tagName || '').toLowerCase();"
        " const type = (el.getAttribute('type') || '').toLowerCase();"
        " const editable = el.isContentEditable || tag === 'textarea' || tag === 'select' || el.getAttribute('role') === 'combobox' || el.getAttribute('contenteditable') === 'true' || (tag === 'input' && type !== 'hidden');"
        " const enabled = !el.disabled && el.getAttribute('aria-disabled') !== 'true';"
        " return visible && enabled && editable;"
        "}"
    )


def _is_generic_field_target(target: dict[str, object]) -> bool:
    semantic_keys = ("label", "placeholder", "role", "name", "text")
    if any(str(target.get(key) or "").strip() for key in semantic_keys):
        return False
    selector = str(target.get("selector") or "").strip().lower()
    return selector in {"input", "textarea", "select", "[role=combobox]", "[role='combobox']", '[role="combobox"]'}


def _locator_for_target(page: object, target: dict[str, object]):
    locator = _base_locator_for_target(page, target)
    return _first_locator(locator) if locator is not None else None


async def _is_visible_locator(locator: Any) -> bool:
    is_visible = getattr(locator, "is_visible", None)
    if not callable(is_visible):
        return True
    try:
        return bool(await _maybe_await(is_visible(timeout=300)))
    except TypeError:
        try:
            return bool(await _maybe_await(is_visible()))
        except Exception:
            return False
    except Exception:
        return False


async def _is_enabled_locator(locator: Any) -> bool:
    is_enabled = getattr(locator, "is_enabled", None)
    if not callable(is_enabled):
        try:
            disabled = await _maybe_await(locator.get_attribute("disabled", timeout=300))
            return disabled is None
        except Exception:
            return True
    try:
        return bool(await _maybe_await(is_enabled(timeout=300)))
    except TypeError:
        try:
            return bool(await _maybe_await(is_enabled()))
        except Exception:
            return False
    except Exception:
        return False


async def _candidate_locators_for_target(page: object, target: dict[str, object]) -> list[Any]:
    locators: list[Any] = []
    for variant in _target_locator_variants(target):
        base = _base_locator_for_target(page, variant)
        if base is None:
            continue
        count_fn = getattr(base, "count", None)
        nth_fn = getattr(base, "nth", None)
        if not callable(count_fn) or not callable(nth_fn):
            locators.append(_first_locator(base))
            continue
        try:
            count = int(await _maybe_await(count_fn()))
        except Exception:
            locators.append(_first_locator(base))
            continue
        if count == 0:
            if str(variant.get("selector") or "").strip():
                locators.append(_first_locator(base))
            continue
        if count == 1:
            locators.append(_first_locator(base))
            continue
        candidates = [nth_fn(index) for index in range(min(count, 8))]
        visible: list[Any] = []
        hidden: list[Any] = []
        for candidate in candidates:
            if not await _is_enabled_locator(candidate):
                continue
            if await _is_visible_locator(candidate):
                visible.append(candidate)
            else:
                hidden.append(candidate)
        locators.extend(visible + hidden)
    return locators


async def _click_with_dom_fallback(page: object, selector: str) -> None:
    original_error: BaseException | None = None
    click = getattr(page, "click", None)
    if callable(click):
        try:
            result = click(selector, timeout=_CLICK_TIMEOUT_MS)
            if asyncio.iscoroutine(result):
                await result
            return
        except TypeError:
            try:
                result = click(selector)
                if asyncio.iscoroutine(result):
                    await result
                return
            except Exception as exc:
                original_error = exc
        except Exception as exc:
            original_error = exc
    else:
        original_error = RuntimeError("page does not support click")
    locator = getattr(page, "locator", None)
    if callable(locator):
        try:
            target = locator(selector)
            result = target.click(force=True, timeout=_CLICK_TIMEOUT_MS)
            if asyncio.iscoroutine(result):
                await result
            return
        except Exception as exc:
            original_error = original_error or exc
        try:
            target = locator(selector)
            dispatch = getattr(target, "dispatch_event", None)
            if callable(dispatch):
                result = dispatch("click", timeout=_CLICK_TIMEOUT_MS)
                if asyncio.iscoroutine(result):
                    await result
                return
        except Exception as exc:
            original_error = original_error or exc
    evaluate = getattr(page, "evaluate", None)
    if not callable(evaluate):
        raise original_error
    try:
        result = evaluate(_dom_click_script(selector))
        if asyncio.iscoroutine(result):
            await result
    except Exception:
        if original_error is not None:
            raise original_error
        raise


async def _click_target_with_fallback(page: object, target: dict[str, object]) -> None:
    selector = _semantic_selector_for_target(target)
    if await _click_active_combobox_option(page, target):
        return
    original_error: BaseException | None = None
    candidates = await _candidate_locators_for_target(page, target)
    if not candidates and not str(target.get("selector") or "").strip():
        raise RuntimeError("No matching element found.")
    for locator in candidates:
        click = getattr(locator, "click", None)
        if callable(click):
            try:
                result = click(timeout=_CLICK_TIMEOUT_MS)
                if asyncio.iscoroutine(result):
                    await result
                return
            except Exception as exc:
                original_error = exc
            try:
                result = click(force=True, timeout=_CLICK_TIMEOUT_MS)
                if asyncio.iscoroutine(result):
                    await result
                return
            except Exception as exc:
                original_error = original_error or exc
        dispatch = getattr(locator, "dispatch_event", None)
        if callable(dispatch):
            try:
                result = dispatch("click", timeout=_CLICK_TIMEOUT_MS)
                if asyncio.iscoroutine(result):
                    await result
                return
            except Exception as exc:
                original_error = original_error or exc
    try:
        evaluate = getattr(page, "evaluate", None)
        if callable(evaluate) and not str(target.get("selector") or "").strip():
            result = evaluate(_dom_click_target_script(target))
            if asyncio.iscoroutine(result):
                await result
            return
        await _click_with_dom_fallback(page, selector)
    except Exception:
        if original_error is not None:
            raise original_error
        raise


async def _click_active_combobox_option(page: object, target: dict[str, object]) -> bool:
    text = str(target.get("text") or target.get("name") or "").strip()
    if not text:
        return False
    evaluate = getattr(page, "evaluate", None)
    if not callable(evaluate):
        return False
    try:
        result = evaluate(_active_combobox_option_click_script(text))
        if asyncio.iscoroutine(result):
            result = await result
        return bool(result)
    except Exception:
        return False


async def _active_combobox_has_options(page: object) -> bool:
    evaluate = getattr(page, "evaluate", None)
    if not callable(evaluate):
        return False
    try:
        result = evaluate(
            """(() => {
                const active = document.activeElement;
                const controlled = active && (active.getAttribute('aria-controls') || active.getAttribute('aria-owns'));
                const root = controlled && document.getElementById(controlled);
                if (!root) return false;
                return [...root.querySelectorAll('*')].some((el) => {
                    const text = (el.innerText || el.textContent || '').trim();
                    return text && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
                });
            })()"""
        )
        if asyncio.iscoroutine(result):
            result = await result
        return bool(result)
    except Exception:
        return False


async def _type_text_with_dom_fallback(page: object, selector: str, text: str) -> None:
    visible_editable = await _selector_targets_visible_editable(page, selector)
    if visible_editable is False:
        raise RuntimeError("Selector does not point to a visible editable field.")
    original_error: BaseException | None = None
    fill = getattr(page, "fill", None)
    if callable(fill):
        try:
            result = fill(selector, text, timeout=_TYPE_TEXT_TIMEOUT_MS)
            if asyncio.iscoroutine(result):
                await result
            return
        except TypeError:
            try:
                result = fill(selector, text)
                if asyncio.iscoroutine(result):
                    await result
                return
            except Exception as exc:
                original_error = exc
        except Exception as exc:
            original_error = exc
    else:
        original_error = RuntimeError("page does not support fill")
    locator = getattr(page, "locator", None)
    if callable(locator):
        try:
            target = locator(selector)
            result = target.fill(text, force=True, timeout=_TYPE_TEXT_TIMEOUT_MS)
            if asyncio.iscoroutine(result):
                await result
            return
        except Exception as exc:
            original_error = original_error or exc
    evaluate = getattr(page, "evaluate", None)
    if not callable(evaluate):
        raise original_error
    try:
        result = evaluate(_dom_type_script(selector, text))
        if asyncio.iscoroutine(result):
            await result
    except Exception:
        if original_error is not None:
            raise original_error
        raise


async def _type_field_with_dom_target(page: object, target: dict[str, object], text: str) -> bool:
    evaluate = getattr(page, "evaluate", None)
    if not callable(evaluate):
        return False
    try:
        result = evaluate(_dom_type_field_by_target_function(), {"target": target, "text": text})
        if asyncio.iscoroutine(result):
            result = await result
    except Exception:
        return False
    if not isinstance(result, dict) or not result.get("ok"):
        return False
    after = str(result.get("after") or "").strip()
    return bool(after) and (text.strip() in after or after in text.strip())


async def _is_editable_locator(locator: Any) -> bool:
    evaluate = getattr(locator, "evaluate", None)
    if not callable(evaluate):
        return True
    script = """(el) => {
        if (!el) return false;
        const tag = (el.tagName || '').toLowerCase();
        const type = (el.getAttribute('type') || '').toLowerCase();
        return Boolean(
            el.isContentEditable ||
            tag === 'textarea' ||
            tag === 'select' ||
            el.getAttribute('role') === 'combobox' ||
            el.getAttribute('contenteditable') === 'true' ||
            (tag === 'input' && type !== 'hidden')
        );
    }"""
    try:
        result = evaluate(script, timeout=500)
        if asyncio.iscoroutine(result):
            result = await result
    except TypeError:
        try:
            result = evaluate(script)
            if asyncio.iscoroutine(result):
                result = await result
        except Exception:
            return True
    except Exception:
        return True
    return bool(result)


async def _selector_targets_visible_editable(page: object, selector: str) -> bool | None:
    evaluate = getattr(page, "evaluate", None)
    if not callable(evaluate):
        return None
    try:
        result = evaluate(_selector_visible_editable_function(), selector)
        if asyncio.iscoroutine(result):
            result = await result
    except Exception:
        return None
    if result is None:
        return None
    return bool(result)


async def _type_target_with_fallback(page: object, target: dict[str, object], text: str) -> None:
    selector = _semantic_selector_for_target(target)
    original_error: BaseException | None = None
    unverified_success = False
    allow_unverified_success = not _is_generic_field_target(target)
    for locator in await _candidate_locators_for_target(page, target):
        try:
            if not await _is_enabled_locator(locator):
                original_error = original_error or RuntimeError("Matching field is disabled.")
                continue
            if not await _is_visible_locator(locator):
                original_error = original_error or RuntimeError("Matching field is hidden.")
                continue
            if not await _is_editable_locator(locator):
                original_error = original_error or RuntimeError("Matching field is not editable.")
                continue
            result = locator.fill(text, timeout=_TYPE_TEXT_TIMEOUT_MS)
            if asyncio.iscoroutine(result):
                await result
            had_options = await _active_combobox_has_options(page)
            committed = await _click_active_combobox_option(page, {"text": text})
            if had_options and not committed:
                raise RuntimeError("No compatible active combobox suggestion matched the typed text.")
            matches = await _locator_value_matches(locator, text)
            if matches is True:
                return
            if matches is None and allow_unverified_success:
                unverified_success = True
            continue
        except Exception as exc:
            original_error = exc
        try:
            result = locator.fill(text, force=True, timeout=_TYPE_TEXT_TIMEOUT_MS)
            if asyncio.iscoroutine(result):
                await result
            had_options = await _active_combobox_has_options(page)
            committed = await _click_active_combobox_option(page, {"text": text})
            if had_options and not committed:
                raise RuntimeError("No compatible active combobox suggestion matched the typed text.")
            matches = await _locator_value_matches(locator, text)
            if matches is True:
                return
            if matches is None and allow_unverified_success:
                unverified_success = True
            continue
        except Exception as exc:
            original_error = original_error or exc
        press = getattr(locator, "press_sequentially", None)
        if callable(press):
            try:
                result = press(text, timeout=_TYPE_TEXT_TIMEOUT_MS)
                if asyncio.iscoroutine(result):
                    await result
                had_options = await _active_combobox_has_options(page)
                committed = await _click_active_combobox_option(page, {"text": text})
                if had_options and not committed:
                    raise RuntimeError("No compatible active combobox suggestion matched the typed text.")
                matches = await _locator_value_matches(locator, text)
                if matches is True:
                    return
                if matches is None and allow_unverified_success:
                    unverified_success = True
            except Exception as exc:
                original_error = original_error or exc
        evaluate = getattr(locator, "evaluate", None)
        if callable(evaluate):
            try:
                result = evaluate(_dom_type_element_function(), text, timeout=_TYPE_TEXT_TIMEOUT_MS)
                if asyncio.iscoroutine(result):
                    await result
                return
            except TypeError:
                try:
                    result = evaluate(_dom_type_element_function(), text)
                    if asyncio.iscoroutine(result):
                        await result
                    return
                except Exception as exc:
                    original_error = original_error or exc
            except Exception as exc:
                original_error = original_error or exc
    if await _type_field_with_dom_target(page, target, text):
        return
    if unverified_success:
        return
    if _is_generic_field_target(target):
        if original_error is not None:
            raise original_error
        raise RuntimeError("Typed text did not appear in any matching field.")
    try:
        await _type_text_with_dom_fallback(page, selector, text)
    except Exception:
        if original_error is not None:
            raise original_error
        raise


async def _locator_value_matches(locator: Any, text: str) -> bool | None:
    expected = text.strip()
    if not expected:
        return True
    value: str | None = None
    input_value = getattr(locator, "input_value", None)
    if callable(input_value):
        try:
            raw_value = await _maybe_await(input_value(timeout=500))
            value = str(raw_value) if raw_value is not None else None
        except TypeError:
            try:
                raw_value = await _maybe_await(input_value())
                value = str(raw_value) if raw_value is not None else None
            except Exception:
                value = None
        except Exception:
            value = None
    if value is None:
        get_attribute = getattr(locator, "get_attribute", None)
        if callable(get_attribute):
            for attr in ("value", "title"):
                try:
                    attr_value = await _maybe_await(get_attribute(attr, timeout=500))
                except TypeError:
                    try:
                        attr_value = await _maybe_await(get_attribute(attr))
                    except Exception:
                        attr_value = None
                except Exception:
                    attr_value = None
                if attr_value:
                    value = str(attr_value)
                    break
    if value is None:
        return None
    actual = value.strip()
    if not actual:
        return False
    if expected in actual or actual in expected:
        return True
    def normalize(raw: str) -> str:
        lowered = raw.lower()
        for word, abbreviation in (("west", "w"), ("east", "e"), ("north", "n"), ("south", "s")):
            lowered = re.sub(rf"\b{word}\b", abbreviation, lowered)
        return lowered
    expected_tokens = [token for token in re.split(r"[^a-z0-9]+", normalize(expected)) if token]
    actual_tokens = [token for token in re.split(r"[^a-z0-9]+", normalize(actual)) if token]
    expected_numbers = [token for token in expected_tokens if any(char.isdigit() for char in token)]
    if expected_numbers:
        actual_set = set(actual_tokens)
        return all(token in actual_set for token in expected_numbers) and any(
            token in actual_set for token in expected_tokens if not any(char.isdigit() for char in token)
        )
    return False


class _RawCDPClient:
    def __init__(self, websocket_url: str) -> None:
        self.websocket_url = websocket_url
        self._ws = None
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._event_waiters: dict[str, list[asyncio.Future]] = {}
        self._event_backlog: dict[str, list[dict[str, object]]] = {}
        self._reader_task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._ws is not None:
            return
        import websockets

        self._ws = await websockets.connect(self.websocket_url, max_size=None)
        self._reader_task = asyncio.create_task(self._reader())

    async def _reader(self) -> None:
        try:
            async for raw_message in self._ws:
                try:
                    message = json.loads(raw_message)
                except json.JSONDecodeError:
                    continue
                message_id = message.get("id") if isinstance(message, dict) else None
                if isinstance(message_id, int):
                    future = self._pending.pop(message_id, None)
                    if future is not None and not future.done():
                        if "error" in message:
                            future.set_exception(RuntimeError(str(message.get("error"))))
                        else:
                            future.set_result(message.get("result") or {})
                    continue
                method = message.get("method") if isinstance(message, dict) else None
                if isinstance(method, str):
                    params = message.get("params") if isinstance(message.get("params"), dict) else {}
                    waiters = self._event_waiters.pop(method, [])
                    delivered = False
                    for future in waiters:
                        if not future.done():
                            future.set_result(params)
                            delivered = True
                    if not delivered:
                        backlog = self._event_backlog.setdefault(method, [])
                        backlog.append(params)
                        del backlog[:-5]
        except Exception as exc:
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(exc)
            for waiters in list(self._event_waiters.values()):
                for future in waiters:
                    if not future.done():
                        future.set_exception(exc)

    async def send(self, method: str, params: dict[str, object] | None = None, *, timeout: float = 10) -> dict[str, object]:
        await self.connect()
        async with self._send_lock:
            self._next_id += 1
            message_id = self._next_id
            future = asyncio.get_running_loop().create_future()
            self._pending[message_id] = future
            await self._ws.send(json.dumps({"id": message_id, "method": method, "params": params or {}}))
        return await asyncio.wait_for(future, timeout=timeout)

    async def wait_event(self, method: str, *, timeout: float) -> dict[str, object]:
        backlog = self._event_backlog.get(method)
        if backlog:
            return backlog.pop(0)
        future = asyncio.get_running_loop().create_future()
        self._event_waiters.setdefault(method, []).append(future)
        return await asyncio.wait_for(future, timeout=timeout)

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except BaseException:
                pass
            self._reader_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            finally:
                self._ws = None


class _RawCDPPage:
    def __init__(self, target: dict[str, object]) -> None:
        self.target_id = str(target.get("id") or "")
        self.url = str(target.get("url") or "")
        self._client = _RawCDPClient(str(target.get("webSocketDebuggerUrl") or ""))
        self._closed = False

    def is_closed(self) -> bool:
        return self._closed

    async def _ensure_page_enabled(self) -> None:
        await self._client.send("Page.enable", timeout=5)
        try:
            await self._client.send("Runtime.enable", timeout=5)
        except Exception:
            pass

    async def goto(self, url: str, **_kwargs: object) -> object:
        await self._ensure_page_enabled()
        await self._client.send("Page.navigate", {"url": url}, timeout=10)
        try:
            await self._client.wait_event("Page.domContentEventFired", timeout=30)
        except Exception:
            try:
                await self._client.wait_event("Page.loadEventFired", timeout=2)
            except Exception:
                pass
        self.url = url
        return SimpleNamespace(status=0)

    async def click(self, selector: str, **_kwargs: object) -> None:
        await self.evaluate(
            f"(() => {{ const el = document.querySelector({json.dumps(selector)}); if (!el) throw new Error('selector not found'); el.click(); }})()"
        )

    async def fill(self, selector: str, text: str) -> None:
        await self.evaluate(
            "(() => {"
            f" const el = document.querySelector({json.dumps(selector)});"
            " if (!el) throw new Error('selector not found');"
            f" el.value = {json.dumps(text)};"
            " el.dispatchEvent(new Event('input', {bubbles: true}));"
            " el.dispatchEvent(new Event('change', {bubbles: true}));"
            "})()"
        )

    async def inner_text(self, selector: str) -> str:
        value = await self.evaluate(
            f"(() => {{ const el = document.querySelector({json.dumps(selector)}); return el ? el.innerText : ''; }})()"
        )
        return str(value or "")

    def locator(self, selector: str) -> object:
        page = self

        class _Locator:
            @property
            def first(self) -> "_Locator":
                return self

            async def click(self, **_kwargs: object) -> None:
                await page.click(selector)

            async def dispatch_event(self, event_name: str, **_kwargs: object) -> None:
                await page.evaluate(
                    "(() => {"
                    f" const el = document.querySelector({json.dumps(selector)});"
                    " if (!el) throw new Error('selector not found');"
                    f" el.dispatchEvent(new Event({json.dumps(event_name)}, {{bubbles: true, cancelable: true}}));"
                    "})()"
                )

            async def fill(self, text: str, **_kwargs: object) -> None:
                await page.fill(selector, text)

            async def inner_text(self, **_kwargs: object) -> str:
                return await page.inner_text(selector)

            async def input_value(self, **_kwargs: object) -> str:
                value = await page.evaluate(
                    f"(() => {{ const el = document.querySelector({json.dumps(selector)}); return el && 'value' in el ? el.value : ''; }})()"
                )
                return str(value or "")

            async def get_attribute(self, name: str, **_kwargs: object) -> str | None:
                value = await page.evaluate(
                    "(() => {"
                    f" const el = document.querySelector({json.dumps(selector)});"
                    f" return el ? el.getAttribute({json.dumps(name)}) : null;"
                    "})()"
                )
                return str(value) if value is not None else None

            async def is_visible(self, **_kwargs: object) -> bool:
                return bool(
                    await page.evaluate(
                        "(() => {"
                        f" const el = document.querySelector({json.dumps(selector)});"
                        " return !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));"
                        "})()"
                    )
                )

            async def is_enabled(self, **_kwargs: object) -> bool:
                return bool(
                    await page.evaluate(
                        "(() => {"
                        f" const el = document.querySelector({json.dumps(selector)});"
                        " return !!(el && !el.disabled && el.getAttribute('aria-disabled') !== 'true');"
                        "})()"
                    )
                )

            async def evaluate(self, function: str, *args: object, **_kwargs: object) -> object:
                arg_expression = json.dumps(args[0] if args else None)
                return await page.evaluate(
                    "(() => {"
                    f" const el = document.querySelector({json.dumps(selector)});"
                    f" return ({function})(el, {arg_expression});"
                    "})()"
                )

        return _Locator()

    async def evaluate(self, script: str) -> object:
        payload = await self._client.send(
            "Runtime.evaluate",
            {"expression": script, "awaitPromise": True, "returnByValue": True},
            timeout=10,
        )
        result = payload.get("result") if isinstance(payload, dict) else {}
        if isinstance(result, dict) and result.get("subtype") == "error":
            raise RuntimeError(str(result.get("description") or result.get("value") or "browser script failed"))
        return result.get("value") if isinstance(result, dict) else None

    async def wait_for_selector(self, selector: str, timeout: int) -> None:
        deadline = asyncio.get_running_loop().time() + (timeout / 1000)
        while True:
            exists = await self.evaluate(f"Boolean(document.querySelector({json.dumps(selector)}))")
            if exists:
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"Timed out waiting for selector {selector!r}")
            await asyncio.sleep(0.1)

    async def wait_for_url(self, url_pattern: str, timeout: int) -> None:
        deadline = asyncio.get_running_loop().time() + (timeout / 1000)
        while True:
            current = str(await self.evaluate("location.href") or "")
            if fnmatch.fnmatch(current, url_pattern):
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"Timed out waiting for URL {url_pattern!r}")
            await asyncio.sleep(0.1)

    async def wait_for_load_state(self, _state: str, timeout: int) -> None:
        try:
            await self._client.wait_event("Page.loadEventFired", timeout=max(0.1, timeout / 1000))
        except Exception:
            pass

    async def query_selector_all(self, selector: str) -> list[object]:
        items = await self.evaluate(
            "(() => Array.from(document.querySelectorAll("
            f"{json.dumps(selector)}"
            ")).slice(0, 50).map(el => ({"
            "tag: el.tagName.toLowerCase(), text: (el.innerText || '').trim(), href: el.getAttribute('href') || ''"
            "})))()"
        )
        if not isinstance(items, list):
            return []

        class _Element:
            def __init__(self, item: dict[str, object]) -> None:
                self.item = item

            async def inner_text(self) -> str:
                return str(self.item.get("text") or "")

            async def evaluate(self, _script: str) -> str:
                return str(self.item.get("tag") or "")

            async def get_attribute(self, name: str) -> str:
                return str(self.item.get(name) or "")

        return [_Element(item) for item in items if isinstance(item, dict)]

    async def layout_metrics(self) -> dict[str, int]:
        metrics = await self._client.send("Page.getLayoutMetrics", timeout=2)
        layout_viewport = metrics.get("layoutViewport") if isinstance(metrics.get("layoutViewport"), dict) else {}
        visual_viewport = metrics.get("visualViewport") if isinstance(metrics.get("visualViewport"), dict) else {}
        content_size = metrics.get("contentSize") if isinstance(metrics.get("contentSize"), dict) else {}
        viewport_width = int(layout_viewport.get("clientWidth") or visual_viewport.get("clientWidth") or 0)
        viewport_height = int(layout_viewport.get("clientHeight") or visual_viewport.get("clientHeight") or 0)
        document_width = int(content_size.get("width") or viewport_width or 0)
        document_height = int(content_size.get("height") or viewport_height or 0)
        return {
            "viewportWidth": max(0, viewport_width),
            "viewportHeight": max(0, viewport_height),
            "documentWidth": max(0, document_width),
            "documentHeight": max(0, document_height),
        }

    async def capture_screenshot(self, params: dict[str, object]) -> dict[str, object]:
        return await self._client.send("Page.captureScreenshot", params, timeout=8)

    async def bring_to_front(self) -> None:
        try:
            await self._client.send("Page.bringToFront", timeout=2)
        except Exception:
            pass

    async def close(self) -> None:
        self._closed = True
        await self._client.close()


class _RawCDPContext:
    def __init__(self, cdp_url: str, targets: list[dict[str, object]]) -> None:
        self.cdp_url = cdp_url
        self.pages = [_RawCDPPage(target) for target in targets if target.get("webSocketDebuggerUrl")]

    async def new_page(self) -> "_RawCDPPage":
        target = await _async_new_cdp_target(self.cdp_url)
        if not target or not target.get("webSocketDebuggerUrl"):
            raise RuntimeError("Connected to CDP, but could not create a new browser tab.")
        page = _RawCDPPage(target)
        self.pages.append(page)
        return page


class _RawCDPBrowser:
    def __init__(self, cdp_url: str) -> None:
        self.cdp_url = cdp_url
        self.contexts: list[_RawCDPContext] = []

    def is_connected(self) -> bool:
        return True

    async def refresh(self) -> "_RawCDPBrowser":
        targets = await _async_page_targets_or_new(self.cdp_url)
        self.contexts = [_RawCDPContext(self.cdp_url, targets)]
        return self


class CDPBackend:
    """Attaches to a running Chrome or Brave window via the DevTools Protocol.

    The user can see every action in real time in their existing browser.
    Good for OAuth flows, sites that block headless browsers, and human-in-the-loop
    workflows where the operator wants to watch or intervene.
    """

    BACKEND_NAME = "cdp"

    def __init__(self) -> None:
        _require_playwright()
        self._cdp_url = normalized_cdp_url()
        self._playwright = None
        self._browser: "Browser | _RawCDPBrowser | None" = None
        self._pages: dict[str, "Page | _RawCDPPage"] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _new_page_unsupported_error(exc: Exception) -> RuntimeError:
        return RuntimeError(
            "Connected to CDP, but this browser does not allow Playwright to create a new tab. "
            "Open a normal Chrome, Brave, or Edge tab in the remote-debugging browser and try again."
        )

    @staticmethod
    def _requires_dedicated_page(session_id: str) -> bool:
        del session_id
        return False

    async def _ensure_raw_browser(self) -> "_RawCDPBrowser":
        browser = _RawCDPBrowser(self._cdp_url)
        self._browser = await browser.refresh()
        return browser

    async def _ensure_browser(self) -> "Browser | _RawCDPBrowser":
        if self._browser is None or not self._browser.is_connected():
            targets = await _async_page_targets_or_new(self._cdp_url)
            if not targets:
                raise RuntimeError(
                    "Connected to CDP, but no browser page target was available and a new tab could not be created. "
                    "Open a normal Chrome, Brave, or Edge tab in the remote-debugging browser and try again."
                )
            pw = await async_playwright().__aenter__()
            self._playwright = pw
            try:
                self._browser = await asyncio.wait_for(
                    pw.chromium.connect_over_cdp(self._cdp_url),
                    timeout=5,
                )
            except Exception as exc:
                try:
                    await pw.__aexit__(None, None, None)
                except Exception:
                    pass
                self._playwright = None
                return await self._ensure_raw_browser()
        return self._browser

    async def _get_page(self, session_id: str) -> "Page | _RawCDPPage":
        async with self._lock:
            cached = self._pages.get(session_id)
            if cached is not None and cached.is_closed():
                self._pages.pop(session_id, None)
            if session_id not in self._pages:
                browser = await self._ensure_browser()
                # Use the first existing context (the user's real browser session)
                contexts = browser.contexts
                if contexts:
                    ctx = contexts[0]
                else:
                    raise RuntimeError(
                        "Connected to CDP, but no visible browser context was available. "
                        "Open a normal Chrome, Brave, or Edge tab in the remote-debugging browser and try again."
                    )
                pages = await _prune_extra_blank_pages(list(ctx.pages))
                if session_id == DEFAULT_AGENT_BROWSER_SESSION_ID:
                    if pages:
                        self._pages[session_id] = pages[-1]
                    else:
                        try:
                            self._pages[session_id] = await ctx.new_page()
                        except Exception as exc:
                            message = str(exc)
                            if "Browser context management is not supported" in message:
                                raise self._new_page_unsupported_error(exc) from exc
                            raise
                else:
                    reusable_pages = [
                        page
                        for page in pages
                        if _page_is_blank(page)
                    ]
                    if reusable_pages and not self._requires_dedicated_page(session_id):
                        self._pages[session_id] = reusable_pages[-1]
                        return self._pages[session_id]
                    try:
                        self._pages[session_id] = await ctx.new_page()
                    except Exception as exc:
                        message = str(exc)
                        if "Browser context management is not supported" in message:
                            if self._requires_dedicated_page(session_id) or not pages:
                                raise self._new_page_unsupported_error(exc) from exc
                            self._pages[session_id] = pages[-1]
                            return self._pages[session_id]
                        raise
        return self._pages[session_id]

    # ── BrowserBackend protocol ───────────────────────────────────────────────

    async def open(self, session_id: str) -> str:
        page = await self._get_page(session_id)
        try:
            await page.bring_to_front()
        except Exception:
            pass
        return f"Opened browser session {session_id} — visible in your browser"

    async def navigate(self, session_id: str, url: str) -> str:
        page = await self._get_page(session_id)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        status = response.status if response else 0
        return f"Navigated to {url} (status {status}) — visible in your browser"

    async def click(self, session_id: str, selector: str) -> None:
        page = await self._get_page(session_id)
        await _click_with_dom_fallback(page, selector)

    async def click_element(self, session_id: str, target: dict[str, object]) -> None:
        page = await self._get_page(session_id)
        await _click_target_with_fallback(page, target)

    async def type_text(self, session_id: str, selector: str, text: str) -> None:
        page = await self._get_page(session_id)
        await _type_text_with_dom_fallback(page, selector, text)

    async def type_field(self, session_id: str, target: dict[str, object], text: str) -> None:
        page = await self._get_page(session_id)
        await _type_target_with_fallback(page, target, text)

    async def extract_text(self, session_id: str, selector: str | None) -> str:
        page = await self._get_page(session_id)
        if selector:
            el = page.locator(selector).first
            return await el.inner_text(timeout=5_000)
        return await page.inner_text("body")

    async def _layout_metrics(self, client: "CDPSession") -> dict[str, int]:
        metrics = await asyncio.wait_for(client.send("Page.getLayoutMetrics"), timeout=2)
        if not isinstance(metrics, dict):
            return {}
        layout_viewport = metrics.get("layoutViewport") if isinstance(metrics.get("layoutViewport"), dict) else {}
        visual_viewport = metrics.get("visualViewport") if isinstance(metrics.get("visualViewport"), dict) else {}
        content_size = metrics.get("contentSize") if isinstance(metrics.get("contentSize"), dict) else {}
        viewport_width = int(layout_viewport.get("clientWidth") or visual_viewport.get("clientWidth") or 0)
        viewport_height = int(layout_viewport.get("clientHeight") or visual_viewport.get("clientHeight") or 0)
        document_width = int(content_size.get("width") or viewport_width or 0)
        document_height = int(content_size.get("height") or viewport_height or 0)
        return {
            "viewportWidth": max(0, viewport_width),
            "viewportHeight": max(0, viewport_height),
            "documentWidth": max(0, document_width),
            "documentHeight": max(0, document_height),
        }

    async def screenshot(self, session_id: str, mode: str = "auto") -> BrowserScreenshotResult:
        page = await self._get_page(session_id)
        client = None
        if isinstance(page, _RawCDPPage):
            await page.bring_to_front()
        else:
            client = await page.context.new_cdp_session(page)
            try:
                await asyncio.wait_for(client.send("Page.bringToFront"), timeout=2)
            except Exception:
                pass
        layout: dict[str, int] = {}
        try:
            if isinstance(page, _RawCDPPage):
                layout = await page.layout_metrics()
            else:
                layout = await self._layout_metrics(client)
        except Exception:
            layout = {}
        viewport_width = layout.get("viewportWidth")
        viewport_height = layout.get("viewportHeight")
        document_width = layout.get("documentWidth")
        document_height = layout.get("documentHeight")
        requested_mode = mode if mode in {"auto", "viewport", "full_page"} else "auto"
        exceeds_viewport = bool(
            viewport_width
            and viewport_height
            and document_width
            and document_height
            and (document_width > viewport_width + 1 or document_height > viewport_height + 1)
        )
        full_page = requested_mode == "full_page" or (
            requested_mode == "auto"
            and auto_screenshot_uses_full_page(getattr(page, "url", None), exceeds_viewport)
        )
        params: dict[str, object] = {
            "format": "png",
            "captureBeyondViewport": full_page,
            "fromSurface": True,
        }
        if full_page and document_width and document_height:
            params["clip"] = {
                "x": 0,
                "y": 0,
                "width": document_width,
                "height": document_height,
                "scale": 1,
            }
        if isinstance(page, _RawCDPPage):
            payload = await page.capture_screenshot(params)
        else:
            payload = await asyncio.wait_for(
                client.send("Page.captureScreenshot", params),
                timeout=8,
            )
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, str) or not data:
            raise RuntimeError("CDP did not return screenshot data.")
        page_url = _page_url(page)
        try:
            page_title = await page.evaluate("document.title")
        except Exception:
            page_title = None
        return BrowserScreenshotResult(
            data=base64.b64decode(data),
            mode="full_page" if full_page else "viewport",
            requested_mode=requested_mode,
            page_url=page_url,
            page_title=str(page_title or "") or None,
            viewport_width=viewport_width,
            viewport_height=viewport_height,
            document_width=document_width,
            document_height=document_height,
            is_clipped=not full_page and exceeds_viewport,
        )

    async def scroll(self, session_id: str, direction: str, amount: int) -> None:
        page = await self._get_page(session_id)
        delta_y = amount if direction == "down" else -amount
        await page.evaluate(f"window.scrollBy(0, {delta_y})")

    async def wait_for(
        self,
        session_id: str,
        selector: str | None,
        url_pattern: str | None,
        text: str | None,
        timeout: float,
    ) -> None:
        page = await self._get_page(session_id)
        timeout_ms = int(timeout * 1000)
        if selector and not url_pattern and not text:
            await page.wait_for_selector(selector, timeout=timeout_ms)
            return
        if url_pattern and not selector and not text:
            await page.wait_for_url(url_pattern, timeout=timeout_ms)
            return
        if not selector and not url_pattern and not text:
            await page.wait_for_load_state("networkidle", timeout=timeout_ms)
            return
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if selector:
                try:
                    exists = await page.evaluate(f"Boolean(document.querySelector({json.dumps(selector)}))")
                    if exists:
                        return
                except Exception:
                    pass
            if url_pattern:
                try:
                    current = str(await page.evaluate("location.href") or "")
                    if fnmatch.fnmatch(current, url_pattern):
                        return
                except Exception:
                    pass
            if text:
                try:
                    found = await page.evaluate(
                        "(() => (document.body?.innerText || '').toLowerCase().includes("
                        f"{json.dumps(text.lower())}"
                        "))()"
                    )
                    if found:
                        return
                except Exception:
                    pass
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("Timed out waiting for any browser condition.")
            await asyncio.sleep(0.1)

    async def find(self, session_id: str, selector: str) -> list[dict[str, str]]:
        page = await self._get_page(session_id)
        elements = await page.query_selector_all(selector)
        results = []
        for el in elements[:50]:
            text = (await el.inner_text()).strip()
            tag = await el.evaluate("e => e.tagName.toLowerCase()")
            href = await el.get_attribute("href") or ""
            results.append({"tag": tag, "text": text[:200], "href": href})
        return results

    async def run_js(self, session_id: str, script: str) -> Any:
        page = await self._get_page(session_id)
        return await page.evaluate(script)

    async def close_session(self, session_id: str) -> None:
        async with self._lock:
            page = self._pages.pop(session_id, None)
        if page:
            try:
                await page.close()
            except Exception:
                pass

    async def shutdown(self) -> None:
        for session_id in list(self._pages):
            await self.close_session(session_id)
        # Don't close the browser itself — it's the user's existing window
        if self._playwright:
            try:
                await self._playwright.__aexit__(None, None, None)
            except Exception:
                pass
