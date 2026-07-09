"""Browser plugin — Playwright headless backend.

Requires: pip install playwright && playwright install chromium
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import platform
import re
from typing import Any

from nullion.plugins.browser_plugin.browser_session import (
    BrowserScreenshotResult,
    auto_screenshot_uses_full_page,
)

try:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False

try:
    from playwright_stealth import Stealth
    _STEALTH_AVAILABLE = True
except ImportError:
    Stealth = None  # type: ignore[assignment]
    _STEALTH_AVAILABLE = False


logger = logging.getLogger(__name__)
_TYPE_TEXT_TIMEOUT_MS = 3_000
_CLICK_TIMEOUT_MS = 5_000


def _require_playwright() -> None:
    if not _PLAYWRIGHT_AVAILABLE:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright && playwright install chromium"
        )


def _context_user_agent(browser_version: str) -> str:
    """Return a UA aligned with the actual bundled Chromium build."""

    version = str(browser_version or "").strip() or "0.0.0.0"
    system = platform.system()
    if system == "Windows":
        platform_token = "Windows NT 10.0; Win64; x64"
    elif system == "Darwin":
        platform_token = "Macintosh; Intel Mac OS X 10_15_7"
    else:
        platform_token = "X11; Linux x86_64"
    return (
        f"Mozilla/5.0 ({platform_token}) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{version} Safari/537.36"
    )


def _navigator_platform() -> str:
    system = platform.system()
    if system == "Windows":
        return "Win32"
    if system == "Darwin":
        return "MacIntel"
    return "Linux x86_64"


def _stealth_context_manager(playwright_context_manager: Any) -> Any:
    if not _STEALTH_AVAILABLE or Stealth is None:
        logger.warning("playwright-stealth is not installed; continuing without headless stealth patches.")
        return playwright_context_manager
    return Stealth(navigator_platform_override=_navigator_platform()).use_async(playwright_context_manager)


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


def _is_generic_field_target(target: dict[str, Any]) -> bool:
    semantic_keys = ("label", "placeholder", "role", "name", "text")
    if any(str(target.get(key) or "").strip() for key in semantic_keys):
        return False
    selector = str(target.get("selector") or "").strip().lower()
    return selector in {"input", "textarea", "select", "[role=combobox]", "[role='combobox']", '[role="combobox"]'}


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


def _semantic_selector_for_target(target: dict[str, Any]) -> str:
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


def _base_locator_for_target(page: "Page", target: dict[str, Any]):
    selector = str(target.get("selector") or "").strip()
    if selector:
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
    return page.locator(_semantic_selector_for_target(target))


def _target_locator_variants(target: dict[str, Any]) -> list[dict[str, Any]]:
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


def _locator_for_target(page: "Page", target: dict[str, Any]):
    return _first_locator(_base_locator_for_target(page, target))


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return await value
    return value


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


async def _candidate_locators_for_target(page: "Page", target: dict[str, Any]) -> list[Any]:
    locators: list[Any] = []
    for variant in _target_locator_variants(target):
        base = _base_locator_for_target(page, variant)
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


async def _click_with_dom_fallback(page: "Page", selector: str) -> None:
    original_error: BaseException | None = None
    try:
        await page.click(selector, timeout=_CLICK_TIMEOUT_MS)
        return
    except TypeError:
        try:
            await page.click(selector)
            return
        except Exception as exc:
            original_error = exc
    except Exception as exc:
        original_error = exc
    locator = getattr(page, "locator", None)
    if callable(locator):
        try:
            await locator(selector).click(force=True, timeout=_CLICK_TIMEOUT_MS)
            return
        except Exception as exc:
            original_error = original_error or exc
        try:
            await locator(selector).dispatch_event("click", timeout=_CLICK_TIMEOUT_MS)
            return
        except Exception as exc:
            original_error = original_error or exc
    try:
        await page.evaluate(_dom_click_script(selector))
    except Exception:
        if original_error is not None:
            raise original_error
        raise


async def _click_locator_with_fallback(locator, fallback_selector: str) -> None:
    original_error: BaseException | None = None
    try:
        await locator.click(timeout=_CLICK_TIMEOUT_MS)
        return
    except Exception as exc:
        original_error = exc
    try:
        await locator.click(force=True, timeout=_CLICK_TIMEOUT_MS)
        return
    except Exception as exc:
        original_error = original_error or exc
    try:
        await locator.dispatch_event("click", timeout=_CLICK_TIMEOUT_MS)
        return
    except Exception as exc:
        original_error = original_error or exc
    try:
        page = getattr(locator, "page", None)
        if page is not None:
            await page.evaluate(_dom_click_script(fallback_selector))
            return
    except Exception:
        pass
    if original_error is not None:
        raise original_error


async def _click_active_combobox_option(page: "Page", target: dict[str, Any]) -> bool:
    text = str(target.get("text") or target.get("name") or "").strip()
    if not text:
        return False
    try:
        return bool(await page.evaluate(_active_combobox_option_click_script(text)))
    except Exception:
        return False


async def _active_combobox_has_options(page: "Page") -> bool:
    try:
        return bool(await page.evaluate(
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
        ))
    except Exception:
        return False


async def _type_text_with_dom_fallback(page: "Page", selector: str, text: str) -> None:
    visible_editable = await _selector_targets_visible_editable(page, selector)
    if visible_editable is False:
        raise RuntimeError("Selector does not point to a visible editable field.")
    original_error: BaseException | None = None
    try:
        await page.fill(selector, text, timeout=_TYPE_TEXT_TIMEOUT_MS)
        return
    except TypeError:
        try:
            await page.fill(selector, text)
            return
        except Exception as exc:
            original_error = exc
    except Exception as exc:
        original_error = exc
    locator = getattr(page, "locator", None)
    if callable(locator):
        try:
            await locator(selector).fill(text, force=True, timeout=_TYPE_TEXT_TIMEOUT_MS)
            return
        except Exception as exc:
            original_error = original_error or exc
        try:
            await locator(selector).evaluate(_dom_type_script(":scope", text))
            return
        except Exception as exc:
            original_error = original_error or exc
    try:
        await page.evaluate(_dom_type_script(selector, text))
        return
    except Exception:
        if original_error is not None:
            raise original_error
        raise


async def _type_locator_with_fallback(locator, fallback_selector: str, text: str) -> None:
    original_error: BaseException | None = None
    try:
        await locator.fill(text, timeout=_TYPE_TEXT_TIMEOUT_MS)
        return
    except Exception as exc:
        original_error = exc
    try:
        await locator.fill(text, force=True, timeout=_TYPE_TEXT_TIMEOUT_MS)
        return
    except Exception as exc:
        original_error = original_error or exc
    try:
        await locator.press_sequentially(text, timeout=_TYPE_TEXT_TIMEOUT_MS)
        return
    except Exception as exc:
        original_error = original_error or exc
    evaluate = getattr(locator, "evaluate", None)
    if callable(evaluate):
        try:
            await evaluate(_dom_type_element_function(), text, timeout=_TYPE_TEXT_TIMEOUT_MS)
            return
        except TypeError:
            try:
                await evaluate(_dom_type_element_function(), text)
                return
            except Exception as exc:
                original_error = original_error or exc
        except Exception as exc:
            original_error = original_error or exc
    try:
        page = getattr(locator, "page", None)
        if page is not None:
            await page.evaluate(_dom_type_script(fallback_selector, text))
            return
    except Exception:
        pass
    if original_error is not None:
        raise original_error


async def _is_editable_locator(locator: Any) -> bool:
    evaluate = getattr(locator, "evaluate", None)
    if not callable(evaluate):
        return True
    try:
        result = await _maybe_await(evaluate(
            """(el) => {
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
            }""",
            timeout=500,
        ))
    except TypeError:
        try:
            result = await _maybe_await(evaluate(
                """(el) => {
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
            ))
        except Exception:
            return True
    except Exception:
        return True
    return bool(result)


async def _type_field_with_dom_target(page: "Page", target: dict[str, Any], text: str) -> bool:
    try:
        result = await page.evaluate(_dom_type_field_by_target_function(), {"target": target, "text": text})
    except Exception:
        return False
    if not isinstance(result, dict) or not result.get("ok"):
        return False
    after = str(result.get("after") or "").strip()
    return bool(after) and (text.strip() in after or after in text.strip())


async def _selector_targets_visible_editable(page: "Page", selector: str) -> bool | None:
    try:
        result = await page.evaluate(_selector_visible_editable_function(), selector)
    except Exception:
        return None
    if result is None:
        return None
    return bool(result)


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


class PlaywrightBackend:
    """Headless Chromium backend via Playwright.

    One Page per session_id. Sessions are isolated browser contexts.
    """

    BACKEND_NAME = "playwright"

    def __init__(self) -> None:
        _require_playwright()
        self._playwright = None
        self._browser: "Browser | None" = None
        self._pages: dict[str, "Page"] = {}
        self._lock = asyncio.Lock()
        self._headless = os.environ.get("NULLION_BROWSER_HEADLESS", "true").lower() != "false"

    async def _ensure_browser(self) -> "Browser":
        if self._browser is None or not self._browser.is_connected():
            playwright_context_manager: Any = async_playwright()
            if self._headless:
                playwright_context_manager = _stealth_context_manager(playwright_context_manager)
            pw = await playwright_context_manager.__aenter__()
            self._playwright = playwright_context_manager
            self._browser = await pw.chromium.launch(headless=self._headless)
        return self._browser

    async def _get_page(self, session_id: str) -> "Page":
        async with self._lock:
            if session_id not in self._pages:
                browser = await self._ensure_browser()
                ctx: "BrowserContext" = await browser.new_context(
                    user_agent=_context_user_agent(browser.version),
                )
                self._pages[session_id] = await ctx.new_page()
        return self._pages[session_id]

    # ── BrowserBackend protocol ───────────────────────────────────────────────

    async def open(self, session_id: str) -> str:
        page = await self._get_page(session_id)
        try:
            await page.bring_to_front()
        except Exception:
            pass
        return f"Opened browser session {session_id}"

    async def navigate(self, session_id: str, url: str) -> str:
        page = await self._get_page(session_id)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        status = response.status if response else 0
        return f"Navigated to {url} (status {status})"

    async def click(self, session_id: str, selector: str) -> None:
        page = await self._get_page(session_id)
        await _click_with_dom_fallback(page, selector)

    async def click_element(self, session_id: str, target: dict[str, Any]) -> None:
        page = await self._get_page(session_id)
        if await _click_active_combobox_option(page, target):
            return
        original_error: BaseException | None = None
        for locator in await _candidate_locators_for_target(page, target):
            try:
                await _click_locator_with_fallback(locator, _semantic_selector_for_target(target))
                return
            except Exception as exc:
                original_error = original_error or exc
        if original_error is not None:
            raise original_error
        raise RuntimeError("No matching element found.")

    async def type_text(self, session_id: str, selector: str, text: str) -> None:
        page = await self._get_page(session_id)
        await _type_text_with_dom_fallback(page, selector, text)

    async def type_field(self, session_id: str, target: dict[str, Any], text: str) -> None:
        page = await self._get_page(session_id)
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
                await _type_locator_with_fallback(locator, _semantic_selector_for_target(target), text)
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
        if await _type_field_with_dom_target(page, target, text):
            return
        if unverified_success:
            return
        if original_error is not None:
            raise original_error
        raise RuntimeError("Typed text did not appear in any matching field.")

    async def extract_text(self, session_id: str, selector: str | None) -> str:
        page = await self._get_page(session_id)
        if selector:
            el = page.locator(selector).first
            return await el.inner_text(timeout=5_000)
        return await page.inner_text("body")

    async def _page_layout(self, page: "Page") -> dict[str, int]:
        layout = await page.evaluate(
            """() => {
                const doc = document.documentElement || {};
                const body = document.body || {};
                const viewportWidth = window.innerWidth || doc.clientWidth || 0;
                const viewportHeight = window.innerHeight || doc.clientHeight || 0;
                const documentWidth = Math.max(
                    doc.scrollWidth || 0,
                    body.scrollWidth || 0,
                    viewportWidth
                );
                const documentHeight = Math.max(
                    doc.scrollHeight || 0,
                    body.scrollHeight || 0,
                    viewportHeight
                );
                return { viewportWidth, viewportHeight, documentWidth, documentHeight };
            }"""
        )
        if not isinstance(layout, dict):
            return {}
        return {
            key: max(0, int(layout.get(key) or 0))
            for key in ("viewportWidth", "viewportHeight", "documentWidth", "documentHeight")
        }

    async def screenshot(self, session_id: str, mode: str = "auto") -> BrowserScreenshotResult:
        page = await self._get_page(session_id)
        requested_mode = mode if mode in {"auto", "viewport", "full_page"} else "auto"
        layout = await self._page_layout(page)
        viewport_width = layout.get("viewportWidth")
        viewport_height = layout.get("viewportHeight")
        document_width = layout.get("documentWidth")
        document_height = layout.get("documentHeight")
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
        data = await page.screenshot(type="png", full_page=full_page, timeout=8_000)
        try:
            page_title = await page.title()
        except Exception:
            page_title = None
        return BrowserScreenshotResult(
            data=data,
            mode="full_page" if full_page else "viewport",
            requested_mode=requested_mode,
            page_url=getattr(page, "url", None),
            page_title=page_title,
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
        conditions: list[object] = []
        if selector:
            conditions.append(page.wait_for_selector(selector, timeout=timeout_ms))
        if url_pattern:
            conditions.append(page.wait_for_url(url_pattern, timeout=timeout_ms))
        if text:
            conditions.append(
                page.wait_for_function(
                    "(needle) => (document.body?.innerText || '').toLowerCase().includes(String(needle || '').toLowerCase())",
                    text,
                    timeout=timeout_ms,
                )
            )
        if not conditions:
            await page.wait_for_load_state("networkidle", timeout=timeout_ms)
            return
        if len(conditions) == 1:
            await conditions[0]
            return
        tasks = [asyncio.create_task(condition) for condition in conditions if asyncio.iscoroutine(condition)]
        done, pending = await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        if not done:
            raise TimeoutError("Timed out waiting for any browser condition.")
        first = next(iter(done))
        await first

    async def find(self, session_id: str, selector: str) -> list[dict[str, str]]:
        page = await self._get_page(session_id)
        elements = await page.query_selector_all(selector)
        results = []
        for el in elements[:50]:  # cap at 50
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
                await page.context.close()
            except Exception:
                pass

    async def shutdown(self) -> None:
        for session_id in list(self._pages):
            await self.close_session(session_id)
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                await self._playwright.__aexit__(None, None, None)
            except Exception:
                pass
