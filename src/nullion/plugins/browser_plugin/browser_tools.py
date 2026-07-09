"""Browser plugin — tool implementations (backend-agnostic)."""
from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import url2pathname

from nullion.artifacts import artifact_path_for_generated_workspace_file, path_is_within
from nullion.plugins.browser_plugin.browser_config import DEFAULT_AGENT_BROWSER_SESSION_ID
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy, BrowserPolicyViolation
from nullion.plugins.browser_plugin.browser_session import BrowserBackend, BrowserScreenshotResult, BrowserSessionPool
from nullion.tools import ToolInvocation, ToolResult
from nullion.workspace_storage import workspace_storage_roots_for_principal


def _ok(invocation: ToolInvocation, output: dict[str, Any]) -> ToolResult:
    return ToolResult(
        invocation_id=invocation.invocation_id,
        tool_name=invocation.tool_name,
        status="completed",
        output=output,
    )


def _fail(invocation: ToolInvocation, message: str, output: dict[str, Any] | None = None) -> ToolResult:
    return ToolResult(
        invocation_id=invocation.invocation_id,
        tool_name=invocation.tool_name,
        status="failed",
        output=output or {},
        error=message,
    )


def _compact_failure_message(message: object, *, max_chars: int = 360) -> str:
    text = re.sub(r"\s+", " ", str(message or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _browser_failure_reason(message: object) -> str:
    text = str(message or "").lower()
    if "not editable" in text:
        return "field_not_editable"
    if "not visible" in text or "hidden" in text:
        return "field_not_visible"
    if "not found" in text or "no matching" in text or "waiting for locator" in text:
        return "field_not_found"
    if "timeout" in text or "timed out" in text:
        return "field_timeout"
    return "browser_action_failed"


def _resize_browser_screenshot_to_layout_pixels(
    png_bytes: bytes,
    *,
    mode: str,
    viewport_width: int | None,
    viewport_height: int | None,
    document_width: int | None,
    document_height: int | None,
) -> tuple[bytes, dict[str, object]]:
    """Normalize high-DPI browser screenshots to their reported layout size."""

    try:
        from PIL import Image
    except Exception:
        return png_bytes, {}
    try:
        with Image.open(io.BytesIO(png_bytes)) as image:
            actual_width, actual_height = image.size
            if mode == "full_page":
                target_width = int(document_width or 0)
                target_height = int(document_height or 0)
            else:
                target_width = int(viewport_width or 0)
                target_height = int(viewport_height or 0)
            if target_width <= 0 or target_height <= 0:
                return png_bytes, {"image_width": actual_width, "image_height": actual_height}
            width_ratio = actual_width / target_width
            height_ratio = actual_height / target_height
            high_dpi = width_ratio > 1.25 and height_ratio > 1.25 and abs(width_ratio - height_ratio) <= 0.25
            if not high_dpi:
                return png_bytes, {"image_width": actual_width, "image_height": actual_height}
            resized = image.convert("RGBA").resize((target_width, target_height), Image.Resampling.LANCZOS)
            output = io.BytesIO()
            resized.save(output, format="PNG", optimize=True)
            return output.getvalue(), {
                "image_width": target_width,
                "image_height": target_height,
                "original_image_width": actual_width,
                "original_image_height": actual_height,
                "normalized_device_scale_factor": round((width_ratio + height_ratio) / 2, 3),
            }
    except Exception:
        return png_bytes, {}


def _int_metadata_value(metadata: dict[str, object], key: str) -> int | None:
    value = metadata.get(key)
    return value if isinstance(value, int) else None


_BROWSER_LOOP: asyncio.AbstractEventLoop | None = None
_BROWSER_LOOP_THREAD: threading.Thread | None = None
_BROWSER_LOOP_LOCK = threading.Lock()

# Cap concurrent browser-tool calls so that a flood of parallel agent tasks
# fails fast instead of starving the worker thread pool.
_MAX_CONCURRENT_BROWSER_OPS = 8
_BROWSER_SEMAPHORE = threading.Semaphore(_MAX_CONCURRENT_BROWSER_OPS)
_LOCAL_PREVIEW_SUFFIXES = frozenset({".html", ".htm", ".pdf"})
_ACTIVE_BROWSER_SESSION_TTL_SECONDS = 10 * 60


def _browser_snapshot_script(*, max_elements: int = 120) -> str:
    return f"""
(() => {{
  const maxElements = {int(max_elements)};
  window.__nullionBrowserElementSeq = window.__nullionBrowserElementSeq || 0;
  const selector = [
    'a[href]', 'button', 'input', 'textarea', 'select', 'summary',
    '[role]', '[contenteditable="true"]', '[tabindex]:not([tabindex="-1"])'
  ].join(',');
  const textOf = (value) => (value || '').replace(/\\s+/g, ' ').trim();
  const visible = (el) => {{
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  }};
  const includeHiddenEditable = (el) => {{
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    const hasName = Boolean(
      el.getAttribute('aria-label') || el.getAttribute('placeholder') ||
      el.getAttribute('name') || (el.labels && el.labels.length)
    );
    return hasName && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  }};
  const labelFor = (el) => {{
    const labels = [];
    const ariaLabelledBy = el.getAttribute('aria-labelledby');
    if (ariaLabelledBy) {{
      for (const id of ariaLabelledBy.split(/\\s+/)) {{
        const labelEl = document.getElementById(id);
        if (labelEl) labels.push(textOf(labelEl.innerText || labelEl.textContent));
      }}
    }}
    const aria = el.getAttribute('aria-label');
    if (aria) labels.push(aria);
    if (el.labels) for (const label of el.labels) labels.push(textOf(label.innerText || label.textContent));
    const id = el.getAttribute('id');
    if (id) {{
      const label = document.querySelector(`label[for="${{CSS.escape(id)}}"]`);
      if (label) labels.push(textOf(label.innerText || label.textContent));
    }}
    const closestLabel = el.closest('label');
    if (closestLabel) labels.push(textOf(closestLabel.innerText || closestLabel.textContent));
    const placeholder = el.getAttribute('placeholder');
    if (placeholder) labels.push(placeholder);
    const title = el.getAttribute('title');
    if (title) labels.push(title);
    const name = el.getAttribute('name');
    if (name) labels.push(name);
    return textOf(labels.find(Boolean) || '');
  }};
  const roleFor = (el) => {{
    const explicit = el.getAttribute('role');
    if (explicit) return explicit;
    const tag = el.tagName.toLowerCase();
    const type = (el.getAttribute('type') || '').toLowerCase();
    if (tag === 'a') return 'link';
    if (tag === 'button' || type === 'button' || type === 'submit') return 'button';
    if (tag === 'select') return 'combobox';
    if (tag === 'textarea') return 'textbox';
    if (tag === 'input') return type === 'checkbox' ? 'checkbox' : type === 'radio' ? 'radio' : 'textbox';
    if (el.isContentEditable) return 'textbox';
    return '';
  }};
  const editable = (el) => {{
    const tag = el.tagName.toLowerCase();
    const type = (el.getAttribute('type') || '').toLowerCase();
    return el.isContentEditable || tag === 'textarea' || tag === 'select' ||
      (tag === 'input' && !['button', 'submit', 'reset', 'checkbox', 'radio', 'file', 'hidden'].includes(type));
  }};
  const disabled = (el) => Boolean(el.disabled || el.getAttribute('aria-disabled') === 'true');
  const elements = [];
  for (const el of Array.from(document.querySelectorAll(selector))) {{
    const isEditable = editable(el);
    const isVisible = visible(el);
    if (!isVisible && !(isEditable && includeHiddenEditable(el))) continue;
    if (!el.dataset.nullionEid) el.dataset.nullionEid = `n-${{++window.__nullionBrowserElementSeq}}`;
    const rect = el.getBoundingClientRect();
    elements.push({{
      element_id: el.dataset.nullionEid,
      tag: el.tagName.toLowerCase(),
      role: roleFor(el),
      label: labelFor(el),
      text: textOf(el.innerText || el.textContent).slice(0, 240),
      value: 'value' in el ? String(el.value || '') : '',
      placeholder: el.getAttribute('placeholder') || '',
      name: el.getAttribute('name') || '',
      type: el.getAttribute('type') || '',
      visible: isVisible,
      disabled: disabled(el),
      editable: isEditable,
      expanded: el.getAttribute('aria-expanded'),
      aria_controls: el.getAttribute('aria-controls') || el.getAttribute('aria-owns') || '',
      checked: 'checked' in el ? Boolean(el.checked) : undefined,
      rect: {{
        x: Math.round(rect.x), y: Math.round(rect.y),
        width: Math.round(rect.width), height: Math.round(rect.height)
      }}
    }});
    if (elements.length >= maxElements) break;
  }}
  const active = document.activeElement && document.activeElement.dataset
    ? document.activeElement.dataset.nullionEid || ''
    : '';
  return {{url: location.href, title: document.title, active_element_id: active, element_count: elements.length, elements}};
}})()
"""


def _browser_extract_items_script(*, max_items: int = 30, selector: str | None = None) -> str:
    return f"""
(() => {{
  const maxItems = Math.max(1, Math.min(100, {int(max_items)}));
  const rootSelector = {json.dumps(selector or "")};
  const textOf = (value, limit = 360) => (value || '').replace(/\\s+/g, ' ').trim().slice(0, limit);
  const visible = (el) => {{
    if (!el) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  }};
  const absoluteUrl = (value) => {{
    try {{
      const url = new URL(String(value || ''), location.href);
      if (!['http:', 'https:'].includes(url.protocol)) return '';
      url.hash = '';
      return url.href;
    }} catch (_) {{
      return '';
    }}
  }};
  const pathDepth = (url) => {{
    try {{
      return new URL(url).pathname.split('/').filter(Boolean).length;
    }} catch (_) {{
      return 0;
    }}
  }};
  const nestedHttpUrl = (value) => {{
    const raw = String(value || '');
    const candidates = [];
    const looksLikeNestedUrl = (candidate) => {{
      const text = String(candidate || '').trim();
      return /^https?:\/\//i.test(text) || /^\/(?!\/)/.test(text) || /^\/\/[^/]/.test(text);
    }};
    try {{
      const parsed = new URL(raw, location.href);
      for (const paramValue of parsed.searchParams.values()) {{
        const decoded = (() => {{
          try {{ return decodeURIComponent(paramValue); }} catch (_) {{ return String(paramValue || ''); }}
        }})();
        if (!looksLikeNestedUrl(decoded)) continue;
        const nested = absoluteUrl(decoded);
        if (nested && nested !== parsed.href) candidates.push(nested);
      }}
    }} catch (_) {{}}
    const decodedRaw = (() => {{
      try {{ return decodeURIComponent(raw); }} catch (_) {{ return raw; }}
    }})();
    for (const match of decodedRaw.matchAll(/https?:\/\//ig)) {{
      const start = Number(match.index || 0);
      if (start <= 0) continue;
      const nested = absoluteUrl(decodedRaw.slice(start));
      if (nested) candidates.push(nested);
    }}
    const matches = decodedRaw.match(/https?:\/\/[^\s"'<>]+/g) || [];
    for (const match of matches.slice(1)) {{
      const nested = absoluteUrl(match);
      if (nested) candidates.push(nested);
    }}
    return candidates.filter(Boolean).pop() || '';
  }};
  const canonicalUrl = (value) => {{
    const resolved = nestedHttpUrl(value) || absoluteUrl(value);
    if (!resolved) return '';
    try {{
      const url = new URL(resolved);
      url.hash = '';
      if (pathDepth(url.href) >= 2) url.search = '';
      return url.href;
    }} catch (_) {{
      return resolved;
    }}
  }};
	  const tokenSet = (value) => new Set((String(value || '').toLowerCase().match(/[\p{{L}}\p{{N}}]{{3,}}/gu) || []).slice(0, 50));
	  const searchQueryTokens = (() => {{
	    try {{
	      const params = new URL(location.href).searchParams;
	      const values = [];
	      for (const name of ['q', 'k', 'query', 'search']) {{
	        for (const value of params.getAll(name)) {{
	          if (value) values.push(value);
	        }}
	      }}
	      return Array.from(tokenSet(values.join(' '))).slice(0, 8);
	    }} catch (_) {{
	      return [];
	    }}
	  }})();
	  const matchesSearchQuery = (value) => {{
	    if (!searchQueryTokens.length) return true;
	    const haystack = String(value || '').toLowerCase();
	    return searchQueryTokens.some((token) => haystack.includes(token));
	  }};
	  const stableTextKey = (value) => (String(value || '').toLowerCase().match(/[\p{{L}}\p{{N}}]+/gu) || [])
	    .slice(0, 32)
	    .join(' ');
  const tokenOverlap = (left, right) => {{
    const leftTokens = tokenSet(left);
    const rightTokens = tokenSet(right);
    if (!leftTokens.size || !rightTokens.size) return 0;
    let overlap = 0;
    for (const token of leftTokens) {{
      if (rightTokens.has(token)) overlap += 1;
    }}
    return overlap / Math.max(1, Math.min(leftTokens.size, rightTokens.size));
  }};
  const firstSrcsetUrl = (value) => {{
    const first = String(value || '').split(',').map((part) => part.trim().split(/\\s+/)[0]).find(Boolean);
    return absoluteUrl(first);
  }};
  const imageUrlFor = (img) => {{
    if (!img) return '';
    const candidates = [
      img.currentSrc,
      img.src,
      img.getAttribute('src'),
      img.getAttribute('data-src'),
      img.getAttribute('data-original'),
      img.getAttribute('data-lazy-src'),
      img.getAttribute('data-image'),
      img.getAttribute('data-image-src'),
      firstSrcsetUrl(img.getAttribute('srcset') || img.getAttribute('data-srcset')),
    ];
    return candidates.map(absoluteUrl).find(Boolean) || '';
  }};
  const usefulImage = (root) => {{
    for (const img of Array.from(root.querySelectorAll('img, source'))) {{
      const url = imageUrlFor(img);
      if (!url) continue;
      const rect = img.getBoundingClientRect();
      const naturalWidth = Number(img.naturalWidth || img.width || rect.width || 0);
      const naturalHeight = Number(img.naturalHeight || img.height || rect.height || 0);
      if ((visible(img) || naturalWidth >= 80 || naturalHeight >= 80) && naturalWidth >= 48 && naturalHeight >= 48) {{
        return {{
          url,
          alt: textOf(img.getAttribute('alt') || img.getAttribute('title') || '', 140),
          width: Math.round(naturalWidth || rect.width || 0),
          height: Math.round(naturalHeight || rect.height || 0),
        }};
      }}
    }}
    return {{url: '', alt: '', width: 0, height: 0}};
  }};
  const bestRootFor = (anchor) => {{
    const semantic = anchor.closest('article, li, [role="listitem"], [role="article"], [itemtype], [itemscope]');
    if (semantic && visible(semantic)) {{
      const semanticText = String(semantic.innerText || semantic.textContent || '');
      const semanticLinks = semantic.querySelectorAll ? semantic.querySelectorAll('a[href]').length : 0;
      if (semanticText.length <= 1400 && semanticLinks <= 12) return semantic;
    }}
    let best = anchor;
    let node = anchor;
    for (let depth = 0; depth < 6 && node && node !== document.body; depth += 1, node = node.parentElement) {{
      const rect = node.getBoundingClientRect();
      const linkCount = node.querySelectorAll ? node.querySelectorAll('a[href]').length : 0;
      const imageCount = node.querySelectorAll ? node.querySelectorAll('img, source').length : 0;
      const rawText = String(node.innerText || node.textContent || '');
      const text = textOf(rawText, 600);
      if (
        visible(node) && rect.width >= 80 && rect.height >= 40 &&
        rawText.length <= 1400 && text.length >= 12 &&
        (imageCount || linkCount <= 8)
      ) {{
        best = node;
        if (imageCount && text.length >= 20) break;
      }}
    }}
    return best;
  }};
  const titleFrom = (anchor, root, image) => {{
    const candidates = [
      anchor.getAttribute('aria-label'),
      anchor.getAttribute('title'),
      anchor.innerText,
      anchor.textContent,
      image.alt,
      root.querySelector('h1,h2,h3,h4,[role="heading"]')?.innerText,
      root.innerText,
    ];
    for (const value of candidates) {{
      const text = textOf(value, 160);
      if (text.length >= 3) return text;
    }}
    return '';
  }};
  const numericSnippets = (text) => {{
    const snippets = [];
    for (const line of String(text || '').split(/\\n+/)) {{
      const compact = textOf(line, 90);
      if (!compact || compact.length < 2) continue;
      if (/[\\p{{Sc}}]|\\b[A-Z]{{3}}\\b|\\d/u.test(compact)) snippets.push(compact);
      if (snippets.length >= 3) break;
    }}
    return snippets;
  }};
  const priceSnippets = (text) => {{
    const snippets = [];
    const source = String(text || '');
    const pattern = /(?:[\\p{{Sc}}]\\s*\\d[\\d,]*(?:\\.\\d{{2}})?|\\d[\\d,]*(?:\\.\\d{{2}})?\\s*(?:USD|CAD|EUR|GBP|AUD|NZD|JPY|CNY|INR))/giu;
    for (const match of source.matchAll(pattern)) {{
      const compact = textOf(match[0], 40);
      if (!compact || snippets.includes(compact)) continue;
      snippets.push(compact);
      if (snippets.length >= 3) break;
    }}
    return snippets;
  }};
  const roots = [];
  if (rootSelector) {{
    for (const root of Array.from(document.querySelectorAll(rootSelector))) {{
      if (visible(root)) roots.push(root);
    }}
  }}
  const anchors = roots.length
    ? roots.flatMap((root) => Array.from(root.querySelectorAll('a[href]')))
    : Array.from(document.querySelectorAll('a[href]'));
  const candidates = [];
  let sourceIndex = 0;
  for (const anchor of anchors) {{
    if (!visible(anchor) && !anchor.querySelector('img, source')) continue;
    const url = absoluteUrl(anchor.getAttribute('href') || anchor.href);
    if (!url) continue;
    const root = bestRootFor(anchor);
    const image = usefulImage(root);
    const rawRootText = root.innerText || root.textContent || anchor.innerText || anchor.textContent || '';
    const compactText = textOf(rawRootText, 180);
    const snippets = numericSnippets(rawRootText);
    const prices = priceSnippets(rawRootText);
	    const title = titleFrom(anchor, root, image);
	    const canonical = canonicalUrl(url);
	    const depth = pathDepth(canonical || url);
	    const titleImageOverlap = tokenOverlap(title, image.alt);
	    const anchorTitleOverlap = tokenOverlap(anchor.innerText || anchor.textContent || '', title);
	    if (!title && !image.url && compactText.length < 12) continue;
	    if (!matchesSearchQuery([title, image.alt, compactText, anchor.innerText || anchor.textContent || ''].join(' '))) continue;
	    if (!image.url && !snippets.length && depth < 2) continue;
    sourceIndex += 1;
    const score =
      (image.url ? 40 : 0) +
      (snippets.length ? 14 : 0) +
      Math.min(depth, 4) * 4 +
      (title.length >= 8 ? 6 : 0) +
      (titleImageOverlap >= 0.5 ? 18 : titleImageOverlap >= 0.25 ? 8 : 0) +
      (anchorTitleOverlap >= 0.5 ? 6 : 0) +
      (compactText.length >= 30 ? 4 : 0) -
      (depth < 2 ? 12 : 0) -
      (image.url && titleImageOverlap < 0.15 && anchorTitleOverlap < 0.15 ? 14 : 0);
    candidates.push({{
      score,
      source_index: sourceIndex,
      url: canonical || url,
      canonical_url: canonical,
      url_path_depth: depth,
      title,
      link_text: textOf(anchor.innerText || anchor.textContent || '', 140),
      image_url: image.url,
      image_alt: image.alt,
      image_width: image.width,
      image_height: image.height,
      price_text: prices[0] || '',
      price_candidates: prices,
      compact_text: compactText,
      numeric_snippets: snippets,
    }});
  }}
  candidates.sort((left, right) => right.score - left.score || left.source_index - right.source_index);
  const unique = [];
  const seenUrls = new Set();
  const seenImages = new Set();
  const seenTitles = new Set();
  for (const item of candidates) {{
    const urlKey = item.canonical_url || item.url;
    const imageKey = item.image_url || '';
    const titleKey = stableTextKey(item.title || item.link_text || item.compact_text);
    if (urlKey && seenUrls.has(urlKey)) continue;
    if (imageKey && seenImages.has(imageKey)) continue;
    if (titleKey && seenTitles.has(titleKey)) continue;
    if (urlKey) seenUrls.add(urlKey);
    if (imageKey) seenImages.add(imageKey);
    if (titleKey) seenTitles.add(titleKey);
    unique.push(item);
    if (unique.length >= maxItems) break;
  }}
  const items = unique.map((item, index) => {{
    const {{score, canonical_url, ...rest}} = item;
    return {{...rest, source_index: index + 1}};
  }});
  return {{
	    url: location.href,
	    title: document.title,
	    query_tokens: searchQueryTokens,
	    item_count: items.length,
	    items,
  }};
}})()
"""


def _browser_click_id_script(element_id: str) -> str:
    return f"""
(() => {{
  const elementId = {json.dumps(element_id)};
  const el = document.querySelector(`[data-nullion-eid="${{CSS.escape(elementId)}}"]`);
  if (!el) return {{ok: false, reason: 'element_not_found', element_id: elementId}};
  if (el.disabled || el.getAttribute('aria-disabled') === 'true') {{
    return {{ok: false, reason: 'element_disabled', element_id: elementId}};
  }}
  const before = {{text: (el.innerText || el.textContent || '').trim(), value: 'value' in el ? String(el.value || '') : ''}};
  el.scrollIntoView({{block: 'center', inline: 'center'}});
  if (typeof el.focus === 'function') el.focus({{preventScroll: true}});
  for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {{
    el.dispatchEvent(new MouseEvent(type, {{bubbles: true, cancelable: true, view: window}}));
  }}
  const active = document.activeElement && document.activeElement.dataset ? document.activeElement.dataset.nullionEid || '' : '';
  const after = {{text: (el.innerText || el.textContent || '').trim(), value: 'value' in el ? String(el.value || '') : ''}};
  return {{ok: true, clicked: true, element_id: elementId, active_element_id: active, before, after}};
}})()
"""


def _browser_type_id_script(element_id: str, text: str, *, clear: bool = True) -> str:
    return f"""
(() => {{
  const elementId = {json.dumps(element_id)};
  const text = {json.dumps(text)};
  const clear = {json.dumps(bool(clear))};
  const el = document.querySelector(`[data-nullion-eid="${{CSS.escape(elementId)}}"]`);
  if (!el) return {{ok: false, reason: 'element_not_found', element_id: elementId}};
  if (el.disabled || el.getAttribute('aria-disabled') === 'true') {{
    return {{ok: false, reason: 'element_disabled', element_id: elementId}};
  }}
  const style = window.getComputedStyle(el);
  const rect = el.getBoundingClientRect();
  const visible = style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  if (!visible) return {{ok: false, reason: 'element_not_visible', element_id: elementId}};
  const tag = el.tagName.toLowerCase();
  const type = (el.getAttribute('type') || '').toLowerCase();
  const editable = el.isContentEditable || tag === 'textarea' || tag === 'select' || (tag === 'input' && type !== 'hidden');
  if (!editable) return {{ok: false, reason: 'element_not_editable', element_id: elementId}};
  el.scrollIntoView({{block: 'center', inline: 'center'}});
  if (typeof el.focus === 'function') el.focus({{preventScroll: true}});
  const normalize = (value) => String(value || '').replace(/\s+/g, ' ').trim().toLowerCase();
  const before = 'value' in el ? String(el.value || '') : String(el.textContent || '');
  if ('value' in el) {{
    const proto = tag === 'textarea' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
    if (setter) setter.call(el, clear ? text : before + text);
    else el.value = clear ? text : before + text;
  }} else {{
    el.textContent = clear ? text : before + text;
  }}
  el.dispatchEvent(new InputEvent('input', {{bubbles: true, inputType: 'insertText', data: text}}));
  el.dispatchEvent(new Event('change', {{bubbles: true}}));
  const after = 'value' in el ? String(el.value || '') : String(el.textContent || '');
  const beforeNorm = normalize(before);
  const afterNorm = normalize(after);
  const textNorm = normalize(text);
  const verified = Boolean(
    textNorm && (
      afterNorm.includes(textNorm) ||
      textNorm.includes(afterNorm) ||
      (afterNorm && afterNorm !== beforeNorm)
    )
  );
  return {{
    ok: true,
    element_id: elementId,
    before_value: before,
    after_value: after,
    typed: text.length,
    verified,
    verification: afterNorm.includes(textNorm) ? 'exact_or_contains' : afterNorm !== beforeNorm ? 'changed' : 'unverified'
  }};
}})()
"""


def _browser_select_combobox_script(
    *,
    query: str,
    expected_text: str,
    element_id: str | None = None,
    label: str | None = None,
    placeholder: str | None = None,
    name: str | None = None,
) -> str:
    return f"""
(async () => {{
  const args = {json.dumps({
      "query": query,
      "expectedText": expected_text,
      "elementId": element_id or "",
      "label": label or "",
      "placeholder": placeholder or "",
      "name": name or "",
  })};
  const textOf = (value) => (value || '').replace(/\\s+/g, ' ').trim();
  const normalize = (value) => textOf(value).toLowerCase()
    .replace(/\\bwest\\b/g, 'w').replace(/\\beast\\b/g, 'e')
    .replace(/\\bnorth\\b/g, 'n').replace(/\\bsouth\\b/g, 's')
    .replace(/[^a-z0-9]+/g, ' ').trim();
  const tokens = (value) => normalize(value).split(/\\s+/).filter(Boolean);
  const expectedTokens = new Set(tokens(args.expectedText || args.query));
  const numericTokens = [...expectedTokens].filter((token) => /\\d/.test(token));
  const wordTokens = [...expectedTokens].filter((token) => !/\\d/.test(token));
  const meaningfulWordTokens = wordTokens.filter((token) => token.length >= 3);
  const compatible = (candidate) => {{
    const candidateTokens = new Set(tokens(candidate));
    const normalizedCandidate = normalize(candidate);
    const normalizedExpected = normalize(args.expectedText || args.query);
    if (normalizedCandidate.includes(normalizedExpected)) return true;
    if (numericTokens.length && normalizedExpected.includes(normalizedCandidate)) return true;
    if (numericTokens.length) {{
      if (!numericTokens.every((token) => candidateTokens.has(token))) return false;
      return !meaningfulWordTokens.length || meaningfulWordTokens.some((token) => candidateTokens.has(token));
    }}
    if (meaningfulWordTokens.length > 1) return meaningfulWordTokens.every((token) => candidateTokens.has(token));
    if (meaningfulWordTokens.length === 1) return candidateTokens.has(meaningfulWordTokens[0]);
    return wordTokens.length > 0 && wordTokens.every((token) => candidateTokens.has(token));
  }};
  const score = (candidate) => {{
    const candidateTokens = new Set(tokens(candidate));
    let value = 0;
    for (const token of expectedTokens) if (candidateTokens.has(token)) value += /\\d/.test(token) ? 4 : 1;
    return value;
  }};
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const visible = (el) => {{
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  }};
  const disabled = (el) => Boolean(el.disabled || el.getAttribute('aria-disabled') === 'true');
  const labelFor = (el) => {{
    const pieces = [el.getAttribute('aria-label'), el.getAttribute('placeholder'), el.getAttribute('name')];
    if (el.labels) for (const candidate of el.labels) pieces.push(textOf(candidate.innerText || candidate.textContent));
    const id = el.getAttribute('id');
    if (id) {{
      const label = document.querySelector(`label[for="${{CSS.escape(id)}}"]`);
      if (label) pieces.push(textOf(label.innerText || label.textContent));
    }}
    return normalize(pieces.filter(Boolean).join(' '));
  }};
  const desiredLabel = normalize([args.label, args.placeholder, args.name].filter(Boolean).join(' '));
  const candidates = Array.from(document.querySelectorAll('input, textarea, [role="combobox"], [contenteditable="true"]'))
    .filter((el) => visible(el) && !disabled(el));
  let el = args.elementId ? document.querySelector(`[data-nullion-eid="${{CSS.escape(args.elementId)}}"]`) : null;
  if (el && (!visible(el) || disabled(el))) el = null;
  if (!el && desiredLabel) {{
    el = candidates.find((candidate) => labelFor(candidate).includes(desiredLabel) || desiredLabel.includes(labelFor(candidate)));
  }}
  if (!el) el = candidates[0] || null;
  if (!el) return {{ok: false, reason: 'combobox_not_found'}};
  if (!el.dataset.nullionEid) {{
    window.__nullionBrowserElementSeq = window.__nullionBrowserElementSeq || 0;
    el.dataset.nullionEid = `n-${{++window.__nullionBrowserElementSeq}}`;
  }}
  const before = 'value' in el ? String(el.value || '') : textOf(el.textContent);
  el.scrollIntoView({{block: 'center', inline: 'center'}});
  const populate = (value) => {{
    if (typeof el.focus === 'function') el.focus({{preventScroll: true}});
    if ('value' in el) {{
      const proto = el.tagName.toLowerCase() === 'textarea' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
      const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
      if (setter) setter.call(el, value);
      else el.value = value;
    }} else {{
      el.textContent = value;
    }}
    el.dispatchEvent(new InputEvent('input', {{bubbles: true, inputType: 'insertText', data: value}}));
    el.dispatchEvent(new KeyboardEvent('keydown', {{bubbles: true, cancelable: true, key: 'ArrowDown'}}));
    el.dispatchEvent(new Event('change', {{bubbles: true}}));
  }};
  populate(args.query);
  const readOptions = () => {{
    const controlledId = el.getAttribute('aria-controls') || el.getAttribute('aria-owns') || '';
    const optionSelectors = [
      controlledId ? `#${{CSS.escape(controlledId)}} [role="option"]` : '',
      '[role="listbox"] [role="option"]',
      '[role="option"]',
      '[data-testid*="option" i]',
      '[data-baseweb*="menu" i] [role="option"]',
      '[data-baseweb*="menu" i] li',
      '[data-baseweb*="menu" i] div',
      'li'
    ].filter(Boolean);
    const seen = new Set();
    const options = [];
    for (const selector of optionSelectors) {{
      for (const option of Array.from(document.querySelectorAll(selector))) {{
        if (!visible(option)) continue;
        const text = textOf(option.innerText || option.textContent);
        if (!text || seen.has(text)) continue;
        seen.add(text);
        options.push({{text, element: option, score: score(text), compatible: compatible(text)}});
        if (options.length >= 20) break;
      }}
      if (options.length >= 20) break;
    }}
    return options;
  }};
  let options = [];
  for (let attempt = 0; attempt < 25; attempt++) {{
    options = readOptions();
    if (options.some((option) => option.compatible)) break;
    await sleep(150);
  }}
  if (!options.some((option) => option.compatible) && args.expectedText && normalize(args.expectedText) !== normalize(args.query)) {{
    populate(args.expectedText);
    for (let attempt = 0; attempt < 25; attempt++) {{
      options = readOptions();
      if (options.some((option) => option.compatible)) break;
      await sleep(150);
    }}
  }}
  const selected = options.filter((option) => option.compatible).sort((a, b) => b.score - a.score)[0];
  if (!selected) {{
    const after = 'value' in el ? String(el.value || '') : textOf(el.textContent);
    const pageText = textOf(document.body.innerText || '');
    if (options.length === 0 && (compatible(after) || pageText.includes(args.expectedText || args.query))) {{
      return {{
        ok: true,
        element_id: el.dataset.nullionEid,
        before_value: before,
        after_value: after,
        selected_text: after || args.expectedText || args.query,
        verified: true,
        options: options.map((option) => option.text).slice(0, 12),
        already_committed: true
      }};
    }}
    return {{
      ok: false,
      reason: 'no_compatible_option',
      element_id: el.dataset.nullionEid,
      before_value: before,
      query: args.query,
      expected_text: args.expectedText,
      options: options.map((option) => option.text).slice(0, 12)
    }};
  }}
  selected.element.scrollIntoView({{block: 'center', inline: 'center'}});
  for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {{
    selected.element.dispatchEvent(new MouseEvent(type, {{bubbles: true, cancelable: true, view: window}}));
  }}
  if (typeof selected.element.click === 'function') selected.element.click();
  await sleep(100);
  const after = 'value' in el ? String(el.value || '') : textOf(el.textContent);
  const pageText = textOf(document.body.innerText || '');
  const verified = compatible(selected.text) && (compatible(after) || pageText.includes(selected.text));
  return {{
    ok: true,
    element_id: el.dataset.nullionEid,
    before_value: before,
    after_value: after,
    selected_text: selected.text,
    verified,
    options: options.map((option) => option.text).slice(0, 12)
  }};
}})()
"""


def _browser_assert_page_state_script(*, required: list[str], forbidden: list[str]) -> str:
    return f"""
(() => {{
  const args = {json.dumps({"required": required, "forbidden": forbidden})};
  const textOf = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
  const normalize = (value) => textOf(value).toLowerCase()
    .replace(/\\bwest\\b/g, 'w').replace(/\\beast\\b/g, 'e')
    .replace(/\\bnorth\\b/g, 'n').replace(/\\bsouth\\b/g, 's')
    .replace(/[^a-z0-9]+/g, ' ').trim();
  const tokens = (value) => normalize(value).split(/\\s+/).filter(Boolean);
  const visible = (el) => {{
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  }};
  const labelFor = (el) => {{
    const pieces = [el.getAttribute('aria-label'), el.getAttribute('placeholder'), el.getAttribute('name'), el.getAttribute('title')];
    if (el.labels) for (const label of el.labels) pieces.push(textOf(label.innerText || label.textContent));
    const id = el.getAttribute('id');
    if (id) {{
      const label = document.querySelector(`label[for="${{CSS.escape(id)}}"]`);
      if (label) pieces.push(textOf(label.innerText || label.textContent));
    }}
    const labelledBy = el.getAttribute('aria-labelledby');
    if (labelledBy) {{
      for (const labelId of labelledBy.split(/\\s+/)) {{
        const label = document.getElementById(labelId);
        if (label) pieces.push(textOf(label.innerText || label.textContent));
      }}
    }}
    const seen = new Set();
    return textOf(pieces.filter(Boolean).map((piece) => textOf(piece)).filter((piece) => {{
      const key = normalize(piece);
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    }}).join(' '));
  }};
  const rawCandidates = [];
  for (const el of Array.from(document.querySelectorAll('body, main, section, article, p, span, div, input, textarea, select, button, a, [role], [contenteditable="true"]'))) {{
    if (el !== document.body && !visible(el)) continue;
    const pieces = [];
    if (el === document.body) {{
      pieces.push(textOf(el.innerText || el.textContent));
    }} else {{
      const label = labelFor(el);
      const text = textOf(el.innerText || el.textContent);
      const value = 'value' in el ? textOf(el.value) : '';
      if (label && value) pieces.push(`${{label}} ${{value}}`);
      if (label && text) pieces.push(`${{label}} ${{text}}`);
      if (value) pieces.push(value);
      if (text) pieces.push(text);
      if (el.tagName.toLowerCase() === 'select') {{
        const selected = el.selectedOptions && el.selectedOptions[0]
          ? textOf(el.selectedOptions[0].innerText || el.selectedOptions[0].textContent)
          : '';
        if (selected) pieces.push(selected);
      }}
    }}
    for (const piece of pieces) if (piece) rawCandidates.push(piece);
  }}
  const seen = new Set();
  const candidates = rawCandidates
    .map((candidate) => textOf(candidate))
    .filter(Boolean)
    .filter((candidate) => {{
      const key = normalize(candidate);
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    }})
    .sort((a, b) => normalize(a).length - normalize(b).length)
    .slice(0, 80);
  const compatible = (expected, candidate) => {{
    const expectedNorm = normalize(expected);
    const candidateNorm = normalize(candidate);
    if (!expectedNorm || !candidateNorm) return false;
    if (candidateNorm.includes(expectedNorm)) return true;
    const expectedTokens = new Set(tokens(expected));
    const candidateTokens = new Set(tokens(candidate));
    const numericTokens = [...expectedTokens].filter((token) => /\\d/.test(token));
    const wordTokens = [...expectedTokens].filter((token) => !/\\d/.test(token));
    const meaningfulWordTokens = wordTokens.filter((token) => token.length >= 3);
    if (numericTokens.length) {{
      const monthNumbers = {{
        jan: '1', january: '1', feb: '2', february: '2', mar: '3', march: '3',
        apr: '4', april: '4', may: '5', jun: '6', june: '6',
        jul: '7', july: '7', aug: '8', august: '8', sep: '9', sept: '9', september: '9',
        oct: '10', october: '10', nov: '11', november: '11', dec: '12', december: '12'
      }};
      const normalizeNumber = (token) => String(parseInt(token, 10));
      const candidateNumberTokens = new Set(
        [...candidateTokens]
          .filter((token) => /\\d/.test(token))
          .map((token) => normalizeNumber(token))
      );
      for (const token of candidateTokens) {{
        if (monthNumbers[token]) candidateNumberTokens.add(monthNumbers[token]);
      }}
      if (!numericTokens.every((token) => candidateNumberTokens.has(normalizeNumber(token)))) return false;
      return !meaningfulWordTokens.length || meaningfulWordTokens.some((token) => candidateTokens.has(token));
    }}
    if (meaningfulWordTokens.length > 1) return meaningfulWordTokens.every((token) => candidateTokens.has(token));
    if (meaningfulWordTokens.length === 1) return candidateTokens.has(meaningfulWordTokens[0]);
    return wordTokens.length > 0 && wordTokens.every((token) => candidateTokens.has(token));
  }};
  const findMatch = (expected) => candidates.find((candidate) => compatible(expected, candidate)) || '';
  const required = args.required.map((expected) => ({{expected, match: findMatch(expected)}}));
  const forbidden = args.forbidden.map((expected) => ({{expected, match: findMatch(expected)}}));
  const missing = required.filter((item) => !item.match);
  const forbiddenFound = forbidden.filter((item) => item.match);
  return {{
    ok: missing.length === 0 && forbiddenFound.length === 0,
    url: location.href,
    title: document.title,
    required,
    forbidden,
    missing,
    forbidden_found: forbiddenFound,
    candidates: candidates.slice(0, 20)
  }};
}})()
"""


def _ensure_browser_loop() -> asyncio.AbstractEventLoop:
    global _BROWSER_LOOP, _BROWSER_LOOP_THREAD
    with _BROWSER_LOOP_LOCK:
        if _BROWSER_LOOP is not None and _BROWSER_LOOP.is_running():
            return _BROWSER_LOOP
        loop = asyncio.new_event_loop()

        def _runner() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=_runner, name="nullion-browser-loop", daemon=True)
        thread.start()
        _BROWSER_LOOP = loop
        _BROWSER_LOOP_THREAD = thread
        return loop


def _run(coro) -> Any:
    """Run a browser coroutine on one shared loop.

    Browser backends keep session state and async locks. Running each sync tool
    call through a fresh event loop can wedge those locks across navigate /
    screenshot pairs, especially when attached to a visible CDP browser.

    A semaphore caps concurrency so that parallel agent tasks cannot exhaust
    the worker thread pool while waiting on browser I/O.
    """
    if not _BROWSER_SEMAPHORE.acquire(blocking=False):
        close = getattr(coro, "close", None)
        if close is not None:
            close()
        raise RuntimeError(
            f"Browser operation queue full — too many concurrent requests "
            f"(max {_MAX_CONCURRENT_BROWSER_OPS})"
        )
    try:
        loop = _ensure_browser_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return future.result(timeout=60)
        except Exception:
            future.cancel()
            raise
    finally:
        _BROWSER_SEMAPHORE.release()


def _workspace_html_preview_url(raw_url: str, *, principal_id: str | None) -> str | None:
    parsed = urlparse(raw_url)
    if parsed.scheme == "file":
        if parsed.netloc and parsed.netloc.lower() != "localhost":
            return None
        raw_path = url2pathname(parsed.path)
    elif not parsed.scheme:
        raw_path = raw_url
    else:
        return None

    path = Path(raw_path).expanduser()
    if not path.is_absolute() or path.suffix.lower() not in _LOCAL_PREVIEW_SUFFIXES:
        return None
    resolved = path.resolve(strict=False)
    if not resolved.is_file():
        return None

    roots = workspace_storage_roots_for_principal(principal_id, create=False)
    if any(path_is_within(resolved, root) for root in roots.all_roots()):
        return resolved.as_uri()
    return None


def _principal_allows_private_host_navigation(principal_id: str | None) -> bool:
    try:
        from nullion.connections import principal_has_admin_access

        return principal_has_admin_access(principal_id)
    except Exception:
        return False


class BrowserTools:
    """Sync wrappers around the async backend, registered as kernel tools."""

    def __init__(self, backend: BrowserBackend, pool: BrowserSessionPool, policy: BrowserPolicy) -> None:
        self._backend = backend
        self._pool = pool
        self._policy = policy
        self._cleanup_lock = threading.Lock()
        self._sessions_by_scope: dict[str, set[str]] = {}
        self._active_session_lock = threading.Lock()
        self._active_sessions_by_principal: dict[str, tuple[str, float]] = {}
        self._element_snapshot_lock = threading.Lock()
        self._element_snapshots: dict[str, dict[str, dict[str, Any]]] = {}

    def _session_id(self, invocation: ToolInvocation) -> str:
        raw_session_id = str(invocation.arguments.get("session_id", "") or "").strip()
        if self._uses_shared_default_session():
            workspace_session_id = self._workspace_browser_session_id(invocation)
            active_session_id = self._recent_active_session_id(invocation)
            if raw_session_id and raw_session_id != "default":
                if self._shared_session_prefers_workspace_scope(invocation, workspace_session_id):
                    if active_session_id:
                        return active_session_id
                    return workspace_session_id
                return raw_session_id
            if active_session_id:
                return active_session_id
            return workspace_session_id
        if raw_session_id and raw_session_id != "default":
            return raw_session_id
        scope = self._cleanup_scope(invocation)
        digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:16]
        return f"default-{digest}"

    def _workspace_browser_session_id(self, invocation: ToolInvocation) -> str:
        context = invocation.flow_context if isinstance(invocation.flow_context, dict) else {}
        workspace_id = str(context.get("workspace_id") or "").strip()
        if not workspace_id:
            try:
                from nullion.connections import workspace_id_for_principal

                workspace_id = str(workspace_id_for_principal(invocation.principal_id) or "").strip()
            except Exception:
                workspace_id = ""
        if not workspace_id:
            workspace_id = "workspace_admin"
        try:
            from nullion.workspace_storage import sanitize_workspace_id

            workspace_id = sanitize_workspace_id(workspace_id)
        except Exception:
            workspace_id = re.sub(r"[^A-Za-z0-9_.-]+", "-", workspace_id).strip("-") or "workspace_admin"
        if workspace_id == "workspace_admin":
            return DEFAULT_AGENT_BROWSER_SESSION_ID
        digest = hashlib.sha256(workspace_id.encode("utf-8")).hexdigest()[:16]
        return f"workspace-{digest}"

    def _shared_session_prefers_workspace_scope(
        self,
        invocation: ToolInvocation,
        workspace_session_id: str,
    ) -> bool:
        context = invocation.flow_context if isinstance(invocation.flow_context, dict) else {}
        if str(context.get("workspace_id") or "").strip():
            return True
        return workspace_session_id != DEFAULT_AGENT_BROWSER_SESSION_ID

    def _cleanup_scope(self, invocation: ToolInvocation) -> str:
        return str(invocation.capsule_id or invocation.principal_id or "global")

    def _uses_shared_default_session(self) -> bool:
        backend_name = str(getattr(self._backend, "BACKEND_NAME", "") or "").strip().lower()
        if backend_name == "cdp":
            return True
        if backend_name == "auto":
            return os.environ.get("NULLION_BROWSER_HEADLESS", "").strip().lower() != "true"
        return False

    def _connection_notice_output(self) -> dict[str, Any]:
        notice_fn = getattr(self._backend, "connection_notice", None)
        if not callable(notice_fn):
            return {}
        notice = str(notice_fn() or "").strip()
        if not notice:
            return {}
        return {
            "browser_connection_notice": notice,
            "used_shared_authenticated_browser": False,
        }

    def _remember_session(self, invocation: ToolInvocation, session_id: str) -> None:
        if invocation.tool_name != "browser_assert_page_state":
            self._remember_active_session(invocation, session_id)
        if self._uses_shared_default_session():
            return
        scope = self._cleanup_scope(invocation)
        with self._cleanup_lock:
            self._sessions_by_scope.setdefault(scope, set()).add(session_id)

    def _active_session_keys(self, invocation: ToolInvocation) -> tuple[str, ...]:
        context = invocation.flow_context if isinstance(invocation.flow_context, dict) else {}
        keys: list[str] = []
        for key in ("browser_session_scope", "conversation_id", "chat_id", "request_id"):
            value = str(context.get(key) or "").strip()
            if value:
                keys.append(f"{key}:{value}")
        principal_key = str(invocation.principal_id or "").strip()
        if principal_key:
            keys.append(principal_key)
        return tuple(dict.fromkeys(keys or ["global"]))

    def _active_session_key(self, invocation: ToolInvocation) -> str:
        return self._active_session_keys(invocation)[0]

    def _remember_active_session(self, invocation: ToolInvocation, session_id: str) -> None:
        session_id = str(session_id or "").strip()
        if not session_id:
            return
        with self._active_session_lock:
            remembered_at = time.monotonic()
            for key in self._active_session_keys(invocation):
                self._active_sessions_by_principal[key] = (session_id, remembered_at)

    def _recent_active_session_id(self, invocation: ToolInvocation, *, exclude: str | None = None) -> str | None:
        now = time.monotonic()
        with self._active_session_lock:
            for key in self._active_session_keys(invocation):
                item = self._active_sessions_by_principal.get(key)
                if not item:
                    continue
                session_id, remembered_at = item
                if now - remembered_at > _ACTIVE_BROWSER_SESSION_TTL_SECONDS:
                    self._active_sessions_by_principal.pop(key, None)
                    continue
                if exclude and session_id == exclude:
                    continue
                return session_id
        return None

    def _forget_session(self, session_id: str) -> None:
        with self._cleanup_lock:
            empty_scopes: list[str] = []
            for scope, session_ids in self._sessions_by_scope.items():
                session_ids.discard(session_id)
                if not session_ids:
                    empty_scopes.append(scope)
            for scope in empty_scopes:
                self._sessions_by_scope.pop(scope, None)
        with self._element_snapshot_lock:
            self._element_snapshots.pop(session_id, None)

    def _remember_element_snapshot(self, session_id: str, snapshot: dict[str, Any]) -> None:
        elements = snapshot.get("elements")
        if not isinstance(elements, list):
            return
        remembered: dict[str, dict[str, Any]] = {}
        for element in elements:
            if not isinstance(element, dict):
                continue
            element_id = str(element.get("element_id") or "").strip()
            if element_id:
                remembered[element_id] = dict(element)
        if remembered:
            with self._element_snapshot_lock:
                self._element_snapshots[session_id] = remembered

    def _cached_element(self, session_id: str, element_id: str) -> dict[str, Any] | None:
        with self._element_snapshot_lock:
            cached = self._element_snapshots.get(session_id, {}).get(element_id)
        return dict(cached) if isinstance(cached, dict) else None

    def _visible_editable_field_candidates(self, session_id: str, *, limit: int = 8) -> list[dict[str, object]]:
        run_js = getattr(self._backend, "run_js", None)
        if not callable(run_js):
            return []
        try:
            snapshot = _run(run_js(session_id, _browser_snapshot_script(max_elements=80)))
        except Exception:
            return []
        if not isinstance(snapshot, dict):
            return []
        self._remember_element_snapshot(session_id, snapshot)
        raw_elements = snapshot.get("elements")
        if not isinstance(raw_elements, list):
            return []
        candidates: list[dict[str, object]] = []
        for element in raw_elements:
            if not isinstance(element, dict):
                continue
            if not element.get("visible") or element.get("disabled") or not element.get("editable"):
                continue
            candidate: dict[str, object] = {}
            for key in ("element_id", "role", "label", "placeholder", "name", "type", "value"):
                value = str(element.get(key) or "").strip()
                if value:
                    candidate[key] = value[:120]
            if candidate:
                candidates.append(candidate)
            if len(candidates) >= max(1, limit):
                break
        return candidates

    def _field_failure_output(
        self,
        *,
        session_id: str,
        target: dict[str, object],
        text: object,
        error: object,
    ) -> dict[str, object]:
        output: dict[str, object] = {
            "reason": _browser_failure_reason(error),
            "message": _compact_failure_message(error),
            "session_id": session_id,
            "target": dict(target),
            "text_length": len(str(text or "")),
        }
        candidates = self._visible_editable_field_candidates(session_id)
        if candidates:
            output["visible_editable_fields"] = candidates
        return output

    @staticmethod
    def _semantic_target_from_cached_element(element: dict[str, Any] | None, *, for_click: bool) -> dict[str, object]:
        if not element:
            return {}
        target: dict[str, object] = {}
        for key in ("label", "placeholder", "name"):
            value = str(element.get(key) or "").strip()
            if value:
                target[key] = value
        role = str(element.get("role") or "").strip()
        text = str(element.get("text") or "").strip()
        if role:
            target["role"] = role
        if for_click and text:
            target["text"] = text
            if role and "name" not in target:
                target["name"] = text
        elif text and not target:
            target["text"] = text
        return target

    def close_tracked_sessions(self, scope_id: str | None = None) -> None:
        with self._cleanup_lock:
            if scope_id is None:
                session_ids = {
                    session_id
                    for scoped_session_ids in self._sessions_by_scope.values()
                    for session_id in scoped_session_ids
                }
                self._sessions_by_scope.clear()
            else:
                session_ids = set(self._sessions_by_scope.pop(str(scope_id), set()))
        for session_id in sorted(session_ids):
            try:
                _run(self._backend.close_session(session_id))
            except Exception:
                continue

    # ── Tools ─────────────────────────────────────────────────────────────────

    def browser_open(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            result = _run(self._backend.open(session_id))
            return _ok(invocation, {"result": result, "session_id": session_id, **self._connection_notice_output()})
        except Exception as e:
            return _fail(invocation, f"Open failed: {e}")

    def browser_navigate(self, invocation: ToolInvocation) -> ToolResult:
        url = invocation.arguments.get("url", "")
        if not url:
            return _fail(invocation, "Missing required argument: url")
        navigate_url = _workspace_html_preview_url(str(url), principal_id=invocation.principal_id)
        try:
            if navigate_url is None:
                self._policy.check_url(
                    str(url),
                    allow_private_host=_principal_allows_private_host_navigation(invocation.principal_id),
                )
                navigate_url = str(url)
        except BrowserPolicyViolation as e:
            return _fail(invocation, str(e))

        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            result = _run(self._backend.navigate(session_id, navigate_url))
            return _ok(invocation, {"result": result, "session_id": session_id, **self._connection_notice_output()})
        except Exception as e:
            return _fail(invocation, f"Navigation failed: {e}")

    def browser_click(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector", "")
        if not selector:
            return _fail(invocation, "Missing required argument: selector")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            _run(self._backend.click(session_id, str(selector)))
            return _ok(invocation, {"clicked": selector, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Click failed: {e}")

    def browser_click_element(self, invocation: ToolInvocation) -> ToolResult:
        target = {
            key: invocation.arguments.get(key)
            for key in ("selector", "label", "placeholder", "role", "name", "text")
            if invocation.arguments.get(key)
        }
        if not target:
            return _fail(invocation, "Missing target: provide selector, label, placeholder, text, or role/name")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            click_element = getattr(self._backend, "click_element", None)
            if callable(click_element):
                try:
                    _run(click_element(session_id, target))
                except Exception:
                    selector = str(target.get("selector") or "").strip()
                    if not selector:
                        raise
                    _run(self._backend.click(session_id, selector))
            else:
                selector = str(target.get("selector") or target.get("label") or target.get("text") or target.get("name") or "")
                _run(self._backend.click(session_id, selector))
            return _ok(invocation, {"clicked": target, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Click failed: {e}")

    def browser_type(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector", "")
        text = invocation.arguments.get("text", "")
        if not selector:
            return _fail(invocation, "Missing required argument: selector")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            _run(self._backend.type_text(session_id, str(selector), str(text)))
            return _ok(invocation, {"typed": len(str(text)), "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Type failed: {e}")

    def browser_type_field(self, invocation: ToolInvocation) -> ToolResult:
        text = invocation.arguments.get("text", "")
        target = {
            key: invocation.arguments.get(key)
            for key in ("selector", "label", "placeholder", "role", "name")
            if invocation.arguments.get(key)
        }
        if not target:
            return _fail(invocation, "Missing target: provide selector, label, placeholder, or role/name")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            type_field = getattr(self._backend, "type_field", None)
            if callable(type_field):
                _run(type_field(session_id, target, str(text)))
            else:
                selector = str(target.get("selector") or target.get("label") or target.get("name") or "")
                _run(self._backend.type_text(session_id, selector, str(text)))
            return _ok(invocation, {"typed": len(str(text)), "target": target, "session_id": session_id})
        except Exception as e:
            return _fail(
                invocation,
                f"Type failed: {e}",
                self._field_failure_output(session_id=session_id, target=target, text=text, error=e),
            )

    def browser_snapshot(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        max_elements = int(invocation.arguments.get("max_elements") or 120)
        if max_elements < 1:
            return _fail(invocation, "max_elements must be at least 1")
        max_elements = min(max_elements, 250)
        try:
            result = _run(self._backend.run_js(session_id, _browser_snapshot_script(max_elements=max_elements)))
            if not isinstance(result, dict):
                return _fail(invocation, "Snapshot returned an invalid result.")
            self._remember_element_snapshot(session_id, result)
            return _ok(invocation, {"snapshot": result, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Snapshot failed: {e}")

    def browser_extract_items(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            max_items = int(invocation.arguments.get("max_items") or 30)
        except (TypeError, ValueError):
            return _fail(invocation, "max_items must be an integer")
        max_items = max(1, min(max_items, 100))
        selector = invocation.arguments.get("selector")
        if selector is not None and not isinstance(selector, str):
            return _fail(invocation, "selector must be a string")
        try:
            result = _run(
                self._backend.run_js(
                    session_id,
                    _browser_extract_items_script(max_items=max_items, selector=selector.strip() if selector else None),
                )
            )
            if not isinstance(result, dict):
                return _fail(invocation, "Item extraction returned an invalid result.")
            items = result.get("items")
            if not isinstance(items, list):
                return _fail(invocation, "Item extraction returned no item list.")
            page = {
                "url": result.get("url"),
                "title": result.get("title"),
            }
            return _ok(invocation, {"page": page, "items": items, "item_count": len(items), "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Item extraction failed: {e}")

    def browser_click_id(self, invocation: ToolInvocation) -> ToolResult:
        element_id = str(invocation.arguments.get("element_id") or "").strip()
        if not element_id:
            return _fail(invocation, "Missing required argument: element_id")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            result = _run(self._backend.run_js(session_id, _browser_click_id_script(element_id)))
            if not isinstance(result, dict) or not result.get("ok"):
                reason = result.get("reason") if isinstance(result, dict) else "invalid_result"
                target = self._semantic_target_from_cached_element(
                    self._cached_element(session_id, element_id),
                    for_click=True,
                )
                click_element = getattr(self._backend, "click_element", None)
                if target and callable(click_element):
                    try:
                        _run(click_element(session_id, target))
                        return _ok(
                            invocation,
                            {
                                "clicked": element_id,
                                "session_id": session_id,
                                "recovered_by": "cached_semantic_target",
                                "target": target,
                                "previous_error": reason,
                            },
                        )
                    except Exception as fallback_error:
                        return _fail(invocation, f"Click failed: {reason}; fallback failed: {fallback_error}")
                return _fail(invocation, f"Click failed: {reason}")
            return _ok(invocation, {"result": result, "clicked": element_id, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Click failed: {e}")

    def browser_type_id(self, invocation: ToolInvocation) -> ToolResult:
        element_id = str(invocation.arguments.get("element_id") or "").strip()
        text = str(invocation.arguments.get("text") or "")
        clear = bool(invocation.arguments.get("clear", True))
        if not element_id:
            return _fail(invocation, "Missing required argument: element_id")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            cached_element = self._cached_element(session_id, element_id)
            if cached_element and cached_element.get("visible") is False and cached_element.get("editable") is True:
                target = self._semantic_target_from_cached_element(cached_element, for_click=False)
                type_field = getattr(self._backend, "type_field", None)
                if target and callable(type_field):
                    try:
                        _run(type_field(session_id, target, text))
                        return _ok(
                            invocation,
                            {
                                "typed": len(text),
                                "session_id": session_id,
                                "recovered_by": "hidden_cached_semantic_target",
                                "target": target,
                            },
                        )
                    except Exception:
                        pass
            result = _run(self._backend.run_js(session_id, _browser_type_id_script(element_id, text, clear=clear)))
            if not isinstance(result, dict) or not result.get("ok"):
                reason = result.get("reason") if isinstance(result, dict) else "invalid_result"
                cached_element = self._cached_element(session_id, element_id)
                if cached_element and cached_element.get("editable") is False:
                    return _fail(
                        invocation,
                        f"Type failed: {reason}",
                        self._field_failure_output(
                            session_id=session_id,
                            target={"element_id": element_id},
                            text=text,
                            error=reason,
                        ),
                    )
                target = self._semantic_target_from_cached_element(cached_element, for_click=False)
                type_field = getattr(self._backend, "type_field", None)
                if target and callable(type_field):
                    try:
                        _run(type_field(session_id, target, text))
                        return _ok(
                            invocation,
                            {
                                "typed": len(text),
                                "session_id": session_id,
                                "recovered_by": "cached_semantic_target",
                                "target": target,
                                "previous_error": reason,
                            },
                        )
                    except Exception as fallback_error:
                        return _fail(
                            invocation,
                            f"Type failed: {reason}; fallback failed: {fallback_error}",
                            self._field_failure_output(
                                session_id=session_id,
                                target=target,
                                text=text,
                                error=fallback_error,
                            ),
                        )
                return _fail(
                    invocation,
                    f"Type failed: {reason}",
                    self._field_failure_output(
                        session_id=session_id,
                        target={"element_id": element_id},
                        text=text,
                        error=reason,
                    ),
                )
            if not result.get("verified"):
                target = self._semantic_target_from_cached_element(
                    self._cached_element(session_id, element_id),
                    for_click=False,
                )
                type_field = getattr(self._backend, "type_field", None)
                if target and callable(type_field):
                    try:
                        _run(type_field(session_id, target, text))
                        return _ok(
                            invocation,
                            {
                                "result": result,
                                "typed": len(text),
                                "session_id": session_id,
                                "recovered_by": "cached_semantic_target",
                                "target": target,
                                "previous_error": "field value did not verify after input",
                            },
                        )
                    except Exception as fallback_error:
                        return _fail(
                            invocation,
                            f"Type failed: field value did not verify after input; fallback failed: {fallback_error}",
                            self._field_failure_output(
                                session_id=session_id,
                                target=target,
                                text=text,
                                error=fallback_error,
                            ),
                        )
                return _fail(
                    invocation,
                    "Type failed: field value did not verify after input",
                    self._field_failure_output(
                        session_id=session_id,
                        target={"element_id": element_id},
                        text=text,
                        error="field value did not verify after input",
                    ),
                )
            return _ok(invocation, {"result": result, "typed": len(text), "session_id": session_id})
        except Exception as e:
            return _fail(
                invocation,
                f"Type failed: {e}",
                self._field_failure_output(
                    session_id=session_id,
                    target={"element_id": element_id},
                    text=text,
                    error=e,
                ),
            )

    def browser_select_combobox(self, invocation: ToolInvocation) -> ToolResult:
        query = str(invocation.arguments.get("query") or "").strip()
        expected_text = str(invocation.arguments.get("expected_text") or query).strip()
        element_id = str(invocation.arguments.get("element_id") or "").strip() or None
        label = str(invocation.arguments.get("label") or "").strip() or None
        placeholder = str(invocation.arguments.get("placeholder") or "").strip() or None
        name = str(invocation.arguments.get("name") or "").strip() or None
        if not query:
            return _fail(invocation, "Missing required argument: query")
        if not (element_id or label or placeholder or name):
            return _fail(invocation, "Missing target: provide element_id, label, placeholder, or name")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            script = _browser_select_combobox_script(
                query=query,
                expected_text=expected_text,
                element_id=element_id,
                label=label,
                placeholder=placeholder,
                name=name,
            )
            result = _run(self._backend.run_js(session_id, script))
            if not isinstance(result, dict) or not result.get("ok"):
                reason = result.get("reason") if isinstance(result, dict) else "invalid_result"
                output = {"result": result, "session_id": session_id} if isinstance(result, dict) else {"session_id": session_id}
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="failed",
                    output=output,
                    error=f"Combobox selection failed: {reason}",
                )
            if not result.get("verified"):
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="failed",
                    output={"result": result, "session_id": session_id},
                    error="Combobox selection failed: selected value did not verify",
                )
            return _ok(invocation, {"result": result, "selected": result.get("selected_text"), "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Combobox selection failed: {e}")

    def browser_extract_text(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector") or None
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            text = _run(self._backend.extract_text(session_id, selector))
            return _ok(
                invocation,
                {"text": text, "length": len(text), "session_id": session_id, **self._connection_notice_output()},
            )
        except Exception as e:
            return _fail(invocation, f"Extract text failed: {e}")

    def browser_screenshot(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        mode = str(invocation.arguments.get("mode") or "auto").strip().lower()
        if mode not in {"auto", "viewport", "full_page"}:
            return _fail(invocation, "mode must be one of: auto, viewport, full_page")
        try:
            screenshot = _run(self._backend.screenshot(session_id, mode=mode))
            if isinstance(screenshot, BrowserScreenshotResult):
                png_bytes = screenshot.data
                screenshot_metadata = {
                    "mode": screenshot.mode,
                    "requested_mode": screenshot.requested_mode,
                    "page_url": screenshot.page_url,
                    "page_title": screenshot.page_title,
                    "viewport_width": screenshot.viewport_width,
                    "viewport_height": screenshot.viewport_height,
                    "document_width": screenshot.document_width,
                    "document_height": screenshot.document_height,
                    "is_clipped": screenshot.is_clipped,
                }
            else:
                png_bytes = screenshot
                screenshot_metadata = {
                    "mode": mode,
                    "requested_mode": mode,
                    "is_clipped": False,
                }
            if not png_bytes:
                return _fail(invocation, "Screenshot returned no image data.")
            png_bytes, image_metadata = _resize_browser_screenshot_to_layout_pixels(
                png_bytes,
                mode=str(screenshot_metadata.get("mode") or mode),
                viewport_width=_int_metadata_value(screenshot_metadata, "viewport_width"),
                viewport_height=_int_metadata_value(screenshot_metadata, "viewport_height"),
                document_width=_int_metadata_value(screenshot_metadata, "document_width"),
                document_height=_int_metadata_value(screenshot_metadata, "document_height"),
            )
            artifact_path = artifact_path_for_generated_workspace_file(
                principal_id=invocation.principal_id,
                suffix=".png",
                stem="screenshot",
            )
            artifact_path.write_bytes(png_bytes)
            path = str(artifact_path)
            return _ok(
                invocation,
                {
                    "path": path,
                    "artifact_path": path,
                    "artifact_paths": [path],
                    "format": "png",
                    "size_bytes": len(png_bytes),
                    "session_id": session_id,
                    **screenshot_metadata,
                    **image_metadata,
                    **self._connection_notice_output(),
                },
            )
        except Exception as e:
            return _fail(invocation, f"Screenshot failed: {e}")

    def browser_scroll(self, invocation: ToolInvocation) -> ToolResult:
        direction = str(invocation.arguments.get("direction", "down"))
        if direction not in {"up", "down"}:
            return _fail(invocation, "direction must be 'up' or 'down'")
        amount = int(invocation.arguments.get("amount", 500))
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            _run(self._backend.scroll(session_id, direction, amount))
            return _ok(invocation, {"scrolled": direction, "amount": amount, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Scroll failed: {e}")

    def browser_wait_for(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector") or None
        url_pattern = invocation.arguments.get("url_pattern") or None
        text = invocation.arguments.get("text") or None
        timeout = float(invocation.arguments.get("timeout", 10.0))
        if not selector and not url_pattern and not text:
            return _fail(invocation, "Provide selector, url_pattern, or text")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            wait_for = getattr(self._backend, "wait_for")
            try:
                _run(wait_for(session_id, selector, url_pattern, text, timeout))
            except TypeError:
                _run(wait_for(session_id, selector, url_pattern, timeout))
            return _ok(invocation, {"waited_for": selector or url_pattern or text, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Wait failed: {e}")

    def browser_assert_page_state(self, invocation: ToolInvocation) -> ToolResult:
        raw_required = invocation.arguments.get("required") or invocation.arguments.get("required_text") or []
        raw_forbidden = invocation.arguments.get("forbidden") or invocation.arguments.get("forbidden_text") or []
        required = [str(raw_required)] if isinstance(raw_required, str) else [str(item) for item in raw_required if str(item).strip()]
        forbidden = [str(raw_forbidden)] if isinstance(raw_forbidden, str) else [str(item) for item in raw_forbidden if str(item).strip()]
        if not required and not forbidden:
            return _fail(invocation, "Provide required or forbidden page text/state assertions")
        raw_session_id = str(invocation.arguments.get("session_id", "") or "").strip()
        has_explicit_session = bool(raw_session_id and raw_session_id != "default")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            script = _browser_assert_page_state_script(required=required, forbidden=forbidden)
            result = _run(
                self._backend.run_js(
                    session_id,
                    script,
                )
            )
            if not isinstance(result, dict):
                return _fail(invocation, "Page state assertion returned an invalid result.")
            if not result.get("ok"):
                current_url = str(result.get("url") or "").strip()
                if current_url in {"", "about:blank"}:
                    fallback_session_id = None if has_explicit_session else self._recent_active_session_id(
                        invocation,
                        exclude=session_id,
                    )
                    if fallback_session_id:
                        try:
                            fallback_result = _run(self._backend.run_js(fallback_session_id, script))
                        except Exception:
                            fallback_result = None
                        if isinstance(fallback_result, dict):
                            fallback_url = str(fallback_result.get("url") or "").strip()
                            if fallback_result.get("ok") or fallback_url not in {"", "about:blank"}:
                                session_id = fallback_session_id
                                result = fallback_result
                                current_url = fallback_url
                                self._remember_active_session(invocation, session_id)
                    if current_url in {"", "about:blank"} and not result.get("ok"):
                        return _fail(
                            invocation,
                            "Browser page is blank; navigate to a source page or search result before asserting visible page state.",
                        )
                if not result.get("ok"):
                    missing = result.get("missing") if isinstance(result.get("missing"), list) else []
                    forbidden_found = (
                        result.get("forbidden_found") if isinstance(result.get("forbidden_found"), list) else []
                    )
                    return _ok(
                        invocation,
                        {
                            "result": result,
                            "verified": False,
                            "missing_required": [
                                str(item.get("expected", "")).strip()
                                for item in missing
                                if isinstance(item, dict) and str(item.get("expected", "")).strip()
                            ],
                            "forbidden_found": [
                                str(item.get("expected") or item.get("match") or "").strip()
                                for item in forbidden_found
                                if isinstance(item, dict)
                                and str(item.get("expected") or item.get("match") or "").strip()
                            ],
                            "session_id": session_id,
                        },
                    )
            return _ok(invocation, {"result": result, "verified": True, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Page state assertion failed: {e}")

    def browser_find(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector", "")
        if not selector:
            return _fail(invocation, "Missing required argument: selector")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            elements = _run(self._backend.find(session_id, str(selector)))
            return _ok(invocation, {"elements": elements, "count": len(elements), "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Find failed: {e}")

    def browser_run_js(self, invocation: ToolInvocation) -> ToolResult:
        script = invocation.arguments.get("script", "")
        if not script:
            return _fail(invocation, "Missing required argument: script")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            script_text = str(script)
            try:
                result = _run(self._backend.run_js(session_id, script_text))
            except Exception as first_exc:
                if "Illegal return statement" not in str(first_exc):
                    raise
                wrapped_script = f"async () => {{\n{script_text}\n}}"
                result = _run(self._backend.run_js(session_id, wrapped_script))
            return _ok(invocation, {"result": result, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"JavaScript execution failed: {e}")

    def browser_close(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        try:
            _run(self._backend.close_session(session_id))
            self._forget_session(session_id)
            return _ok(invocation, {"closed": session_id})
        except Exception as e:
            return _fail(invocation, f"Close failed: {e}")
