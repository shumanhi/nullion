"""Browser plugin — tool implementations (backend-agnostic)."""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import io
import json
import os
import re
import threading
import time
import weakref
from functools import wraps
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
  const resolvedItemUrl = (value) => nestedHttpUrl(value) || absoluteUrl(value);
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
	    const resolvedUrl = resolvedItemUrl(url);
	    const depth = pathDepth(resolvedUrl || url);
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
      url: resolvedUrl || url,
      canonical_url: resolvedUrl || url,
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
    const {{score, ...rest}} = item;
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


_BROWSER_DOM_DETAIL_SCHEMA_VERSION = 1
_BROWSER_DOM_DETAIL_KIND = "browser_dom_detail"
_BROWSER_DOM_DETAIL_ORIGIN = "runtime_owned_dom_extractor"


def _browser_extract_detail_script(*, selector: str | None = None) -> str:
    """Build the fixed, runtime-owned current-page detail extractor.

    The model may choose a CSS scope, but it cannot provide or alter the
    extraction program. This keeps authoritative record fields separate from
    arbitrary ``browser_run_js`` return values.
    """

    return f"""
(() => {{
  const rootSelector = {json.dumps(selector or "")};
  const compact = (value, limit = 1200) => String(value || '')
    .replace(/\\s+/g, ' ')
    .trim()
    .slice(0, limit);
  const paintAlphaIsZero = (value) => {{
    const normalized = String(value || '').trim().toLocaleLowerCase();
    if (!normalized || normalized === 'none') return false;
    if (normalized === 'transparent') return true;
    if (/(?:rgba|hsla)\\([^)]*[,/]\\s*(?:0(?:\\.0*)?|\\.0+)\\s*\\)$/u.test(normalized)) {{
      return true;
    }}
    return /\\/\\s*(?:0(?:\\.0*)?|\\.0+)(?:%\\s*)?\\)$/u.test(normalized);
  }};
  const paintValueIsFullyTransparent = (value) => {{
    const normalized = String(value || '').trim().toLocaleLowerCase();
    if (!normalized || normalized === 'none' || normalized === 'transparent') return true;
    const colors = normalized.match(/(?:rgba|hsla|color|oklch|oklab|lab|lch)\\([^)]*\\)/gu) || [];
    return Boolean(colors.length && colors.every(paintAlphaIsZero));
  }};
  const paintCssLayers = (value) => {{
    const layers = [];
    let depth = 0;
    let current = '';
    for (const character of String(value || '')) {{
      if (character === '(') depth += 1;
      if (character === ')') depth = Math.max(0, depth - 1);
      if (character === ',' && depth === 0) {{
        layers.push(current.trim());
        current = '';
        continue;
      }}
      current += character;
    }}
    if (current.trim() || !layers.length) layers.push(current.trim());
    return layers.filter(Boolean);
  }};
  const paintCssTokens = (value) => {{
    const tokens = [];
    let depth = 0;
    let current = '';
    for (const character of String(value || '').trim()) {{
      if (/\\s/u.test(character) && depth === 0) {{
        if (current) tokens.push(current);
        current = '';
        continue;
      }}
      current += character;
      if (character === '(') depth += 1;
      if (character === ')') depth = Math.max(0, depth - 1);
    }}
    if (current) tokens.push(current);
    return tokens;
  }};
  const paintNumericCssValue = (token, basis, {{autoValue = basis}} = {{}}) => {{
    const normalized = String(token || '').trim().toLocaleLowerCase();
    if (normalized === 'auto') return autoValue;
    const calcMatch = normalized.match(
      /^calc\\(\\s*([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))%\\s*([+-])\\s*([0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)px\\s*\\)$/u
    );
    if (calcMatch) {{
      return (
        basis * Number(calcMatch[1]) / 100
        + Number(calcMatch[3]) * (calcMatch[2] === '-' ? -1 : 1)
      );
    }}
    const match = normalized.match(
      /^([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))(px|%)?$/u
    );
    if (!match) return Number.NaN;
    const amount = Number(match[1]);
    if (!match[2] && amount !== 0) return Number.NaN;
    return match[2] === '%' ? basis * amount / 100 : amount;
  }};
  const paintImageSize = (value, rect) => {{
    const tokens = paintCssTokens(value || 'auto');
    if (!tokens.length) return null;
    if (tokens.length === 1 && /^(?:cover|contain)$/u.test(tokens[0])) {{
      return [rect.width, rect.height];
    }}
    const width = paintNumericCssValue(tokens[0], rect.width);
    const height = paintNumericCssValue(tokens[1] || 'auto', rect.height);
    if (![width, height].every(Number.isFinite)) return null;
    return [width, height];
  }};
  const paintPositionComponent = (token) => {{
    const normalized = String(token || '').trim().toLocaleLowerCase();
    if (normalized === 'center') return {{percent: 0.5, pixels: 0}};
    if (normalized === 'left' || normalized === 'top') return {{percent: 0, pixels: 0}};
    if (normalized === 'right' || normalized === 'bottom') return {{percent: 1, pixels: 0}};
    const calcMatch = normalized.match(
      /^calc\\(\\s*([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))%\\s*([+-])\\s*([0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)px\\s*\\)$/u
    );
    if (calcMatch) {{
      return {{
        percent: Number(calcMatch[1]) / 100,
        pixels: Number(calcMatch[3]) * (calcMatch[2] === '-' ? -1 : 1),
      }};
    }}
    const match = normalized.match(
      /^([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))(px|%)?$/u
    );
    if (!match) return null;
    const amount = Number(match[1]);
    if (!match[2] && amount !== 0) return null;
    return match[2] === '%'
      ? {{percent: amount / 100, pixels: 0}}
      : {{percent: 0, pixels: amount}};
  }};
  const paintEdgePositionComponent = (edge, distance) => {{
    const anchor = paintPositionComponent(edge);
    const offset = paintNumericCssValue(distance, 0, {{autoValue: Number.NaN}});
    if (!anchor || !Number.isFinite(offset)) return null;
    const reverse = edge === 'right' || edge === 'bottom';
    return {{percent: anchor.percent, pixels: reverse ? -offset : offset}};
  }};
  const paintBackgroundPosition = (value) => {{
    const tokens = paintCssTokens(value || '0% 0%');
    if (
      tokens.length >= 4
      && /^(?:left|right)$/u.test(tokens[0])
      && /^(?:top|bottom)$/u.test(tokens[2])
    ) {{
      return [
        paintEdgePositionComponent(tokens[0], tokens[1]),
        paintEdgePositionComponent(tokens[2], tokens[3]),
      ];
    }}
    if (tokens.length === 1) {{
      if (/^(?:top|bottom)$/u.test(tokens[0])) {{
        return [paintPositionComponent('center'), paintPositionComponent(tokens[0])];
      }}
      return [paintPositionComponent(tokens[0]), paintPositionComponent('center')];
    }}
    return [
      paintPositionComponent(tokens[0] || '0%'),
      paintPositionComponent(tokens[1] || '0%'),
    ];
  }};
  const paintRepeatedAxes = (value) => {{
    const tokens = paintCssTokens(value || 'repeat').map((token) => token.toLocaleLowerCase());
    if (tokens[0] === 'repeat-x') return [true, false];
    if (tokens[0] === 'repeat-y') return [false, true];
    const repeats = (token) => token !== 'no-repeat';
    return [repeats(tokens[0] || 'repeat'), repeats(tokens[1] || tokens[0] || 'repeat')];
  }};
  const paintLayerGeometryCanPaint = (size, position, repeat, rect) => {{
    const container = [rect.width, rect.height];
    const imageSize = paintImageSize(size, rect);
    const offsets = paintBackgroundPosition(position);
    const repeats = paintRepeatedAxes(repeat);
    if (
      !imageSize
      || offsets.some((offset) => !offset)
      || container.some((dimension) => !(dimension > 0))
    ) return false;
    return imageSize.every((dimension, axis) => {{
      if (!(dimension > 0)) return false;
      if (repeats[axis]) return true;
      const offset = (
        (container[axis] - dimension) * offsets[axis].percent
        + offsets[axis].pixels
      );
      return offset < container[axis] && offset + dimension > 0;
    }});
  }};
  const filterSuppressesPaint = (value) => Array.from(
    String(value || '').matchAll(/opacity\\(\\s*([0-9.]+)\\s*(%)?\\s*\\)/giu)
  ).some((match) => Number(match[1]) <= 0);
  const maskSuppressesPaint = (image, size, position, repeat, rect) => {{
    const imageLayers = paintCssLayers(image);
    if (!imageLayers.length || imageLayers.every((layer) => layer === 'none')) return false;
    const sizeLayers = paintCssLayers(size || 'auto');
    const positionLayers = paintCssLayers(position || '0% 0%');
    const repeatLayers = paintCssLayers(repeat || 'repeat');
    const layerValue = (layers, index, fallback) => (
      layers.length ? layers[index % layers.length] : fallback
    );
    return imageLayers.every((layer, index) => (
      layer === 'none'
      || paintValueIsFullyTransparent(layer)
      || !paintLayerGeometryCanPaint(
        layerValue(sizeLayers, index, 'auto'),
        layerValue(positionLayers, index, '0% 0%'),
        layerValue(repeatLayers, index, 'repeat'),
        rect,
      )
    ));
  }};
  const cssInsetValue = (token, basis) => {{
    const match = String(token || '').match(/^(-?[0-9.]+)(%|px)?$/u);
    if (!match) return Number.NaN;
    return match[2] === '%' ? Number(match[1]) * basis / 100 : Number(match[1]);
  }};
  const clipPathSuppressesPaint = (value, rect) => {{
    const normalized = String(value || '').trim();
    const inset = normalized.match(/^inset\\(([^)]*)\\)/iu);
    if (inset) {{
      const tokens = inset[1].trim().split(/\\s+/u).filter(Boolean);
      if (!tokens.length || tokens.some((token) => token.includes('round'))) return false;
      const sides = tokens.length === 1
        ? [tokens[0], tokens[0], tokens[0], tokens[0]]
        : tokens.length === 2
          ? [tokens[0], tokens[1], tokens[0], tokens[1]]
          : tokens.length === 3
            ? [tokens[0], tokens[1], tokens[2], tokens[1]]
            : tokens.slice(0, 4);
      const top = cssInsetValue(sides[0], rect.height);
      const right = cssInsetValue(sides[1], rect.width);
      const bottom = cssInsetValue(sides[2], rect.height);
      const left = cssInsetValue(sides[3], rect.width);
      return [top, right, bottom, left].every(Number.isFinite)
        && (top + bottom >= rect.height || left + right >= rect.width);
    }}
    const radial = normalized.match(/^(?:circle|ellipse)\\(\\s*([^\\s,)]+)/iu);
    if (radial) {{
      const radius = cssInsetValue(radial[1], Math.min(rect.width, rect.height));
      if (Number.isFinite(radius) && radius <= 0) return true;
    }}
    const polygon = normalized.match(/^polygon\\((.*)\\)$/iu);
    if (polygon) {{
      const points = polygon[1].split(',').map((point) => {{
        const coordinates = point.trim().split(/\\s+/u);
        return [
          cssInsetValue(coordinates[0], rect.width),
          cssInsetValue(coordinates[1], rect.height),
        ];
      }}).filter((point) => point.every(Number.isFinite));
      if (points.length >= 3) {{
        const twiceArea = Math.abs(points.reduce((area, point, index) => {{
          const next = points[(index + 1) % points.length];
          return area + point[0] * next[1] - next[0] * point[1];
        }}, 0));
        if (twiceArea <= 1e-7) return true;
      }}
    }}
    const path = normalized.match(
      /^path\\(\\s*(?:(?:evenodd|nonzero)\\s*,\\s*)?["']([^"']*)["']\\s*\\)$/iu
    );
    if (path) {{
      const commands = path[1].match(/[a-z]/giu) || [];
      if (
        !commands.length
        || commands.every((command) => command.toLocaleLowerCase() === 'm')
      ) return true;
    }}
    return false;
  }};
  const legacyClipSuppressesPaint = (value) => {{
    const normalized = String(value || '').trim();
    if (!normalized || normalized === 'auto') return false;
    const numbers = normalized.match(/-?[0-9.]+/g) || [];
    if (numbers.length < 4) return false;
    const [top, right, bottom, left] = numbers.slice(0, 4).map(Number);
    return bottom <= top || right <= left;
  }};
  const transformSuppressesPaint = (value) => {{
    const normalized = String(value || '').trim();
    if (!normalized || normalized === 'none') return false;
    try {{
      const matrix = new DOMMatrixReadOnly(normalized);
      return Math.hypot(matrix.m11, matrix.m12, matrix.m13) <= 1e-7
        || Math.hypot(matrix.m21, matrix.m22, matrix.m23) <= 1e-7;
    }} catch (_error) {{
      return false;
    }}
  }};
  const styleSuppressesPaint = (element, style) => {{
    const rect = element.getBoundingClientRect();
    return filterSuppressesPaint(style.filter)
      || maskSuppressesPaint(
        style.maskImage,
        style.maskSize,
        style.maskPosition,
        style.maskRepeat,
        rect,
      )
      || maskSuppressesPaint(
        style.webkitMaskImage,
        style.webkitMaskSize,
        style.webkitMaskPosition,
        style.webkitMaskRepeat,
        rect,
      )
      || transformSuppressesPaint(style.transform)
      || clipPathSuppressesPaint(style.clipPath, rect)
      || legacyClipSuppressesPaint(style.clip);
  }};
  const paintVisible = (element) => {{
    if (!element) return false;
    const elementRect = element.getBoundingClientRect();
    let clippedLeft = elementRect.left;
    let clippedRight = elementRect.right;
    let clippedTop = elementRect.top;
    let clippedBottom = elementRect.bottom;
    let current = element;
    while (current && current.nodeType === Node.ELEMENT_NODE) {{
      const currentStyle = window.getComputedStyle(current);
      if (
        currentStyle.visibility === 'hidden'
        || currentStyle.display === 'none'
        || currentStyle.contentVisibility === 'hidden'
        || Number.parseFloat(currentStyle.opacity) === 0
        || styleSuppressesPaint(current, currentStyle)
      ) return false;
      const currentRect = current.getBoundingClientRect();
      if (
        currentStyle.position === 'fixed'
        && (
          currentRect.right <= 0
          || currentRect.left >= window.innerWidth
          || currentRect.bottom <= 0
          || currentRect.top >= window.innerHeight
        )
      ) return false;
      const clipsX = /^(?:hidden|clip)$/u.test(currentStyle.overflowX);
      const clipsY = /^(?:hidden|clip)$/u.test(currentStyle.overflowY);
      if (clipsX) {{
        clippedLeft = Math.max(clippedLeft, currentRect.left);
        clippedRight = Math.min(clippedRight, currentRect.right);
      }}
      if (clipsY) {{
        clippedTop = Math.max(clippedTop, currentRect.top);
        clippedBottom = Math.min(clippedBottom, currentRect.bottom);
      }}
      if (clippedRight <= clippedLeft || clippedBottom <= clippedTop) return false;
      if (
        (clipsX || clipsY)
        && currentRect.width <= 1
        && currentRect.height <= 1
      ) return false;
      if (
        typeof current.checkVisibility === 'function'
        && !current.checkVisibility({{
          checkOpacity: true,
          checkVisibilityCSS: true,
          contentVisibilityAuto: true,
        }})
      ) return false;
      current = current.parentElement;
    }}
    const rect = elementRect;
    return rect.width > 0
      && rect.height > 0
      && rect.right + window.scrollX > 0
      && rect.bottom + window.scrollY > 0;
  }};
  const visible = (element) => {{
    let current = element;
    while (current && current.nodeType === Node.ELEMENT_NODE) {{
      if (current.getAttribute('aria-hidden') === 'true') return false;
      current = current.parentElement;
    }}
    return paintVisible(element);
  }};
  const initialRoot = rootSelector
    ? document.querySelector(rootSelector)
    : (document.querySelector('main, [role="main"]') || document.body);
  if (!initialRoot) {{
    return {{
      ok: false,
      reason: 'scope_not_found',
      selector: rootSelector || null,
      page: {{url: location.href, title: document.title}},
    }};
  }}
  const headingSelectors = '[itemprop="name"], h1, [role="heading"][aria-level="1"]';
  const priceSelectors = '[itemprop~="price"], [data-price], [class*="price" i], [id*="price" i]';
  const candidateRoots = Array.from(initialRoot.querySelectorAll(
    'article, [role="article"], [role="listitem"], [itemscope]'
  )).filter((element) => {{
    if (!visible(element)) return false;
    const heading = element.querySelector(headingSelectors);
    const link = element.querySelector('a[href]');
    const price = element.querySelector(priceSelectors);
    return Boolean(heading && (link || price));
  }});
  const primaryHeadings = Array.from(initialRoot.querySelectorAll(
    'h1, [role="heading"][aria-level="1"]'
  )).filter(visible);
  if (primaryHeadings.length > 1) {{
    return {{
      ok: false,
      reason: 'ambiguous_detail_scope',
      selector: rootSelector || null,
      candidate_count: primaryHeadings.length,
      page: {{url: location.href, title: document.title}},
    }};
  }}
  const titleElement = primaryHeadings[0]
    || Array.from(initialRoot.querySelectorAll('[itemprop="name"]')).find(visible)
    || null;
  if (!titleElement) {{
    const visibleRecordLinks = Array.from(initialRoot.querySelectorAll('a[href]'))
      .filter((element) => visible(element) && compact(element.textContent, 300));
    return {{
      ok: false,
      reason: visibleRecordLinks.length > 1
        ? 'ambiguous_detail_scope'
        : 'missing_primary_detail_title',
      selector: rootSelector || null,
      candidate_count: visibleRecordLinks.length,
      page: {{url: location.href, title: document.title}},
    }};
  }}
  const containingCandidate = titleElement
    ? candidateRoots.find((candidate) => candidate.contains(titleElement))
    : null;
  let primaryContainer = null;
  if (primaryHeadings.length === 1 && titleElement) {{
    const detailEvidenceSelectors = `${{priceSelectors}}, [itemprop="description"], [itemprop="availability"], [class*="availability" i], [id*="availability" i]`;
    const hasSubstantiveDetailEvidence = (candidate) =>
      Array.from(candidate.querySelectorAll(detailEvidenceSelectors)).some((element) => {{
        const value = compact(
          element.getAttribute('content')
          || element.getAttribute('value')
          || element.getAttribute('data-price')
          || element.getAttribute('aria-label')
          || element.getAttribute('href')
          || element.textContent,
          600,
        );
        return Boolean(value && (element.matches('meta, link') || visible(element)));
      }});
    let candidate = titleElement.parentElement;
    while (candidate && initialRoot.contains(candidate)) {{
      if (candidate !== initialRoot && hasSubstantiveDetailEvidence(candidate)) {{
        primaryContainer = candidate;
        break;
      }}
      if (candidate === initialRoot) break;
      candidate = candidate.parentElement;
    }}
  }}
  const root = containingCandidate || primaryContainer || initialRoot;
  const availabilitySelectors = [
    '[itemprop="availability"]',
    '[class*="availability" i]',
    '[id*="availability" i]',
  ];
  const availabilityBelongsToSelectedRecord = (element) => {{
    if (candidateRoots.some((candidate) => (
      candidate.contains(element)
      && !candidate.contains(titleElement)
    ))) return false;
    let current = element.parentElement;
    while (current && initialRoot.contains(current)) {{
      if (current.contains(titleElement)) return true;
      const hasLinkedRecordIdentity = Array.from(current.querySelectorAll(
        'h1 a[href], h2 a[href], h3 a[href], [role="heading"] a[href], [itemprop="name"] a[href]'
      )).some(visible);
      const hasPriceEvidence = Array.from(current.querySelectorAll(priceSelectors)).some(
        (price) => price.matches('meta, link') || visible(price)
      );
      if (hasLinkedRecordIdentity && hasPriceEvidence) return false;
      current = current.parentElement;
    }}
    return true;
  }};
  let availabilityScope = root;
  if (root !== initialRoot && root.contains(titleElement)) {{
    let candidate = root.parentElement;
    while (candidate && initialRoot.contains(candidate)) {{
      const hasEligibleSiblingAvailability = availabilitySelectors.some(
        (selector) => Array.from(candidate.querySelectorAll(selector)).some((element) => {{
          if (root.contains(element) || !availabilityBelongsToSelectedRecord(element)) {{
            return false;
          }}
          const value = compact(
            element.getAttribute('content')
            || element.getAttribute('href')
            || element.getAttribute('value')
            || element.getAttribute('aria-label')
            || element.textContent,
            600,
          );
          return Boolean(value && (element.matches('meta, link') || visible(element)));
        }})
      );
      if (hasEligibleSiblingAvailability) {{
        availabilityScope = candidate;
        break;
      }}
      if (candidate === initialRoot) break;
      candidate = candidate.parentElement;
    }}
  }}
  const recordLikePeerGroups = new Map();
  if (root === initialRoot && !containingCandidate && !primaryContainer) {{
    for (const anchor of Array.from(root.querySelectorAll('a[href]')).filter(visible)) {{
      const anchorText = compact(anchor.textContent, 300);
      if (!anchorText) continue;
      let container = anchor.parentElement;
      while (container && container !== root) {{
        const containerText = compact(container.innerText || container.textContent, 1600);
        const visibleLinks = Array.from(container.querySelectorAll('a[href]')).filter(visible);
        if (
          visibleLinks.length <= 3
          && containerText.length >= anchorText.length + 20
          && containerText.length <= 1400
        ) {{
          const parent = container.parentElement;
          if (parent) {{
            const peers = recordLikePeerGroups.get(parent) || new Set();
            peers.add(container);
            recordLikePeerGroups.set(parent, peers);
          }}
          break;
        }}
        container = container.parentElement;
      }}
    }}
  }}
  const repeatedRecordLikePeers = Math.max(
    0,
    ...Array.from(recordLikePeerGroups.values()).map((peers) => peers.size),
  );
  const linkedSubheadings = Array.from(root.querySelectorAll(
    'h1 a[href], h2 a[href], h3 a[href], [itemprop="name"] a[href], [role="heading"] a[href]'
  ))
    .filter(visible);
  const namedRecordSignals = Array.from(root.querySelectorAll('[itemprop="name"]')).filter(visible);
  const unrelatedCandidateWithoutPrimaryScope = Boolean(
    primaryHeadings.length === 1
    && candidateRoots.length
    && !containingCandidate
    && !primaryContainer
  );
  if (
    (!primaryContainer && candidateRoots.length > 1)
    || linkedSubheadings.length > 1
    || namedRecordSignals.length > 1
    || repeatedRecordLikePeers > 1
    || unrelatedCandidateWithoutPrimaryScope
  ) {{
    return {{
      ok: false,
      reason: 'ambiguous_detail_scope',
      selector: rootSelector || null,
      candidate_count: Math.max(
        candidateRoots.length,
        linkedSubheadings.length,
        repeatedRecordLikePeers,
      ),
      page: {{url: location.href, title: document.title}},
    }};
  }}
  const firstVisible = (selectors) => {{
    for (const query of selectors) {{
      for (const element of Array.from(root.querySelectorAll(query))) {{
        if (!visible(element)) continue;
        const value = compact(element.getAttribute('content') || element.textContent, 800);
        if (value) return value;
      }}
    }}
    return '';
  }};
  const firstContent = (
    selectors,
    {{
      requirePaintedDom = false,
      preferCurrent = false,
      returnMatch = false,
      scope = root,
      candidateFilter = null,
    }} = {{}},
  ) => {{
    const candidates = [];
    const seen = new Set();
    for (const query of selectors) {{
      for (const element of Array.from(scope.querySelectorAll(query))) {{
        if (seen.has(element) || (candidateFilter && !candidateFilter(element))) continue;
        seen.add(element);
        candidates.push(element);
      }}
    }}
    const structuredStateRank = (element) => {{
      let hasExplicitState = false;
      let isSelected = false;
      if (element.hasAttribute('aria-current')) {{
        hasExplicitState = true;
        const value = element.getAttribute('aria-current')?.trim().toLocaleLowerCase() || '';
        if (value && value !== 'false') isSelected = true;
      }}
      if (element.hasAttribute('aria-selected')) {{
        hasExplicitState = true;
        if (element.getAttribute('aria-selected')?.trim().toLocaleLowerCase() === 'true') {{
          isSelected = true;
        }}
      }}
      for (const attribute of ['data-current', 'data-selected']) {{
        if (!element.hasAttribute(attribute)) continue;
        hasExplicitState = true;
        const value = element.getAttribute(attribute)?.trim().toLocaleLowerCase() || '';
        if (value !== 'false' && value !== '0') isSelected = true;
      }}
      if (isSelected) return 2;
      return hasExplicitState ? 0 : null;
    }};
    const inheritedStructuredStateRank = (element) => {{
      let current = element;
      while (current && scope.contains(current)) {{
        const rank = structuredStateRank(current);
        if (rank !== null) return rank;
        if (current === scope) break;
        current = current.parentElement;
      }}
      return 1;
    }};
    const orderedCandidates = preferCurrent
      ? candidates
          .map((element, index) => ({{
            element,
            index,
            rank: inheritedStructuredStateRank(element),
          }}))
          .sort((left, right) => right.rank - left.rank || left.index - right.index)
          .map((candidate) => candidate.element)
      : candidates;
    for (const element of orderedCandidates) {{
        const structuredMetadata = element.matches('meta, link');
        if (requirePaintedDom && !structuredMetadata) {{
          const value = paintedText(element, 1200);
          if (value) return returnMatch ? {{value, element}} : value;
          continue;
        }}
        const value = compact(
          element.getAttribute('content') ||
          (element.matches('link') ? element.getAttribute('href') : '') ||
          element.getAttribute('value') ||
          element.getAttribute('aria-label') ||
          element.textContent,
          1200,
        );
        if (value) return returnMatch ? {{value, element}} : value;
    }}
    return returnMatch ? null : '';
  }};
  const textNodeIsPainted = (node) => {{
    const parent = node && node.parentElement;
    if (!parent || !paintVisible(parent)) return false;
    const value = String(node.nodeValue || '');
    if (!value.trim()) return false;
    const style = window.getComputedStyle(parent);
    const shadow = String(style.textShadow || '').trim();
    const strokeWidth = Number.parseFloat(style.webkitTextStrokeWidth || '0');
    const shadowPaints = shadow !== 'none' && !paintValueIsFullyTransparent(shadow);
    const strokePaints = strokeWidth > 0
      && !paintValueIsFullyTransparent(style.webkitTextStrokeColor);
    const cssLayers = (value) => {{
      const layers = [];
      let depth = 0;
      let current = '';
      for (const character of String(value || '')) {{
        if (character === '(') depth += 1;
        if (character === ')') depth = Math.max(0, depth - 1);
        if (character === ',' && depth === 0) {{
          layers.push(current.trim());
          current = '';
          continue;
        }}
        current += character;
      }}
      if (current.trim() || !layers.length) layers.push(current.trim());
      return layers.filter(Boolean);
    }};
    const clipLayers = cssLayers(
      style.webkitBackgroundClip || style.backgroundClip
    );
    const cssTokens = (value) => {{
      const tokens = [];
      let depth = 0;
      let current = '';
      for (const character of String(value || '').trim()) {{
        if (/\s/u.test(character) && depth === 0) {{
          if (current) tokens.push(current);
          current = '';
          continue;
        }}
        current += character;
        if (character === '(') depth += 1;
        if (character === ')') depth = Math.max(0, depth - 1);
      }}
      if (current) tokens.push(current);
      return tokens;
    }};
    const numericCssValue = (token, basis, {{autoValue = basis}} = {{}}) => {{
      const normalized = String(token || '').trim().toLocaleLowerCase();
      if (normalized === 'auto') return autoValue;
      const calcMatch = normalized.match(
        /^calc\(\s*([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))%\s*([+-])\s*([0-9]+(?:\.[0-9]*)?|\.[0-9]+)px\s*\)$/u
      );
      if (calcMatch) {{
        return (
          basis * Number(calcMatch[1]) / 100
          + Number(calcMatch[3]) * (calcMatch[2] === '-' ? -1 : 1)
        );
      }}
      const match = normalized.match(
        /^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(px|%)?$/u
      );
      if (!match) return Number.NaN;
      const amount = Number(match[1]);
      if (!match[2] && amount !== 0) return Number.NaN;
      return match[2] === '%' ? basis * amount / 100 : amount;
    }};
    const backgroundImageSize = (value, rect) => {{
      const tokens = cssTokens(value || 'auto');
      if (!tokens.length) return null;
      if (tokens.length === 1 && /^(?:cover|contain)$/u.test(tokens[0])) {{
        return [rect.width, rect.height];
      }}
      const width = numericCssValue(tokens[0], rect.width);
      const height = numericCssValue(tokens[1] || 'auto', rect.height);
      if (![width, height].every(Number.isFinite)) return null;
      return [width, height];
    }};
    const positionComponent = (token, axis) => {{
      const normalized = String(token || '').trim().toLocaleLowerCase();
      if (normalized === 'center') return {{percent: 0.5, pixels: 0}};
      if (normalized === 'left' || normalized === 'top') {{
        return {{percent: 0, pixels: 0}};
      }}
      if (normalized === 'right' || normalized === 'bottom') {{
        return {{percent: 1, pixels: 0}};
      }}
      const calcMatch = normalized.match(
        /^calc\(\s*([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))%\s*([+-])\s*([0-9]+(?:\.[0-9]*)?|\.[0-9]+)px\s*\)$/u
      );
      if (calcMatch) {{
        return {{
          percent: Number(calcMatch[1]) / 100,
          pixels: Number(calcMatch[3]) * (calcMatch[2] === '-' ? -1 : 1),
        }};
      }}
      const match = normalized.match(
        /^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(px|%)?$/u
      );
      if (!match) return null;
      const amount = Number(match[1]);
      if (!match[2] && amount !== 0) return null;
      return match[2] === '%'
        ? {{percent: amount / 100, pixels: 0}}
        : {{percent: 0, pixels: amount}};
    }};
    const edgePositionComponent = (edge, distance, axis) => {{
      const anchor = positionComponent(edge, axis);
      const offset = numericCssValue(distance, 0, {{autoValue: Number.NaN}});
      if (!anchor || !Number.isFinite(offset)) return null;
      const reverse = edge === 'right' || edge === 'bottom';
      return {{percent: anchor.percent, pixels: reverse ? -offset : offset}};
    }};
    const backgroundPosition = (value) => {{
      const tokens = cssTokens(value || '0% 0%');
      if (
        tokens.length >= 4
        && /^(?:left|right)$/u.test(tokens[0])
        && /^(?:top|bottom)$/u.test(tokens[2])
      ) {{
        return [
          edgePositionComponent(tokens[0], tokens[1], 'x'),
          edgePositionComponent(tokens[2], tokens[3], 'y'),
        ];
      }}
      if (tokens.length === 1) {{
        if (/^(?:top|bottom)$/u.test(tokens[0])) {{
          return [positionComponent('center', 'x'), positionComponent(tokens[0], 'y')];
        }}
        return [positionComponent(tokens[0], 'x'), positionComponent('center', 'y')];
      }}
      return [
        positionComponent(tokens[0] || '0%', 'x'),
        positionComponent(tokens[1] || '0%', 'y'),
      ];
    }};
    const repeatedAxes = (value) => {{
      const tokens = cssTokens(value || 'repeat').map((token) => token.toLocaleLowerCase());
      if (tokens[0] === 'repeat-x') return [true, false];
      if (tokens[0] === 'repeat-y') return [false, true];
      const repeats = (token) => token !== 'no-repeat';
      return [repeats(tokens[0] || 'repeat'), repeats(tokens[1] || tokens[0] || 'repeat')];
    }};
    const backgroundLayerCanPaint = (size, position, repeat, rect) => {{
      const container = [rect.width, rect.height];
      const imageSize = backgroundImageSize(size, rect);
      const offsets = backgroundPosition(position);
      const repeats = repeatedAxes(repeat);
      if (
        !imageSize
        || offsets.some((offset) => !offset)
        || container.some((dimension) => !(dimension > 0))
      ) return false;
      return imageSize.every((dimension, axis) => {{
        if (!(dimension > 0)) return false;
        if (repeats[axis]) return true;
        const offset = (
          (container[axis] - dimension) * offsets[axis].percent
          + offsets[axis].pixels
        );
        return offset < container[axis] && offset + dimension > 0;
      }});
    }};
    const imageLayers = cssLayers(style.backgroundImage);
    const sizeLayers = cssLayers(style.backgroundSize || 'auto');
    const positionLayers = cssLayers(style.backgroundPosition || '0% 0%');
    const repeatLayers = cssLayers(style.backgroundRepeat || 'repeat');
    const parentRect = parent.getBoundingClientRect();
    const layerValue = (layers, index, fallback) => (
      layers.length ? layers[index % layers.length] : fallback
    );
    const backgroundImagePaints = imageLayers.some((image, index) => (
      layerValue(clipLayers, index, '').toLocaleLowerCase() === 'text'
      && !paintValueIsFullyTransparent(image)
      && backgroundLayerCanPaint(
        layerValue(sizeLayers, index, 'auto'),
        layerValue(positionLayers, index, '0% 0%'),
        layerValue(repeatLayers, index, 'repeat'),
        parentRect,
      )
    ));
    const colorClip = layerValue(
      clipLayers,
      Math.max(0, imageLayers.length - 1),
      '',
    ).toLocaleLowerCase();
    const backgroundPaints = backgroundImagePaints || (
      colorClip === 'text'
      && !paintValueIsFullyTransparent(style.backgroundColor)
    );
    const textFillIsTransparent = Boolean(
      style.webkitTextFillColor
      && paintAlphaIsZero(style.webkitTextFillColor)
    );
    if (
      (paintAlphaIsZero(style.color) || textFillIsTransparent)
      && !shadowPaints
      && !strokePaints
      && !backgroundPaints
    ) return false;
    const range = document.createRange();
    range.selectNodeContents(node);
    return Array.from(range.getClientRects()).some((rect) => (
      rect.width > 0
      && rect.height > 0
      && rect.right + window.scrollX > 0
      && rect.bottom + window.scrollY > 0
    ));
  }};
  const paintedText = (element, limit = 160) => {{
    const values = [];
    const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
    while (walker.nextNode()) {{
      if (textNodeIsPainted(walker.currentNode)) {{
        values.push(walker.currentNode.nodeValue || '');
      }}
    }}
    return compact(values.join(''), limit);
  }};
  const title = compact(
    titleElement && root.contains(titleElement)
      ? (titleElement.getAttribute('content') || paintedText(titleElement, 800))
      : '',
    800,
  );
  const priceValue = (element) => {{
    const accessibleValue = element.getAttribute('aria-label');
    return compact(
      element.getAttribute('content')
      || element.getAttribute('data-price')
      || element.getAttribute('value')
      || (
        accessibleValue
        && accessiblePriceHasPaintedCorroboration(element, accessibleValue)
          ? accessibleValue
          : ''
      )
      || paintedText(element, 160),
      160,
    );
  }};
  const amountPattern = '\\\\p{{Nd}}(?:[\\\\p{{Nd}}\\\\s.,\\'’٬٫]*\\\\p{{Nd}})?';
  const priceDecimalDigitMap = new Map(
    Array.from({{length: 10}}, (_unused, digit) => [String(digit), String(digit)])
  );
  if (typeof Intl.supportedValuesOf === 'function') {{
    for (const numberingSystem of Intl.supportedValuesOf('numberingSystem')) {{
      try {{
        const formatter = new Intl.NumberFormat('en', {{
          numberingSystem,
          useGrouping: false,
          maximumFractionDigits: 0,
        }});
        for (let digit = 0; digit <= 9; digit += 1) {{
          const rendered = formatter.format(digit).normalize('NFKC');
          const digits = Array.from(rendered).filter((character) => /\\p{{Nd}}/u.test(character));
          if (digits.length === 1) priceDecimalDigitMap.set(digits[0], String(digit));
        }}
      }} catch (_error) {{
        // Ignore numbering systems that this browser cannot format.
      }}
    }}
  }}
  const canonicalPriceDigits = (value) => Array.from(String(value || ''))
    .map((character) => priceDecimalDigitMap.get(character) || character)
    .join('');
  const supportedCurrencyCodes = new Set(
    typeof Intl.supportedValuesOf === 'function'
      ? Intl.supportedValuesOf('currency').map((value) => String(value).toUpperCase())
      : []
  );
  const validLocaleTag = (value) => {{
    const locale = String(value || '').trim();
    if (!locale) return '';
    try {{
      return new Intl.Locale(locale).toString();
    }} catch (_error) {{
      return '';
    }}
  }};
  const declaredPageLocale = validLocaleTag(
    document.documentElement && document.documentElement.lang
  );
  const pageLocales = declaredPageLocale
    ? [declaredPageLocale]
    : Array.from(new Set([
      ...(Array.isArray(navigator.languages) ? navigator.languages : []),
      navigator.language,
    ].map(validLocaleTag).filter(Boolean)));
  const pageRegionCurrencyCodes = (() => {{
    const codes = new Set();
    for (const locale of pageLocales) {{
      try {{
        const region = new Intl.Locale(locale).maximize().region;
        if (!region) continue;
        for (const code of supportedCurrencyCodes) {{
          if (code.startsWith(region)) codes.add(code);
        }}
      }} catch (_error) {{
        // Ignore invalid or unsupported page locale tags.
      }}
    }}
    return codes;
  }})();
  const canonicalCurrencyMarker = (value) => String(value || '')
    .normalize('NFKC')
    .toLocaleLowerCase()
    .replace(/[\\s\\p{{Cf}}\\p{{P}}]+/gu, '')
    .trim();
  const currencyLocales = (code) => {{
    const locales = new Set(pageLocales);
    try {{
      locales.add(
        new Intl.Locale('und-' + String(code || '').slice(0, 2))
          .maximize()
          .toString()
      );
    }} catch (_error) {{
      // Some non-geographic currency codes do not imply a usable locale.
    }}
    if (!locales.size) locales.add(undefined);
    return locales;
  }};
  const currencyMarkerOwners = (() => {{
    const owners = new Map();
    const add = (value, code) => {{
      const marker = canonicalCurrencyMarker(value);
      if (!marker) return;
      if (!owners.has(marker)) owners.set(marker, new Set());
      owners.get(marker).add(code);
    }};
    for (const code of supportedCurrencyCodes) {{
      add(code, code);
      for (const locale of currencyLocales(code)) {{
        for (const currencyDisplay of ['symbol', 'narrowSymbol', 'name']) {{
          try {{
            const part = new Intl.NumberFormat(locale, {{
              style: 'currency', currency: code, currencyDisplay,
            }}).formatToParts(1).find((candidate) => candidate.type === 'currency');
            if (!part || !part.value) continue;
            add(part.value, code);
            if (/\\p{{Sc}}/u.test(part.value)) {{
              add(code.slice(0, 2) + part.value, code);
            }}
            const letterRuns = part.value.match(/\\p{{L}}+/gu) || [];
            const terminalRun = letterRuns[letterRuns.length - 1] || '';
            const characters = Array.from(terminalRun);
            if (characters.some((character) => character.codePointAt(0) > 127)) {{
              for (let length = 3; length <= Math.min(4, characters.length); length += 1) {{
                add(characters.slice(0, length).join(''), code);
              }}
            }}
            if (/[\\p{{Script=Han}}\\p{{Script=Hangul}}]/u.test(terminalRun)) {{
              add(terminalRun, code);
            }}
            if (/\\p{{Script=Han}}/u.test(terminalRun) && characters.length > 1) {{
              add(characters[characters.length - 1], code);
            }}
          }} catch (_error) {{
            // Ignore unsupported locale/currency display combinations.
          }}
        }}
      }}
    }}
    return owners;
  }})();
  const localizedCurrencyMarker = (value) => (
    currencyMarkerOwners.has(canonicalCurrencyMarker(value))
  );
  const validCurrencyMarker = (value) => {{
    const marker = String(value || '').trim();
    if (!marker) return false;
    if (/^(?:[A-Z]{{3}})$/u.test(marker)) {{
      return supportedCurrencyCodes.has(marker.toUpperCase());
    }}
    if (/^[A-Za-z]$/u.test(marker)) return false;
    return localizedCurrencyMarker(marker);
  }};
  const normalizedCompletePriceValue = (
    value,
    structured = false,
    strongVisual = false,
  ) => {{
    const normalized = String(value || '').normalize('NFKC').trim();
    if (!/\\p{{Nd}}/u.test(normalized)) return '';
    const amountOnly = new RegExp('^(?:' + amountPattern + ')$', 'u');
    if (amountOnly.test(normalized)) {{
      if (structured) return normalized;
      return (
        strongVisual
        && new RegExp('[.,٫]\\\\p{{Nd}}{{1,4}}$', 'u').test(normalized)
      ) ? normalized : '';
    }}
    const prefixed = normalized.match(
      new RegExp('^(.+?)\\\\s*(' + amountPattern + ')$', 'u')
    );
    if (prefixed && validCurrencyMarker(prefixed[1])) {{
      const marker = prefixed[1].trim();
      if (/\\p{{Sc}}/u.test(marker)) {{
        return marker + prefixed[2].replace(/\\s*([.,٫])\\s*/gu, '$1');
      }}
      return normalized;
    }}
    const suffixed = normalized.match(
      new RegExp('^(' + amountPattern + ')\\\\s*(.+?)$', 'u')
    );
    if (suffixed && validCurrencyMarker(suffixed[2])) return normalized;

    const markerCandidates = (segment, fromEnd) => {{
      const characters = Array.from(segment);
      const candidates = [];
      for (let index = 0; index < characters.length; index += 1) {{
        const rawMarker = (
          fromEnd
            ? characters.slice(index).join('')
            : characters.slice(0, characters.length - index).join('')
        );
        const marker = rawMarker.trim();
        const boundaryCharacter = fromEnd
          ? characters[index - 1]
          : characters[characters.length - index];
        const asciiLetterMarker = /[A-Za-z]/u.test(marker);
        if (
          marker
          && validCurrencyMarker(marker)
          && !(
            asciiLetterMarker
            && boundaryCharacter
            && /[\\p{{L}}\\p{{N}}]/u.test(boundaryCharacter)
          )
        ) {{
          candidates.push({{
            marker,
            consumedLength: Array.from(rawMarker).length,
          }});
        }}
      }}
      candidates.sort(
        (left, right) => (
          Array.from(right.marker).length - Array.from(left.marker).length
        )
      );
      return candidates;
    }};
    const hasSubstantiveRemainder = (value) => /[\\p{{L}}\\p{{N}}\\p{{Sc}}]/u.test(
      String(value || '')
    );
    const amountMatches = Array.from(
      normalized.matchAll(new RegExp(amountPattern, 'gu'))
    );
    const monetaryTokens = [];
    for (const match of amountMatches) {{
      const amount = match[0];
      const start = Number(match.index || 0);
      const end = start + amount.length;
      const beforeMarkers = markerCandidates(normalized.slice(0, start), true);
      if (beforeMarkers.length) {{
        monetaryTokens.push({{
          marker: beforeMarkers[0].marker,
          markerBefore: true,
          start: start - beforeMarkers[0].consumedLength,
          end,
          amount,
        }});
      }}
      const afterText = normalized.slice(end);
      const afterMarkers = markerCandidates(afterText, false);
      if (afterMarkers.length) {{
        monetaryTokens.push({{
          marker: afterMarkers[0].marker,
          markerBefore: false,
          start,
          end: end + afterMarkers[0].consumedLength,
          amount,
        }});
      }}
    }}
    const uniqueTokens = monetaryTokens.filter((token, index, tokens) => (
      tokens.findIndex((candidate) => (
        candidate.start === token.start
        && candidate.end === token.end
        && canonicalCurrencyMarker(candidate.marker) === canonicalCurrencyMarker(token.marker)
      )) === index
    ));
    if (uniqueTokens.length !== 1) return '';
    const token = uniqueTokens[0];
    const beforeRemainder = normalized.slice(0, Math.max(0, token.start));
    const afterRemainder = normalized.slice(token.end);
    if (hasSubstantiveRemainder(afterRemainder)) return '';
    const strongVisualLeadingLabel = (
      strongVisual
      && !/\\p{{Nd}}/u.test(beforeRemainder)
    );
    if (
      hasSubstantiveRemainder(beforeRemainder)
      && !/\\p{{L}}/u.test(token.marker)
      // An exact price field may include a nonnumeric label before one terminal amount.
      && !strongVisualLeadingLabel
    ) return '';
    return normalized.slice(Math.max(0, token.start), token.end).trim();
  }};
  const completePriceValue = (value, structured = false, strongVisual = false) => Boolean(
    normalizedCompletePriceValue(value, structured, strongVisual)
  );
  const accessiblePriceHasPaintedCorroboration = (element, value) => {{
    const normalized = normalizedCompletePriceValue(value, false, false);
    const rendered = paintedText(element, 160);
    if (!normalized || !rendered) return false;
    const normalizedDigits = canonicalPriceDigits(normalized).replace(/[^0-9]/g, '');
    const renderedDigits = canonicalPriceDigits(rendered).replace(/[^0-9]/g, '');
    if (!normalizedDigits || normalizedDigits !== renderedDigits) return false;
    const marker = canonicalCurrencyMarker(
      canonicalPriceDigits(normalized).replace(/[0-9\\s.,'’٬٫]/g, '')
    );
    const renderedMarker = canonicalCurrencyMarker(
      canonicalPriceDigits(rendered).replace(/[0-9\\s.,'’٬٫]/g, '')
    );
    return Boolean(marker && renderedMarker.includes(marker));
  }};
  const accessiblePriceHasPaintedMonetaryContext = (element) => Boolean(
    normalizedCompletePriceValue(paintedText(element, 160), false, true)
  );
  const semanticallyExposed = (element, boundary) => {{
    let current = element;
    while (current && current !== boundary) {{
      if (current.getAttribute && current.getAttribute('aria-hidden') === 'true') return false;
      const style = window.getComputedStyle(current);
      if (
        style.visibility === 'hidden'
        || style.display === 'none'
        || style.contentVisibility === 'hidden'
        || Number.parseFloat(style.opacity) === 0
        || styleSuppressesPaint(current, style)
      ) return false;
      if (
        typeof current.checkVisibility === 'function'
        && !current.checkVisibility({{
          checkOpacity: true,
          checkVisibilityCSS: true,
          contentVisibilityAuto: true,
        }})
      ) return false;
      current = current.parentElement;
    }}
    return true;
  }};
  const matchedPriceElements = Array.from(root.querySelectorAll(priceSelectors));
  const allPriceElements = [
    ...matchedPriceElements.filter((element) => element.matches('[itemprop~="price"], [data-price]')).slice(0, 80),
    ...matchedPriceElements.filter((element) => !element.matches('[itemprop~="price"], [data-price]')).slice(0, 160),
  ].filter((element, index, elements) => elements.indexOf(element) === index);
  const isAttributePrice = (element) => element.matches(
    '[itemprop~="price"], [data-price]'
  );
  const isExactVisualPrice = (element) => Boolean(
    Array.from(element.classList || []).some((name) => name.toLocaleLowerCase() === 'price')
    || String(element.id || '').toLocaleLowerCase() === 'price'
  );
  const isStrongVisualPrice = (element) => element.matches(priceSelectors);
  const schemaAwarePriceValue = (element, explicitValue = null) => {{
    const value = explicitValue === null ? priceValue(element) : compact(explicitValue, 160);
    if (!element.matches('[itemprop~="price"]')) return value;
    const owner = element.closest('[itemscope]') || element.parentElement;
    if (!owner || !root.contains(owner)) return value;
    const currencyElement = Array.from(
      owner.querySelectorAll('[itemprop~="priceCurrency"]')
    ).find((candidate) => candidate.closest('[itemscope]') === owner);
    if (!currencyElement) return value;
    const currency = compact(
      currencyElement.getAttribute('content')
      || currencyElement.getAttribute('value')
      || currencyElement.textContent,
      16,
    ).toUpperCase();
    if (!supportedCurrencyCodes.has(currency)) return value;
    if (!new RegExp('^(?:' + amountPattern + ')$', 'u').test(value)) return value;
    return currency + ' ' + value;
  }};
  const ambiguousSplitAmount = (element, value) => {{
    if (/[.,٫]\\s*\\p{{Nd}}{{1,4}}(?:\\D*)$/u.test(String(value || ''))) return false;
    const directNumericTextParts = Array.from(element.childNodes || []).filter(
      (candidate) => (
        candidate.nodeType === Node.TEXT_NODE
        && /\\p{{Nd}}/u.test(String(candidate.nodeValue || ''))
      )
    );
    const numericLeaves = Array.from(element.querySelectorAll('*')).filter((candidate) => {{
      if (!semanticallyExposed(candidate, element)) return false;
      if (!/^\\p{{Nd}}+$/u.test(String(candidate.textContent || '').trim())) return false;
      return !Array.from(candidate.children).some(
        (child) => /\\p{{Nd}}/u.test(String(child.textContent || ''))
      );
    }});
    return directNumericTextParts.length + numericLeaves.length >= 2;
  }};
  const rawPriceSources = [];
  const addRawPriceSource = (element, value, source, structured, rendered) => {{
    const strictNormalizedValue = normalizedCompletePriceValue(
      value,
      structured,
      false,
    );
    const labeledVisual = Boolean(
      !strictNormalizedValue
      && !structured
      && isStrongVisualPrice(element)
    );
    const normalizedValue = strictNormalizedValue || (
      labeledVisual
        ? normalizedCompletePriceValue(value, false, true)
        : ''
    );
    if (!normalizedValue) return;
    if (labeledVisual && !isExactVisualPrice(element)) {{
      const marker = canonicalCurrencyMarker(
        canonicalPriceDigits(normalizedValue).replace(/[0-9\\s.,'’٬٫]/g, '')
      );
      if (!marker || !validCurrencyMarker(marker)) return;
    }}
    if (rendered && ambiguousSplitAmount(element, normalizedValue)) return;
    if (rawPriceSources.some(
      (candidate) => candidate.element === element && candidate.value === normalizedValue
    )) return;
    rawPriceSources.push({{
      element,
      value: normalizedValue,
      source,
      structured,
      labeledVisual,
    }});
  }};
  for (const element of allPriceElements.filter(isAttributePrice)) {{
    if (element.tagName !== 'META' && !paintVisible(element)) continue;
    addRawPriceSource(
      element,
      schemaAwarePriceValue(element),
      element.matches('[itemprop~="price"]') ? 'schema_property' : 'dom_attribute',
      true,
      false,
    );
  }}
  const visualPriceRoots = allPriceElements
    .filter((element) => !isAttributePrice(element))
    .filter((element, _index, candidates) =>
      !candidates.some((candidate) => candidate !== element && candidate.contains(element))
  );
  for (const element of visualPriceRoots) {{
    if (!paintVisible(element)) continue;
    const atomicElements = [element, ...Array.from(element.querySelectorAll('*'))]
      .filter((candidate) => semanticallyExposed(candidate, root))
      .map((candidate) => ({{
        element: candidate,
        value: schemaAwarePriceValue(candidate),
        structured: isAttributePrice(candidate),
      }}))
      .filter((candidate) => completePriceValue(
        candidate.value,
        candidate.structured,
        isStrongVisualPrice(candidate.element),
      ) && !ambiguousSplitAmount(candidate.element, candidate.value));
    const deepestAtomicElements = atomicElements.filter(
      (candidate) => !atomicElements.some(
        (other) => other !== candidate && candidate.element.contains(other.element)
      )
    );
    const sources = deepestAtomicElements.length
      ? deepestAtomicElements
      : [{{element, value: priceValue(element), structured: false}}];
    for (const source of sources) {{
      addRawPriceSource(
        source.element,
        source.value,
        source.structured ? 'dom_attribute' : 'rendered_dom',
        source.structured,
        !source.structured,
      );
    }}
  }}
  for (const element of allPriceElements) {{
    if (element.tagName === 'META' || !paintVisible(element)) continue;
    const renderedValue = paintedText(element, 160);
    if (renderedValue) {{
      addRawPriceSource(element, renderedValue, 'rendered_dom', false, true);
    }}
    const ariaValue = compact(element.getAttribute('aria-label'), 160);
    if (ariaValue && accessiblePriceHasPaintedMonetaryContext(element)) {{
      addRawPriceSource(element, ariaValue, 'accessibility_attribute', false, false);
    }}
    const dataValue = compact(element.getAttribute('data-price'), 160);
    if (dataValue) {{
      addRawPriceSource(element, dataValue, 'dom_attribute', true, false);
    }}
  }}
  const schemaTypeName = (element) => {{
    const types = String(element.getAttribute('itemtype') || '').split(/\s+/).filter(Boolean);
    for (const type of types) {{
      const name = String(type.split(/[\/#]/).filter(Boolean).pop() || '').toLowerCase();
      if (name === 'offer' || name === 'product') return name;
    }}
    return '';
  }};
  const schemaPriceRole = (element) => {{
    if (!element.matches('[itemprop~="price"]')) return '';
    let current = element;
    while (current && root.contains(current)) {{
      const typeName = schemaTypeName(current);
      if (typeName === 'offer') return 'schema_offer_price';
      if (typeName === 'product') return 'schema_product_price';
      if (current !== element && current.matches('[itemprop~="offers"]')) {{
        return 'schema_offer_price';
      }}
      if (current === root) break;
      current = current.parentElement;
    }}
    return '';
  }};
  const selectedProductOwner = (() => {{
    let current = titleElement;
    while (current && root.contains(current)) {{
      if (schemaTypeName(current) === 'product') return current;
      if (current === root) break;
      current = current.parentElement;
    }}
    return null;
  }})();
  const directSelectionState = (element) => {{
    if (element.tagName === 'LABEL' && element.control) {{
      const delegated = directSelectionState(element.control);
      return delegated ? {{...delegated, group: element}} : null;
    }}
    const role = String(element.getAttribute('role') || '').toLocaleLowerCase();
    const nativeRadio = element.matches('input[type="radio"]');
    const nativeOption = element.matches('option');
    const offerControl = nativeRadio || nativeOption || role === 'radio' || role === 'option';
    if (!offerControl) return null;
    let state = '';
    for (const attribute of ['aria-selected', 'aria-checked']) {{
      const value = element.getAttribute(attribute);
      if (value === 'true') state = 'selected';
      if (value === 'false') state = 'unselected';
      if (state) break;
    }}
    if (!state && nativeRadio) state = element.checked ? 'selected' : 'unselected';
    if (!state && nativeOption) state = element.selected ? 'selected' : 'unselected';
    return state ? {{state, group: element, control: element}} : null;
  }};
  const offerControlHasPeers = (control) => {{
    if (!control) return false;
    if (control.matches('option')) {{
      return Boolean(control.closest('select')?.querySelectorAll('option').length >= 2);
    }}
    if (control.matches('input[type="radio"]')) {{
      const name = String(control.getAttribute('name') || '');
      if (name) {{
        const peers = Array.from(root.querySelectorAll('input[type="radio"]'))
          .filter((candidate) => String(candidate.getAttribute('name') || '') === name);
        if (peers.length >= 2) return true;
      }}
    }}
    let current = control.parentElement;
    let depth = 0;
    while (current && root.contains(current) && current !== root && depth < 4) {{
      const peers = current.querySelectorAll(
        'input[type="radio"], option, [role="radio"], [role="option"]'
      );
      if (peers.length >= 2) return true;
      current = current.parentElement;
      depth += 1;
    }}
    return false;
  }};
  const nearestLocalControl = (element, selector) => {{
    let current = element.parentElement;
    let depth = 0;
    while (current && root.contains(current) && current !== root && depth < 4) {{
      if (current.contains(titleElement)) break;
      const controls = Array.from(current.querySelectorAll(selector));
      const localPrices = Array.from(current.querySelectorAll(priceSelectors));
      if (controls.length === 1 && localPrices.length === 1) {{
        return {{control: controls[0], group: current}};
      }}
      current = current.parentElement;
      depth += 1;
    }}
    return null;
  }};
  const selectionContext = (element) => {{
    let current = element;
    while (current && root.contains(current)) {{
      const directState = directSelectionState(current);
      if (directState && offerControlHasPeers(directState.control)) return directState;
      if (current === root) break;
      current = current.parentElement;
    }}
    const local = nearestLocalControl(
      element,
      'input[type="radio"], option, [role="radio"], [role="option"]',
    );
    if (local) {{
      const state = directSelectionState(local.control);
      if (state && offerControlHasPeers(state.control)) return {{...state, group: local.group}};
    }}
    return {{state: '', group: null}};
  }};
  const nonOfferControlContext = (element) => {{
    let current = element;
    while (current && root.contains(current)) {{
      const role = String(current.getAttribute('role') || '').toLocaleLowerCase();
      if (
        role === 'checkbox'
        || role === 'switch'
        || current.matches('input[type="checkbox"]')
      ) return true;
      if (
        (role === 'radio' || role === 'option' || current.matches('input[type="radio"], option'))
        && !offerControlHasPeers(current)
      ) return true;
      if (
        current.tagName === 'LABEL'
        && current.control
        && current.control.matches('input[type="checkbox"]')
      ) return true;
      if (
        current.tagName === 'LABEL'
        && current.control
        && current.control.matches('input[type="radio"]')
        && !offerControlHasPeers(current.control)
      ) return true;
      if (current === root) break;
      current = current.parentElement;
    }}
    let local = element.parentElement;
    let depth = 0;
    while (local && root.contains(local) && local !== root && depth < 4) {{
      if (local.contains(titleElement)) break;
      const localCheckboxes = Array.from(
        local.querySelectorAll('input[type="checkbox"], [role="checkbox"], [role="switch"]')
      );
      const localPrices = Array.from(local.querySelectorAll(priceSelectors));
      const hasDirectCheckbox = localCheckboxes.some(
        (candidate) => candidate.parentElement === local
      );
      if (
        localCheckboxes.length === 1
        && localPrices.length >= 1
        && (hasDirectCheckbox || localPrices.length === 1)
      ) return true;
      local = local.parentElement;
      depth += 1;
    }}
    const localRadio = nearestLocalControl(
      element,
      'input[type="radio"], option, [role="radio"], [role="option"]',
    );
    return Boolean(
      localRadio
      && !offerControlHasPeers(localRadio.control)
    );
  }};
  const distinctRecordIsland = (element) => {{
    let current = element;
    while (current && root.contains(current)) {{
      const isBoundary = current.matches(
        'article, li, [role="article"], [role="listitem"], [itemprop~="itemListElement"], [itemscope]'
      );
      const schemaType = schemaTypeName(current);
      const offerRelation = current.closest('[itemprop~="offers"]');
      const isOwnedOffer = Boolean(
        selectedProductOwner
        && offerRelation
        && selectedProductOwner.contains(offerRelation)
        && (
          schemaType === 'offer'
          || current.matches('[itemprop~="offers"]')
        )
      );
      if (isBoundary && !current.contains(titleElement)) {{
        if (isOwnedOffer) {{
          current = current.parentElement;
          continue;
        }}
        const recordHeading = current.querySelector(
          'h1, h2, h3, [role="heading"], [itemprop~="name"]'
        );
        const selection = selectionContext(element);
        const isSelectableOfferBoundary = Boolean(
          selection.group
          && current.contains(selection.group)
          && !current.querySelector('a[href]')
        );
        if (isSelectableOfferBoundary) {{
          current = current.parentElement;
          continue;
        }}
        const ownRecordSignal = Boolean(
          schemaType === 'offer'
          || current.matches('[itemprop~="itemListElement"]')
          || recordHeading
          || current.querySelector('a[href]')
        );
        if (ownRecordSignal) return true;
      }}
      if (current === root) break;
      current = current.parentElement;
    }}
    return false;
  }};
  const struckOrListValue = (element) => {{
    let current = element;
    while (current && root.contains(current)) {{
      if (current.matches('s, del, [itemprop~="highPrice"], [itemprop~="lowPrice"]')) return true;
      const style = window.getComputedStyle(current);
      if (String(style.textDecorationLine || style.textDecoration || '').includes('line-through')) {{
        return true;
      }}
      if (current === root) break;
      current = current.parentElement;
    }}
    return false;
  }};
  const visualProminence = (element) => {{
    if (element.tagName === 'META' || !paintVisible(element)) return null;
    const visibleParts = [element, ...Array.from(element.querySelectorAll('*'))]
      .filter((candidate) => paintVisible(candidate) && semanticallyExposed(candidate, root));
    let prominentPart = element;
    let prominentScore = 0;
    for (const candidate of visibleParts) {{
      const candidateStyle = window.getComputedStyle(candidate);
      const candidateSize = Number.parseFloat(candidateStyle.fontSize) || 0;
      const candidateParsedWeight = Number.parseFloat(candidateStyle.fontWeight);
      const candidateWeight = Number.isFinite(candidateParsedWeight)
        ? candidateParsedWeight
        : (candidateStyle.fontWeight === 'bold' ? 700 : 400);
      const candidateScore = candidateSize
        * (0.75 + Math.min(900, Math.max(100, candidateWeight)) / 1600);
      if (candidateScore > prominentScore) {{
        prominentPart = candidate;
        prominentScore = candidateScore;
      }}
    }}
    const style = window.getComputedStyle(prominentPart);
    const rect = element.getBoundingClientRect();
    const fontSize = Number.parseFloat(style.fontSize) || 0;
    const parsedWeight = Number.parseFloat(style.fontWeight);
    const fontWeight = Number.isFinite(parsedWeight)
      ? parsedWeight
      : (style.fontWeight === 'bold' ? 700 : 400);
    return {{
      font_size_px: Math.round(fontSize * 100) / 100,
      font_weight: Math.round(fontWeight),
      area_px2: Math.round(Math.max(0, rect.width * rect.height)),
      score: prominentScore,
    }};
  }};
  const priceObservations = rawPriceSources.slice(0, 40).map((source) => {{
    const selection = selectionContext(source.element);
    const schemaRole = schemaPriceRole(source.element);
    const role = schemaRole
      || (selection.state === 'selected' ? 'selected_offer_price' : 'primary_record_price');
    const reasons = [];
    if (distinctRecordIsland(source.element)) reasons.push('distinct_record_island');
    if (struckOrListValue(source.element)) reasons.push('struck_or_list_value');
    if (selection.state === 'unselected') reasons.push('explicitly_unselected_offer');
    if (nonOfferControlContext(source.element)) reasons.push('non_offer_control_price');
    return {{
      ...source,
      role,
      roleRank: schemaRole ? 3 : (selection.state === 'selected' ? 2 : 1),
      selectionState: selection.state,
      selectionGroup: selection.group,
      prominence: visualProminence(source.element),
      reasons,
      eligible: reasons.length === 0,
    }};
  }});
  const nearestCommonAncestor = (left, right) => {{
    const ancestors = new Set();
    let current = left;
    while (current && root.contains(current)) {{
      ancestors.add(current);
      if (current === root) break;
      current = current.parentElement;
    }}
    current = right;
    while (current && root.contains(current)) {{
      if (ancestors.has(current)) return current;
      if (current === root) break;
      current = current.parentElement;
    }}
    return null;
  }};
  const distanceToAncestor = (element, ancestor) => {{
    let distance = 0;
    let current = element;
    while (current && current !== ancestor) {{
      distance += 1;
      current = current.parentElement;
    }}
    return current === ancestor ? distance : Number.POSITIVE_INFINITY;
  }};
  const sameLocalPriceContext = (left, right) => {{
    if (left.selectionGroup || right.selectionGroup) {{
      return Boolean(
        left.selectionGroup
        && right.selectionGroup
        && left.selectionGroup === right.selectionGroup
      );
    }}
    const ancestor = nearestCommonAncestor(left.element, right.element);
    if (!ancestor) return false;
    return distanceToAncestor(left.element, ancestor) + distanceToAncestor(right.element, ancestor) <= 6;
  }};
  const samePriceEvidenceContext = (left, right) => {{
    if (sameLocalPriceContext(left, right)) return true;
    return Boolean(
      (
        left.roleRank === 3
        && right.role === 'primary_record_price'
        && !right.selectionGroup
      )
      || (
        right.roleRank === 3
        && left.role === 'primary_record_price'
        && !left.selectionGroup
      )
    );
  }};
  const priceAmountIdentity = (value) => {{
    let numeric = (
      canonicalPriceDigits(String(value || '').normalize('NFKC'))
        .match(/[0-9]+(?:[.,'’٬٫][0-9]+)*/g) || []
    ).join('').replace(/[\\s'’٬]/g, '').replace(/٫/g, '.');
    if (!numeric) return '';
    const lastDot = numeric.lastIndexOf('.');
    const lastComma = numeric.lastIndexOf(',');
    let decimalIndex = -1;
    if (lastDot >= 0 && lastComma >= 0) {{
      decimalIndex = Math.max(lastDot, lastComma);
    }} else {{
      const separatorIndex = Math.max(lastDot, lastComma);
      if (separatorIndex >= 0) {{
        const trailingDigits = numeric.length - separatorIndex - 1;
        const separatorCount = (numeric.match(/[.,]/g) || []).length;
        if (separatorCount === 1 && trailingDigits === 3) {{
          return 'ambiguous:' + numeric;
        }}
        if (trailingDigits > 0 && trailingDigits !== 3) decimalIndex = separatorIndex;
      }}
    }}
    let whole;
    let fraction = '';
    if (decimalIndex >= 0) {{
      whole = numeric.slice(0, decimalIndex).replace(/[.,]/g, '');
      fraction = numeric.slice(decimalIndex + 1).replace(/[.,]/g, '').replace(/0+$/, '');
    }} else {{
      whole = numeric.replace(/[.,]/g, '');
    }}
    whole = whole.replace(/^0+/, '') || '0';
    return fraction ? whole + '.' + fraction : whole;
  }};
  const priceMarkerIdentity = (value) => canonicalCurrencyMarker(
    canonicalPriceDigits(String(value || '').normalize('NFKC'))
      .replace(/[0-9\\s.,'’٬٫]/g, '')
  );
  const priceMarkerOwners = (value) => {{
    const marker = priceMarkerIdentity(value);
    return marker ? (currencyMarkerOwners.get(marker) || new Set()) : new Set();
  }};
  const priceEvidenceConflicts = (leftValue, rightValue) => {{
    const leftAmount = priceAmountIdentity(leftValue);
    const rightAmount = priceAmountIdentity(rightValue);
    if (!leftAmount || !rightAmount) return false;
    if (leftAmount !== rightAmount) return true;
    const leftMarker = priceMarkerIdentity(leftValue);
    const rightMarker = priceMarkerIdentity(rightValue);
    if (!leftMarker || !rightMarker) return false;
    const leftOwners = priceMarkerOwners(leftValue);
    const rightOwners = priceMarkerOwners(rightValue);
    if (leftOwners.size && rightOwners.size) {{
      const ambiguousLocalizedMarker = (marker, owners) => Boolean(
        marker
        && /\\p{{L}}/u.test(marker)
        && !/\\p{{Sc}}/u.test(marker)
        && owners.size > 1
      );
      const leftAmbiguous = ambiguousLocalizedMarker(leftMarker, leftOwners);
      const rightAmbiguous = ambiguousLocalizedMarker(rightMarker, rightOwners);
      if (
        [...leftOwners].some((code) => rightOwners.has(code))
        && !leftAmbiguous
        && !rightAmbiguous
      ) return false;
      const ambiguousRegionalMarker = (marker, owners, otherOwners) => Boolean(
        ambiguousLocalizedMarker(marker, owners)
        && [...otherOwners].some((code) => pageRegionCurrencyCodes.has(code))
      );
      if (
        ambiguousRegionalMarker(leftMarker, leftOwners, rightOwners)
        || ambiguousRegionalMarker(rightMarker, rightOwners, leftOwners)
      ) return false;
      return true;
    }}
    return leftMarker !== rightMarker;
  }};
  for (const candidate of priceObservations) {{
    if (!candidate.labeledVisual) continue;
    const hasIndependentTypedOwnerPrice = priceObservations.some((other) => (
      other !== candidate
      && other.structured
      && !other.labeledVisual
      && other.eligible
      && samePriceEvidenceContext(candidate, other)
    ));
    if (!hasIndependentTypedOwnerPrice) {{
      candidate.reasons.push('uncorroborated_labeled_visual_price');
      candidate.eligible = false;
    }}
  }}
  let hasPriceConflict = false;
  for (const selected of priceObservations) {{
    if (
      !selected.eligible
      || selected.role !== 'selected_offer_price'
      || !selected.selectionGroup
    ) continue;
    for (const primary of priceObservations) {{
      if (
        !primary.eligible
        || primary.role !== 'primary_record_price'
        || primary.selectionGroup
      ) continue;
      const ambiguityRank = Math.max(selected.roleRank, primary.roleRank);
      selected.roleRank = ambiguityRank;
      primary.roleRank = ambiguityRank;
      selected.priceConflict = true;
      primary.priceConflict = true;
      hasPriceConflict = true;
    }}
  }}
  for (let leftIndex = 0; leftIndex < priceObservations.length; leftIndex += 1) {{
    const left = priceObservations[leftIndex];
    if (!left.eligible) continue;
    for (let rightIndex = leftIndex + 1; rightIndex < priceObservations.length; rightIndex += 1) {{
      const right = priceObservations[rightIndex];
      if (!right.eligible || !samePriceEvidenceContext(left, right)) continue;
      const evidenceChannelsConflict = left.element === right.element
        || left.roleRank === 3
        || right.roleRank === 3;
      if (!evidenceChannelsConflict) continue;
      if (!priceEvidenceConflicts(left.value, right.value)) continue;
      const conflictRank = Math.max(left.roleRank, right.roleRank);
      left.roleRank = conflictRank;
      right.roleRank = conflictRank;
      left.priceConflict = true;
      right.priceConflict = true;
      hasPriceConflict = true;
    }}
  }}
  const preliminaryStrongestPriceRole = Math.max(
    0,
    ...priceObservations
      .filter((candidate) => candidate.eligible && !candidate.priceConflict)
      .map((candidate) => candidate.roleRank),
  );
  const preliminaryLeaders = priceObservations.filter(
    (candidate) => (
      candidate.eligible
      && !candidate.priceConflict
      && candidate.roleRank === preliminaryStrongestPriceRole
    )
  );
  const preliminaryLeaderValues = Array.from(new Set(
    preliminaryLeaders.map((candidate) => candidate.value)
  ));
  if (preliminaryLeaderValues.length === 1) {{
    const leaderValue = preliminaryLeaderValues[0];
    const compatibleAliases = priceObservations.filter((candidate) => (
      candidate.eligible
      && !candidate.priceConflict
      && candidate.value !== leaderValue
      && !priceEvidenceConflicts(leaderValue, candidate.value)
    ));
    let aliasesConflict = false;
    for (let leftIndex = 0; leftIndex < compatibleAliases.length; leftIndex += 1) {{
      for (
        let rightIndex = leftIndex + 1;
        rightIndex < compatibleAliases.length;
        rightIndex += 1
      ) {{
        if (priceEvidenceConflicts(
          compatibleAliases[leftIndex].value,
          compatibleAliases[rightIndex].value,
        )) {{
          aliasesConflict = true;
        }}
      }}
    }}
    if (aliasesConflict) {{
      for (const candidate of [...preliminaryLeaders, ...compatibleAliases]) {{
        candidate.roleRank = preliminaryStrongestPriceRole;
        candidate.priceConflict = true;
      }}
      hasPriceConflict = true;
    }}
  }}
  for (const candidate of priceObservations) {{
    if (
      !candidate.eligible
      || !candidate.prominence
      || candidate.roleRank === 3
      || candidate.priceConflict
    ) continue;
    const dominantPeer = priceObservations.find((other) => {{
      if (
        other === candidate
        || !other.eligible
        || other.priceConflict
        || !other.prominence
        || other.roleRank !== candidate.roleRank
        || other.value === candidate.value
        || !sameLocalPriceContext(candidate, other)
      ) return false;
      return Boolean(
        other.prominence.font_size_px >= candidate.prominence.font_size_px * 1.35
        || (
          other.prominence.score >= candidate.prominence.score * 1.35
          && other.prominence.font_size_px > candidate.prominence.font_size_px
        )
      );
    }});
    if (dominantPeer) {{
      candidate.reasons.push('subordinate_visual_prominence');
      candidate.eligible = false;
    }}
  }}
  const strongestPriceRole = Math.max(
    0,
    ...priceObservations.filter((candidate) => candidate.eligible).map((candidate) => candidate.roleRank),
  );
  const strongestPriceValues = [];
  for (const candidate of priceObservations) {{
    if (!candidate.eligible || candidate.roleRank !== strongestPriceRole) continue;
    if (!strongestPriceValues.includes(candidate.value)) {{
      strongestPriceValues.push(candidate.value);
    }}
    if (strongestPriceValues.length >= 8) break;
  }}
  const strongestValuesAreEquivalent = strongestPriceValues.every(
    (left, leftIndex) => strongestPriceValues.slice(leftIndex + 1).every(
      (right) => !priceEvidenceConflicts(left, right)
    )
  );
  const priceCandidates = strongestValuesAreEquivalent
    ? strongestPriceValues.slice(0, 1)
    : [...strongestPriceValues];
  const priceAliases = strongestValuesAreEquivalent
    ? strongestPriceValues.slice(1)
    : [];
  if (priceCandidates.length === 1) {{
    const selectedPrice = priceCandidates[0];
    for (const candidate of priceObservations) {{
      if (
        !candidate.eligible
        || candidate.priceConflict
        || candidate.value === selectedPrice
        || priceEvidenceConflicts(selectedPrice, candidate.value)
        || priceAliases.some((alias) => priceEvidenceConflicts(alias, candidate.value))
      ) continue;
      if (!priceAliases.includes(candidate.value)) priceAliases.push(candidate.value);
      if (priceAliases.length >= 8) break;
    }}
  }}
  const auditPriceObservations = priceObservations.slice(0, 24).map((candidate) => {{
    const status = candidate.reasons.length
      ? 'excluded'
      : (candidate.roleRank === strongestPriceRole ? 'current_candidate' : 'superseded_role');
    const observation = {{
      value: candidate.value,
      source: candidate.source,
      role: candidate.role,
      role_rank: candidate.roleRank,
      status,
      reasons: [...candidate.reasons],
    }};
    if (candidate.selectionState) observation.offer_state = candidate.selectionState;
    if (candidate.priceConflict) observation.conflict = true;
    if (candidate.prominence) {{
      observation.prominence = {{
        font_size_px: candidate.prominence.font_size_px,
        font_weight: candidate.prominence.font_weight,
        area_px2: candidate.prominence.area_px2,
      }};
    }}
    return observation;
  }});
  const availabilityMatch = firstContent([
    '[itemprop="availability"]:not(meta):not(link)',
    '[class*="availability" i]',
    '[id*="availability" i]',
  ], {{
    requirePaintedDom: true,
    preferCurrent: true,
    returnMatch: true,
    scope: availabilityScope,
    candidateFilter: availabilityBelongsToSelectedRecord,
  }}) || firstContent([
    'link[itemprop="availability"]',
    'meta[itemprop="availability"]',
  ], {{
    returnMatch: true,
    scope: availabilityScope,
    candidateFilter: availabilityBelongsToSelectedRecord,
  }});
  const availabilitySourceUrl = (() => {{
    if (!availabilityMatch) return '';
    if (availabilityMatch.element.matches('link')) {{
      return String(availabilityMatch.element.href || availabilityMatch.value || '');
    }}
    if (!availabilityMatch.element.matches('meta')) return '';
    const value = String(availabilityMatch.value || '').trim();
    if (!/^(?:[a-z][a-z0-9+.-]*:|\/\/)/iu.test(value)) return '';
    try {{
      return new URL(value, location.href).href;
    }} catch (_error) {{
      return '';
    }}
  }})();
  const structuredUriLabel = (value) => {{
    try {{
      const parsed = new URL(value, location.href);
      const pathParts = parsed.pathname.split('/').filter(Boolean);
      const encoded = parsed.hash.slice(1) || pathParts[pathParts.length - 1] || '';
      const decoded = decodeURIComponent(encoded);
      return compact(
        decoded
          .replace(/([a-z0-9])([A-Z])/gu, '$1 $2')
          .replace(/([A-Z]+)([A-Z][a-z])/gu, '$1 $2')
          .replace(/[-_]+/gu, ' '),
        240,
      ) || compact(value, 240);
    }} catch (_error) {{
      return compact(value, 240);
    }}
  }};
  const availability = availabilityMatch
    ? (
        availabilitySourceUrl
          ? structuredUriLabel(availabilitySourceUrl)
          : availabilityMatch.value
      )
    : '';
  const rating = firstContent([
    'meta[itemprop="ratingValue"]',
    '[itemprop="ratingValue"]',
    '[data-rating]',
  ]);
  const description = firstContent([
    '[itemprop="description"]',
    'meta[name="description"]',
    'meta[property="og:description"]',
  ]);
  const visibleTextWithoutPrices = (boundary, limit) => {{
    const values = [];
    const walker = document.createTreeWalker(boundary, NodeFilter.SHOW_TEXT);
    while (walker.nextNode()) {{
      const parent = walker.currentNode.parentElement;
      if (!parent || !textNodeIsPainted(walker.currentNode)) continue;
      const priceContainer = parent.closest(priceSelectors);
      if (priceContainer && boundary.contains(priceContainer)) continue;
      const value = compact(walker.currentNode.nodeValue, 600);
      if (value) values.push(value);
    }}
    return compact(values.join(' '), limit);
  }};
  const bullets = [];
  for (const element of Array.from(root.querySelectorAll('li'))) {{
    if (!visible(element)) continue;
    const value = visibleTextWithoutPrices(element, 360);
    if (!value || bullets.includes(value)) continue;
    bullets.push(value);
    if (bullets.length >= 12) break;
  }}
  const details = visibleTextWithoutPrices(root, 2400);
  const record = {{title, url: location.href}};
  if (priceCandidates.length === 1) {{
    let displayPrice = priceCandidates[0];
    const displayAliases = [...priceAliases];
    if (!priceMarkerIdentity(displayPrice)) {{
      const markedAliasIndex = displayAliases.findIndex(
        (candidate) => Boolean(priceMarkerIdentity(candidate))
      );
      if (markedAliasIndex >= 0) {{
        const structuredAmount = displayPrice;
        displayPrice = displayAliases.splice(markedAliasIndex, 1)[0];
        displayAliases.unshift(structuredAmount);
      }}
    }}
    record.price = displayPrice;
    const displayPriceOwners = Array.from(priceMarkerOwners(displayPrice)).sort();
    if (displayPriceOwners.length) record.price_currency_codes = displayPriceOwners;
    if (displayAliases.length) {{
      record.price_aliases = displayAliases;
      const aliasCurrencyCodes = {{}};
      for (const alias of displayAliases) {{
        const owners = Array.from(priceMarkerOwners(alias)).sort();
        if (owners.length) aliasCurrencyCodes[alias] = owners;
      }}
      if (Object.keys(aliasCurrencyCodes).length) {{
        record.price_alias_currency_codes = aliasCurrencyCodes;
      }}
    }}
  }}
  if (priceCandidates.length > 1) record.price_candidates = priceCandidates;
  if (priceCandidates.length > 1 && hasPriceConflict) record.price_conflict = true;
  if (auditPriceObservations.length) record.price_observations = auditPriceObservations;
  if (availability) record.availability = availability;
  if (availabilitySourceUrl) record.availability_source_url = availabilitySourceUrl;
  if (rating) record.rating = rating;
  if (description) record.description = description;
  if (bullets.length) record.bullets = bullets;
  if (details) record.details = details;
  return {{
    ok: Boolean(title && location.href),
    selector: rootSelector || null,
    record_scope: containingCandidate ? 'semantic_record' : 'selected_root',
    page: {{url: location.href, title: document.title}},
    record,
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


def _browser_assert_page_state_script(
    *,
    required: list[str],
    forbidden: list[str],
    selector: str | None = None,
) -> str:
    return f"""
(() => {{
  const args = {json.dumps({"required": required, "forbidden": forbidden, "selector": selector})};
  const textOf = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
  const unicodeText = (value) => textOf(value).normalize('NFKC').toLocaleLowerCase();
  const decimalDigitMap = new Map(
    Array.from({{length: 10}}, (_unused, digit) => [String(digit), String(digit)])
  );
  if (typeof Intl.supportedValuesOf === 'function') {{
    for (const numberingSystem of Intl.supportedValuesOf('numberingSystem')) {{
      try {{
        const formatter = new Intl.NumberFormat('en', {{
          numberingSystem,
          useGrouping: false,
          maximumFractionDigits: 0,
        }});
        for (let digit = 0; digit <= 9; digit += 1) {{
          const rendered = formatter.format(digit).normalize('NFKC');
          const digits = Array.from(rendered).filter((character) => /\\p{{Nd}}/u.test(character));
          if (digits.length === 1) decimalDigitMap.set(digits[0], String(digit));
        }}
      }} catch (_error) {{
        // Ignore numbering systems that this browser cannot format.
      }}
    }}
  }}
  const canonicalDigits = (value) => Array.from(String(value || ''))
    .map((character) => decimalDigitMap.get(character) || character)
    .join('');
  const tokenList = (value) => (
    unicodeText(value).match(/[\\p{{L}}]+|[\\p{{N}}]+/gu) || []
  ).map((token) => canonicalDigits(token));
  const normalize = (value) => tokenList(value).join(' ');
  const tokens = (value) => tokenList(value);
  const assertionPaintAlphaIsZero = (value) => {{
    const normalized = String(value || '').trim().toLocaleLowerCase();
    if (!normalized || normalized === 'none') return false;
    if (normalized === 'transparent') return true;
    if (/(?:rgba|hsla)\\([^)]*[,/]\\s*(?:0(?:\\.0*)?|\\.0+)\\s*\\)$/u.test(normalized)) {{
      return true;
    }}
    return /\\/\\s*(?:0(?:\\.0*)?|\\.0+)(?:%\\s*)?\\)$/u.test(normalized);
  }};
  const assertionPaintValueIsFullyTransparent = (value) => {{
    const normalized = String(value || '').trim().toLocaleLowerCase();
    if (!normalized || normalized === 'none' || normalized === 'transparent') return true;
    const colors = normalized.match(/(?:rgba|hsla|color|oklch|oklab|lab|lch)\\([^)]*\\)/gu) || [];
    return Boolean(colors.length && colors.every(assertionPaintAlphaIsZero));
  }};
  const assertionPaintCssLayers = (value) => {{
    const layers = [];
    let depth = 0;
    let current = '';
    for (const character of String(value || '')) {{
      if (character === '(') depth += 1;
      if (character === ')') depth = Math.max(0, depth - 1);
      if (character === ',' && depth === 0) {{
        layers.push(current.trim());
        current = '';
        continue;
      }}
      current += character;
    }}
    if (current.trim() || !layers.length) layers.push(current.trim());
    return layers.filter(Boolean);
  }};
  const assertionPaintCssTokens = (value) => {{
    const tokens = [];
    let depth = 0;
    let current = '';
    for (const character of String(value || '').trim()) {{
      if (/\\s/u.test(character) && depth === 0) {{
        if (current) tokens.push(current);
        current = '';
        continue;
      }}
      current += character;
      if (character === '(') depth += 1;
      if (character === ')') depth = Math.max(0, depth - 1);
    }}
    if (current) tokens.push(current);
    return tokens;
  }};
  const assertionPaintNumericCssValue = (
    token,
    basis,
    {{autoValue = basis}} = {{}},
  ) => {{
    const normalized = String(token || '').trim().toLocaleLowerCase();
    if (normalized === 'auto') return autoValue;
    const calcMatch = normalized.match(
      /^calc\\(\\s*([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))%\\s*([+-])\\s*([0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)px\\s*\\)$/u
    );
    if (calcMatch) {{
      return (
        basis * Number(calcMatch[1]) / 100
        + Number(calcMatch[3]) * (calcMatch[2] === '-' ? -1 : 1)
      );
    }}
    const match = normalized.match(
      /^([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))(px|%)?$/u
    );
    if (!match) return Number.NaN;
    const amount = Number(match[1]);
    if (!match[2] && amount !== 0) return Number.NaN;
    return match[2] === '%' ? basis * amount / 100 : amount;
  }};
  const assertionPaintImageSize = (value, rect) => {{
    const tokens = assertionPaintCssTokens(value || 'auto');
    if (!tokens.length) return null;
    if (tokens.length === 1 && /^(?:cover|contain)$/u.test(tokens[0])) {{
      return [rect.width, rect.height];
    }}
    const width = assertionPaintNumericCssValue(tokens[0], rect.width);
    const height = assertionPaintNumericCssValue(tokens[1] || 'auto', rect.height);
    if (![width, height].every(Number.isFinite)) return null;
    return [width, height];
  }};
  const assertionPaintPositionComponent = (token) => {{
    const normalized = String(token || '').trim().toLocaleLowerCase();
    if (normalized === 'center') return {{percent: 0.5, pixels: 0}};
    if (normalized === 'left' || normalized === 'top') return {{percent: 0, pixels: 0}};
    if (normalized === 'right' || normalized === 'bottom') return {{percent: 1, pixels: 0}};
    const calcMatch = normalized.match(
      /^calc\\(\\s*([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))%\\s*([+-])\\s*([0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)px\\s*\\)$/u
    );
    if (calcMatch) {{
      return {{
        percent: Number(calcMatch[1]) / 100,
        pixels: Number(calcMatch[3]) * (calcMatch[2] === '-' ? -1 : 1),
      }};
    }}
    const match = normalized.match(
      /^([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+))(px|%)?$/u
    );
    if (!match) return null;
    const amount = Number(match[1]);
    if (!match[2] && amount !== 0) return null;
    return match[2] === '%'
      ? {{percent: amount / 100, pixels: 0}}
      : {{percent: 0, pixels: amount}};
  }};
  const assertionPaintEdgePositionComponent = (edge, distance) => {{
    const anchor = assertionPaintPositionComponent(edge);
    const offset = assertionPaintNumericCssValue(
      distance,
      0,
      {{autoValue: Number.NaN}},
    );
    if (!anchor || !Number.isFinite(offset)) return null;
    const reverse = edge === 'right' || edge === 'bottom';
    return {{percent: anchor.percent, pixels: reverse ? -offset : offset}};
  }};
  const assertionPaintBackgroundPosition = (value) => {{
    const tokens = assertionPaintCssTokens(value || '0% 0%');
    if (
      tokens.length >= 4
      && /^(?:left|right)$/u.test(tokens[0])
      && /^(?:top|bottom)$/u.test(tokens[2])
    ) {{
      return [
        assertionPaintEdgePositionComponent(tokens[0], tokens[1]),
        assertionPaintEdgePositionComponent(tokens[2], tokens[3]),
      ];
    }}
    if (tokens.length === 1) {{
      if (/^(?:top|bottom)$/u.test(tokens[0])) {{
        return [
          assertionPaintPositionComponent('center'),
          assertionPaintPositionComponent(tokens[0]),
        ];
      }}
      return [
        assertionPaintPositionComponent(tokens[0]),
        assertionPaintPositionComponent('center'),
      ];
    }}
    return [
      assertionPaintPositionComponent(tokens[0] || '0%'),
      assertionPaintPositionComponent(tokens[1] || '0%'),
    ];
  }};
  const assertionPaintRepeatedAxes = (value) => {{
    const tokens = assertionPaintCssTokens(value || 'repeat').map(
      (token) => token.toLocaleLowerCase()
    );
    if (tokens[0] === 'repeat-x') return [true, false];
    if (tokens[0] === 'repeat-y') return [false, true];
    const repeats = (token) => token !== 'no-repeat';
    return [repeats(tokens[0] || 'repeat'), repeats(tokens[1] || tokens[0] || 'repeat')];
  }};
  const assertionPaintLayerGeometryCanPaint = (size, position, repeat, rect) => {{
    const container = [rect.width, rect.height];
    const imageSize = assertionPaintImageSize(size, rect);
    const offsets = assertionPaintBackgroundPosition(position);
    const repeats = assertionPaintRepeatedAxes(repeat);
    if (
      !imageSize
      || offsets.some((offset) => !offset)
      || container.some((dimension) => !(dimension > 0))
    ) return false;
    return imageSize.every((dimension, axis) => {{
      if (!(dimension > 0)) return false;
      if (repeats[axis]) return true;
      const offset = (
        (container[axis] - dimension) * offsets[axis].percent
        + offsets[axis].pixels
      );
      return offset < container[axis] && offset + dimension > 0;
    }});
  }};
  const assertionFilterSuppressesPaint = (value) => Array.from(
    String(value || '').matchAll(/opacity\\(\\s*([0-9.]+)\\s*(%)?\\s*\\)/giu)
  ).some((match) => Number(match[1]) <= 0);
  const assertionMaskSuppressesPaint = (image, size, position, repeat, rect) => {{
    const imageLayers = assertionPaintCssLayers(image);
    if (!imageLayers.length || imageLayers.every((layer) => layer === 'none')) return false;
    const sizeLayers = assertionPaintCssLayers(size || 'auto');
    const positionLayers = assertionPaintCssLayers(position || '0% 0%');
    const repeatLayers = assertionPaintCssLayers(repeat || 'repeat');
    const layerValue = (layers, index, fallback) => (
      layers.length ? layers[index % layers.length] : fallback
    );
    return imageLayers.every((layer, index) => (
      layer === 'none'
      || assertionPaintValueIsFullyTransparent(layer)
      || !assertionPaintLayerGeometryCanPaint(
        layerValue(sizeLayers, index, 'auto'),
        layerValue(positionLayers, index, '0% 0%'),
        layerValue(repeatLayers, index, 'repeat'),
        rect,
      )
    ));
  }};
  const assertionCssInsetValue = (token, basis) => {{
    const match = String(token || '').match(/^(-?[0-9.]+)(%|px)?$/u);
    if (!match) return Number.NaN;
    return match[2] === '%' ? Number(match[1]) * basis / 100 : Number(match[1]);
  }};
  const assertionClipPathSuppressesPaint = (value, rect) => {{
    const normalized = String(value || '').trim();
    const inset = normalized.match(/^inset\\(([^)]*)\\)/iu);
    if (inset) {{
      const parts = inset[1].trim().split(/\\s+/u).filter(Boolean);
      if (!parts.length || parts.some((part) => part.includes('round'))) return false;
      const sides = parts.length === 1
        ? [parts[0], parts[0], parts[0], parts[0]]
        : parts.length === 2
          ? [parts[0], parts[1], parts[0], parts[1]]
          : parts.length === 3
            ? [parts[0], parts[1], parts[2], parts[1]]
            : parts.slice(0, 4);
      const top = assertionCssInsetValue(sides[0], rect.height);
      const right = assertionCssInsetValue(sides[1], rect.width);
      const bottom = assertionCssInsetValue(sides[2], rect.height);
      const left = assertionCssInsetValue(sides[3], rect.width);
      return [top, right, bottom, left].every(Number.isFinite)
        && (top + bottom >= rect.height || left + right >= rect.width);
    }}
    const radial = normalized.match(/^(?:circle|ellipse)\\(\\s*([^\\s,)]+)/iu);
    if (radial) {{
      const radius = assertionCssInsetValue(radial[1], Math.min(rect.width, rect.height));
      if (Number.isFinite(radius) && radius <= 0) return true;
    }}
    const polygon = normalized.match(/^polygon\\((.*)\\)$/iu);
    if (polygon) {{
      const points = polygon[1].split(',').map((point) => {{
        const coordinates = point.trim().split(/\\s+/u);
        return [
          assertionCssInsetValue(coordinates[0], rect.width),
          assertionCssInsetValue(coordinates[1], rect.height),
        ];
      }}).filter((point) => point.every(Number.isFinite));
      if (points.length >= 3) {{
        const twiceArea = Math.abs(points.reduce((area, point, index) => {{
          const next = points[(index + 1) % points.length];
          return area + point[0] * next[1] - next[0] * point[1];
        }}, 0));
        if (twiceArea <= 1e-7) return true;
      }}
    }}
    const path = normalized.match(
      /^path\\(\\s*(?:(?:evenodd|nonzero)\\s*,\\s*)?["']([^"']*)["']\\s*\\)$/iu
    );
    if (path) {{
      const commands = path[1].match(/[a-z]/giu) || [];
      if (
        !commands.length
        || commands.every((command) => command.toLocaleLowerCase() === 'm')
      ) return true;
    }}
    return false;
  }};
  const assertionLegacyClipSuppressesPaint = (value) => {{
    const normalized = String(value || '').trim();
    if (!normalized || normalized === 'auto') return false;
    const numbers = normalized.match(/-?[0-9.]+/g) || [];
    if (numbers.length < 4) return false;
    const [top, right, bottom, left] = numbers.slice(0, 4).map(Number);
    return bottom <= top || right <= left;
  }};
  const assertionTransformSuppressesPaint = (value) => {{
    const normalized = String(value || '').trim();
    if (!normalized || normalized === 'none') return false;
    try {{
      const matrix = new DOMMatrixReadOnly(normalized);
      return Math.hypot(matrix.m11, matrix.m12, matrix.m13) <= 1e-7
        || Math.hypot(matrix.m21, matrix.m22, matrix.m23) <= 1e-7;
    }} catch (_error) {{
      return false;
    }}
  }};
  const assertionStyleSuppressesPaint = (element, style) => {{
    const rect = element.getBoundingClientRect();
    return assertionFilterSuppressesPaint(style.filter)
      || assertionMaskSuppressesPaint(
        style.maskImage,
        style.maskSize,
        style.maskPosition,
        style.maskRepeat,
        rect,
      )
      || assertionMaskSuppressesPaint(
        style.webkitMaskImage,
        style.webkitMaskSize,
        style.webkitMaskPosition,
        style.webkitMaskRepeat,
        rect,
      )
      || assertionTransformSuppressesPaint(style.transform)
      || assertionClipPathSuppressesPaint(style.clipPath, rect)
      || assertionLegacyClipSuppressesPaint(style.clip);
  }};
  const assertionPaintVisible = (el) => {{
    const elementRect = el.getBoundingClientRect();
    let clippedLeft = elementRect.left;
    let clippedRight = elementRect.right;
    let clippedTop = elementRect.top;
    let clippedBottom = elementRect.bottom;
    let current = el;
    while (current && current.nodeType === Node.ELEMENT_NODE) {{
      const style = window.getComputedStyle(current);
      if (
        style.visibility === 'hidden'
        || style.display === 'none'
        || style.contentVisibility === 'hidden'
        || Number.parseFloat(style.opacity) === 0
        || assertionStyleSuppressesPaint(current, style)
      ) return false;
      const currentRect = current.getBoundingClientRect();
      if (
        style.position === 'fixed'
        && (
          currentRect.right <= 0
          || currentRect.left >= window.innerWidth
          || currentRect.bottom <= 0
          || currentRect.top >= window.innerHeight
        )
      ) return false;
      const clipsX = /^(?:hidden|clip)$/u.test(style.overflowX);
      const clipsY = /^(?:hidden|clip)$/u.test(style.overflowY);
      if (clipsX) {{
        clippedLeft = Math.max(clippedLeft, currentRect.left);
        clippedRight = Math.min(clippedRight, currentRect.right);
      }}
      if (clipsY) {{
        clippedTop = Math.max(clippedTop, currentRect.top);
        clippedBottom = Math.min(clippedBottom, currentRect.bottom);
      }}
      if (clippedRight <= clippedLeft || clippedBottom <= clippedTop) return false;
      if (
        (clipsX || clipsY)
        && currentRect.width <= 1
        && currentRect.height <= 1
      ) return false;
      if (
        typeof current.checkVisibility === 'function'
        && !current.checkVisibility({{
          checkOpacity: true,
          checkVisibilityCSS: true,
          contentVisibilityAuto: true,
        }})
      ) return false;
      current = current.parentElement;
    }}
    const rect = elementRect;
    return rect.width > 0
      && rect.height > 0
      && rect.right + window.scrollX > 0
      && rect.bottom + window.scrollY > 0;
  }};
  const visible = (el) => {{
    let current = el;
    while (current && current.nodeType === Node.ELEMENT_NODE) {{
      if (current.getAttribute('aria-hidden') === 'true') return false;
      current = current.parentElement;
    }}
    return assertionPaintVisible(el);
  }};
  const assertionTextNodeIsPainted = (node) => {{
    const parent = node && node.parentElement;
    if (!parent || !assertionPaintVisible(parent) || !String(node.nodeValue || '').trim()) return false;
    const style = window.getComputedStyle(parent);
    const shadow = String(style.textShadow || '').trim();
    const strokeWidth = Number.parseFloat(style.webkitTextStrokeWidth || '0');
    const shadowPaints = shadow !== 'none'
      && !assertionPaintValueIsFullyTransparent(shadow);
    const strokePaints = strokeWidth > 0
      && !assertionPaintValueIsFullyTransparent(style.webkitTextStrokeColor);
    const cssLayers = (value) => {{
      const layers = [];
      let depth = 0;
      let current = '';
      for (const character of String(value || '')) {{
        if (character === '(') depth += 1;
        if (character === ')') depth = Math.max(0, depth - 1);
        if (character === ',' && depth === 0) {{
          layers.push(current.trim());
          current = '';
          continue;
        }}
        current += character;
      }}
      if (current.trim() || !layers.length) layers.push(current.trim());
      return layers.filter(Boolean);
    }};
    const clipLayers = cssLayers(
      style.webkitBackgroundClip || style.backgroundClip
    );
    const cssTokens = (value) => {{
      const tokens = [];
      let depth = 0;
      let current = '';
      for (const character of String(value || '').trim()) {{
        if (/\s/u.test(character) && depth === 0) {{
          if (current) tokens.push(current);
          current = '';
          continue;
        }}
        current += character;
        if (character === '(') depth += 1;
        if (character === ')') depth = Math.max(0, depth - 1);
      }}
      if (current) tokens.push(current);
      return tokens;
    }};
    const numericCssValue = (token, basis, {{autoValue = basis}} = {{}}) => {{
      const normalized = String(token || '').trim().toLocaleLowerCase();
      if (normalized === 'auto') return autoValue;
      const calcMatch = normalized.match(
        /^calc\(\s*([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))%\s*([+-])\s*([0-9]+(?:\.[0-9]*)?|\.[0-9]+)px\s*\)$/u
      );
      if (calcMatch) {{
        return (
          basis * Number(calcMatch[1]) / 100
          + Number(calcMatch[3]) * (calcMatch[2] === '-' ? -1 : 1)
        );
      }}
      const match = normalized.match(
        /^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(px|%)?$/u
      );
      if (!match) return Number.NaN;
      const amount = Number(match[1]);
      if (!match[2] && amount !== 0) return Number.NaN;
      return match[2] === '%' ? basis * amount / 100 : amount;
    }};
    const backgroundImageSize = (value, rect) => {{
      const tokens = cssTokens(value || 'auto');
      if (!tokens.length) return null;
      if (tokens.length === 1 && /^(?:cover|contain)$/u.test(tokens[0])) {{
        return [rect.width, rect.height];
      }}
      const width = numericCssValue(tokens[0], rect.width);
      const height = numericCssValue(tokens[1] || 'auto', rect.height);
      if (![width, height].every(Number.isFinite)) return null;
      return [width, height];
    }};
    const positionComponent = (token, axis) => {{
      const normalized = String(token || '').trim().toLocaleLowerCase();
      if (normalized === 'center') return {{percent: 0.5, pixels: 0}};
      if (normalized === 'left' || normalized === 'top') {{
        return {{percent: 0, pixels: 0}};
      }}
      if (normalized === 'right' || normalized === 'bottom') {{
        return {{percent: 1, pixels: 0}};
      }}
      const calcMatch = normalized.match(
        /^calc\(\s*([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))%\s*([+-])\s*([0-9]+(?:\.[0-9]*)?|\.[0-9]+)px\s*\)$/u
      );
      if (calcMatch) {{
        return {{
          percent: Number(calcMatch[1]) / 100,
          pixels: Number(calcMatch[3]) * (calcMatch[2] === '-' ? -1 : 1),
        }};
      }}
      const match = normalized.match(
        /^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(px|%)?$/u
      );
      if (!match) return null;
      const amount = Number(match[1]);
      if (!match[2] && amount !== 0) return null;
      return match[2] === '%'
        ? {{percent: amount / 100, pixels: 0}}
        : {{percent: 0, pixels: amount}};
    }};
    const edgePositionComponent = (edge, distance, axis) => {{
      const anchor = positionComponent(edge, axis);
      const offset = numericCssValue(distance, 0, {{autoValue: Number.NaN}});
      if (!anchor || !Number.isFinite(offset)) return null;
      const reverse = edge === 'right' || edge === 'bottom';
      return {{percent: anchor.percent, pixels: reverse ? -offset : offset}};
    }};
    const backgroundPosition = (value) => {{
      const tokens = cssTokens(value || '0% 0%');
      if (
        tokens.length >= 4
        && /^(?:left|right)$/u.test(tokens[0])
        && /^(?:top|bottom)$/u.test(tokens[2])
      ) {{
        return [
          edgePositionComponent(tokens[0], tokens[1], 'x'),
          edgePositionComponent(tokens[2], tokens[3], 'y'),
        ];
      }}
      if (tokens.length === 1) {{
        if (/^(?:top|bottom)$/u.test(tokens[0])) {{
          return [positionComponent('center', 'x'), positionComponent(tokens[0], 'y')];
        }}
        return [positionComponent(tokens[0], 'x'), positionComponent('center', 'y')];
      }}
      return [
        positionComponent(tokens[0] || '0%', 'x'),
        positionComponent(tokens[1] || '0%', 'y'),
      ];
    }};
    const repeatedAxes = (value) => {{
      const tokens = cssTokens(value || 'repeat').map((token) => token.toLocaleLowerCase());
      if (tokens[0] === 'repeat-x') return [true, false];
      if (tokens[0] === 'repeat-y') return [false, true];
      const repeats = (token) => token !== 'no-repeat';
      return [repeats(tokens[0] || 'repeat'), repeats(tokens[1] || tokens[0] || 'repeat')];
    }};
    const backgroundLayerCanPaint = (size, position, repeat, rect) => {{
      const container = [rect.width, rect.height];
      const imageSize = backgroundImageSize(size, rect);
      const offsets = backgroundPosition(position);
      const repeats = repeatedAxes(repeat);
      if (
        !imageSize
        || offsets.some((offset) => !offset)
        || container.some((dimension) => !(dimension > 0))
      ) return false;
      return imageSize.every((dimension, axis) => {{
        if (!(dimension > 0)) return false;
        if (repeats[axis]) return true;
        const offset = (
          (container[axis] - dimension) * offsets[axis].percent
          + offsets[axis].pixels
        );
        return offset < container[axis] && offset + dimension > 0;
      }});
    }};
    const imageLayers = cssLayers(style.backgroundImage);
    const sizeLayers = cssLayers(style.backgroundSize || 'auto');
    const positionLayers = cssLayers(style.backgroundPosition || '0% 0%');
    const repeatLayers = cssLayers(style.backgroundRepeat || 'repeat');
    const parentRect = parent.getBoundingClientRect();
    const layerValue = (layers, index, fallback) => (
      layers.length ? layers[index % layers.length] : fallback
    );
    const backgroundImagePaints = imageLayers.some((image, index) => (
      layerValue(clipLayers, index, '').toLocaleLowerCase() === 'text'
      && !assertionPaintValueIsFullyTransparent(image)
      && backgroundLayerCanPaint(
        layerValue(sizeLayers, index, 'auto'),
        layerValue(positionLayers, index, '0% 0%'),
        layerValue(repeatLayers, index, 'repeat'),
        parentRect,
      )
    ));
    const colorClip = layerValue(
      clipLayers,
      Math.max(0, imageLayers.length - 1),
      '',
    ).toLocaleLowerCase();
    const backgroundPaints = backgroundImagePaints || (
      colorClip === 'text'
      && !assertionPaintValueIsFullyTransparent(style.backgroundColor)
    );
    const textFillIsTransparent = Boolean(
      style.webkitTextFillColor
      && assertionPaintAlphaIsZero(style.webkitTextFillColor)
    );
    if (
      (assertionPaintAlphaIsZero(style.color) || textFillIsTransparent)
      && !shadowPaints
      && !strokePaints
      && !backgroundPaints
    ) return false;
    const range = document.createRange();
    range.selectNodeContents(node);
    return Array.from(range.getClientRects()).some((rect) => (
      rect.width > 0
      && rect.height > 0
      && rect.right + window.scrollX > 0
      && rect.bottom + window.scrollY > 0
    ));
  }};
  const visibleText = (el) => {{
    const parts = [];
    const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
    let node = walker.nextNode();
    while (node) {{
      if (assertionTextNodeIsPainted(node)) parts.push(node.nodeValue || '');
      node = walker.nextNode();
    }}
    return textOf(parts.join(' '));
  }};
  const labelFor = (el) => {{
    const pieces = [el.getAttribute('aria-label'), el.getAttribute('placeholder'), el.getAttribute('name'), el.getAttribute('title')];
    if (el.labels) for (const label of el.labels) pieces.push(visibleText(label));
    const id = el.getAttribute('id');
    if (id) {{
      const label = document.querySelector(`label[for="${{CSS.escape(id)}}"]`);
      if (label) pieces.push(visibleText(label));
    }}
    const labelledBy = el.getAttribute('aria-labelledby');
    if (labelledBy) {{
      for (const labelId of labelledBy.split(/\\s+/)) {{
        const label = document.getElementById(labelId);
        if (label) pieces.push(visibleText(label));
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
  let scopeRoot = document.body;
  if (args.selector) {{
    try {{
      scopeRoot = document.querySelector(args.selector);
    }} catch (error) {{
      return {{
        ok: false,
        reason: 'invalid_scope_selector',
        selector: args.selector,
        error: String(error && error.message ? error.message : error),
        url: location.href,
        title: document.title
      }};
    }}
  }}
  if (!scopeRoot) {{
    return {{
      ok: false,
      reason: 'scope_not_found',
      selector: args.selector,
      url: location.href,
      title: document.title
    }};
  }}
  const rawCandidates = [];
  if (!args.selector) rawCandidates.push(textOf(document.title));
  rawCandidates.push(visibleText(scopeRoot));
  const scopedNodes = [
    scopeRoot,
    ...Array.from(scopeRoot.querySelectorAll('main, section, article, p, span, div, input, textarea, select, button, a, [role], [contenteditable="true"]'))
  ];
  for (const el of scopedNodes) {{
    if (el !== scopeRoot && !visible(el)) continue;
    const pieces = [];
    if (el === scopeRoot) {{
      pieces.push(visibleText(el));
    }} else {{
      const label = labelFor(el);
      const text = visibleText(el);
      const value = 'value' in el ? textOf(el.value) : '';
      if (label && value) pieces.push(`${{label}} ${{value}}`);
      if (label && text) pieces.push(`${{label}} ${{text}}`);
      if (value) pieces.push(value);
      if (text) pieces.push(text);
      if (el.tagName.toLowerCase() === 'select') {{
        const selected = el.selectedOptions && el.selectedOptions[0]
          ? visibleText(el.selectedOptions[0])
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
    .sort((a, b) => normalize(a).length - normalize(b).length);
  const amountPattern = '\\\\p{{Nd}}(?:[\\\\p{{Nd}}\\\\s.,\\'’٬٫]*\\\\p{{Nd}})?';
  const amountIdentity = (value) => {{
    const identity = Array.from(String(value || ''))
      .map((character) => decimalDigitMap.get(character) || character)
      .join('')
      .replace(/[\\s,\\'’٬]/g, '')
      .replace(/٫/g, '.');
    if ((identity.match(/\\./g) || []).length === 1) {{
      const [rawWhole, rawFraction] = identity.split('.');
      const whole = rawWhole.replace(/^0+/, '') || '0';
      const fraction = rawFraction.replace(/0+$/, '');
      return fraction ? whole + '.' + fraction : whole;
    }}
    return identity.replace(/^0+/, '') || '0';
  }};
  const markerIdentity = (value) => String(value || '')
    .normalize('NFKC')
    .toLocaleLowerCase()
    .replace(/[\\s\\p{{Cf}}\\p{{P}}]+/gu, '');
  const assertionCurrencyCodes = new Set(
    typeof Intl.supportedValuesOf === 'function'
      ? Intl.supportedValuesOf('currency').map((value) => String(value).toUpperCase())
      : []
  );
  const assertionValidLocaleTag = (value) => {{
    const locale = String(value || '').trim();
    if (!locale) return '';
    try {{
      return new Intl.Locale(locale).toString();
    }} catch (_error) {{
      return '';
    }}
  }};
  const assertionDeclaredPageLocale = assertionValidLocaleTag(
    document.documentElement && document.documentElement.lang
  );
  const assertionPageLocales = assertionDeclaredPageLocale
    ? [assertionDeclaredPageLocale]
    : Array.from(new Set([
      ...(Array.isArray(navigator.languages) ? navigator.languages : []),
      navigator.language,
    ].map(assertionValidLocaleTag).filter(Boolean)));
  const assertionRegionCurrencies = (() => {{
    const codes = new Set();
    for (const locale of assertionPageLocales) {{
      try {{
        const region = new Intl.Locale(locale).maximize().region;
        if (!region) continue;
        for (const code of assertionCurrencyCodes) {{
          if (code.startsWith(region)) codes.add(code);
        }}
      }} catch (_error) {{
        // Ignore invalid or unsupported page locale tags.
      }}
    }}
    return codes;
  }})();
  const assertionCurrencySymbolCache = new Map();
  const assertionCurrencyLocales = (code) => {{
    const locales = new Set(assertionPageLocales);
    try {{
      locales.add(
        new Intl.Locale('und-' + String(code || '').slice(0, 2))
          .maximize()
          .toString()
      );
    }} catch (_error) {{
      // Some non-geographic currency codes do not imply a usable locale.
    }}
    if (!locales.size) locales.add(undefined);
    return locales;
  }};
  const runtimeCurrencyMarkers = (code) => {{
    if (assertionCurrencySymbolCache.has(code)) {{
      return assertionCurrencySymbolCache.get(code);
    }}
    const markers = new Set([markerIdentity(code)]);
    if (assertionCurrencyCodes.has(code)) {{
      for (const locale of assertionCurrencyLocales(code)) {{
        for (const currencyDisplay of ['symbol', 'narrowSymbol']) {{
          try {{
            const part = new Intl.NumberFormat(locale, {{
              style: 'currency', currency: code, currencyDisplay,
            }}).formatToParts(1).find((candidate) => candidate.type === 'currency');
            if (!part || !part.value) continue;
            const marker = markerIdentity(part.value);
            if (marker) markers.add(marker);
            if (/\\p{{Sc}}/u.test(part.value)) {{
              markers.add(markerIdentity(code.slice(0, 2) + part.value));
            }}
          }} catch (_error) {{
            // Ignore unsupported locale/currency combinations.
          }}
        }}
      }}
    }}
    assertionCurrencySymbolCache.set(code, markers);
    return markers;
  }};
  const assertionLocalizedOwnerCache = new Map();
  const localizedCurrencyMarkerOwners = (targetMarker) => {{
    if (assertionLocalizedOwnerCache.has(targetMarker)) {{
      return assertionLocalizedOwnerCache.get(targetMarker);
    }}
    const owners = new Set();
    for (const code of assertionCurrencyCodes) {{
      for (const locale of assertionCurrencyLocales(code)) {{
        for (const currencyDisplay of ['symbol', 'narrowSymbol', 'name']) {{
          try {{
            const part = new Intl.NumberFormat(locale, {{
              style: 'currency', currency: code, currencyDisplay,
            }}).formatToParts(1).find((candidate) => candidate.type === 'currency');
            if (!part || !part.value) continue;
            const candidates = new Set([markerIdentity(part.value)]);
            if (/\\p{{Sc}}/u.test(part.value)) {{
              candidates.add(markerIdentity(code.slice(0, 2) + part.value));
            }}
            const letterRuns = part.value.match(/\\p{{L}}+/gu) || [];
            const terminalRun = letterRuns[letterRuns.length - 1] || '';
            const characters = Array.from(terminalRun);
            if (characters.some((character) => character.codePointAt(0) > 127)) {{
              for (let length = 3; length <= Math.min(4, characters.length); length += 1) {{
                candidates.add(markerIdentity(characters.slice(0, length).join('')));
              }}
            }}
            if (/[\\p{{Script=Han}}\\p{{Script=Hangul}}]/u.test(terminalRun)) {{
              candidates.add(markerIdentity(terminalRun));
            }}
            if (/\\p{{Script=Han}}/u.test(terminalRun) && characters.length > 1) {{
              candidates.add(markerIdentity(characters[characters.length - 1]));
            }}
            if (candidates.has(targetMarker)) owners.add(code);
          }} catch (_error) {{
            // Ignore unsupported locale/currency combinations.
          }}
        }}
      }}
    }}
    assertionLocalizedOwnerCache.set(targetMarker, owners);
    return owners;
  }};
  const currencyMarkersCompatible = (leftMarker, rightMarker) => {{
    if (leftMarker === rightMarker) return true;
    const leftIsCode = /^[a-z]{{3}}$/u.test(leftMarker);
    const rightIsCode = /^[a-z]{{3}}$/u.test(rightMarker);
    const codeMarker = leftIsCode ? leftMarker : (rightIsCode ? rightMarker : '');
    const localizedMarker = leftIsCode ? rightMarker : (rightIsCode ? leftMarker : '');
    if (!codeMarker || !localizedMarker) return false;
    const code = codeMarker.toUpperCase();
    const prefixedSymbol = localizedMarker.match(/^([a-z]{{1,2}})(\\p{{Sc}})$/u);
    if (prefixedSymbol && !codeMarker.startsWith(prefixedSymbol[1])) return false;
    if (runtimeCurrencyMarkers(code).has(localizedMarker)) return true;
    const owners = localizedCurrencyMarkerOwners(localizedMarker);
    if (owners.size === 1 && owners.has(code)) return true;
    return Boolean(
      /\\p{{L}}/u.test(localizedMarker)
      && !/\\p{{Sc}}/u.test(localizedMarker)
      && owners.size > 1
      && assertionRegionCurrencies.has(code)
    );
  }};
  const semanticIdentities = (value) => {{
    const raw = unicodeText(value);
    const percents = new Set();
    const markers = new Set();
    const percentAfter = new RegExp('(' + amountPattern + ')\\\\s*[%٪]', 'gu');
    const percentBefore = new RegExp('[%٪]\\\\s*(' + amountPattern + ')', 'gu');
    for (const pattern of [percentAfter, percentBefore]) {{
      for (const match of raw.matchAll(pattern)) percents.add(amountIdentity(match[1]));
    }}
    const markerIsSemantic = (marker) => Boolean(
      marker
      && (
        /^(?:(?:\\p{{L}}{{1,2}})?\\p{{Sc}}|\\p{{Sc}}(?:\\p{{L}}{{1,2}})?)$/u.test(marker)
        || (
          /^[a-z]{{3}}$/u.test(marker)
          && assertionCurrencyCodes.has(marker.toUpperCase())
        )
        || localizedCurrencyMarkerOwners(marker).size
      )
    );
    const adjacentCurrencyMarker = (segment, fromEnd) => {{
      const characters = Array.from(segment);
      const limit = Math.min(24, characters.length);
      const candidates = [];
      for (let length = 1; length <= limit; length += 1) {{
        const rawMarker = fromEnd
          ? characters.slice(characters.length - length).join('')
          : characters.slice(0, length).join('');
        const marker = markerIdentity(rawMarker);
        const boundaryCharacter = fromEnd
          ? characters[characters.length - length - 1]
          : characters[length];
        if (
          markerIsSemantic(marker)
          && !(
            /[a-z]/u.test(marker)
            && boundaryCharacter
            && /[\\p{{L}}\\p{{N}}]/u.test(boundaryCharacter)
          )
        ) {{
          candidates.push({{marker, length}});
        }}
      }}
      candidates.sort((left, right) => right.length - left.length);
      return candidates.length ? candidates[0].marker : '';
    }};
    for (const match of raw.matchAll(new RegExp(amountPattern, 'gu'))) {{
      const amount = amountIdentity(match[0]);
      if (!amount) continue;
      const start = Number(match.index || 0);
      const end = start + match[0].length;
      const beforeMarker = adjacentCurrencyMarker(raw.slice(0, start), true);
      const afterMarker = adjacentCurrencyMarker(raw.slice(end), false);
      if (beforeMarker) markers.add(beforeMarker + ':' + amount);
      if (afterMarker) markers.add(afterMarker + ':' + amount);
    }}
    return {{percents, markers}};
  }};
  const semanticCompatible = (expected, candidate) => {{
    const wanted = semanticIdentities(expected);
    const found = semanticIdentities(candidate);
    return [...wanted.percents].every((identity) => found.percents.has(identity))
      && [...wanted.markers].every((identity) => {{
        if (found.markers.has(identity)) return true;
        const separator = identity.lastIndexOf(':');
        if (separator < 0) return false;
        const wantedMarker = identity.slice(0, separator);
        const wantedAmount = identity.slice(separator + 1);
        return [...found.markers].some((candidateIdentity) => {{
          const candidateSeparator = candidateIdentity.lastIndexOf(':');
          if (candidateSeparator < 0) return false;
          const candidateMarker = candidateIdentity.slice(0, candidateSeparator);
          const candidateAmount = candidateIdentity.slice(candidateSeparator + 1);
          return candidateAmount === wantedAmount
            && currencyMarkersCompatible(wantedMarker, candidateMarker);
        }});
      }});
  }};
  const hasAmbiguousNumericDate = (value) => {{
    const raw = canonicalDigits(textOf(value));
    return Array.from(raw.matchAll(
      /(?<![0-9])([0-9]{{1,2}})[\\/.-]([0-9]{{1,2}})[\\/.-]([0-9]{{4}})(?![0-9])/gu
    )).some((match) => (
      Number(match[1]) >= 1
      && Number(match[1]) <= 12
      && Number(match[2]) >= 1
      && Number(match[2]) <= 12
    ));
  }};
  const dateSignatures = (value) => {{
    const raw = canonicalDigits(textOf(value));
    if (hasAmbiguousNumericDate(raw)) return new Set();
    const parts = raw.match(/[\\p{{L}}]+|[\\p{{N}}]+/gu) || [];
    const dateCandidates = [raw];
    for (let start = 0; start < parts.length; start += 1) {{
      for (let length = 3; length <= 5 && start + length <= parts.length; length += 1) {{
        const slice = parts.slice(start, start + length);
        const numericCount = slice.filter((part) => /\\p{{N}}/u.test(part)).length;
        if (numericCount >= 2 && slice.some((part) => /^\\p{{N}}{{4}}$/u.test(part))) {{
          dateCandidates.push(slice.join(' '));
        }}
      }}
    }}
    const signatures = new Set();
    const validatedDateSignature = (candidate) => {{
      const normalizedCandidate = canonicalDigits(textOf(candidate));
      let year = 0;
      let month = 0;
      let day = 0;
      const yearFirst = normalizedCandidate.match(
        /^([0-9]{{4}})[\\/.-]([0-9]{{1,2}})[\\/.-]([0-9]{{1,2}})$/u
      );
      const yearLast = normalizedCandidate.match(
        /^([0-9]{{1,2}})[\\/.-]([0-9]{{1,2}})[\\/.-]([0-9]{{4}})$/u
      );
      const localizedYearFirst = normalizedCandidate.match(
        /^([0-9]{{4}})[\\p{{L}}\\s]+([0-9]{{1,2}})[\\p{{L}}\\s]+([0-9]{{1,2}})[\\p{{L}}\\s]*$/u
      );
      if (yearFirst || localizedYearFirst) {{
        const matched = yearFirst || localizedYearFirst;
        year = Number(matched[1]);
        month = Number(matched[2]);
        day = Number(matched[3]);
      }} else if (yearLast) {{
        const first = Number(yearLast[1]);
        const second = Number(yearLast[2]);
        year = Number(yearLast[3]);
        if (first <= 12 && second > 12) {{
          month = first;
          day = second;
        }} else if (first > 12 && second <= 12) {{
          day = first;
          month = second;
        }} else {{
          return '';
        }}
      }} else {{
        const candidateParts = normalizedCandidate.match(
          /[\\p{{L}}]+|[0-9]+/gu
        ) || [];
        const yearTokens = candidateParts.filter(
          (part) => /^[0-9]{{4}}$/u.test(part)
        );
        const dayTokens = candidateParts.filter(
          (part) => (
            /^[0-9]{{1,2}}$/u.test(part)
            && Number(part) >= 1
            && Number(part) <= 31
          )
        );
        if (yearTokens.length !== 1 || dayTokens.length !== 1) return '';
        const timestamp = Date.parse(normalizedCandidate);
        if (!Number.isFinite(timestamp)) return '';
        const parsed = new Date(timestamp);
        year = Number(yearTokens[0]);
        month = parsed.getUTCMonth() + 1;
        day = Number(dayTokens[0]);
        if (parsed.getUTCFullYear() !== year || parsed.getUTCDate() !== day) return '';
      }}
      const validated = new Date(Date.UTC(year, month - 1, day));
      if (
        validated.getUTCFullYear() !== year
        || validated.getUTCMonth() + 1 !== month
        || validated.getUTCDate() !== day
      ) return '';
      return [year, month, day].join('-');
    }};
    for (const candidate of dateCandidates) {{
      if (!/\\p{{N}}{{4}}/u.test(candidate)) continue;
      const signature = validatedDateSignature(candidate);
      if (signature) signatures.add(signature);
    }}
    return signatures;
  }};
  const adjacentScriptContains = (candidate, expected) => {{
    let start = candidate.indexOf(expected);
    while (start >= 0) {{
      const before = start > 0 ? candidate[start - 1] : '';
      const afterIndex = start + expected.length;
      const after = afterIndex < candidate.length ? candidate[afterIndex] : '';
      const expectedStartsNumeric = /^[0-9]/u.test(expected);
      const expectedEndsNumeric = /[0-9]$/u.test(expected);
      if (
        !(expectedStartsNumeric && /[0-9]/u.test(before))
        && !(expectedEndsNumeric && /[0-9]/u.test(after))
      ) return true;
      start = candidate.indexOf(expected, start + 1);
    }}
    return false;
  }};
  const compatible = (expected, candidate) => {{
    const expectedNorm = normalize(expected);
    const candidateNorm = normalize(candidate);
    if (!expectedNorm || !candidateNorm) return false;
    const expectedRaw = canonicalDigits(unicodeText(expected));
    const candidateRaw = canonicalDigits(unicodeText(candidate));
    const hasAdjacencyScript = /[\\p{{Script=Han}}\\p{{Script=Hiragana}}\\p{{Script=Katakana}}]/u
      .test(expectedRaw);
    if (hasAdjacencyScript && adjacentScriptContains(candidateRaw, expectedRaw)) return true;
    if (hasAmbiguousNumericDate(expected) || hasAmbiguousNumericDate(candidate)) {{
      return (' ' + candidateNorm + ' ').includes(' ' + expectedNorm + ' ');
    }}
    const expectedDates = dateSignatures(expected);
    const expectedIsLocalizedNumericDate = /^\\s*\\p{{Nd}}{{4}}[\\p{{L}}\\s]+\\p{{Nd}}{{1,2}}[\\p{{L}}\\s]+\\p{{Nd}}{{1,2}}[\\p{{L}}\\s]*$/u
      .test(unicodeText(expected));
    if (
      expectedDates.size
      && (tokens(expected).length <= 3 || expectedIsLocalizedNumericDate)
      && [...dateSignatures(candidate)].some((signature) => expectedDates.has(signature))
    ) return true;
    if (!semanticCompatible(expected, candidate)) return false;
    const expectedSemanticMarkers = semanticIdentities(expected).markers;
    const expectedMarkerTokens = new Set();
    for (const identity of expectedSemanticMarkers) {{
      const separator = identity.lastIndexOf(':');
      if (separator < 0) continue;
      for (const token of tokens(identity.slice(0, separator))) {{
        expectedMarkerTokens.add(token);
      }}
    }}
    const expectedTokens = tokens(expected).filter(
      (token) => !expectedMarkerTokens.has(token)
    );
    const candidateTokens = new Set(tokens(candidate));
    if (expectedTokens.length === 1) return candidateTokens.has(expectedTokens[0]);
    return expectedTokens.every((token) => candidateTokens.has(token));
  }};
  const compactMatch = (expected, candidate) => {{
    if (!candidate) return '';
    const expectedText = textOf(expected).toLowerCase();
    const candidateText = textOf(candidate);
    const index = candidateText.toLowerCase().indexOf(expectedText);
    if (index >= 0) {{
      const start = Math.max(0, index - 100);
      const end = Math.min(candidateText.length, index + expectedText.length + 180);
      return `${{start > 0 ? '\u2026' : ''}}${{candidateText.slice(start, end)}}${{end < candidateText.length ? '\u2026' : ''}}`;
    }}
    return candidateText.length <= 320 ? candidateText : `${{candidateText.slice(0, 317)}}...`;
  }};
  const compactPreview = (candidate) => {{
    const candidateText = textOf(candidate);
    return candidateText.length <= 320 ? candidateText : `${{candidateText.slice(0, 317)}}...`;
  }};
  const findMatch = (expected) => {{
    const candidate = candidates.find((value) => compatible(expected, value)) || '';
    return compactMatch(expected, candidate);
  }};
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
    selector: args.selector || null,
    candidate_count: candidates.length,
    candidates: candidates.slice(0, 20).map(compactPreview)
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


def _serialized_browser_operation(method):
    """Keep page mutations and their typed evidence observations atomic."""

    @wraps(method)
    def _wrapped(self, invocation: ToolInvocation, *args, **kwargs):
        session_id = self._session_id(invocation)
        resolved = getattr(self._resolved_session_local, "by_invocation", None)
        if not isinstance(resolved, dict):
            resolved = {}
            self._resolved_session_local.by_invocation = resolved
        invocation_key = id(invocation)
        resolved[invocation_key] = session_id
        try:
            with self._session_operation_lock(session_id):
                return method(self, invocation, *args, **kwargs)
        finally:
            resolved.pop(invocation_key, None)

    return _wrapped


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
        self._page_assertion_lock = threading.Lock()
        self._page_assertion_contracts: dict[tuple[str, str], dict[str, object]] = {}
        self._session_operation_locks_lock = threading.Lock()
        self._session_operation_locks: weakref.WeakValueDictionary[str, threading.RLock] = (
            weakref.WeakValueDictionary()
        )
        self._resolved_session_local = threading.local()

    def _session_id(self, invocation: ToolInvocation) -> str:
        resolved = getattr(self._resolved_session_local, "by_invocation", None)
        if isinstance(resolved, dict):
            cached_session_id = resolved.get(id(invocation))
            if isinstance(cached_session_id, str) and cached_session_id:
                return cached_session_id
        raw_session_id = str(invocation.arguments.get("session_id", "") or "").strip()
        context = invocation.flow_context if isinstance(invocation.flow_context, dict) else {}
        bound_session_id = str(context.get("browser_session_id") or "").strip()
        if bound_session_id:
            return bound_session_id
        if self._uses_shared_default_session():
            if raw_session_id == DEFAULT_AGENT_BROWSER_SESSION_ID:
                return DEFAULT_AGENT_BROWSER_SESSION_ID
            if raw_session_id and raw_session_id != "default":
                return self._scoped_model_session_id(invocation, raw_session_id)
            active_session_id = self._recent_active_session_id(invocation)
            if active_session_id:
                return active_session_id
            scope = self._active_session_key(invocation)
            digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:16]
            return f"task-{digest}"
        if raw_session_id and raw_session_id != "default":
            return self._scoped_model_session_id(invocation, raw_session_id)
        scope = self._cleanup_scope(invocation)
        digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:16]
        return f"default-{digest}"

    @staticmethod
    def _scoped_model_session_id(invocation: ToolInvocation, raw_session_id: str) -> str:
        context = invocation.flow_context if isinstance(invocation.flow_context, dict) else {}
        browser_scope = str(context.get("browser_session_scope") or "").strip()
        scope = f"browser_session_scope:{browser_scope}" if browser_scope else ""
        if not scope:
            scope = str(invocation.capsule_id or "").strip()
        if not scope:
            for key in ("request_id", "turn_id"):
                if value := str(context.get(key) or "").strip():
                    scope = f"{key}:{value}"
                    break
        if not scope:
            scope = str(invocation.principal_id or "global")
        digest = hashlib.sha256(f"{scope}\0{raw_session_id}".encode("utf-8")).hexdigest()[:16]
        return f"task-{digest}"

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
        if self._uses_shared_default_session() and session_id == DEFAULT_AGENT_BROWSER_SESSION_ID:
            return
        scope = self._cleanup_scope(invocation)
        with self._cleanup_lock:
            self._sessions_by_scope.setdefault(scope, set()).add(session_id)

    def _active_session_keys(self, invocation: ToolInvocation) -> tuple[str, ...]:
        context = invocation.flow_context if isinstance(invocation.flow_context, dict) else {}
        browser_scope = str(context.get("browser_session_scope") or "").strip()
        if browser_scope:
            return (f"browser_session_scope:{browser_scope}",)
        capsule_id = str(invocation.capsule_id or "").strip()
        if capsule_id:
            return (f"capsule_id:{capsule_id}",)
        for key in ("request_id", "turn_id"):
            value = str(context.get(key) or "").strip()
            if value:
                return (f"{key}:{value}",)
        principal_key = str(invocation.principal_id or "").strip()
        if principal_key:
            return (principal_key,)
        return ("global",)

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
        with self._active_session_lock:
            stale_keys = [
                key
                for key, (active_session_id, _remembered_at) in self._active_sessions_by_principal.items()
                if active_session_id == session_id
            ]
            for key in stale_keys:
                self._active_sessions_by_principal.pop(key, None)
        with self._element_snapshot_lock:
            self._element_snapshots.pop(session_id, None)
        self._forget_page_assertion_contract(session_id)

    @staticmethod
    def _page_assertion_scope_id(invocation: ToolInvocation) -> str:
        context = invocation.flow_context if isinstance(invocation.flow_context, dict) else {}
        if browser_scope := str(context.get("browser_session_scope") or "").strip():
            return f"browser_session_scope:{browser_scope}"
        if capsule_id := str(invocation.capsule_id or "").strip():
            return capsule_id
        for key in ("request_id", "turn_id"):
            if value := str(context.get(key) or "").strip():
                return f"{key}:{value}"
        return ""

    def _session_operation_lock(self, session_id: str) -> threading.RLock:
        with self._session_operation_locks_lock:
            return self._session_operation_locks.setdefault(session_id, threading.RLock())

    def _forget_page_assertion_contract(self, session_id: str, *, scope_id: str | None = None) -> None:
        with self._page_assertion_lock:
            keys = [
                key
                for key in self._page_assertion_contracts
                if key[0] == session_id and (scope_id is None or key[1] == scope_id)
            ]
            for key in keys:
                self._page_assertion_contracts.pop(key, None)

    def _forget_page_assertion_scope(self, scope_id: str) -> None:
        if not scope_id:
            return
        with self._page_assertion_lock:
            keys = [key for key in self._page_assertion_contracts if key[1] == scope_id]
            for key in keys:
                self._page_assertion_contracts.pop(key, None)

    def _remember_page_assertion_contract(
        self,
        invocation: ToolInvocation,
        session_id: str,
        *,
        required: list[str],
        forbidden: list[str],
        selector: str | None,
    ) -> None:
        scope_id = self._page_assertion_scope_id(invocation)
        if not scope_id:
            return
        contract: dict[str, object] = {
            "required": tuple(required),
            "forbidden": tuple(forbidden),
            "selector": selector,
        }
        with self._page_assertion_lock:
            self._page_assertion_contracts[(session_id, scope_id)] = contract

    def _remembered_page_assertion_selector(
        self,
        invocation: ToolInvocation,
        session_id: str,
    ) -> str | None:
        scope_id = self._page_assertion_scope_id(invocation)
        if not scope_id:
            return None
        with self._page_assertion_lock:
            contract = self._page_assertion_contracts.get((session_id, scope_id))
            selector = contract.get("selector") if isinstance(contract, dict) else None
        return str(selector).strip() if isinstance(selector, str) and selector.strip() else None

    def _current_page_assertion(
        self,
        invocation: ToolInvocation,
        session_id: str,
    ) -> dict[str, object] | None:
        """Re-run the turn-scoped typed contract at the evidence-read boundary.

        This observation is taken immediately after the browser returns the
        candidate evidence. It therefore verifies the page that produced that
        evidence instead of guessing whether an earlier click was an anchor,
        a navigation, or an application route.
        """

        scope_id = self._page_assertion_scope_id(invocation)
        if not scope_id:
            return None
        with self._page_assertion_lock:
            cached = self._page_assertion_contracts.get((session_id, scope_id))
            contract = dict(cached) if isinstance(cached, dict) else None
        if contract is None:
            return None
        required = [str(value) for value in contract.get("required", ())]
        forbidden = [str(value) for value in contract.get("forbidden", ())]
        selector_value = contract.get("selector")
        selector = str(selector_value) if selector_value else None
        assertion_script = _browser_assert_page_state_script(required=required, forbidden=forbidden, selector=selector)
        try:
            result = _run(self._backend.run_js(session_id, assertion_script))
        except Exception as exc:
            result = {
                "ok": False,
                "reason": "current_page_assertion_failed",
                "error": _compact_failure_message(exc),
                "required": [{"expected": value, "match": ""} for value in required],
                "forbidden": [{"expected": value, "match": ""} for value in forbidden],
                "missing": [{"expected": value, "match": ""} for value in required],
                "forbidden_found": [],
                "selector": selector,
            }
        if not isinstance(result, dict):
            result = {
                "ok": False,
                "reason": "invalid_current_page_assertion_result",
                "required": [{"expected": value, "match": ""} for value in required],
                "forbidden": [{"expected": value, "match": ""} for value in forbidden],
                "missing": [{"expected": value, "match": ""} for value in required],
                "forbidden_found": [],
                "selector": selector,
            }
        result = self._canonical_page_assertion_result(
            result,
            required=required,
            forbidden=forbidden,
            selector=selector,
        )
        return {
            "schema_version": 1,
            "kind": "page_state_assertion",
            "session_id": session_id,
            "verified": result.get("ok") is True,
            "contract": {
                "required": required,
                "forbidden": forbidden,
                "selector": selector,
            },
            "result": result,
        }

    @staticmethod
    def _assertion_selector(assertion: dict[str, object] | None) -> str | None:
        if not isinstance(assertion, dict):
            return None
        contract = assertion.get("contract")
        if not isinstance(contract, dict):
            return None
        selector = contract.get("selector")
        return str(selector).strip() if isinstance(selector, str) and selector.strip() else None

    def _runtime_dom_detail_extraction(
        self,
        session_id: str,
        *,
        selector: str | None,
    ) -> dict[str, object] | None:
        """Return only a record produced by Nullion's fixed DOM extractor."""

        result = _run(
            self._backend.run_js(
                session_id,
                _browser_extract_detail_script(selector=selector),
            )
        )
        if not isinstance(result, dict) or result.get("ok") is not True:
            return None
        record = result.get("record")
        page = result.get("page")
        if not isinstance(record, dict) or not isinstance(page, dict):
            return None
        record_scope = str(result.get("record_scope") or "").strip()
        if record_scope not in {"semantic_record", "selected_root"}:
            return None
        title = str(record.get("title") or "").strip()
        url = str(record.get("url") or "").strip()
        if not title or not url:
            return None
        substantive_keys = (
            "price_text",
            "price",
            "price_candidates",
            "availability",
            "rating",
            "description",
            "details",
            "bullets",
            "ingredients",
            "features",
        )
        if not any(record.get(key) for key in substantive_keys):
            return None
        return {
            "schema_version": _BROWSER_DOM_DETAIL_SCHEMA_VERSION,
            "kind": _BROWSER_DOM_DETAIL_KIND,
            "origin": _BROWSER_DOM_DETAIL_ORIGIN,
            "session_id": session_id,
            "selector": selector,
            "record_scope": record_scope,
            "page": dict(page),
            "record": dict(record),
        }

    @staticmethod
    def _mapping_has_detail_record_shape(value: object) -> bool:
        if not isinstance(value, dict):
            return False
        has_title = any(
            str(value.get(key) or "").strip()
            for key in ("title", "name", "productTitle", "product_title")
        )
        has_url = any(
            str(value.get(key) or "").strip()
            for key in ("url", "href", "link", "action_url", "booking_url")
        )
        has_detail = any(
            value.get(key)
            for key in (
                "price_text",
                "price",
                "price_candidates",
                "availability",
                "rating",
                "description",
                "details",
                "bullets",
                "ingredients",
                "features",
            )
        )
        return bool(has_title and has_url and has_detail)

    @staticmethod
    def _canonical_page_assertion_result(
        result: dict[str, object],
        *,
        required: list[str],
        forbidden: list[str],
        selector: str | None,
    ) -> dict[str, object]:
        """Echo the cached contract in every typed revalidation outcome."""

        normalized = dict(result)

        def _states(key: str, labels: list[str]) -> tuple[list[dict[str, str]], bool]:
            raw_states = result.get(key)
            by_label: dict[str, str] = {}
            schema_valid = isinstance(raw_states, list)
            if isinstance(raw_states, list):
                for raw_state in raw_states:
                    if not isinstance(raw_state, dict):
                        schema_valid = False
                        continue
                    expected = str(raw_state.get("expected") or "").strip()
                    if expected not in labels or expected in by_label:
                        schema_valid = False
                        continue
                    raw_match = raw_state.get("match")
                    if not isinstance(raw_match, str):
                        schema_valid = False
                    by_label[expected] = str(raw_match or "").strip()
            schema_valid = schema_valid and len(by_label) == len(labels)
            return (
                [
                    {"expected": label, "match": by_label.get(label, "")}
                    for label in labels
                ],
                schema_valid,
            )

        required_states, required_schema_valid = _states("required", required)
        forbidden_states, forbidden_schema_valid = _states("forbidden", forbidden)
        normalized["required"] = required_states
        normalized["forbidden"] = forbidden_states
        normalized["selector"] = selector
        missing_labels = {
            state["expected"]
            for state in required_states
            if not state["match"]
        }
        normalized["missing"] = [
            {"expected": label, "match": ""}
            for label in required
            if label in missing_labels
        ]
        forbidden_found_labels = {
            state["expected"]
            for state in forbidden_states
            if state["match"]
        }
        forbidden_matches = {
            state["expected"]: state["match"]
            for state in forbidden_states
        }
        normalized["forbidden_found"] = [
            {"expected": label, "match": forbidden_matches.get(label, "")}
            for label in forbidden
            if label in forbidden_found_labels
        ]
        normalized["ok"] = (
            result.get("ok") is True
            and required_schema_valid
            and forbidden_schema_valid
            and not missing_labels
            and not forbidden_found_labels
        )
        return normalized

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
        if scope_id is None:
            with self._page_assertion_lock:
                self._page_assertion_contracts.clear()
        else:
            self._forget_page_assertion_scope(str(scope_id))
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
                sessions_still_in_use = {
                    session_id
                    for scoped_session_ids in self._sessions_by_scope.values()
                    for session_id in scoped_session_ids
                }
                session_ids.difference_update(sessions_still_in_use)
        for session_id in sorted(session_ids):
            with self._session_operation_lock(session_id):
                with self._cleanup_lock:
                    if any(
                        session_id in scoped_session_ids
                        for scoped_session_ids in self._sessions_by_scope.values()
                    ):
                        continue
                try:
                    _run(self._backend.close_session(session_id))
                except Exception:
                    pass
                with self._cleanup_lock:
                    if any(
                        session_id in scoped_session_ids
                        for scoped_session_ids in self._sessions_by_scope.values()
                    ):
                        continue
                self._forget_session(session_id)

    # ── Tools ─────────────────────────────────────────────────────────────────

    @_serialized_browser_operation
    def browser_open(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        self._forget_page_assertion_contract(
            session_id,
            scope_id=self._page_assertion_scope_id(invocation),
        )
        try:
            result = _run(self._backend.open(session_id))
            return _ok(invocation, {"result": result, "session_id": session_id, **self._connection_notice_output()})
        except Exception as e:
            return _fail(invocation, f"Open failed: {e}")

    @_serialized_browser_operation
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
        self._forget_page_assertion_contract(
            session_id,
            scope_id=self._page_assertion_scope_id(invocation),
        )
        try:
            result = _run(self._backend.navigate(session_id, navigate_url))
            return _ok(invocation, {"result": result, "session_id": session_id, **self._connection_notice_output()})
        except Exception as e:
            return _fail(invocation, f"Navigation failed: {e}")

    @_serialized_browser_operation
    def browser_click(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector", "")
        if not selector:
            return _fail(invocation, "Missing required argument: selector")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            _run(self._backend.click(session_id, str(selector)))
            output: dict[str, object] = {"clicked": selector, "session_id": session_id}
            return _ok(invocation, output)
        except Exception as e:
            return _fail(invocation, f"Click failed: {e}")

    @_serialized_browser_operation
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
            output: dict[str, object] = {"clicked": target, "session_id": session_id}
            return _ok(invocation, output)
        except Exception as e:
            return _fail(invocation, f"Click failed: {e}")

    @_serialized_browser_operation
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

    @_serialized_browser_operation
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

    @_serialized_browser_operation
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

    @_serialized_browser_operation
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
            output: dict[str, object] = {
                "page": page,
                "items": items,
                "item_count": len(items),
                "session_id": session_id,
            }
            if assertion := self._current_page_assertion(invocation, session_id):
                output["page_state_assertion"] = assertion
            return _ok(invocation, output)
        except Exception as e:
            return _fail(invocation, f"Item extraction failed: {e}")

    @_serialized_browser_operation
    def browser_extract_detail(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector")
        if selector is not None and not isinstance(selector, str):
            return _fail(invocation, "selector must be a string")
        normalized_selector = selector.strip() if isinstance(selector, str) and selector.strip() else None
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        if normalized_selector is None:
            normalized_selector = self._remembered_page_assertion_selector(invocation, session_id)
        try:
            extraction = self._runtime_dom_detail_extraction(
                session_id,
                selector=normalized_selector,
            )
            if extraction is None:
                return _fail(
                    invocation,
                    "The current page did not expose a substantive detail record in the selected DOM scope.",
                )
            output = dict(extraction)
            if assertion := self._current_page_assertion(invocation, session_id):
                output["page_state_assertion"] = assertion
            return _ok(invocation, output)
        except Exception as e:
            return _fail(invocation, f"Detail extraction failed: {e}")

    @_serialized_browser_operation
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
                        output: dict[str, object] = {
                            "clicked": element_id,
                            "session_id": session_id,
                            "recovered_by": "cached_semantic_target",
                            "target": target,
                            "previous_error": reason,
                        }
                        return _ok(
                            invocation,
                            output,
                        )
                    except Exception as fallback_error:
                        return _fail(invocation, f"Click failed: {reason}; fallback failed: {fallback_error}")
                return _fail(invocation, f"Click failed: {reason}")
            output: dict[str, object] = {
                "result": result,
                "clicked": element_id,
                "session_id": session_id,
            }
            return _ok(invocation, output)
        except Exception as e:
            return _fail(invocation, f"Click failed: {e}")

    @_serialized_browser_operation
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

    @_serialized_browser_operation
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

    @_serialized_browser_operation
    def browser_extract_text(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector") or None
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            text = _run(self._backend.extract_text(session_id, selector))
            output: dict[str, object] = {
                "text": text,
                "length": len(text),
                "selector": selector,
                "session_id": session_id,
                **self._connection_notice_output(),
            }
            if assertion := self._current_page_assertion(invocation, session_id):
                output["page_state_assertion"] = assertion
            return _ok(invocation, output)
        except Exception as e:
            return _fail(invocation, f"Extract text failed: {e}")

    @_serialized_browser_operation
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

    @_serialized_browser_operation
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

    @_serialized_browser_operation
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
                inspect.signature(wait_for).bind(session_id, selector, url_pattern, text, timeout)
            except TypeError:
                _run(wait_for(session_id, selector, url_pattern, timeout))
            else:
                _run(wait_for(session_id, selector, url_pattern, text, timeout))
            return _ok(invocation, {"waited_for": selector or url_pattern or text, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Wait failed: {e}")

    @_serialized_browser_operation
    def browser_assert_page_state(self, invocation: ToolInvocation) -> ToolResult:
        raw_required = invocation.arguments.get("required") or invocation.arguments.get("required_text") or []
        raw_forbidden = invocation.arguments.get("forbidden") or invocation.arguments.get("forbidden_text") or []
        required = (
            [raw_required.strip()]
            if isinstance(raw_required, str) and raw_required.strip()
            else [str(item).strip() for item in raw_required if str(item).strip()]
            if not isinstance(raw_required, str)
            else []
        )
        forbidden = (
            [raw_forbidden.strip()]
            if isinstance(raw_forbidden, str) and raw_forbidden.strip()
            else [str(item).strip() for item in raw_forbidden if str(item).strip()]
            if not isinstance(raw_forbidden, str)
            else []
        )
        selector = str(invocation.arguments.get("selector") or "").strip() or None
        if not required and not forbidden:
            return _fail(invocation, "Provide required or forbidden page text/state assertions")
        raw_session_id = str(invocation.arguments.get("session_id", "") or "").strip()
        context = invocation.flow_context if isinstance(invocation.flow_context, dict) else {}
        bound_session_id = str(context.get("browser_session_id") or "").strip()
        has_explicit_session = bool(
            bound_session_id or (raw_session_id and raw_session_id != "default")
        )
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        assertion_scope_id = self._page_assertion_scope_id(invocation)
        self._forget_page_assertion_contract(session_id, scope_id=assertion_scope_id)
        try:
            script = _browser_assert_page_state_script(
                required=required,
                forbidden=forbidden,
                selector=selector,
            )
            result = _run(
                self._backend.run_js(
                    session_id,
                    script,
                )
            )
            if not isinstance(result, dict):
                return _fail(invocation, "Page state assertion returned an invalid result.")
            result = self._canonical_page_assertion_result(
                result,
                required=required,
                forbidden=forbidden,
                selector=selector,
            )
            if not result.get("ok"):
                reason = str(result.get("reason") or "").strip()
                if reason in {"invalid_scope_selector", "scope_not_found"}:
                    return ToolResult(
                        invocation_id=invocation.invocation_id,
                        tool_name=invocation.tool_name,
                        status="failed",
                        output={"result": result, "session_id": session_id, "selector": selector},
                        error=(
                            "Page state assertion scope selector was invalid."
                            if reason == "invalid_scope_selector"
                            else "Page state assertion scope selector did not match an element."
                        ),
                    )
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
                            fallback_result = self._canonical_page_assertion_result(
                                fallback_result,
                                required=required,
                                forbidden=forbidden,
                                selector=selector,
                            )
                            fallback_url = str(fallback_result.get("url") or "").strip()
                            if fallback_result.get("ok") or fallback_url not in {"", "about:blank"}:
                                session_id = fallback_session_id
                                result = fallback_result
                                current_url = fallback_url
                                self._remember_active_session(invocation, session_id)
                                self._forget_page_assertion_contract(
                                    session_id,
                                    scope_id=assertion_scope_id,
                                )
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
            self._remember_page_assertion_contract(
                invocation,
                session_id,
                required=required,
                forbidden=forbidden,
                selector=selector,
            )
            return _ok(invocation, {"result": result, "verified": True, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Page state assertion failed: {e}")

    @_serialized_browser_operation
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

    @_serialized_browser_operation
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
            output: dict[str, object] = {"result": result, "session_id": session_id}
            assertion = self._current_page_assertion(invocation, session_id)
            if assertion:
                output["page_state_assertion"] = assertion
            if self._mapping_has_detail_record_shape(result):
                extraction = self._runtime_dom_detail_extraction(
                    session_id,
                    selector=self._assertion_selector(assertion),
                )
                if extraction is not None:
                    # This is a separate runtime-owned observation. Never mark
                    # the arbitrary JavaScript mapping itself as verified.
                    output["detail_extraction"] = extraction
            return _ok(invocation, output)
        except Exception as e:
            return _fail(
                invocation,
                f"JavaScript execution failed: {e}",
                {"session_id": session_id},
            )

    @_serialized_browser_operation
    def browser_close(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        try:
            _run(self._backend.close_session(session_id))
            return _ok(invocation, {"closed": session_id})
        except Exception as e:
            return _fail(invocation, f"Close failed: {e}")
        finally:
            self._forget_session(session_id)
