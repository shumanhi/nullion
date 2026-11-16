"""Local infographic rendering with the shared card contract, not a source capture."""
from hashlib import sha256
from io import BytesIO
from pathlib import Path

from nullion.pdf_design import card_pages_schema, render_card_pages


def input_schema():
    return {"type": "object", "additionalProperties": False, "required": ["card_pages"], "properties": {
        "card_pages": {**card_pages_schema(), "maxItems": 1},
        "output_path": {"type": "string", "description": "Optional destination .png inside the workspace."},
        "width": {"type": "integer", "minimum": 720, "maximum": 1440, "default": 800},
        "pixel_ratio": {"type": "integer", "enum": [1, 2], "default": 2}}}


def _render(html, path, width, ratio):
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="nullion-image") as pool:
            return pool.submit(_render, html, path, width, ratio).result()
    from playwright.sync_api import sync_playwright
    from nullion.tools import _pdf_chromium_executable
    executable = _pdf_chromium_executable()
    if not executable:
        raise RuntimeError("The installed Chromium renderer is unavailable")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=executable)
        try:
            context = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=ratio)
            # Rendering never browses sources or fetches remote assets.
            context.route("**/*", lambda route: route.abort())
            page = context.new_page()
            page.set_content('<!doctype html><meta charset="utf-8"><style>body{margin:0}</style>' + html, wait_until="load")
            page.evaluate("async () => { await document.fonts.ready; await Promise.all([...document.images].map(i => i.decode())); }")
            report = page.locator('.n-card-report')
            box = report.bounding_box()
            if not box or box['height'] > 6000:
                raise ValueError("The image exceeds the 6000 CSS-pixel canvas. Use shorter cards or split the content into separate images; preserve required facts.")
            data = report.screenshot(type="png", animations="disabled", scale="device")
            from PIL import Image
            with Image.open(BytesIO(data)) as image:
                dimensions = image.size
            path.parent.mkdir(parents=True, exist_ok=True)
            import os
            from uuid import uuid4
            temporary = path.with_name('.' + uuid4().hex + '.png')
            try:
                temporary.write_bytes(data); os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            return dimensions
        finally:
            browser.close()


def build_handler(workspace_root=None, allowed_roots=None, *, include_principal_workspace=True):
    from nullion.tools import (ToolResult, _effective_filesystem_roots, _explicit_output_path_or_generated_artifact_path,
        _path_within_any_root, _is_approved_filesystem_path, _inline_html_local_images, _HtmlImageSrcParser)
    root = Path(workspace_root).resolve() if workspace_root is not None else None
    roots = tuple(Path(p).resolve() for p in allowed_roots) if allowed_roots is not None else None

    def handler(invocation):
        effective = _effective_filesystem_roots(invocation=invocation, resolved_root=root,
            resolved_allowed_roots=roots, include_principal_workspace=include_principal_workspace)
        def fail(reason, message, *, repair=False):
            output = {"reason": reason, "failure_scope": "invocation", "executed": False}
            if repair:
                output["recovery"] = {"kind": "correct_arguments", "retry_tool_name": invocation.tool_name,
                    "approval_bypass": False, "argument_constraints": input_schema()}
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", output, message)
        if not effective:
            return fail("workspace_required", "image_render requires an authorized workspace")
        arguments = invocation.arguments
        try:
            raw_path = arguments.get("output_path")
            if raw_path is not None and (not isinstance(raw_path, str) or not raw_path.strip() or Path(raw_path).suffix.lower() != ".png"):
                raise ValueError("Use a .png output path or omit output_path")
            pages = render_card_pages(arguments.get("card_pages"), output_kind="image")
            width = arguments.get("width", 800); ratio = arguments.get("pixel_ratio", 2)
            if len(pages) != 1 or type(width) is not int or not 720 <= width <= 1440 or type(ratio) is not int or ratio not in (1, 2):
                raise ValueError("Use exactly one card page, width 720–1440 and pixel_ratio 1 or 2")
        except (ValueError, TypeError) as error:
            return fail("invalid_tool_arguments", str(error), repair=True)
        path = _explicit_output_path_or_generated_artifact_path(invocation,
            raw_path=arguments.get("output_path"), suffix=".png", stem="infographic", roots=effective)
        if path.suffix.lower() != '.png':
            return fail("invalid_tool_arguments", "Use a .png output path", repair=True)
        if not _path_within_any_root(path, effective) and not _is_approved_filesystem_path(path, invocation.trusted_filesystem_selectors):
            return fail("output_path_not_allowed", "Output path is outside the authorized workspace")
        html, embedded = _inline_html_local_images(pages[0], principal_id=invocation.principal_id,
            output_path=path, effective_roots=effective, trusted_filesystem_selectors=invocation.trusted_filesystem_selectors)
        parser = _HtmlImageSrcParser(); parser.feed(html)
        if any(not source.lower().startswith('data:image/') for source in parser.sources):
            return fail("invalid_tool_arguments", "Supply authorized local image files. Collect remote assets before rendering; image_render does not fetch sources.", repair=True)
        try:
            dimensions = _render(html, path, width, ratio)
        except ValueError as error:
            return fail("invalid_tool_arguments", str(error), repair=True)
        except Exception as error:
            return fail("image_render_failed", str(error))
        from nullion.artifacts import artifact_output_descriptor
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {
            "path": str(path), "artifact_path": str(path), "artifact_paths": [str(path)],
            "artifact_descriptors": [artifact_output_descriptor(path, role="deliverable", kind="image")],
            "type": "image", "media_type": "image/png", "width": dimensions[0], "height": dimensions[1],
            "css_width": width, "pixel_ratio": ratio, "bytes": path.stat().st_size,
            "quality_profile": "designed_image_v1", "embedded_html_images": embedded,
            "image_sha256": sha256(path.read_bytes()).hexdigest(), "visual_preview_available": path.stat().st_size <= 8 * 1024 * 1024,
            "visual_content_verified": False, "preview_guidance": "Inspect the rendered pixels before final delivery; preserve original PNG bytes."})
    return handler
