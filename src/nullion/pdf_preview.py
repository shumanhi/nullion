"""Bounded previews of exact PDF bytes, for the existing composer to inspect."""
from hashlib import sha256
from pathlib import Path
from threading import Lock

# PDFium must never run concurrently in threads, even for distinct documents.
_render_lock = Lock()


def render_pdf_previews(path: Path) -> dict:
    if path.stat().st_size > 20 * 1024 * 1024:
        return {"visual_preview_available": False, "preview_reason": "pdf_size_limit"}
    try:
        import pypdfium2 as pdfium
        from io import BytesIO
        import os
        from uuid import uuid4

        data = path.read_bytes()
        digest = sha256(data).hexdigest()
        root = path.parent / ".nullion-pdf-previews"
        root.mkdir(exist_ok=True)
        previews = []
        with _render_lock, pdfium.PdfDocument(data) as document:
            total = len(document)
            for index in range(min(total, 3)):
                page = document[index]
                try:
                    width, height = page.get_size()
                    bitmap = page.render(scale=min(2.0, 1600 / max(width, height, 1)))
                    try:
                        image = bitmap.to_pil()
                        try:
                            with BytesIO() as buffer:
                                image.convert("RGB").save(buffer, format="JPEG", quality=88)
                                pixels = buffer.getvalue()
                        finally:
                            image.close()
                    finally:
                        bitmap.close()
                finally:
                    page.close()
                target = root / f"{digest}-page-{index + 1}.jpg"
                temporary = root / f".{uuid4().hex}.tmp"
                try:
                    temporary.write_bytes(pixels)
                    os.replace(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
                previews.append({"path": str(target), "image_sha256": sha256(pixels).hexdigest(),
                                 "page_number": index + 1})
        return {"pdf_sha256": digest, "rendered_page_previews": previews,
                "preview_page_count": len(previews), "unpreviewed_page_count": total - len(previews),
                "visual_preview_available": bool(previews), "visual_content_verified": False,
                "preview_guidance": "Inspect these rendered PDF pages for clipping, readability and layout before finalizing. "
                "Text extraction and rendering success alone do not verify visual quality. Preview files are internal."}
    except Exception:
        return {"visual_preview_available": False, "preview_reason": "pdf_render_unavailable",
                "visual_content_verified": False}
