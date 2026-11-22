"""Bounded previews of exact PDF bytes, for the existing composer to inspect."""
from hashlib import sha256
from pathlib import Path
from threading import Lock

# PDFium must never run concurrently in threads, even for distinct documents.
_render_lock = Lock()



def render_single_page_pdf_png(data: bytes, *, width: int, height: int) -> bytes:
    """Render a complete single-page document inside the observed viewport."""
    from io import BytesIO

    from PIL import Image
    import pypdfium2 as pdfium

    if not data.startswith(b"%PDF-") or len(data) > 20 * 1024 * 1024:
        raise ValueError("Incomplete or invalid PDF")
    if not 1 <= width <= 4096 or not 1 <= height <= 4096:
        raise ValueError("Unsupported PDF viewport")
    with _render_lock, pdfium.PdfDocument(data) as document:
        if len(document) != 1:
            raise ValueError("A single viewport cannot represent the entire PDF")
        page = document[0]
        try:
            page_width, page_height = page.get_size()
            if page_width <= 0 or page_height <= 0:
                raise ValueError("Invalid PDF page size")
            bitmap = page.render(scale=min(width / page_width, height / page_height))
            try:
                with bitmap.to_pil() as image, Image.new("RGB", (width, height), "white") as canvas:
                    # Check the page itself before letterboxing can add contrast.
                    if not any(low != high for low, high in image.convert("RGB").getextrema()):
                        raise ValueError("PDF page is visually blank")
                    canvas.paste(image, ((width - image.width) // 2, (height - image.height) // 2))
                    with BytesIO() as output:
                        canvas.save(output, format="PNG")
                        return output.getvalue()
            finally:
                bitmap.close()
        finally:
            page.close()

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
