# PDF layout and pagination

Use the existing native PDF producer with semantic content and a print layout.
Ordinary reports should flow naturally across pages at readable size. A supplied
content section is not, by itself, a reason to reserve an entire sheet. Deliberate
page boundaries belong to an explicit page-layout contract, such as an exact
page count or a separately designed poster.

## Layout rules

- Set paper size and margins together. Measure with print media and the same
  usable width used by the final PDF. Let content determine panel height. Resolve
  supplied fonts before measuring, using a bounded wait; do not add remote fonts
  or network calls merely to render an ordinary report.
  The producer's selected paper and usable area govern custom HTML as well,
  including named page rules, so a conflicting CSS sheet cannot silently shrink
  the design to another paper size. The current HTML profile reserves 0.42-inch
  margins; the text profile reserves 0.45-inch margins.
- Keep headings with the first lines or small block they introduce. Use widows
  and orphans to avoid isolated lines; let long paragraphs split between lines.
- Keep compact cards, captions and ordinary table rows together when practical.
  Allow long containers, tables and oversized cards to continue on another page.
  Never apply `break-inside: avoid` to every section or the entire table.
- Repeat table headers on continuation pages. Wrap long URLs and cell text.
  Use a wider page or a different table arrangement when columns cannot remain
  readable; do not squeeze a dense report into tiny text.
- Preserve an image's aspect ratio and full frame. Limit an indivisible image
  to the usable page area. Crop only artwork explicitly marked as decorative.
- Use consistent type, line spacing, heading hierarchy and restrained spacing.
  Keep related text closer than separate sections. Do not introduce fixed-height
  panels merely to align their bottoms.
- Avoid fixed-height overflow containers, absolute-positioned body content and
  screenshot slices as substitutes for paginated text. Preserve the full source
  content; an extra page is preferable to clipped or unreadable content.
- Treat an exact page count as a constraint to validate. If content cannot fit
  legibly, repair the layout or report the constraint failure. Do not discard
  content or silently shrink the complete document to force a fit.

Whitespace is sometimes necessary to keep a figure or short related block
together. A short final page, a deliberate cover, and an explicit section break
are valid. Large gaps on ordinary interior pages should prompt inspection of
forced breaks, oversized keep-together groups, fixed heights and scaling. A
single occupancy threshold cannot establish that every design is good.

## Verification

Exercise the actual producer and delivered PDF bytes. Check page dimensions,
complete text, images, table continuation, heading placement and text bounds.
Include both A4 and Letter, long content, mixed scripts, and explicit page layouts.
Render the resulting pages and inspect typography, clipping, overlap and gaps at
readable size. Successful text extraction or a valid PDF container does not prove
visual quality. Inspect every page of a small fixture and disclose any bounded
preview limit for longer documents; do not claim unseen pages were reviewed.

## Primary references

- [W3C CSS Fragmentation](https://www.w3.org/TR/css-break-3/): break controls,
  widows/orphans, overflow fallback, and the tradeoff between filling pages and
  avoiding awkward breaks.
- [W3C CSS Paged Media](https://www.w3.org/TR/css-page-3/): paper size, page area,
  margins and content outside the page box. This is a draft specification;
  verify actual Chromium support rather than assuming every feature is available.
- [Playwright PDF API](https://playwright.dev/python/docs/api/class-page#page-pdf):
  print-media rendering, paper options, CSS page-size precedence and scale.
- [W3C table model](https://www.w3.org/TR/CSS22/tables.html#table-display): table
  header groups may repeat in print; verify that behavior in the actual renderer.
- [W3C font loading](https://www.w3.org/TR/css-font-loading-3/#font-face-set-ready):
  font readiness and its effect on layout measurement.

Research reconciled with the installed Chromium/Playwright generation path on
November 21, 2026. These rules guide the shared producer and its metadata; they
do not add per-request research or a separate classification call.
