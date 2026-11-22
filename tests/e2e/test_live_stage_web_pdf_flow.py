"""Opt-in real-provider PDF flow through the stage Web composer and Download.

The bounds/gap checks are specific to this text-only synthetic report, without
a cover, images or requested page boundaries. Saved PNGs still require visual
inspection; extracted text and geometry do not establish pixel-level quality.
Primary design references: docs/pdf-layout-guidelines.md, Playwright page.pdf
(print media/scale), and W3C CSS Fragmentation section 4.5 (unforced breaks).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue
from tests.e2e.live_web_artifact_assertions import pdf_web, save_artifact_through_ui, pdf_source_phrase_visible

pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_LIVE_STAGE_E2E") != "1", reason="Opt-in live stage PDF acceptance"
)


def _synthetic_records():
    names = ["Aurora", "Birch", "Cedar", "Delta", "Elm", "Fern", "Grove", "Harbor", "Iris"]
    return [
        {
            "record_id": f"SYNTH-{index:02d}",
            "title": f"{name} community workshop",
            "overview": (
                f"The fictional {name} workshop is preparing its next public demonstration. "
                f"Its sample inventory contains {index + 12} reusable kits and {index + 4} spare labels. "
                "The team will arrange the kits on ordinary tables, retain a clear walking route, "
                "and place written instructions beside each activity. This record is synthetic "
                "and does not describe a real organization, person, reservation, or purchase. "
                "The report should preserve the whole description so a reader can compare "
                "the preparation work without opening another document."
            ),
            "details": (
                f"For {name}, the first review covers inventory labels and the second covers "
                "the written demonstration sequence. A reviewer should check that every step "
                "has a clear owner, that spare materials remain easy to identify, and that "
                "the final handover explains where the equipment belongs. Record the review "
                "result before packing the kits. Keep this full paragraph and its closing "
                f"checkpoint in the delivered report: CHECK-{index:02d}-COMPLETE."
            ),
        }
        for index, name in enumerate(names, 1)
    ]


def _normalized(text):
    return " ".join(text.split())


def _inspect_pdf(body, destination):
    import pypdfium2 as pdfium

    pages = []
    with pdfium.PdfDocument(body) as document:
        count = len(document)
        # Save every page of this small report, including a nine-page failure
        # caused by reserving one sheet per record. Bound pathological output
        # and disclose that limit instead of claiming unseen-page inspection.
        for number in range(min(count, 12)):
            page = document[number]
            bitmap = page.render(scale=1.4)
            text = page.get_textpage()
            try:
                image = f"page-{number + 1:02d}.png"
                bitmap.to_pil().save(destination / image)
                chars = []
                for index in range(text.count_chars()):
                    character = text.get_text_range(index, 1)
                    if not character.strip():
                        continue
                    left, bottom, right, top = text.get_charbox(index)
                    chars.append({"left": left, "bottom": bottom, "right": right, "top": top,
                        "size": pdfium.raw.FPDFText_GetFontSize(text.raw, index)})
                # Merge occupied vertical bands. This fixture contains text
                # only, so a very large gap between bands is unexplained space.
                bands = []
                for char in sorted(chars, key=lambda value: value["bottom"]):
                    if bands and char["bottom"] <= bands[-1][1] + 1:
                        bands[-1][1] = max(bands[-1][1], char["top"])
                    else:
                        bands.append([char["bottom"], char["top"]])
                pages.append({"page": number + 1, "image": image, "text": text.get_text_range(),
                    "width": page.get_width(), "height": page.get_height(), "chars": chars,
                    "largest_interior_gap_pt": max(
                        (right[0] - left[1] for left, right in zip(bands, bands[1:])), default=0)})
            finally:
                text.close()
                bitmap.close()
                page.close()
    return count, pages


def test_live_web_pdf_report_flows_without_clipping_or_large_gaps(pdf_web, record_property):
    from playwright.sync_api import expect

    live_web=pdf_web.surface
    page, frames, errors, database, conversation, base = live_web
    records = _synthetic_records()
    prompt = (
        "Crea un informe PDF descargable titulado 'Synthetic workshop readiness'. "
        "Usa únicamente los nueve registros ficticios JSON adjuntos a este mensaje. "
        "Conserva literalmente cada record_id, title, overview y details, incluidos los CHECK finales; "
        "no resumas ni elimines contenido. Diseña un informe A4 continuo con texto de al menos 11 puntos, "
        "márgenes normales y títulos claros junto a sus párrafos. Deja que el contenido fluya naturalmente "
        "entre hojas. No pido un número exacto de páginas ni una hoja por registro. "
        "Sin portada, imágenes, pies de página, paneles de altura fija ni saltos deliberados. "
        "Entrega un único PDF y una confirmación breve. No consultes fuentes externas ni envíes mensajes.\n\n"
        + json.dumps({"records": records}, ensure_ascii=False)
    )
    evidence = Path(os.environ.get("NULLION_LIVE_EVIDENCE_DIR", "/tmp/live-web-evidence")) / "pdf-flow" / conversation.replace(":", "_")
    evidence.mkdir(parents=True, exist_ok=True, mode=0o700)
    (evidence / "synthetic-source.json").write_text(json.dumps(records, ensure_ascii=False, indent=2))
    response, turn, elapsed = pdf_web.saved if pdf_web.replay else _submit(live_web, prompt, timeout=300)
    receipts = [result for result in turn.get("tool_results", []) if result.get("tool_name") == "pdf_create"]
    proof = {"conversation_id": conversation, "turn_id": turn.get("turn_id"), "elapsed_ms": elapsed,
        "reply": response["text"], "artifacts": response.get("artifacts", []), "pdf_create_receipts": receipts,
        "evidence_kind":"saved_boundary_reuse" if pdf_web.replay else "fresh_generation", "replay":pdf_web.replay,
        "visual_review": "pending: inspect every saved page PNG; geometry is not pixel review"}
    proof_path = evidence / "result.json"
    proof_path.write_text(json.dumps(proof, ensure_ascii=False, indent=2))
    assert turn.get("response_fulfilled") is True, turn
    completed = [result["output"] for result in receipts if result.get("status") == "completed"]
    assert completed, receipts
    assert completed[-1]["pagination"] == "flow", completed[-1]
    assert completed[-1]["pagination_receipt"]["mode"] == "flow", completed[-1]
    assert completed[-1]["pagination_receipt"]["scale"] == 1, completed[-1]
    artifacts = response.get("artifacts", [])
    assert len(artifacts) == 1 and catalogue._artifact_name(artifacts[0]).lower().endswith(".pdf"), response
    filename = Path(catalogue._artifact_name(artifacts[0])).name
    download_link = page.locator("#messages .artifact-list a.artifact-link[href="+json.dumps(artifacts[0]["url"])+"]")
    expect(download_link).to_have_text(f"Download {filename}")
    expect(download_link).to_have_count(1)
    download_link.scroll_into_view_if_needed()
    expect(download_link).to_be_visible()
    body,save_receipt=save_artifact_through_ui(page,base,artifacts[0],download_link)
    proof['save_receipt']=save_receipt
    (evidence / "report.pdf").write_bytes(body)
    page.screenshot(path=str(evidence / "web.png"), full_page=False)
    assert body.startswith(b"%PDF-"), body[:40]
    assert Path(completed[-1]["artifact_path"]).name == filename, completed[-1]
    assert completed[-1]["bytes_written"] == len(body), completed[-1]
    count, pages = _inspect_pdf(body, evidence)
    proof["pages"] = pages
    proof["pdf_page_count"] = count
    proof["rendered_page_count"] = len(pages)
    proof["artifact_bytes"] = len(body)
    proof_path.write_text(json.dumps(proof, ensure_ascii=False, indent=2))
    record_property("pdf_page_images", json.dumps([str(evidence / item["image"]) for item in pages]))
    record_property("pdf_latency_ms", elapsed)
    assert count >= 2, f"The complete readable fixture must naturally span multiple pages, got {count}"
    assert count == len(pages), f"Unexpectedly large report: rendered only {len(pages)} of {count} pages"
    from scripts.live_stage_chat_contract_probe import _validate_pdf_visual_content
    logical_text,_ = _validate_pdf_visual_content(filename,body)
    raw_page_text = "\n".join(item["text"] for item in pages)
    proof["logical_visible_text"] = logical_text
    proof["text_oracle"] = "shared logical text; source dash tokens may cross a physical line break"
    proof_path.write_text(json.dumps(proof, ensure_ascii=False, indent=2))
    for record in records:
        for field, value in record.items():
            assert pdf_source_phrase_visible(value,logical_text,raw_page_text), (record["record_id"], field, logical_text)
    assert completed[-1]["page_count"] == count, completed[-1]
    for index, item in enumerate(pages):
        assert abs(item["width"] - 595.28) < 1 and abs(item["height"] - 841.89) < 1, item
        chars = item["chars"]
        assert chars and min(char["size"] for char in chars) >= 10.5, (index, chars)
        assert all(char["left"] >= 24 and char["right"] <= item["width"] - 24
            and char["bottom"] >= 24 and char["top"] <= item["height"] - 24 for char in chars), item
        assert item["largest_interior_gap_pt"] < 96, item
        if index < len(pages) - 1:
            # No cover, figures, forced breaks or footer in this fixture: an
            # ordinary interior sheet must not leave most of its body empty.
            assert min(char["bottom"] for char in chars) < item["height"] * .25, item
    assert not errors, errors


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_CODEMODE_RECALL_SOURCE'),reason='Owned existing PDF fixture required')
def test_live_web_code_mode_recalls_original_pdf_with_native_reference_receipt(live_web,record_property):
    """Fresh history recall and UI save; source PDF generation is retained."""
    import hashlib
    from playwright.sync_api import expect
    source=json.loads(os.environ['NULLION_LIVE_CODEMODE_RECALL_SOURCE'])
    assert isinstance(source,dict)
    filename=source.get('filename');source_path=source.get('path');expected_sha=source.get('sha256')
    assert isinstance(filename,str) and filename==Path(filename).name and filename.lower().endswith('.pdf'),source
    assert isinstance(source_path,str) and Path(source_path).is_absolute() and Path(source_path).name==filename,source
    assert isinstance(expected_sha,str) and len(expected_sha)==64 and int(expected_sha,16)>=0,source
    page,frames,errors,database,conversation,base=live_web
    prompt=('Use Code Mode to call chat_history_search and find '+json.dumps(filename)+
        ' in saved history. Send the original existing PDF using its native available artifact reference. '
        'Do not regenerate, rewrite, convert, or create any file. Return a brief confirmation and the original PDF.')
    if source.get('source_turn_id'):
        prompt+=' The original saved source turn is '+json.dumps(source['source_turn_id'])+'.'
    response,turn,elapsed=_submit(live_web,prompt,timeout=180)
    output=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))/'code-mode-pdf-recall'/conversation.replace(':','_')
    output.mkdir(parents=True,exist_ok=True,mode=0o700)
    proof={'conversation_id':conversation,'turn_id':turn.get('turn_id'),'source':source,
        'evidence_kind':'fresh_recall_existing_artifact','fresh_source_generation':False,
        'elapsed_ms':elapsed,'response':response,'tool_results':turn.get('tool_results',[])}
    proof_path=output/'result.json';proof_path.write_text(json.dumps(proof,ensure_ascii=False,indent=2))
    assert turn.get('response_fulfilled') is True,turn
    results=turn.get('tool_results',[])
    allowed={'request_tool_scope','chat_history_search','memory_search','code_mode','file_read',
        'file_search','workspace_summary','inspect_retained_evidence'}
    assert all(result.get('tool_name') in allowed for result in results),results
    histories={result['invocation_id']:result for result in results
        if result.get('tool_name')=='chat_history_search' and result.get('status')=='completed'}
    assert histories,results
    bridges=[]
    for program in results:
        if program.get('tool_name')!='code_mode' or program.get('status')!='completed':continue
        envelope=program.get('output',{})
        assert envelope.get('automatic_replay') is False,envelope
        operations=envelope.get('operations',[])
        assert all(item.get('operation_kind')=='read' for item in operations),operations
        owned={item['invocation_id'] for item in operations if item.get('tool_name')=='chat_history_search'
            and item.get('status')=='completed' and item.get('invocation_id') in histories}
        for receipt in envelope.get('native_artifact_receipts',[]):
            artifact=receipt.get('artifact',{})
            if receipt.get('invocation_id') not in owned or artifact.get('path')!=source_path:continue
            assert artifact.get('role')=='reference' and artifact.get('availability')=='available',receipt
            inspection=receipt.get('inspection_reference',{})
            assert inspection.get('invocation_id')==receipt['invocation_id'],receipt
            pointer=inspection.get('json_pointer')
            assert isinstance(pointer,str) and pointer.startswith(('/matches/','/structured_matches/')),receipt
            tokens=[value.replace('~1','/').replace('~0','~') for value in pointer.split('/')[1:]]
            native=histories[receipt['invocation_id']]['output']
            selected=native
            for token in tokens:selected=selected[int(token)] if isinstance(selected,list) else selected[token]
            assert selected.get('path')==source_path and selected.get('availability')=='available',selected
            owner=native[tokens[0]][int(tokens[1])]
            if source.get('source_turn_id'):assert owner.get('turn_id')==source['source_turn_id'],owner
            if source.get('source_conversation_id'):assert owner.get('conversation_id')==source['source_conversation_id'],owner
            bridges.append({'program_invocation_id':program['invocation_id'],'receipt':receipt,'source_owner':owner})
    assert bridges,'No native history call inside Code Mode supplied the exact original reference receipt'
    artifacts=response.get('artifacts',[])
    assert len(artifacts)==1 and catalogue._artifact_name(artifacts[0])==filename,response
    artifact=artifacts[0]
    control=page.locator('#messages .artifact-list a.artifact-link[href='+json.dumps(artifact['url'])+']')
    expect(control).to_have_count(1);control.scroll_into_view_if_needed()
    expect(control).to_have_text('Download '+filename)
    body,save_receipt=save_artifact_through_ui(page,base,artifact,control)
    assert body.startswith(b'%PDF-') and hashlib.sha256(body).hexdigest()==expected_sha.lower(),save_receipt
    assert Path(artifact['path'])==Path(source_path),artifact
    (output/'original.pdf').write_bytes(body)
    page.screenshot(path=str(output/'web.png'),full_page=False)
    proof.update(native_reference_bridges=bridges,save_receipt=save_receipt,artifact_sha256=hashlib.sha256(body).hexdigest())
    proof_path.write_text(json.dumps(proof,ensure_ascii=False,indent=2))
    record_property('code_mode_history_recall_latency_ms',elapsed)
    assert not errors,errors
