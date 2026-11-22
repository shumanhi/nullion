# Saved Web artifact acceptance

The normal capture and PDF probes submit fresh requests through the stage Web composer. Their optional replay modes restore an existing, completed test conversation through the default history endpoint and UI. They validate the exact native turn, visible artifact card, native **Saved** receipt, and served file bytes without submitting a chat request or cleanup command.

Set `NULLION_LIVE_CAPTURE_REPLAY` or `NULLION_LIVE_PDF_REPLAY` to JSON containing the exact `conversation_id` and `turn_id`. These modes accept only the probes' owned `web:live-ui-audit:` conversations. Capture reuse also requires `latency_frames_path`, the retained JSONL frame evidence from its original generation run. A relative path resolves against the checkout.

```json
{
  "conversation_id": "web:live-ui-audit:OWNED_ID",
  "turn_id": "turn:ORIGINAL_ID",
  "latency_frames_path": ".e2e-artifacts/ORIGINAL_RUN/active-frames/ORIGINAL_CONVERSATION.jsonl"
}
```

The wrapper passes both replay variables to the container. Run only the intended probe selectors. Leave the variables unset for fresh generation.

Replay evidence is labeled `saved_boundary_reuse` and `fresh_generation: false`. Its native source generation and tool receipts are retained evidence; its default-history rendering and save interaction are current UI checks. Capture's three plain-turn latency intervals are derived from the original submitted/done frame pairs and labeled `retained_generation_timing`, with the source file hash. Replay does not report a new generation latency.

The Web Download control posts to the artifact's `/save` route and changes to **Saved**. The response supplies its native destination, name, and byte count. The probe records that receipt and the source artifact hash. When validating a host runtime, verify the exact receipt destination's bytes separately; a successful source GET alone does not prove the host file was saved. PDF page images still require visual inspection.

Saved reuse sets the existing explicit-conversation selection flag before page load. Otherwise Web startup deliberately selects the latest active conversation, which can be a different completed probe. History still uses the normal endpoint without `sync_runtime` or a fabricated transcript.

PDF content checks first use the shared Poppler logical-text oracle. A source token containing a dash may additionally span a physical PDF line break immediately after that dash, with indentation on the next line. This preserves normal word-space requirements and rejects a literal added space on one line or fused required words. Raw per-page text, source records, logical text, and artifact hashes remain in the evidence; page images still need visual review.

## Code Mode history recall

`test_live_web_code_mode_recalls_original_pdf_with_native_reference_receipt` is separate from the PDF generation probe. It submits one fresh recall request through the Web composer and saves the original existing file. It never runs the nine-record generation request.

Set `NULLION_LIVE_CODEMODE_RECALL_SOURCE` to a JSON object with `filename`, the native absolute `path`, and the original `sha256`. Optional `source_turn_id` and `source_conversation_id` bind the returned reference to the original history record. The probe does not include the expected path or hash in its model prompt. It verifies them against native history, Code Mode's operation and reference receipts, delivered bytes, and the actual Web Saved response. Its evidence is labeled `fresh_recall_existing_artifact`; source generation remains retained evidence.
