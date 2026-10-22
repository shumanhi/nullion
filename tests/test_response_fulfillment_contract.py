from nullion.response_fulfillment_contract import evaluate_response_fulfillment
from nullion.runtime_store import RuntimeStore
from nullion.tools import ToolResult


def test_completed_web_search_preserves_model_prose():
    reply = (
        "I fetched the page. If you want, I can pull a concise top headlines "
        "briefing now."
    )

    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="web:abc123",
        user_message="fetch current site results",
        reply=reply,
        tool_results=[
            ToolResult(
                "search-1",
                "web_search",
                "completed",
                {
                    "results": [
                        {
                            "title": "Structured current result",
                            "url": "https://www.cnn.com/",
                            "snippet": "View the latest news and breaking news today.",
                        },
                        {
                            "title": "Latest updates",
                            "url": "https://www.cnn.com/live",
                            "snippet": "Updated 8:39 PM EDT.",
                        },
                    ]
                },
            )
        ],
    )

    assert decision.satisfied
    assert decision.reply == reply


def test_delivered_artifact_replaces_stale_attachment_failure_reply(tmp_path):
    workbook = tmp_path / "stage_archive_transform_field_notes.xlsx"
    workbook.write_bytes(b"PK\x03\x04fake workbook")

    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="web:abc123",
        user_message="structured archive transform",
        reply="I couldn't attach all of the requested files. The task is still open.",
        tool_results=[
            ToolResult(
                "xlsx-1",
                "spreadsheet_create",
                "completed",
                {
                    "artifact_path": str(workbook),
                    "artifact_descriptors": [
                        {
                            "path": str(workbook),
                            "role": "deliverable",
                            "extension": ".xlsx",
                        }
                    ],
                },
            )
        ],
        artifact_roots=[tmp_path],
        platform_artifact_count=1,
    )

    assert decision.satisfied
    assert decision.reply.startswith("Done")
    assert "couldn't attach" not in decision.reply
    assert f"MEDIA:{workbook}" in decision.reply


def test_terminal_created_requested_artifact_replaces_stale_attachment_failure_reply(tmp_path):
    workbook = tmp_path / "lizzie_fortunato_necklaces_first_107.xlsx"
    workbook.write_bytes(b"PK\x03\x04fake workbook")

    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="telegram:100001",
        user_message="I need an excel for first 107 products",
        reply="I couldn't attach the requested file. The task is still open.",
        tool_results=[
            ToolResult(
                "shell-1",
                "terminal_exec",
                "completed",
                {
                    "exit_code": 0,
                    "stdout": f"Created workbook: {workbook}\nRows: 107\n",
                },
            )
        ],
        artifact_roots=[tmp_path],
        required_attachment_extensions=(".xlsx",),
    )

    assert decision.satisfied
    assert "couldn't attach" not in decision.reply
    assert f"MEDIA:{workbook}" in decision.reply


def test_terminal_stdout_pure_path_listing_is_not_artifact_delivery(tmp_path):
    workbook = tmp_path / "already_existing_listing.xlsx"
    workbook.write_bytes(b"PK\x03\x04fake workbook")

    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="web:abc123",
        user_message="Find this workbook in my workspace.",
        reply="I found already_existing_listing.xlsx.",
        tool_results=[
            ToolResult(
                "shell-1",
                "terminal_exec",
                "completed",
                {
                    "exit_code": 0,
                    "stdout": f"{workbook}\n",
                    "stderr": "",
                },
            )
        ],
        artifact_roots=[tmp_path],
        required_attachment_extensions=(".xlsx",),
    )

    assert decision.satisfied is False
    assert "MEDIA:" not in decision.reply


def test_terminal_stdout_paths_must_exist_under_artifact_roots(tmp_path):
    outside = tmp_path.parent / "outside.xlsx"
    outside.write_bytes(b"PK\x03\x04outside")

    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="telegram:100001",
        user_message="I need an excel file",
        reply="Done.",
        tool_results=[
            ToolResult(
                "shell-1",
                "terminal_exec",
                "completed",
                {"exit_code": 0, "stdout": f"Created workbook: {outside}\n"},
            )
        ],
        artifact_roots=[tmp_path],
        required_attachment_extensions=(".xlsx",),
    )

    assert decision.satisfied is False
    assert decision.reply == "I couldn't attach the requested file."


def test_scheduler_mutation_defers_future_artifact_requirements_to_cron_run():
    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="web:scheduler-future-artifacts",
        user_message="Set up a cron that creates xlsx and pdf artifacts when it runs.",
        reply="Cron created.",
        tool_results=[
            ToolResult(
                "cron-1",
                "create_cron",
                "completed",
                {"message": "Cron created."},
            )
        ],
        required_attachment_extensions=(".xlsx", ".pdf"),
        required_embedded_media_extensions=(".xlsx",),
    )

    assert decision.satisfied
    assert decision.reply == "Cron created."


def test_model_prose_is_preserved_without_completed_tool_evidence():
    reply = "If you want, I can pull a concise top headlines briefing now."

    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="web:abc123",
        user_message="fetch current site results",
        reply=reply,
        tool_results=[
            ToolResult(
                "browser-1",
                "browser_navigate",
                "failed",
                {},
                "Navigation failed",
            )
        ],
    )

    assert decision.satisfied
    assert decision.reply == reply


def test_completed_web_search_fallback_uses_typed_summary_not_prompt_language():
    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="web:abc123",
        user_message="最新情報を確認して",
        reply="(no reply)",
        tool_results=[
            ToolResult(
                "search-1",
                "web_search",
                "completed",
                {
                    "summary": "<b>現在の結果</b>: 構造化されたツール出力から取得しました。",
                    "results": [{"title": "Ignored raw row", "url": "https://example.com/result"}],
                },
            )
        ],
    )

    assert decision.satisfied
    assert decision.reply == "現在の結果: 構造化されたツール出力から取得しました。"
    assert "Ignored raw row" not in decision.reply
    assert "<b>" not in decision.reply


def test_completed_web_search_fallback_does_not_render_raw_result_rows():
    decision = evaluate_response_fulfillment(
        store=RuntimeStore(),
        conversation_id="telegram:123",
        user_message="run scheduled task",
        reply="",
        tool_results=[
            ToolResult(
                "search-1",
                "web_search",
                "completed",
                {
                    "results": [
                        {
                            "title": "<b>Category</b>",
                            "url": "https://search.example/redirect?target=opaque&amp;tracking=1",
                            "snippet": "",
                        },
                        {
                            "title": "Another raw result",
                            "url": "https://search.example/next",
                            "snippet": "Raw snippets are evidence, not final delivery text.",
                        },
                    ]
                },
            )
        ],
    )

    assert decision.satisfied
    assert decision.reply == "The search completed, but it did not return a clean user-visible summary."
    assert "<b>" not in decision.reply
    assert "search.example" not in decision.reply
    assert "Raw snippets" not in decision.reply
