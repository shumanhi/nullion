from nullion.attachment_format_graph import plan_attachment_format
from nullion.chat_operator import _chat_ambiguity_fallback
from nullion.telegram_app import _is_telegram_message_not_modified_error


def test_chat_ambiguity_fallback_does_not_continue_from_prior_url_reply():
    thread = [
        {
            "user": "Can you fetch latest news from cnn.com?",
            "assistant": "Here is the result: https://www.cnn.com/",
        }
    ]

    fallback, reason = _chat_ambiguity_fallback(thread, "Whats weather like tomorrow?")

    assert reason == "url"
    assert fallback("Whats weather like tomorrow?", True) is None


def test_attachment_format_uses_literal_extension_evidence():
    assert plan_attachment_format("save as .pdf").extension == ".pdf"
    assert plan_attachment_format("attach /tmp/report.xlsx").extension == ".xlsx"


def test_telegram_message_not_modified_bad_request_is_idempotent():
    BadRequest = type("BadRequest", (Exception,), {})

    assert _is_telegram_message_not_modified_error(BadRequest("Message is not modified"))
    assert not _is_telegram_message_not_modified_error(BadRequest("can't parse entities"))
