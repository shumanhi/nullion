import pytest

from nullion import telegram_app


class ReplyMessage:
    def __init__(self) -> None:
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


@pytest.mark.asyncio
async def test_callback_follow_up_does_not_require_attachment_for_plain_text():
    message = ReplyMessage()

    await telegram_app._send_callback_follow_up(
        message,
        "Here are the latest CNN headlines.",
        principal_id="telegram_chat",
    )

    assert message.replies
    assert message.replies[-1][0] == "Here are the latest CNN headlines."
    assert "couldn't attach" not in message.replies[-1][0].lower()
