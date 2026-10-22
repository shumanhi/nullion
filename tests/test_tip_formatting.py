from __future__ import annotations


def test_user_facing_tip_formatter_prefixes_bulb() -> None:
    from nullion.tips import (
        IMAGE_GENERATION_SETUP_TIP,
        WEB_RESEARCH_SETUP_TIP,
        format_setup_tip,
        format_tip,
        setup_tip_instruction,
    )

    assert format_tip("try again later") == "💡 Tip: try again later"
    assert format_tip("add a search key", label="Setup tip") == "💡 Setup tip: add a search key"
    assert format_setup_tip(IMAGE_GENERATION_SETUP_TIP).startswith("💡 Setup tip:")
    assert "image generation provider in Settings" in format_setup_tip(IMAGE_GENERATION_SETUP_TIP)
    assert setup_tip_instruction(WEB_RESEARCH_SETUP_TIP).startswith(
        "include one short setup tip in this format: 💡 Setup tip:"
    )


def test_media_provider_setup_output_uses_shared_setup_tip() -> None:
    from nullion import tools
    from nullion.tools import ToolInvocation

    result = tools._missing_media_provider_result(
        ToolInvocation(
            invocation_id="inv-media",
            tool_name="image_generate",
            principal_id="operator",
            arguments={},
        ),
        "image_generate",
    )

    assert result.output["reason"] == "provider_not_configured"
    assert str(result.output["setup"]).startswith("💡 Setup tip:")
    assert "NULLION_IMAGE_GENERATE_COMMAND" in str(result.output["setup"])
