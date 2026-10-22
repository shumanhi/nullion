from __future__ import annotations

from nullion.attachment_format_graph import plan_attachment_format
import pytest


class FakeFormatModel:
    def __init__(self, extension: str | None, embedded_media_extensions: tuple[str, ...] = ()) -> None:
        self.extension = extension
        self.embedded_media_extensions = embedded_media_extensions

    def create(self, **kwargs):  # noqa: ANN003
        assert kwargs["tools"][0]["name"] == "select_attachment_format"
        return {
            "content": [
                {
                    "type": "tool_use",
                    "name": "select_attachment_format",
                    "input": {
                        "extension": self.extension,
                        "confidence": 0.92,
                        "embedded_media_extensions": list(self.embedded_media_extensions),
                    },
                }
            ]
        }


@pytest.mark.parametrize('prompt',[
    'Save a reusable tool with JSON schemas; return its result in chat.',
    'JSONスキーマで再利用する関数を保存して、結果はここに表示して',
    'Guarda una herramienta con esquema JSON; muestra el resultado aquí',
])
def test_internal_structured_output_is_not_a_downloadable_attachment(prompt):
    class Internal(FakeFormatModel):
        def create(self,**kwargs):
            result=super().create(**kwargs)
            result['content'][0]['input']['output_kind']='internal'
            return result
    plan=plan_attachment_format(prompt,model_client=Internal('.json'))
    assert plan.extension is None and plan.embedded_media_extensions==()


def test_decimal_arguments_are_not_literal_file_extensions():
    plan=plan_attachment_format('Run with [{"price":4.5,"quantity":3},{"price":7.25,"quantity":2}]')
    assert plan.extension is None


@pytest.mark.parametrize('prompt',['Save a tool with JSON schemas, no attachment.',
    'Guarda una herramienta con esquemas JSON; sin adjunto.', 'JSON スキーマで関数を保存。添付なし。'])
def test_bare_format_in_schema_does_not_override_the_structured_internal_plan(prompt):
    from nullion.chat_operator import _literal_requested_attachment_extensions,_requested_attachment_extensions
    class Internal(FakeFormatModel):
        def create(self,**kw):
            result=super().create(**kw);result['content'][0]['input']['output_kind']='internal'
            return result
    assert _literal_requested_attachment_extensions(prompt)==()
    assert _requested_attachment_extensions(prompt,model_client=Internal('.json'),allow_model_planning=True)==()


def test_attachment_format_graph_plans_explicit_formats() -> None:
    text = plan_attachment_format("artifact.txt")
    word = plan_attachment_format("artifact.docx")
    missing = plan_attachment_format("send it")

    assert text.extension == ".txt"
    assert text.evidence == "literal_extension"
    assert word.extension == ".docx"
    assert missing.extension is None


def test_attachment_format_graph_prefers_artifact_path_over_workspace_state_file() -> None:
    plan = plan_attachment_format(
        "state=/workspace/files/nullion_usage_buzz_history.json "
        "artifact=/workspace/artifacts/nullion_usage_buzz_report.html"
    )

    assert plan.extension == ".html"
    assert plan.evidence == "literal_extension"


def test_attachment_format_graph_plans_audio_artifact_extensions() -> None:
    plan = plan_attachment_format("artifact=/workspace/artifacts/daily-summary.mp3")
    fallback = plan_attachment_format("artifact=/workspace/artifacts/daily-summary.wav")

    assert plan.extension == ".mp3"
    assert plan.evidence == "literal_extension"
    assert fallback.extension == ".wav"


def test_attachment_format_graph_plans_video_artifact_extensions() -> None:
    plan = plan_attachment_format("artifact=/workspace/artifacts/demo-reel.mp4")
    fallback = plan_attachment_format("artifact=/workspace/artifacts/demo-reel.mov")

    assert plan.extension == ".mp4"
    assert plan.evidence == "literal_extension"
    assert fallback.extension == ".mov"


def test_attachment_format_graph_uses_file_path_suffix_without_known_type_list() -> None:
    plan = plan_attachment_format("artifact=/workspace/artifacts/model-output.blend")
    explicit = plan_attachment_format(".sqlite")

    assert plan.extension == ".blend"
    assert plan.evidence == "literal_extension"
    assert explicit.extension == ".sqlite"


def test_attachment_format_graph_does_not_treat_domains_as_unknown_extensions() -> None:
    plan = plan_attachment_format("brand=https://nullion.ai source=Nullion.ai")

    assert plan.extension is None


def test_attachment_format_graph_does_not_treat_hidden_app_path_as_extension() -> None:
    hidden_app = plan_attachment_format("No there's a hermes app in ~/.hermes")
    hidden_file = plan_attachment_format("Inspect /tmp/.env")
    explicit_extension = plan_attachment_format(".sqlite")

    assert hidden_app.extension is None
    assert hidden_file.extension is None
    assert explicit_extension.extension == ".sqlite"


def test_attachment_format_graph_does_not_treat_ellipsis_text_as_extension() -> None:
    plan = plan_attachment_format(
        "fetch yahoo.com for latest news and send me as a json file ...for all the titles",
        model_client=FakeFormatModel(".json"),
    )
    no_model = plan_attachment_format("fetch yahoo.com for latest news ...for all the titles")

    assert plan.extension == ".json"
    assert plan.evidence == "model_structured_output"
    assert no_model.extension is None


def test_attachment_format_graph_accepts_validated_structured_model_plan() -> None:
    plan = plan_attachment_format(
        "画像を5つ入れた表計算ファイルを添付して",
        model_client=FakeFormatModel(".blend"),
    )

    assert plan.extension == ".blend"
    assert plan.evidence == "model_structured_output"


def test_attachment_format_graph_preserves_literal_extension_and_model_media_contract() -> None:
    plan = plan_attachment_format(
        "Deliver cafe_report.xlsx with generated visual markers inside the workbook.",
        model_client=FakeFormatModel(".xlsx", embedded_media_extensions=(".xlsx",)),
        include_media_requirements=True,
    )

    assert plan.extension == ".xlsx"
    assert plan.evidence == "model_structured_output"
    assert plan.embedded_media_extensions == (".xlsx",)


def test_attachment_format_graph_rejects_invalid_structured_model_extension() -> None:
    plan = plan_attachment_format(
        "画像を5つ入れた表計算ファイルを添付して",
        model_client=FakeFormatModel(".tar.gz"),
    )

    assert plan.extension is None


def test_attachment_format_graph_does_not_use_english_synonyms_without_structured_signal() -> None:
    plan = plan_attachment_format("make this a spreadsheet deliverable with rows and columns")
    product_name = plan_attachment_format("make this an excel sheet with rows and columns")
    markdown_word = plan_attachment_format("make this a markdown report")
    literal_markdown = plan_attachment_format(".markdown")

    assert plan.extension is None
    assert product_name.extension is None
    assert markdown_word.extension is None
    assert literal_markdown.extension == ".markdown"


def test_source_url_suffixes_do_not_create_output_contracts():
    from nullion.task_frames import TaskFrameOutputContract, _detect_output_override
    from nullion.web_app import _web_prompt_requested_attachment_extensions

    for prompt in (
        "価格 https://example.com/product.html",
        "Lee https://example.com/source.pdf?preview=.png",
        "المخزون https://example.com/products.csv",
    ):
        assert plan_attachment_format(prompt).extension is None
        assert _web_prompt_requested_attachment_extensions(prompt) == ()
        assert _detect_output_override(prompt, TaskFrameOutputContract()) is None
    assert plan_attachment_format("artifact=https://example.com/report.pdf").extension == ".pdf"
    assert plan_attachment_format("https://example.com/product.html .pdf").extension == ".pdf"


@pytest.mark.parametrize('prompt',[
    'Read /workspace/files/source.txt and report the value.',
    'Lee /workspace/artifacts/source.pdf sin crear archivos.',
    '/workspace/files/入力.csv を読んで値を教えて。',
    'Inspect C:\\workspace\\files\\source.txt',
])
def test_source_file_reference_does_not_authorize_output_format(prompt):
    from nullion.chat_operator import _requested_attachment_extensions
    from nullion.task_frames import TaskFrameOutputContract,_detect_output_override
    assert plan_attachment_format(prompt).extension is None
    assert _requested_attachment_extensions(prompt,model_client=FakeFormatModel(None),allow_model_planning=True)==()
    assert _detect_output_override(prompt,TaskFrameOutputContract()) is None


def test_multiple_source_names_do_not_bypass_structured_output_role():
    from nullion.chat_operator import _requested_attachment_extensions
    class Inline(FakeFormatModel):
        def create(self,**kwargs):
            result=super().create(**kwargs);result['content'][0]['input']['output_kind']='inline';return result
    assert _requested_attachment_extensions('Compare alpha.pdf beta.xlsx gamma.txt here.',
        model_client=Inline('.pdf'),allow_model_planning=True)==()
    assert plan_attachment_format('source=/workspace/files/input.txt output=/workspace/artifacts/report.pdf').extension=='.pdf'
