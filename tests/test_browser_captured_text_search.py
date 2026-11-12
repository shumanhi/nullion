"""Literal page search preserves captured indexes and bounded evidence."""
import asyncio
from types import SimpleNamespace
import pytest
from nullion.plugins.browser_plugin.browser_use_adapter import PageEvidence,BrowserReport,BrowserPageRead
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy

@pytest.mark.parametrize("query",["Compact","コンパクト","قيمة"])
def test_deep_page_literal_search_returns_grounded_context_without_serial_paging(query):
    lines=["Unrelated item "+str(i) for i in range(3000)]
    lines[2400:2403]=[query,"Model B","$88 total"]
    async def read():return {"url":"https://source.example/items","text":"\n".join(lines)}
    evidence=PageEvidence("fixture",BrowserPolicy(),text_capture=read)
    asyncio.run(evidence.capture(SimpleNamespace(url="https://source.example/items",title="Items",dom_state=None,screenshot=None)))
    assert "$88 total" not in str(evidence.page_state())
    result=evidence.page_state(query=query)
    assert result["matched_line_indices"]==[2400] and result["total_matches"]==1
    assert {r["index"]:r["text"] for r in result["text_lines"]}[2402]=="$88 total"
    assert len(result["text_lines"])<=120 and result["next_start_line"] is None
    report=BrowserReport(goal_status="answered",next_action="report",observations=[{
        "label":query,"semantic_role":"entity","text_selection":{"source_id":result["source_id"],"start_line":2400,"end_line":2402}}])
    assert evidence.retain(report)==1 and evidence.observations[0]["quote"]=="\n".join(lines[2400:2403])
    absent=evidence.page_state(query="Not present in the captured page")
    assert absent["total_matches"]==0 and absent["text_lines"]==[]


def test_search_is_bounded_and_has_a_progressing_cursor_after_large_context():
    evidence=PageEvidence("fixture",BrowserPolicy())
    evidence.pages=[{"url":"https://source.example","title":"Page","visible_text":"",
        "text_lines":["x"*20000,"needle",*["needle" for _ in range(10000)]]}]
    first=evidence.page_state(query="needle")
    assert sum(len(r["text"]) for r in first["text_lines"])<=14000
    assert first["next_match_start_line"]==1
    second=evidence.page_state(query="needle",start_line=first["next_match_start_line"])
    assert second["matched_line_indices"] and second["text_lines"][0]["index"]==1
    assert second["next_match_start_line"]>1 and len(second["text_lines"])<=120
    assert evidence.page_state(source_id=50,query="needle")["status"]=="source_not_captured"
    with pytest.raises(ValueError):BrowserPageRead(query="")
