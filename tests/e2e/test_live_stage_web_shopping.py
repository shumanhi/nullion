"""Real shopping/profile replay; no login, purchase or profile modification."""
import json,os,re
from pathlib import Path
from urllib.parse import urlsplit
import pytest
from tests.e2e.test_live_stage_web_matrix import live_web,_submit
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in stage Web')


def test_live_web_amazon_powder_comparison_uses_profile_marketplace(live_web):
 page,frames,errors,database,conversation,base=live_web
 out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'amazon_profile_comparison';out.mkdir(parents=True,exist_ok=True)
 proof={'conversation_id':conversation,'status':'running'}
 try:
  response,turn,elapsed=_submit(live_web,
   'Find me the cheapest psyllium husk powder on Amazon. Use my saved city/country to select the marketplace. '
   'Compare the displayed one-time price and price per ounce for up to three powder products, with direct product links. '
   'Use prices before unconfirmed shipping or account discounts. Do not sign in, buy, change account settings, submit my address, '
   'or create files. If Amazon blocks access, report the verified blocker without inventing live prices.',timeout=720)
  results=turn.get('tool_results',[]);reply=response['text']
  proof.update(elapsed_ms=elapsed,reply=reply,turn=turn)
  asins={value.upper() for value in re.findall(r'amazon\.com/(?:[^\s<>]*?/)?(?:dp|gp/product)/([A-Z0-9]{10})',reply,re.I)}
  reports=[r['output'] for r in results if r.get('tool_name')=='browser_run_task' and r.get('status')=='completed']
  blocks=[r for r in reports if r.get('goal_status')=='blocked' and r.get('blocker') and r.get('execution_audit',{}).get('complete') is True
          and any((urlsplit(u).hostname or '').lower()=='amazon.com' or (urlsplit(u).hostname or '').lower().endswith('.amazon.com') for u in r.get('execution_audit',{}).get('visited_urls',[]))]
  if asins and '$' in reply:
   evidence=json.dumps([r.get('output') for r in results if r.get('status')=='completed'],ensure_ascii=False).upper()
   assert asins.issubset(set(re.findall(r'[A-Z0-9]{10}',evidence))),('Product identities not in captured evidence',asins)
   def completed_read(row):
    if row.get('status')!='completed':return False
    if row.get('tool_name') in {'browser_extract_detail','browser_extract_items','browser_extract_text','browser_run_task'}:return True
    native=row.get('response_requirements',{}).get('_native_completed_reads',{})
    return (row.get('tool_name')=='browser_read_pages' and native.get('read_only') is True
        and native.get('parent_tool')==row['tool_name'] and native.get('invocation_id')==row.get('invocation_id')
        and bool(set(native.get('tool_names',[])) & {'browser_extract_detail','browser_extract_items','browser_extract_text'}))
   assert any(completed_read(r) for r in results),results
   proof['outcome_kind']='displayed_product_comparison'
  else:
   assert blocks,('No displayed comparison or verified Amazon access blocker',reply,results)
   assert len(reply)>100
   proof['outcome_kind']='verified_external_blocker'
  assert not response['artifacts'] and not errors
  proof.update(status='passed',elapsed_ms=elapsed,reply=reply,turn=turn)
 except BaseException as error:
  proof.update(status='failed',error=str(error));raise
 finally:
  try:page.screenshot(path=str(out/'web.png'),full_page=False,timeout=10000)
  except Exception as error:proof['screenshot_error']=str(error)
  (out/'result.json').write_text(json.dumps(proof,ensure_ascii=False,indent=2))
