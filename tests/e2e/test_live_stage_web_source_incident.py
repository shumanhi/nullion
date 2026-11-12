"""Real-provider source composition incident replay through stage Web UI."""
import json
import os
import time
import re
from urllib.parse import urlsplit,unquote
from pathlib import Path
import pytest
from tests.e2e.test_live_stage_web_matrix import live_web,_submit

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in real stage UI')


def _assert_artist_sources(reply):
    # Subject identities and provider URL metadata come from the public fixture;
    # no English answer trigger words are used as a proxy for completion.
    urls=[unquote(url.rstrip(').,')).casefold() for url in re.findall(r'https?://[^\s<>]+',reply)]
    identifiers=[('wordofcommand','word-of-command'),('mezmonty','mez-monty'),('cyler-says','cylersays')]
    for variants in identifiers:
        assert any(any(identity in path for identity in variants) for path in urls),(variants,reply)


def test_live_event_artist_research_keeps_original_question(live_web):
    page,frames,errors,database,conversation,base=live_web
    prompt=('Find out about the DJs playing at this event tonight: https://ra.co/events/2556108 . '
            'Tell me about each artist and their music, using verifiable artist sources where available. '
            'The event lineup is Word of Command, Mez Monty, and Cyler Says. Do not buy tickets or send messages.')
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'resident_advisor_artist_research'
    out.mkdir(parents=True,exist_ok=True)
    result={'conversation_id':conversation,'prompt':prompt,'status':'running'}
    try:
        response,turn,elapsed=_submit(live_web,prompt,timeout=300)
        result.update(response=response,turn=turn,elapsed_ms=elapsed)
        reply=response['text']
        assert all(name.casefold() in reply.casefold() for name in ['Word of Command','Mez Monty','Cyler Says']),reply
        assert 'could not turn it into a focused answer' not in reply and 'Please retry' not in reply,reply
        assert 'http' in reply and len(reply)>250,reply
        _assert_artist_sources(reply)
        assert not response['artifacts'] and not errors
        assert any(r['tool_name'].startswith('browser_') for r in turn['tool_results']),turn
        result['status']='passed'
    except BaseException as exc:
        result.update(status='failed',error=str(exc));raise
    finally:
        try:
            page.screenshot(path=str(out/'web.png'),full_page=False,timeout=10000)
        except Exception as screenshot_error:
            result['screenshot_error']=str(screenshot_error)
        (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))


def test_live_image_question_followed_by_event_link(live_web,tmp_path):
    from playwright.sync_api import expect
    page,frames,errors,database,conversation,base=live_web
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'resident_advisor_image_amendment'
    out.mkdir(parents=True,exist_ok=True)
    result={'conversation_id':conversation,'status':'running'}
    try:
        attachment=os.environ.get('NULLION_INCIDENT_IMAGE')
        if not attachment:
            # A disposable, clearly labeled poster exercises attachment plus
            # in-flight URL context without depending on a private old upload.
            from PIL import Image,ImageDraw,ImageFont
            image=Image.new('RGB',(1000,700),'#17152d');draw=ImageDraw.Draw(image)
            font=ImageFont.load_default(size=42)
            for y,line in enumerate(['SYNTHETIC EVENT POSTER','Word of Command','Mez Monty','Cyler Says']):
                draw.text((60,70+y*140),line,font=font,fill='white')
            attachment=str(tmp_path/'owned-lineup.png');image.save(attachment)
        page.locator('#file-input').set_input_files(attachment)
        expect(page.locator('#attachments-bar')).to_contain_text(Path(attachment).name)
        page.locator('#user-input').fill('Find about these DJs that are playing here tonight')
        page.locator('#user-input').press('Enter')
        expect(page.locator('#attachments-bar')).to_be_hidden(timeout=15000)
        # Supply the event URL during the original image question, as in the
        # reported Telegram request. No artist names are manually supplied.
        response,turn,elapsed=_submit(live_web,'https://ra.co/events/2556108',timeout=300)
        result.update(response=response,turn=turn,elapsed_ms=elapsed)
        reply=response['text']
        assert all(name.casefold() in reply.casefold() for name in ['Word of Command','Mez Monty','Cyler Says']),reply
        assert 'focused answer' not in reply and len(reply)>250,reply
        assert not any(name in reply.casefold() for name in ['bambounou','dennis free','matiachetti']),reply
        assert 'http' in reply and not response['artifacts'] and not errors
        _assert_artist_sources(reply)
        result['status']='passed'
    except BaseException as exc:
        result.update(status='failed',error=str(exc));raise
    finally:
        try:
            page.screenshot(path=str(out/'web.png'),full_page=False,timeout=10000)
        except Exception as screenshot_error:
            result['screenshot_error']=str(screenshot_error)
        (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))


def test_live_structured_source_record_comparison_keeps_relationships(live_web):
    page,frames,errors,database,conversation,base=live_web
    first='https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html'
    second='https://books.toscrape.com/catalogue/tipping-the-velvet_999/index.html'
    prompt=(f'Use one browser_run_task invocation to inspect both {first} and {second}. '
        'Compare each book’s displayed price and exact copies in stock, and say whether a guaranteed delivery date is shown. '
        'Use direct source links. Do not buy anything or create files.')
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'structured_source_comparison';out.mkdir(parents=True,exist_ok=True)
    result={'conversation_id':conversation,'status':'running','prompt':prompt}
    try:
        response,turn,elapsed=_submit(live_web,prompt,timeout=300);result.update(response=response,turn=turn,elapsed_ms=elapsed)
        reply=response['text']
        for value in ['51.77','53.74','22','20']:assert value in reply,reply
        assert 'https://books.toscrape.com' in reply and 'focused answer' not in reply,reply
        assert not response['artifacts'] and not errors
        assert sum(row['tool_name']=='browser_run_task' for row in turn['tool_results'])==1,turn['tool_results']
        assert any(row.get('output',{}).get('result_kind')=='source_observations' for row in turn['tool_results'])
        result['status']='passed'
    except BaseException as exc:
        result.update(status='failed',error=str(exc));raise
    finally:
        try:
            page.screenshot(path=str(out/'web.png'),full_page=False,timeout=10000)
        except Exception as screenshot_error:
            result['screenshot_error']=str(screenshot_error)
        (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
