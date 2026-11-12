#!/usr/bin/env python3
"""Opt-in, fixed read-only source incident replay through production Web UI.

Run inside Docker only, after explicit authorization to patch/test production.
The prompt is fixed; this is not a production workflow catalogue runner.
"""
import argparse,json,os,time
from pathlib import Path
from uuid import uuid4
from playwright.sync_api import sync_playwright,expect


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--image',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    assert os.environ.get('NULLION_LIVE_PROD_SOURCE_E2E')=='1','Explicit production source replay opt-in required'
    assert os.environ.get('NULLION_E2E_CONTAINER')=='1','Docker required'
    assert args.image.is_file() and args.image.suffix.lower()=='.png'
    base='http://127.0.0.1:8742';conversation='web:verified-source-fix:'+uuid4().hex
    args.output.mkdir(parents=True,exist_ok=True,mode=0o700)
    result={'conversation_id':conversation,'status':'running'}
    (args.output/'result.json').write_text(json.dumps(result,indent=2))
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True);context=browser.new_context()
        context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(conversation)+");")
        page=context.new_page();frames=[];errors=[]
        def receive(frame):
            try:
                data=json.loads(frame)
                if isinstance(data,dict):frames.append(data)
            except (TypeError,ValueError):pass
        def observe_socket(ws):
            ws.on('framereceived',receive)
            ws.on('framesent',lambda frame:frames.append({'type':'client_sent','payload':json.loads(frame)}))
            ws.on('socketerror',lambda error:errors.append(str(error)))
        page.on('websocket',observe_socket);page.on('pageerror',lambda e:errors.append(str(e)))
        def response(receipt):
            if receipt.request.method=='POST' and receipt.url==base+'/api/chat':
                data=receipt.json();frames.append({**data,'type':'done' if data.get('type')=='message' else data.get('type')})
        page.on('response',response)
        try:
            page.goto(base,wait_until='domcontentloaded');page.locator('#user-input').wait_for(state='visible')
            page.wait_for_function('() => typeof ws !== \'undefined\' && ws?.readyState === WebSocket.OPEN',timeout=15000)
            page.locator('#file-input').set_input_files(str(args.image))
            page.locator('#user-input').fill('Find about these DJs that are playing here tonight');page.locator('#user-input').press('Enter')
            expect(page.locator('#attachments-bar')).to_be_hidden(timeout=15000)
            before=len(frames);started=time.monotonic()
            page.locator('#user-input').fill('https://ra.co/events/2556108');page.locator('#user-input').press('Enter')
            terminal=None
            while time.monotonic()-started<180:
                for frame in frames[before:]:
                    if frame.get('type') not in {'done','error','approval_required'} or frame.get('superseded'):continue
                    value=dict(frame)
                    if not value.get('text'):
                        value['text']=''.join(str(chunk.get('text','')) for chunk in frames if chunk.get('type')=='chunk' and chunk.get('turn_id')==value.get('turn_id'))
                    if value.get('text'):terminal=value
                if terminal:break
                page.wait_for_timeout(100)
            result.update(terminal=terminal,elapsed_ms=(time.monotonic()-started)*1000,page_errors=errors)
            assert terminal and terminal.get('type')=='done',terminal
            reply=terminal.get('text','');assert all(name.casefold() in reply.casefold() for name in ['Word of Command','Mez Monty','Cyler Says']),reply
            assert 'focused answer' not in reply and not terminal.get('artifacts') and not errors,reply
            turn=terminal['turn_id'];bubble=page.locator('.msg.bot .bubble[data-turn-id='+json.dumps(turn)+']')
            expect(bubble).to_have_count(1);expect(bubble).to_be_visible()
            assert bubble.get_attribute('data-raw-text').strip().startswith(reply)
            urls=bubble.locator('a[href^="https://"]').evaluate_all('(nodes)=>nodes.map(node=>node.href)')
            for aliases in [('wordofcommand','word-of-command'),('mezmonty','mez-monty'),('cylersays','cyler-says')]:
                assert any(any(alias in url.casefold() for alias in aliases) for url in urls),(aliases,reply)
            result.update(status='passed',source_links=urls)
        except BaseException as exc:
            result.update(status='failed',error=str(exc));raise
        finally:
            page.screenshot(path=str(args.output/'web.png'),full_page=True)
            (args.output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
            (args.output/'frames.json').write_text(json.dumps(frames,ensure_ascii=False,indent=2))
            try:
                stopped=page.request.post(base+'/api/chat',data={'text':'/stop','conversation_id':conversation},timeout=30000)
                assert stopped.ok,stopped.text()
            finally:browser.close()
    print(json.dumps({key:result.get(key) for key in ['status','conversation_id','elapsed_ms','error']}))

if __name__=='__main__':main()
