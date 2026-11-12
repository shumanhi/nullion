"""Container-only controlled external news site for real-provider stage acceptance."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json,threading

lock=threading.Lock()
changes=threading.Condition(lock)
articles=[{'title':'Original waterfront article','url':'/news/original'}]
sessions=set()
owned_articles={}
region="news"
HTML='''<!doctype html><title>Public news fixture</title><main><h1>Latest articles</h1>
<section id="news"><h2>News</h2><article><a href="/news/original">Original waterfront article</a></article></section></main>
<script>const token=sessionStorage.newsFixtureToken || (sessionStorage.newsFixtureToken=Math.random().toString(36).slice(2));
const owner=new URLSearchParams(location.search).get('observation_owner') || '';
function render(rows){
document.querySelector('#news').innerHTML='<h2>News</h2>'+rows.map(r=>'<article><a href="'+r.url+'">'+r.title+'</a></article>').join('');}
const updates=new EventSource('/events?session='+token+'&owner='+encodeURIComponent(owner));
updates.onmessage=event=>render(JSON.parse(event.data));</script>'''
class News(BaseHTTPRequestHandler):
    def do_GET(self):
        from urllib.parse import urlsplit,parse_qs
        parts=urlsplit(self.path)
        if parts.path=='/events':
            owner=parse_qs(parts.query).get('owner',[''])[0]
            token=parse_qs(parts.query).get('session',[''])[0]
            with lock:
                if token:sessions.add((owner,token))
            self.send_response(200)
            self.send_header('Content-Type','text/event-stream')
            self.send_header('Cache-Control','no-store')
            self.end_headers()
            previous=None
            try:
                while True:
                    with lock:current=json.dumps(owned_articles.get(owner,articles),ensure_ascii=False)
                    data=('data: '+current+'\n\n') if current!=previous else ': keepalive\n\n'
                    self.wfile.write(data.encode());self.wfile.flush();previous=current
                    with changes:changes.wait(timeout=1)
            except (BrokenPipeError,ConnectionResetError):
                return
        with lock:
            requested_owner=parse_qs(parts.query).get('owner',parse_qs(parts.query).get('observation_owner',['']))[0]
            rows=owned_articles.get(requested_owner,articles)
            if parts.path=='/data':
                token=parse_qs(parts.query).get('session',[''])[0]
                owner=parse_qs(parts.query).get('owner',[''])[0]
                if token:sessions.add((owner,token))
                payload=json.dumps(rows).encode();kind='application/json'
            elif parts.path=='/stats':
                owner=parse_qs(parts.query).get('owner')
                count=sum(1 for key,token in sessions if key==owner[0]) if owner else len(sessions)
                payload=json.dumps({'articles':rows,'session_count':count}).encode();kind='application/json'
            else:
                rendered=''.join('<article><a href="'+row['url']+'">'+row['title']+'</a></article>' for row in rows)
                payload=HTML.replace('<article><a href="/news/original">Original waterfront article</a></article>',rendered).replace('id="news"','id="'+region+'"').replace("querySelector('#news')","querySelector('#"+region+"')").encode();kind='text/html'
        self.send_response(200);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
    def do_POST(self):
        global region
        from urllib.parse import urlsplit,parse_qs
        parts=urlsplit(self.path);owner=parse_qs(parts.query).get('owner',[''])[0]
        data=json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))) or b'{}')
        with lock:
            rows=owned_articles.setdefault(owner,[{'title':'Original waterfront article','url':'/news/original'}]) if owner else articles
            if owner and parts.path=='/reset':
                rows[:]=[{'title':'Original waterfront article','url':'/news/original'}]
                sessions.difference_update({entry for entry in sessions if entry[0]==owner})
            elif parts.path=='/reset':articles[:]=[{'title':'Original waterfront article','url':'/news/original'}];sessions.clear();region='news'
            elif parts.path=='/clear-sessions':sessions.clear()
            elif parts.path=='/move-section':region='updated-source'
            elif parts.path=='/edit':articles[0]['title']='Edited original waterfront headline'
            elif parts.path=='/publish':rows.append({'title':data.get('title','New museum opens on waterfront'),'url':data.get('url','/news/new-museum')})
            else:self.send_error(404);return
            changes.notify_all()
        self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(b'{"ok":true}')
    def log_message(self,*args):pass
if __name__=='__main__':
    ThreadingHTTPServer(('0.0.0.0',8000),News).serve_forever()
