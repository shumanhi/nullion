"""Disposable external source for real-provider comparisons and recovery."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
import json


class Sources(BaseHTTPRequestHandler):
    submissions = 0
    watches = {}

    def do_GET(self):
        path=urlsplit(self.path).path
        if path.startswith('/state-watch'):
            owner = parse_qs(urlsplit(self.path).query).get('owner', ['default'])[0]
            state = type(self).watches.setdefault(owner, {'votes': 10, 'clock': 0})
            if path == '/state-watch/data':
                state['clock'] += 1
                data = json.dumps(state).encode()
                self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(data);return
            body = ('<nav>' + 'Sample navigation. ' * 40 + '</nav><main id="results">'
                f'<h1>Controlled results</h1><p>Total votes: {state["votes"]}</p><p>Updated: {state["clock"]}</p></main>'
                '<script>setInterval(async()=>{const d=await(await fetch("/state-watch/data"+location.search)).json();'
                'document.querySelector("#results").innerHTML="<h1>Controlled results</h1><p>Total votes: "+d.votes+"</p><p>Updated: "+d.clock+"</p>";},300);</script>')
        elif path == '/public-fact/':
            body='<main><h1>Recorded sample facts</h1><p>Published sample description. No traffic statistics are supplied.</p></main>'
        elif path == '/cart/':
            body = ('<main style="background:#17324d;color:white;padding:40px;font:20px sans-serif">'
                '<h1>Controlled sample cart</h1><p>Disposable test fixture. No purchases are possible.</p>'
                '<section style="background:#fff;color:#17324d;padding:20px;border-left:12px solid #e6a23c">'
                '<h2>Cart contents</h2><p>2 sample notebooks — $12.00</p><p>Total: $12.00</p></section></main>')
        elif path == '/management/':
            body = '<main><h1>Sample Kitchen customer support</h1><p>Controlled test form; no real recipient.</p>'
            body += ''.join(f'<a href="/management/menu/{i}">Menu {i}</a>' for i in range(85))
            body += '<a href="/management/feedback">Submit feedback</a></main>'
        elif path == '/management/feedback':
            body = ('<main><h1>Sample Kitchen feedback</h1><p>98th Street and Columbus Avenue. Test only.</p>'
                '<form action="/management/submit" method="post"><label>Order number<input name="order"></label>'
                '<label>Message<textarea name="message"></textarea></label><button type="submit">Send complaint</button></form></main>')
        elif path == '/management/state':
            data = json.dumps({'submissions': type(self).submissions}).encode()
            self.send_response(200);self.send_header('Content-Type', 'application/json');self.end_headers();self.wfile.write(data);return
        elif path.startswith('/coverage/') and Path(path).name in {'index.html','primary.html','alternate.html'}:
            body=(Path(__file__).parents[2]/'fixtures/search_coverage'/Path(path).name).read_text()
        elif path=='/fallback.html':
            body='<main><h1>Fictional sample comparison</h1><div id="empty"></div><section id="observations"><h2>Observed records</h2><article><h3>Sample A</h3><p>100 g for $9.00. Price per gram: $0.09. In stock.</p></article><article><h3>Sample B</h3><p>No price has been published. Its value cannot be compared.</p></article></section></main>'
        elif path in {'/comparison','/comparison/'}:
            body='<main><h1>Fictional sample catalog</h1><p>Controlled test data; no purchases are possible.</p><ul>'
            for i in range(16):
                label=['Option A: advertised 100 g for $9','Reference B: advertised 100 g for $75','Option C: advertised 100 g for $7'][i] if i<3 else f'Additional sample {i}'
                body+=f'<li><a href="/sample/{i}">{label}</a></li>'
            body+='</ul></main>'
        elif path.startswith('/sample/') and path.rsplit('/',1)[-1].isdigit():
            i=int(path.rsplit('/',1)[-1]);price=[9,75,7][i] if i<3 else 30+i
            body=f'<main><h1>{["Option A","Reference B","Option C"][i] if i<3 else "Additional sample "+str(i)}</h1><p>Fictional test inventory. All samples have the same material and quality.</p><dl><dt>Package</dt><dd>100 g</dd><dt>One-time price</dt><dd>${price:.2f}</dd><dt>Unit price</dt><dd>${price/100:.2f}/g</dd><dt>Availability</dt><dd>In stock</dd></dl><a href="/comparison/">Catalog</a></main>'
        else:
            self.send_error(404);return
        data=('<!doctype html><html><head><meta charset="utf-8"><title>Controlled source</title></head><body>'+body+'</body></html>').encode()
        self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8')
        self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)

    def do_POST(self):
        if urlsplit(self.path).path == '/state-watch/advance':
            owner = parse_qs(urlsplit(self.path).query).get('owner', ['default'])[0]
            type(self).watches.setdefault(owner, {'votes': 10, 'clock': 0})['votes'] += 1
            self.send_response(204);self.end_headers();return
        if urlsplit(self.path).path != '/management/submit':
            self.send_error(404);return
        type(self).submissions += 1
        self.send_response(204);self.end_headers()

    def log_message(self,*args):pass


if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',8000),Sources).serve_forever()
