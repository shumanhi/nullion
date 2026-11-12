"""Disposable read-only source pages; no booking, purchasing or account access."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Pages(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/rentals':
            body = ('<h1>Sample rental offers</h1><p>Fictional one-day round-trip rentals, noon November 21 to noon November 22.</p>'
                    '<a href="/rentals/oak">Oak Compact — $118 displayed total</a><br>'
                    '<a href="/rentals/trail">Trail SUV — $137 displayed total</a>')
        elif self.path in ('/rentals/oak', '/rentals/trail'):
            vehicle, price, garage = ('Oak Compact', '118', 'Centre garage') if self.path.endswith('oak') else ('Trail SUV', '137', '743 Sample Road')
            body = ('<h1>Selected offer information</h1><p>Fictional test inventory.</p><dl>'
                    f'<dt>Vehicle</dt><dd>{vehicle}</dd><dt>Displayed total</dt><dd>${price}</dd>'
                    f'<dt>Pickup and return</dt><dd>{garage}</dd><dt>Mileage</dt><dd>Unlimited miles</dd>'
                    '<dt>Fuel policy</dt><dd>Full-to-full</dd><dt>Cancellation</dt><dd>Free before pickup</dd>'
                    '</dl><p>Engine fuel type, security deposit and inclusion of mandatory fees are not specified.</p>'
                    '<button disabled>Choose an option</button>')
        elif self.path == '/products':
            body = ('<h1>Sample lamps</h1><p>Fictional test inventory. Delivery date is not specified.</p>'
                    '<article><h2>Maple Lamp</h2><dl><dt>Price</dt><dd>$34</dd><dt>Stock</dt><dd>4 units</dd></dl></article>'
                    '<article><h2>Birch Lamp</h2><dl><dt>Price</dt><dd>$48</dd><dt>Stock</dt><dd>7 units</dd></dl></article>'
                    '<button disabled>Choose an option</button>')
        else:
            self.send_error(404); return
        data = ('<!doctype html><html><head><meta charset="utf-8"><title>Controlled source</title></head>'
                '<body><main>' + body + '</main></body></html>').encode()
        self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(data))); self.end_headers(); self.wfile.write(data)

    def log_message(self, *args): pass


if __name__ == '__main__':
    ThreadingHTTPServer(('0.0.0.0', 8000), Pages).serve_forever()
