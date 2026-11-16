"""One disposable origin for the existing source and source-group cases."""
from http.server import ThreadingHTTPServer
from urllib.parse import urlsplit

from live_source_server import Sources
from source_group_server import Pages


class CombinedSources(Sources):
    def do_GET(self):
        if urlsplit(self.path).path in {'/products','/rentals','/rentals/oak','/rentals/trail'}:
            return Pages.do_GET(self)
        return super().do_GET()


if __name__=='__main__':
    ThreadingHTTPServer(('0.0.0.0',8000),CombinedSources).serve_forever()
