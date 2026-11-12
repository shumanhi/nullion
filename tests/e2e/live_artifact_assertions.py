"""Assert document structure instead of requiring incidental English labels."""
from html.parser import HTMLParser
from urllib.parse import urlsplit


def assert_three_practical_itineraries(body):
    class Plans(HTMLParser):
        def __init__(self):
            super().__init__();self.stack=[];self.completed=[];self.item=[];self.li_depth=0;self.links=set()
        def handle_starttag(self,tag,attrs):
            if tag=='a':
                url=dict(attrs).get('href','');parsed=urlsplit(url)
                if parsed.scheme in {'http','https'} and parsed.netloc:self.links.add(url)
            if tag in {'ol','ul'}:self.stack.append([])
            if tag=='li' and self.stack:
                if not self.li_depth:self.item=[]
                self.li_depth+=1
        def handle_data(self,text):
            if self.li_depth:self.item.append(text)
        def handle_endtag(self,tag):
            if tag=='li' and self.li_depth:
                self.li_depth-=1
                if not self.li_depth:self.stack[-1].append(' '.join(' '.join(self.item).split()))
            if tag in {'ol','ul'} and self.stack:self.completed.append(tuple(self.stack.pop()))
    parser=Plans();parser.feed(body.decode('utf-8') if isinstance(body,bytes) else body)
    plans=[items for items in parser.completed if len(items)>=3 and all(len(item)>20 for item in items)]
    assert len(parser.links)>=3,('Expected actual source URLs',parser.links)
    assert len(set(plans))>=3,('Expected three distinct practical multi-step itineraries',plans)


def assert_visual_workbook_formulas(body):
    import io
    from openpyxl import load_workbook
    workbook=load_workbook(io.BytesIO(body),data_only=False)
    assert len(workbook.worksheets)>=2,'Expected a multi-sheet workbook'
    expected={**{'F'+str(i):'=D'+str(i)+'-E'+str(i) for i in range(2,6)},
              **{'G'+str(i):'=ROUND(C'+str(i)+'/F'+str(i)+',2)' for i in range(2,6)}}
    assert any(all(sheet[cell].data_type=='f' and sheet[cell].value==formula for cell,formula in expected.items())
               for sheet in workbook), 'The requested eight exact Excel formulas are missing'
    assert any(sheet._charts for sheet in workbook),'Expected a native Excel chart'
    rules=[rule for sheet in workbook for item in sheet.conditional_formatting
           for rule in sheet.conditional_formatting[item]]
    assert rules,'Expected actual conditional formatting'
    assert any(sheet._images for sheet in workbook) or any(rule.type in {'dataBar','iconSet'} for rule in rules), \
        'Expected embedded images or generated visual markers, not plain cell colors'
