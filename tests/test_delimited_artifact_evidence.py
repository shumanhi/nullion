import pytest
from nullion.artifact_evidence import artifact_structure_evidence

@pytest.mark.parametrize('suffix,content,rows,width,consistent,escaped',[
    ('.csv','Name,Value\nCedar,37\nMaple,41\n',3,2,True,0),
    ('.tsv','Name\tValue\nCedar\t37\nMaple\t41\n',3,2,True,0),
    ('.csv','Name,Value',1,2,True,0),
    ('.csv','Name,Value\nCedar,37,extra\n',2,2,False,0),
    ('.csv','Name,Value\\nCedar,37\\nMaple,41\\n',1,4,True,3),
    ('.csv','Name,Value\n"Cedar, Grove",37\n',2,2,True,0),
])
def test_native_delimited_structure(tmp_path,suffix,content,rows,width,consistent,escaped):
    path=tmp_path/('report'+suffix);path.write_text(content)
    result=artifact_structure_evidence(path)
    assert result['inspection_complete'] is True
    assert (result['row_count'],result['column_count'],result['consistent_column_count'],result['literal_line_break_escape_count'])==(rows,width,consistent,escaped)

def test_blank_lines_cannot_bypass_scan_bound(tmp_path):
    path=tmp_path/'report.csv';path.write_text('\n'*10002+'Name,Value\nCedar,37\n')
    assert artifact_structure_evidence(path)['inspection_complete'] is False

def test_invalid_encoding_has_no_structure_claim(tmp_path):
    path=tmp_path/'report.csv';path.write_bytes(b'\xff\xff')
    assert artifact_structure_evidence(path)=={'inspection_complete':False,'reason':'invalid_delimited_text'}
