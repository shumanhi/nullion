"""Entity counts belong to their declared table, independently of notes."""
from copy import deepcopy

import pytest

from nullion.tools import ToolInvocation, _build_spreadsheet_create_handler, _default_input_schema_for_tool


def sheets():
    return [
        {'sheet_name':'比較', 'columns':['Name','Value'], 'expected_rows':4,
         'rows':[[f'Row {i}',i] for i in range(4)]},
        {'sheet_name':'Notes', 'columns':['Assumption'], 'expected_rows':14,
         'rows':[[f'Assumption {i}'] for i in range(14)]},
    ]


@pytest.mark.parametrize('total',[None,18])
def test_individual_counts_preserve_all_worksheets_and_total(tmp_path,total):
    from jsonschema import validate
    from openpyxl import load_workbook
    output=tmp_path/'tables.xlsx'
    arguments={'output_path':str(output),'sheets':sheets()}
    if total is not None:arguments['expected_rows']=total
    validate(arguments,_default_input_schema_for_tool('spreadsheet_create'))
    result=_build_spreadsheet_create_handler(tmp_path)(ToolInvocation('tables','spreadsheet_create','fixture:user',arguments))
    assert result.status=='completed',result
    book=load_workbook(output,read_only=True)
    assert [book[name].max_row for name in book.sheetnames]==[5,15]
    assert book['比較']['B5'].value==3 and book['Notes']['A15'].value=='Assumption 13'
    book.close()


def test_workbook_total_is_still_strict_and_explains_worksheet_counts(tmp_path):
    output=tmp_path/'wrong.xlsx'
    result=_build_spreadsheet_create_handler(tmp_path)(ToolInvocation('wrong','spreadsheet_create','fixture:user',
        {'output_path':str(output),'sheets':sheets(),'expected_rows':4}))
    assert result.status=='failed' and not output.exists()
    assert result.output['expected_rows']==4 and result.output['rows']==18
    assert result.output['validation']=={'pointer':'/expected_rows','scope':'workbook_total'}
    assert result.output['worksheet_row_counts']==[{'sheet_name':'比較','rows':4},{'sheet_name':'Notes','rows':14}]


@pytest.mark.parametrize('index',[0,1])
@pytest.mark.parametrize('bad',[False,-1,4.5,'4',99])
def test_each_worksheet_count_fails_before_file_creation(tmp_path,index,bad):
    data=deepcopy(sheets());data[index]['expected_rows']=bad
    output=tmp_path/'bad.xlsx'
    result=_build_spreadsheet_create_handler(tmp_path)(ToolInvocation('bad','spreadsheet_create','fixture:user',
        {'output_path':str(output),'sheets':data}))
    assert result.status=='failed' and not output.exists()
    assert result.output['validation']=={'pointer':f'/sheets/{index}/expected_rows','scope':'worksheet'}
    assert result.output['sheet_name']==data[index]['sheet_name']
