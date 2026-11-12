"""Structured table sources preserve permissions, cardinality and actual rows."""
import json
from pathlib import Path
import pytest
from nullion.tools import _build_spreadsheet_create_handler,ToolInvocation

@pytest.mark.parametrize('format',['xlsx','csv','tsv'])
@pytest.mark.parametrize('keys',[True,False])
def test_table_source_all_rows_and_columns(tmp_path,format,keys):
    source=tmp_path/'table.json';rows=[{'Name':f'項目-{i}','Value':i} for i in range(111)]
    source.write_text(json.dumps({'data':rows,'headers':['Name','Value']} if keys else rows))
    descriptor={'path':str(source),**({'rows_key':'data','columns_key':'headers'} if keys else {})}
    output=tmp_path/('table.'+format)
    result=_build_spreadsheet_create_handler(tmp_path)(ToolInvocation('source','spreadsheet_create','fixture:user',{'output_path':str(output),'rows_source':descriptor,'expected_rows':111}))
    assert result.status=='completed',result.error
    if format=='xlsx':
        from openpyxl import load_workbook
        workbook=load_workbook(output,read_only=True);actual=list(workbook.active.values);workbook.close()
        assert actual[0]==('Name','Value') and actual[-1]==('項目-110',110) and len(actual)==112
    else:
        import csv
        with output.open() as f:actual=list(csv.reader(f,delimiter=',' if format=='csv' else '\t'))
        assert len(actual)==112 and actual[-1]==['項目-110','110']

@pytest.mark.parametrize('case',['outside','symlink','malformed','missing_key','wrong_rows','wrong_columns','mixed_rows','mixed_sheets','wrong_count','too_large'])
def test_table_source_rejects_invalid_or_unpermitted_input(tmp_path,case):
    allowed=tmp_path/'workspace';allowed.mkdir();source=allowed/'table.json';source.write_text('{"rows":[{"Name":"actual"}],"columns":["Name"]}')
    descriptor={'path':str(source),'rows_key':'rows','columns_key':'columns'};extra={}
    if case in {'outside','symlink'}:
        outside=tmp_path/'private.json';outside.write_text(source.read_text())
        if case=='outside':descriptor['path']=str(outside)
        else:source.unlink();source.symlink_to(outside)
    elif case=='malformed':source.write_text('{')
    elif case=='missing_key':descriptor['rows_key']='missing'
    elif case=='wrong_rows':source.write_text('{"rows":[37],"columns":["Name"]}')
    elif case=='wrong_columns':source.write_text('{"rows":[{"Name":"actual"}],"columns":[37]}')
    elif case=='mixed_rows':extra['rows']=[]
    elif case=='mixed_sheets':extra['sheets']=[{'sheet_name':'Table','rows':[]}]
    elif case=='wrong_count':extra['expected_rows']=111
    else:
        with source.open('wb') as f:f.truncate(16*1024*1024+1)
    output=allowed/'result.xlsx'
    result=_build_spreadsheet_create_handler(allowed,include_principal_workspace=False)(ToolInvocation('reject','spreadsheet_create','fixture:user',{'output_path':str(output),'rows_source':descriptor,**extra}))
    assert result.status=='failed' and not output.exists(),result

@pytest.mark.parametrize('format',['xlsx','csv','tsv'])
@pytest.mark.parametrize('field',['Image','image_path','screenshot','画像'])
@pytest.mark.parametrize('declared_columns',[False,True])
def test_imported_table_fields_are_literal_data_not_media_commands(tmp_path,format,field,declared_columns):
    from PIL import Image
    actual=tmp_path/'present.png';Image.new('RGB',(24,24),'blue').save(actual)
    rows=[{'Name':'現物',field:str(actual)}, {'Name':'archivo',field:'missing.png'}]
    source=tmp_path/'source.json'
    source.write_text(json.dumps({'rows':rows,'columns':['Name',field]} if declared_columns else rows))
    descriptor={'path':str(source),**({'rows_key':'rows','columns_key':'columns'} if declared_columns else {})}
    output=tmp_path/('imported.'+format)
    result=_build_spreadsheet_create_handler(tmp_path)(ToolInvocation('literal','spreadsheet_create','fixture:user',{
        'output_path':str(output),'rows_source':descriptor,'expected_rows':2}))
    assert result.status=='completed',result.error
    if format=='xlsx':
        from openpyxl import load_workbook
        import zipfile
        workbook=load_workbook(output,read_only=True);values=list(workbook.active.values);workbook.close()
        with zipfile.ZipFile(output) as archive:assert not any(name.startswith('xl/media/') for name in archive.namelist())
    else:
        import csv
        with output.open() as stream:values=list(csv.reader(stream,delimiter=',' if format=='csv' else '\t'))
    assert list(values[0])==['Name',field]
    assert [list(row) for row in values[1:]]==[['現物',str(actual)],['archivo','missing.png']]

@pytest.mark.parametrize('mode',['selected_source_columns','explicit_image_binding','inline_literal_column'])
def test_table_column_selection_and_explicit_image_bindings_are_independent(tmp_path,mode):
    from PIL import Image
    from openpyxl import load_workbook
    import zipfile
    image=tmp_path/'actual.png';Image.new('RGB',(160,160),'blue').save(image)
    source=tmp_path/'data.json';source.write_text(json.dumps({'columns':['Name','Image'],
        'rows':[{'Name':'البيانات','Image':'missing.png'}]}))
    args={'output_path':str(tmp_path/'data.xlsx'),
          'rows_source':{'path':str(source),'rows_key':'rows','columns_key':'columns'}}
    if mode=='selected_source_columns':args['columns']=['Name']
    elif mode=='explicit_image_binding':args['image_paths']=[str(image)]
    else:
        args.pop('rows_source');args.update(columns=['Name','Image'],rows=[{'Name':'البيانات','Image':str(image)}])
    result=_build_spreadsheet_create_handler(tmp_path)(ToolInvocation('separate','spreadsheet_create','fixture:user',args))
    assert result.status=='completed',result
    book=load_workbook(args['output_path']);sheet=book.active
    assert sheet.cell(2,1).value=='البيانات'
    if mode=='selected_source_columns':assert sheet.max_column==1
    elif mode=='inline_literal_column':assert sheet.cell(2,2).value==str(image)
    else:assert sheet.cell(2,2).value=='missing.png'
    book.close()
    with zipfile.ZipFile(args['output_path']) as package:
        images=[name for name in package.namelist() if name.startswith('xl/media/')]
        assert len(images)==(1 if mode=='explicit_image_binding' else 0)
