"""Workbook review observes features in the file, not claims in tool prose."""
from zipfile import ZipFile, ZIP_DEFLATED


def test_workbook_features_and_multiple_sheets_are_inspected(tmp_path):
    from openpyxl import Workbook
    from openpyxl.chart import BarChart, Reference
    from openpyxl.formatting.rule import CellIsRule
    from nullion.artifact_evidence import artifact_structure_evidence
    path = tmp_path/'report.xlsx'; workbook = Workbook(); sheet = workbook.active; sheet.title = '市場'
    sheet.append(['Name', 'Value']); sheet.append(['A', 31]); sheet.append(['B', '=B2*2'])
    chart = BarChart(); chart.add_data(Reference(sheet, min_col=2, min_row=1, max_row=3), titles_from_data=True)
    sheet.add_chart(chart, 'D2'); sheet.conditional_formatting.add('B2:B3', CellIsRule(operator='greaterThan', formula=['30']))
    workbook.create_sheet('Notes').append(['Literal formula', 'Not =B2*2']); workbook.save(path)
    evidence = artifact_structure_evidence(path)
    assert evidence['workbook_inspection_complete'] is True
    assert evidence['worksheet_count'] == 2 and evidence['formula_cell_count'] == 1
    assert evidence['chart_count'] == 1 and evidence['conditional_formatting_rule_count'] == 1
    assert evidence['worksheets'][0]['name'] == '市場' and evidence['worksheets'][1]['chart_count'] == 0


def test_orphan_chart_part_is_not_evidence_of_a_worksheet_chart(tmp_path):
    from openpyxl import Workbook
    from nullion.artifact_evidence import artifact_structure_evidence
    path = tmp_path/'orphan.xlsx'; Workbook().save(path)
    with ZipFile(path, 'a') as package:
        package.writestr('xl/charts/chart1.xml', '<c:chartSpace xmlns:c="http://schemas.openxmlformats.org/drawingml/2006/chart"><c:chart/></c:chartSpace>')
    evidence = artifact_structure_evidence(path)
    assert evidence['workbook_inspection_complete'] and evidence['chart_count'] == 0


def test_oversized_worksheet_is_bounded_and_marked_uninspected(tmp_path):
    from nullion.artifact_evidence import artifact_structure_evidence
    path = tmp_path/'large.xlsx'
    with ZipFile(path, 'w', compression=ZIP_DEFLATED) as package:
        package.writestr('xl/workbook.xml', '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Large" sheetId="1" r:id="r1"/></sheets></workbook>')
        package.writestr('xl/_rels/workbook.xml.rels', '<Relationships><Relationship Id="r1" Target="worksheets/sheet1.xml"/></Relationships>')
        package.writestr('xl/worksheets/sheet1.xml', '<worksheet>' + ' '*(1024*1024+1) + '</worksheet>')
    evidence = artifact_structure_evidence(path)
    assert not evidence['workbook_inspection_complete'] and evidence['worksheets'] == []


def test_unsupported_workbook_namespace_is_unknown_not_a_verified_empty_workbook(tmp_path):
    from nullion.artifact_evidence import artifact_structure_evidence
    path=tmp_path/'strict.xlsx'
    with ZipFile(path,'w') as package:
        package.writestr('xl/workbook.xml','<workbook xmlns="http://purl.oclc.org/ooxml/spreadsheetml/main"><sheets><sheet name="市場"/></sheets></workbook>')
    evidence=artifact_structure_evidence(path)
    assert not evidence['workbook_inspection_complete'] and 'worksheet_count' not in evidence
    assert evidence['workbook_inspection_reason']=='unsupported_namespace'
