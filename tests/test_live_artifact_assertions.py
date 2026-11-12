import pytest
from tests.e2e.live_artifact_assertions import assert_three_practical_itineraries
LINKS=''.join('<a href="https://example.invalid/source/'+str(n)+'">'+str(n)+'</a>' for n in range(3))

@pytest.mark.parametrize('heading',['Practical route','Ruta práctica','実用的な旅程'])
def test_itinerary_contract_uses_steps_not_an_english_heading(heading):
    lists=['<h2>'+heading+'</h2><ol>'+''.join('<li>'+str(i)+': '+str(n)+' '+('Detailed route planning step. '*2)+'</li>' for n in range(3))+'</ol>' for i in range(3)]
    assert_three_practical_itineraries(''.join(lists)+LINKS)

@pytest.mark.parametrize('count',[0,1,2])
def test_label_or_incomplete_plans_cannot_pass(count):
    with pytest.raises(AssertionError):assert_three_practical_itineraries(LINKS+'<h1>Source itinerary</h1>'+''.join('<ol>'+''.join('<li>'+str(i)+' '+('Planning step. '*5)+'</li>' for j in range(3))+'</ol>' for i in range(count)))

def test_repeated_plan_cannot_prove_three_distinct_itineraries():
    plan='<ol>'+''.join('<li>'+str(n)+' '+('Same long planning step. '*2)+'</li>' for n in range(3))+'</ol>'
    with pytest.raises(AssertionError):assert_three_practical_itineraries(LINKS+plan*3)


@pytest.mark.parametrize('missing_formula,missing_markers',[(False,False),(True,False),(False,True)])
def test_workbook_checks_real_formula_cells_and_visual_markers(missing_formula,missing_markers):
    import io
    from openpyxl import Workbook
    from openpyxl.chart import BarChart,Reference
    from openpyxl.formatting.rule import DataBarRule,ColorScaleRule
    from tests.e2e.live_artifact_assertions import assert_visual_workbook_formulas
    workbook=Workbook();sheet=workbook.active;workbook.create_sheet('Notes')
    sheet.append(['ID','Area','Setup','Revenue','Cost','Profit','Days'])
    for i in range(2,6):sheet.append([i,'Area',100,70,30,'=D'+str(i)+'-E'+str(i),'=ROUND(C'+str(i)+'/F'+str(i)+',2)'])
    if missing_formula:sheet['F3']='40'
    chart=BarChart();chart.add_data(Reference(sheet,min_col=6,min_row=1,max_row=5),titles_from_data=True);sheet.add_chart(chart,'J2')
    rule=ColorScaleRule(start_type='min',start_color='FF0000',end_type='max',end_color='00FF00') if missing_markers else DataBarRule(start_type='min',end_type='max',color='008800')
    sheet.conditional_formatting.add('F2:F5',rule)
    output=io.BytesIO();workbook.save(output)
    if missing_formula or missing_markers:
        with pytest.raises(AssertionError):assert_visual_workbook_formulas(output.getvalue())
    else:assert_visual_workbook_formulas(output.getvalue())


@pytest.mark.parametrize('name,data,valid',[
    ('small.csv',b'id,status\n1,ok\n2,ok\n',True),
    ('small.tsv',b'id\tstatus\n1\tok\n',True),
    ('quoted.csv',b'id,description\n1,"a,b"\n',True),
    ('header.csv',b'id,status\n',False),
    ('ragged.csv',b'id,status\n1,ok,extra\n',False),
    ('empty.csv',b'\n\n',False)])
def test_live_download_contract_checks_delimited_rows_not_character_count(monkeypatch,name,data,valid):
    from scripts import live_stage_chat_contract_probe as probe
    monkeypatch.setattr(probe,'_artifact_bytes',lambda *args,**kwargs:(data,'text/csv'))
    scenario=probe.Scenario(name='tiny',prompt='synthetic',max_average_ms=None,max_single_ms=None)
    call=lambda:probe._download_and_check_artifacts('http://example.invalid',[{'name':name,'url':'/fixture'}],scenario=scenario,timeout=5)
    if valid:assert call()[0]['bytes']==len(data)
    else:
        with pytest.raises(AssertionError):call()


"""Itinerary semantics do not require a particular list marker style or language."""
import pytest
from tests.e2e.live_artifact_assertions import assert_three_practical_itineraries

@pytest.mark.parametrize('tag',['ol','ul'])
@pytest.mark.parametrize('label',['Recorrido','旅行の手順'])
def test_distinct_complete_itineraries_support_native_list_styles(tag,label):
    body=''.join(f'<section><h2>{label} {i}</h2><{tag}>'+''.join(f'<li>09:{j}0 — {label} {i} {j} station and destination details.</li>' for j in range(3))+f'</{tag}><a href="https://example.test/{i}">Fuente</a></section>' for i in range(3))
    assert_three_practical_itineraries(body)

@pytest.mark.parametrize('tag',['ol','ul'])
def test_links_and_one_incomplete_schedule_do_not_satisfy_three_trips(tag):
    with pytest.raises(AssertionError):
        assert_three_practical_itineraries(f'<{tag}><li>Short</li></{tag}>'+''.join(f'<a href="https://example.test/{i}">Source</a>' for i in range(3)))
