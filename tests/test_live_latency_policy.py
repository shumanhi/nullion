import math
import pytest
from scripts.live_stage_chat_contract_probe import assess_live_latency,Scenario,_assert_contract


@pytest.mark.parametrize('actual,target',[(122500,120000),(100302,90000),(53700,45000),(106856,90000),(36017,30000)])
def test_modest_real_provider_overruns_are_recorded_advice(actual,target):
    records=[]
    with pytest.warns(UserWarning,match='latency advisory'):
        result=assess_live_latency(actual,target,label='live request',advisories=records)
    assert result['status']=='advisory' and records==[result]
    assert result['actual_ms']==actual and result['target_ms']==target


@pytest.mark.parametrize('actual,target',[(251700,180000),(151000,120000),(37501,30000),(math.inf,1000),(math.nan,1000),(-1,1000)])
def test_substantial_delays_and_invalid_timing_still_fail(actual,target):
    with pytest.raises(AssertionError):assess_live_latency(actual,target,label='live request',strict=True)


def test_within_target_is_not_an_advisory():
    records=[]
    assert assess_live_latency(800,1000,label='request',advisories=records)['status']=='within_target'
    assert records==[]


def test_timing_advice_cannot_hide_missing_required_tool_work():
    case=Scenario(name='account',prompt='Read source',max_average_ms=90000,max_single_ms=120000,required_tool_all=('required_read',))
    with pytest.raises(AssertionError):
        _assert_contract(case,response={'text':'Done','artifacts':[]},elapsed_ms=122500,
            tool_names=set(),runtime_db_available=True)


def test_completed_complex_work_retains_attention_measurement_without_becoming_a_functional_failure():
    records=[]
    with pytest.warns(UserWarning,match="latency attention"):
        result=assess_live_latency(461922,300000,label="seven-file package",advisories=records)
    assert result["status"]=="attention" and result["actual_ms"]==461922 and records==[result]
