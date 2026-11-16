import pytest
from tests.e2e.live_inventory_assertions import assert_numbered_inventory_identities


def _inventory(names):
    return {'inventory_index':[{'selection_index':i,'name':name} for i,name in enumerate(names,1)]}


@pytest.mark.parametrize('names',[
 ['Email deadline triage','Nullion social monitor   no empty graphs'],
 ['受信箱の確認','地域の監視   追加条件'],
 ['Revisión de correo','Monitor local   condición adicional'],
])
def test_concise_labels_preserve_unique_numbered_inventory_identity(names):
    reply='\n'.join(f'{i}. **{name}** — 9:00 AM' for i,name in enumerate(names,1))
    assert assert_numbered_inventory_identities(reply,_inventory(names))['all_identities_present']
    short=reply.replace(names[-1],names[-1].split('   ')[0])
    assert assert_numbered_inventory_identities(short,_inventory(names))['all_identities_present']


@pytest.mark.parametrize('reply,names',[
 ('1. Alpha — 9:00 AM',['Alpha first','Alpha second']),
 ('1. Alpha — 9:00 AM\n2. Beta — 10:00 AM',['Alpha first','Alpha second']),
 ('1. Beta — 9:00 AM\n2. Alpha — 10:00 AM',['Alpha first','Beta second']),
 ('1. Alpha — 9:00 AM\n1. Alpha — 10:00 AM',['Alpha first']),
 ('1. Alpha — 9:00 AM',['Alphabet']),
])
def test_missing_ambiguous_wrong_or_duplicate_inventory_rows_fail(reply,names):
    with pytest.raises(AssertionError):assert_numbered_inventory_identities(reply,_inventory(names))


@pytest.mark.parametrize('name',['Stock report release check','株式レポート','Informe bursátil'])
def test_exact_identity_accepts_typographic_dash_substitution(name):
    inventory={'inventory_index':[{'selection_index':1,'name':name+' - web - 2026-10-02'}]}
    assert_numbered_inventory_identities('1. '+name+' — Web — 2026-10-02',inventory)
    with pytest.raises(AssertionError):
        assert_numbered_inventory_identities('1. Unrelated — web — 2026-10-02',inventory)
