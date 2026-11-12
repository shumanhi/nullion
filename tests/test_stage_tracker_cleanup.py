import io,json
from urllib.error import URLError


def test_tracker_cleanup_survives_evidence_outage_and_preserves_foreign_jobs(monkeypatch):
    from tests.e2e import test_live_stage_live_tracking as tracking
    from tests.e2e.test_live_stage_web_matrix import catalogue
    import urllib.request
    jobs=[{'id':'owned','name':'Unique fixture','state':'active','conversation_id':'web:owned'},
        {'id':'foreign','name':'Other fixture','state':'active','conversation_id':'web:other'},
        {'id':'foreign-cid','name':'Unique fixture','state':'active','conversation_id':'web:other'}]
    def offline(*a):raise URLError('bridge gone')
    monkeypatch.setattr(tracking,'_tracking_evidence',offline)
    monkeypatch.setattr(catalogue,'_get_json',lambda *a,**kw:{'trackers':jobs})
    sent=[]
    def post(request,**kw):
        sent.append((request.full_url,request.get_method()));return io.BytesIO(json.dumps({'ok':True}).encode())
    monkeypatch.setattr(urllib.request,'urlopen',post)
    tracking._stop_owned_tracker('http://127.0.0.1:8753',None,'Unique fixture','web:owned')
    assert sent==[('http://127.0.0.1:8753/api/live-trackers/owned/stop','POST')]
