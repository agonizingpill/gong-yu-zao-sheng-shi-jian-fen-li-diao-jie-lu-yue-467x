import os,time,json,traceback,threading,uuid
from datetime import datetime,timedelta
import redis
from sqlalchemy import select,update,or_
from .database import Session
from .models import Separation,NoiseEvent
from .common import lock_case,ARTIFACTS_DIR
from .separation import calculate
q=redis.Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
# 心跳超过该时长未刷新即认定执行进程已中断，任务退回队列由存活进程接手。
STALE_AFTER=timedelta(seconds=float(os.getenv('SEPARATION_STALE_SECONDS','12')))
def heartbeat(jobid,token,stop):
    # 计算期间持续证明本进程存活；令牌失效（任务已被接手）即停止。
    while not stop.wait(2):
        try:
            with Session() as db:
                n=db.execute(update(Separation).where(Separation.id==jobid,Separation.run_token==token,Separation.state=='running').values(heartbeat_at=datetime.utcnow())).rowcount
                db.commit()
                if not n:return
        except Exception:traceback.print_exc()
def claim(db):
    # 数据库queued为持久任务源；Redis仅唤醒。中断残留的running先回收，原任务接着处理。
    cutoff=datetime.utcnow()-STALE_AFTER
    db.execute(update(Separation).where(Separation.state=='running',or_(Separation.heartbeat_at.is_(None),Separation.heartbeat_at<cutoff)).values(state='queued',recovered=True))
    db.commit()
    candidate=db.scalar(select(Separation).where(Separation.state=='queued').order_by(Separation.id))
    if candidate is None:return None,None,None
    case=lock_case(db,candidate.complaint_id)
    job=db.scalar(select(Separation).where(Separation.id==candidate.id).with_for_update().execution_options(populate_existing=True))
    if job.state!='queued':return None,None,None
    if not case.consent:
        job.state='failed';job.error='授权已撤回，任务取消；恢复授权后可重新提交';db.commit()
        return None,None,None
    if not job.snapshot:
        # 兼容快照缺失的旧任务：认领时冻结，此后新登记的事件不混入。
        job.snapshot=[{'id':e.id,'energies':e.energies} for e in db.scalars(select(NoiseEvent).where(NoiseEvent.id.in_(job.event_ids)).order_by(NoiseEvent.id))]
        job.event_ids=[e['id'] for e in job.snapshot]
    token=uuid.uuid4().hex
    job.state='running';job.run_token=token;job.attempts+=1;job.heartbeat_at=datetime.utcnow()
    db.commit()
    return job.id,token,list(job.snapshot)
while True:
    with open('/tmp/worker-heartbeat','w'):pass
    try:q.blpop('quiet:separation',timeout=2)
    except redis.RedisError:time.sleep(1)
    jobid=token=None
    try:
        with Session() as db:jobid,token,events=claim(db)
        if jobid is None:continue
        stop=threading.Event();pulse=threading.Thread(target=heartbeat,args=(jobid,token,stop),daemon=True);pulse.start()
        try:result=calculate(events)
        finally:stop.set();pulse.join()
        # 留档与库内结果同源：先写临时文件，库内发布成功后才转正，两边始终是同一份。
        ARTIFACTS_DIR.mkdir(exist_ok=True)
        tmp=ARTIFACTS_DIR/f'separation-{jobid}.tmp';tmp.write_text(json.dumps(result,ensure_ascii=False))
        with Session() as db:
            job=db.get(Separation,jobid);case=lock_case(db,job.complaint_id)
            if not case.consent:raise ValueError('授权撤回，分离结果不发布')
            # 计算期间源数据已更新的，结果发布即失效留档，不能用于归因确认。
            ids=list(db.scalars(select(NoiseEvent.id).where(NoiseEvent.complaint_id==job.complaint_id).order_by(NoiseEvent.id)))
            state='done' if ids==job.event_ids else 'stale'
            # fencing：仅当前令牌持有者可发布；旧进程晚回来影响0行，不覆盖已恢复的结果。
            n=db.execute(update(Separation).where(Separation.id==jobid,Separation.run_token==token,Separation.state=='running').values(result=result,state=state,error='')).rowcount
            db.commit()
            if not n:tmp.unlink(missing_ok=True);continue
        try:os.replace(tmp,ARTIFACTS_DIR/f'separation-{jobid}.json')
        except OSError:traceback.print_exc()
    except Exception as e:
        traceback.print_exc()
        if jobid and token:
            with Session() as db:
                db.execute(update(Separation).where(Separation.id==jobid,Separation.run_token==token,Separation.state=='running').values(state='failed',error=str(e)[:400]))
                db.commit()
