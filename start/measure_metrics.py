"""Independent warmup -> each single-device baseline -> pool, via the same HTTP API."""
import argparse
import json
from pathlib import Path
import time

import requests


def measure(master, source, participants, task_type='convnext_infer', output=None, name=''):
    master = master.rstrip('/')
    session = requests.Session()
    def get(path):
        reply = session.get(master+path,timeout=30)
        reply.raise_for_status()
        return reply.json()
    existing = get('/jobs')['jobs']
    if any(j['status'] in ('running','queued') for j in existing):
        raise RuntimeError('finish existing Jobs before starting an isolated benchmark')
    workers = {w['worker_id']:w for w in get('/workers')['workers']}
    for wid in participants:
        w = workers.get(wid,{})
        if w.get('conn_state')!='online' or task_type not in w.get('supported_task_types',[]):
            raise RuntimeError(f'{wid} is not online/ready for {task_type}')
    def run(ids,label):
        with open(source,'rb') as upload:
            reply = session.post(master+'/jobs',data={'task_type':task_type,'worker_ids':','.join(ids)},
                                 files={'file':('input.zip',upload,'application/zip')},timeout=180)
        reply.raise_for_status()
        jid = reply.json()['job_id']
        print(f'{label}: {jid} workers={ids}',flush=True)
        deadline = time.monotonic()+3600
        while time.monotonic()<deadline:
            job = get('/jobs/'+jid)
            if job['status']=='failed':
                raise RuntimeError(f'{label}: {jid} failed; inspect attempts')
            if job['status']=='completed' and job.get('output_ref'):
                if get('/metrics/summary?job_id='+jid).get('pool_measurement'):
                    return jid
            time.sleep(0.1)
        raise RuntimeError('benchmark Job did not finish within one hour')
    warmups, baselines = {}, {}
    for wid in participants:
        warmups[wid] = run([wid],'warmup')
        baselines[wid] = run([wid],'single-device baseline')
    pool = run(participants,'pool')
    reply = session.post(master+'/metrics/benchmarks',json=dict(name=name,participants=participants,
        warmup_jobs=warmups,baseline_jobs=baselines,pool_job=pool),timeout=30)
    reply.raise_for_status()
    report = reply.json()
    if output:
        Path(output).parent.mkdir(parents=True,exist_ok=True)
        Path(output).write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:report[k] for k in ('benchmark_id','status','per_device_throughput',
          'p_pool','eta_pool_pct','speedup_S','warnings')},ensure_ascii=False,indent=2),flush=True)
    return report


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--master',default='http://127.0.0.1:8000')
    parser.add_argument('--input',required=True)
    parser.add_argument('--workers',nargs='+',required=True)
    parser.add_argument('--task-type',default='convnext_infer')
    parser.add_argument('--output',default='data/benchmark-report.json')
    parser.add_argument('--name',default='')
    args = parser.parse_args()
    measure(args.master,args.input,args.workers,args.task_type,args.output,args.name)
