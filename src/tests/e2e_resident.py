"""Resident GPU gates: reuse, pause/resume, crash/retry, permanent error, cleanup."""
import argparse
import io
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import time
import zipfile

import psutil
from dormpool.src.tests.e2e_local import Harness, http_json, http_bytes, multipart, REPO_ROOT


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0))
        return sock.getsockname()[1]


def until(fn, timeout=30):
    deadline = time.monotonic()+timeout
    while time.monotonic()<deadline:
        result = fn()
        if result:
            return result
        time.sleep(0.03)
    raise AssertionError('condition timed out')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--master-python',default=str(REPO_ROOT/'.venv/Scripts/python.exe'))
    args = parser.parse_args()
    os.environ['DORMPOOL_MASTER_PYTHON'] = str(Path(args.master_python).resolve())
    os.environ['CUPY_CACHE_DIR'] = str(REPO_ROOT/'._tmp/cupy-cache')
    os.environ['DORMPOOL_CONVNEXT_DEVICE'] = 'cuda'
    os.environ['SHARD_SIZE'] = '1'
    root = REPO_ROOT/'._tmp'
    root.mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='resident-gates-',dir=root))
    harness = Harness(free_port(),1,10,1,150,work)
    control = f'http://127.0.0.1:{free_port()}'
    print('Evidence:',work,flush=True)
    worker = None
    seen_pids = set()
    report = {}
    try:
        harness.start_master()
        worker = harness.start_worker('resident-gpu',int(control.rsplit(':',1)[1]))
        workers = harness.wait_registered(1,120)
        assert workers[0]['max_slots']==1
        assert 'convnext_infer' in workers[0]['supported_task_types'],workers
        status = lambda: http_json(control+'/status')
        pid = status()['executor']['pid']
        seen_pids.add(pid)
        from PIL import Image
        raw = io.BytesIO()
        Image.new('RGB',(320,240),(20,90,150)).save(raw,format='PNG')
        def submit(count=1,bad=False):
            source = io.BytesIO()
            with zipfile.ZipFile(source,'w') as z:
                for i in range(count):
                    z.writestr(f'{i}/same.png',b'not an image' if bad else raw.getvalue())
            body, headers = multipart({'task_type':'convnext_infer'},'file','input.zip',source.getvalue())
            jid = http_json(harness.base+'/jobs','POST',body,headers)['job_id']
            return jid,source.getvalue()
        jobview = lambda jid: http_json(harness.base+'/jobs/'+jid)
        def attempts(jid):
            return [a for t in jobview(jid)['tasks'] for a in t['attempts']]
        jid, source = submit(10)
        until(lambda: status()['busy_slots']>0)
        http_json(control+'/pause','POST',b'{}')
        until(lambda: status()['busy_slots']==0)
        before = len(attempts(jid))
        time.sleep(0.5)
        assert len(attempts(jid))==before, 'claimed while paused'
        assert jobview(jid)['done_tasks']<10
        assert status()['executor']['pid']==pid
        http_json(control+'/resume','POST',b'{}')
        job,_ = harness.poll_job(jid)
        assert job['status']=='completed' and job['done_tasks']==10,job
        assert all(len(t['attempts'])==1 for t in job['tasks']),job
        assert sum(a['units_done'] for a in attempts(jid))==10
        assert status()['executor']['pid']==pid
        blob = http_bytes(harness.base+f'/jobs/{jid}/result')
        source_path,result_path = work/'input.zip',work/'result.zip'
        source_path.write_bytes(source)
        result_path.write_bytes(blob)
        from dormpool.src.master.app.c3_validation import validate_convnext
        assert validate_convnext(str(source_path),str(result_path))==10
        # Regression oracle: the retained one-shot CLI must agree numerically.
        # Use the same batch size (1) as each resident Task: cuDNN may choose
        # different algorithms for a batch of 10 and change low logit digits.
        reference = work/'reference.zip'
        reference_input = work/'reference-input.zip'
        with zipfile.ZipFile(reference_input,'w') as archive:
            archive.writestr('same.png',raw.getvalue())
        single = subprocess.run([sys.executable,str(REPO_ROOT/'executor/run.py'),
                                 'convnext_infer',str(reference_input),str(reference)],
                                cwd=REPO_ROOT,capture_output=True,text=True,timeout=180)
        assert single.returncode==0,single.stderr
        import numpy as np
        with zipfile.ZipFile(result_path) as resident, zipfile.ZipFile(reference) as legacy:
            other = json.loads(legacy.read('predictions.json'))[0]
            for entry in json.loads(resident.read('predictions.json')):
                assert entry['top1_index']==other['top1_index']
                assert np.allclose(np.load(io.BytesIO(resident.read(entry['logits_name']))),
                                   np.load(io.BytesIO(legacy.read(other['logits_name']))),rtol=1e-4,atol=1e-4)
        logfile = work/'resident-gpu.log'
        log = logfile.read_text(encoding='utf-8',errors='replace')
        load_count = lambda text: len(re.findall(r'^.*\[executor\] pid=\d+ ConvNeXt loaded weights=',text,re.M))
        assert load_count(log)==1,log
        assert 'agent imports torch=False cupy=False' in log,log
        compute_times = [float(s) for s in re.findall(r'compute_s=([0-9.]+)',log)]
        report['reuse']={'tasks':10,'pid':pid,'model_loads':1,'task_compute_s':compute_times,
                         'artificial_delay_ms':150}
        print('PASS reuse, no Agent model, pause/resume, combined results',flush=True)

        # Kill only this test's Executor while its request is running.
        crash_job,_ = submit()
        crash_task = jobview(crash_job)['tasks'][0]['task_id']
        def request_active():
            active = status()['executor']['active_attempts']
            return active and f'request pid={pid} task={crash_task} ' in logfile.read_text(encoding='utf-8',errors='replace')
        until(request_active)
        psutil.Process(pid).kill()
        crash_view,_ = harness.poll_job(crash_job)
        assert crash_view['status']=='completed',crash_view
        history = crash_view['tasks'][0]['attempts']
        assert len(history)==2 and history[0]['status']=='failed' and history[1]['status']=='succeeded',history
        new_pid = status()['executor']['pid']
        seen_pids.add(new_pid)
        assert new_pid != pid
        report['crash']={'old_pid':pid,'new_pid':new_pid,'attempts':history}
        print('PASS crash recovery via new Master Attempt',flush=True)

        bad_job,_ = submit(bad=True)
        bad_view,_ = harness.poll_job(bad_job)
        bad_attempts = bad_view['tasks'][0]['attempts']
        assert bad_view['status']=='failed' and len(bad_attempts)==3,bad_view
        assert all(a['status']=='failed' and a['units_done']==0 for a in bad_attempts)
        assert status()['executor']['pid']==new_pid,'input error recycled healthy model'
        good_job,_ = submit()
        good_view,_ = harness.poll_job(good_job)
        assert good_view['status']=='completed'
        assert status()['executor']['pid']==new_pid
        report['permanent_input_error']={'attempts':3,'units':0,'next_job':'completed'}
        log = logfile.read_text(encoding='utf-8',errors='replace')
        assert load_count(log)==2
        print('PASS bounded retries and healthy process reuse after bad input',flush=True)

        # Graceful quit while a task is active must finish it and reap child.
        drain_job,_ = submit()
        until(lambda: status()['busy_slots']>0)
        http_json(control+'/quit','POST',b'{}')
        worker.wait(timeout=40)
        assert worker.returncode==0
        assert jobview(drain_job)['status']=='completed'
        until(lambda: all(not psutil.pid_exists(p) for p in seen_pids),5)
        report['cleanup']={'worker_exit':worker.returncode,'executor_pids_gone':sorted(seen_pids)}
        print('PASS graceful active-task quit and process cleanup',flush=True)

        # Real response timeouts must also go through the Master's retry limit.
        # Each task deliberately sleeps 150ms, exceeding this worker's 50ms budget.
        control = f'http://127.0.0.1:{free_port()}'
        previous_timeout = os.environ.get('DORMPOOL_TASK_TIMEOUT')
        os.environ['DORMPOOL_TASK_TIMEOUT'] = '0.05'
        try:
            worker = harness.start_worker('timeout-gpu',int(control.rsplit(':',1)[1]))
        finally:
            if previous_timeout is None:
                os.environ.pop('DORMPOOL_TASK_TIMEOUT',None)
            else:
                os.environ['DORMPOOL_TASK_TIMEOUT'] = previous_timeout
        until(lambda: any(w['worker_id']=='timeout-gpu' for w in http_json(harness.base+'/workers')['workers']),120)
        timed_job,_ = submit()
        timed_view,_ = harness.poll_job(timed_job)
        timed_attempts = timed_view['tasks'][0]['attempts']
        assert timed_view['status']=='failed' and len(timed_attempts)==3,timed_view
        assert all('timeout' in a['error_msg'] and a['units_done']==0 for a in timed_attempts),timed_attempts
        timed_log = (work/'timeout-gpu.log').read_text(encoding='utf-8',errors='replace')
        timeout_pids = set(map(int,re.findall(r'ready pid=(\d+) generation=',timed_log)))
        assert len(timeout_pids)==3, timed_log
        http_json(control+'/quit','POST',b'{}')
        worker.wait(timeout=40)
        until(lambda: all(not psutil.pid_exists(p) for p in timeout_pids),5)
        report['timeout']={'attempts':timed_attempts,'reaped_pids':sorted(timeout_pids)}
        print('PASS real timeouts, three bounded attempts, all process generations reaped',flush=True)
        report['metrics'] = http_json(harness.base+'/metrics/summary')
        (work/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        (work/'PASSED.txt').write_text('all resident GPU gates passed\n',encoding='utf-8')
        print('ALL RESIDENT GPU GATES PASSED',flush=True)
    finally:
        if worker and worker.poll() is None:
            try:
                http_json(control+'/quit','POST',b'{}')
                worker.wait(timeout=40)
            except Exception:
                for child in psutil.Process(worker.pid).children(recursive=True):
                    try: child.kill()
                    except psutil.NoSuchProcess: pass
        harness.stop()


if __name__=='__main__':
    main()
