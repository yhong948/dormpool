"""Real GPU telemetry -> heartbeat -> Master -> frontend data contract + baseline roundtrip."""
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time

from dormpool.src.tests.e2e_local import Harness, http_json, REPO_ROOT
sys.path.insert(0,str(REPO_ROOT/'scripts'))
from measure_metrics import measure


def main():
    os.environ['DORMPOOL_MASTER_PYTHON']=str(REPO_ROOT/'.venv/Scripts/python.exe')
    os.environ['CUPY_CACHE_DIR']=str(REPO_ROOT/'._tmp/cupy-cache')
    os.environ['SHARD_SIZE']='1'
    root = REPO_ROOT/'._tmp'
    work = Path(tempfile.mkdtemp(prefix='metrics-gpu-',dir=root))
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0))
        port = sock.getsockname()[1]
    harness = Harness(port,1,12,1,100,work)
    print('Evidence:',work,flush=True)
    try:
        harness.start_master()
        harness.start_worker('metrics-gpu')
        harness.wait_registered(1,120)
        deadline = time.monotonic()+10
        while time.monotonic()<deadline:
            worker = http_json(harness.base+'/workers')['workers'][0]
            sample = worker.get('resource_sample',{})
            if sample.get('status')=='ok' and worker.get('gpu_pct') is not None: break
            time.sleep(0.1)
        assert sample['source']=='nvml' and sample['gpu_uuid'].startswith('GPU-'),sample
        assert 0<=worker['gpu_pct']<=100 and not sample.get('stale'),sample
        (work/'worker-sample.json').write_text(json.dumps(worker,indent=2),encoding='utf-8')
        source = harness.build_input(work/'input.zip')
        report = measure(harness.base,source,['metrics-gpu'],'convnext_infer',work/'benchmark.json')
        baseline = report['measurements'][report['baseline_jobs']['metrics-gpu']]
        resource = baseline['resources']['metrics-gpu']
        assert resource['avg_gpu_pct'] is not None and resource['gpu_pct_samples']>0,resource
        assert 0<resource['gpu_pct_coverage']<=1,resource
        summary = http_json(harness.base+'/metrics/summary')
        assert summary['benchmark']['benchmark_id']==report['benchmark_id']
        assert summary['p_pool']>0 and summary['live']['units_total']==12
        # A new ordinary Job must not change the frozen official baseline.
        again = http_json(harness.base+'/metrics/summary?job_id='+report['baseline_jobs']['metrics-gpu'])
        assert again['benchmark']['per_device_throughput']==report['per_device_throughput']
        (work/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
        print('PASS: NVML UUID/sample -> Worker heartbeat -> Master window average -> formal report',flush=True)
    finally:
        harness.stop()


if __name__=='__main__': main()
