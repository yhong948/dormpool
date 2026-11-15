"""Engineering G6 gate: two independent CPU baselines then pool, on localhost.
Local Workers share a physical PC; the report must label this as needs_review.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import sys
import tempfile

from dormpool.src.tests.e2e_local import Harness, http_json, REPO_ROOT
sys.path.insert(0,str(REPO_ROOT/'start'))
from measure_metrics import measure


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port',type=int,default=0)
    parser.add_argument('--images',type=int,default=12)
    parser.add_argument('--delay-ms',type=int,default=100)
    args = parser.parse_args()
    port = args.port
    if not port:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0))
            port = sock.getsockname()[1]
    work = Path(tempfile.mkdtemp(prefix='dormpool-g6-'))
    os.environ['SHARD_SIZE']='1'
    harness = Harness(port,2,args.images,1,args.delay_ms,work)
    print('Evidence:',work,flush=True)
    try:
        harness.start_master()
        harness.start_worker('worker-A')
        harness.start_worker('worker-B')
        harness.wait_registered(2)
        initial = http_json(harness.base+'/metrics/summary')
        assert initial['speedup_S'] is None and initial['eta_pool_pct'] is None
        source = harness.build_input(work/'input.zip')
        report = measure(harness.base,source,['worker-A','worker-B'],'image_thumb',work/'summary.json')
        assert set(report['per_device_throughput'])=={'worker-A','worker-B'}
        for wid,rate in report['per_device_throughput'].items():
            base = report['measurements'][report['baseline_jobs'][wid]]
            assert base['workers']==[wid]
            assert abs(rate-base['units_total']/base['wall_s'])<1e-8
        pool = report['measurements'][report['pool_job']]
        assert abs(report['p_pool']-pool['units_total']/pool['wall_s'])<1e-8
        assert abs(report['speedup_S']-report['p_pool']/max(report['per_device_throughput'].values()))<1e-8
        assert report['status']=='needs_review'
        assert any('physical' in warning for warning in report['warnings'])
        print('G6 PASSED: independent runs and explicit local-only fairness warning',flush=True)
    finally:
        harness.stop()


if __name__=='__main__': main()
