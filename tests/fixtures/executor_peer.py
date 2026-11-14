"""Test-only protocol peer for deterministic pipe/crash/timeout fault injection."""
import json
import os
from pathlib import Path
import sys
import time

print(json.dumps({'type':'ready','protocol':1,'pid':os.getpid(),
                  'supported_task_types':['image_thumb'], 'gpu':{},'convnext':{}}),flush=True)
for line in sys.stdin:
    req = json.loads(line)
    if req['type'] == 'shutdown':
        break
    mode = req['task_type'] if req['task_type'] in ('hang','crash') else Path(req['input_path']).name
    if mode == 'crash':
        os._exit(17)
    if mode == 'hang':
        time.sleep(30)
    if mode == 'noise':
        sys.stderr.write('x'*2000+'\n')
        for _ in range(128):
            sys.stderr.write('diagnostic'*100+'\n')
        sys.stderr.flush()
    if mode == 'invalid':
        print('not json',flush=True)
        continue
    result = {key:req[key] for key in ('request_id','task_id','attempt_id')}
    if mode == 'mismatch':
        result['attempt_id'] = 'wrong-attempt'
    if mode != 'bad':
        Path(req['output_path']).write_bytes(b'output')
    print(json.dumps(dict(result,type='result',pid=os.getpid(),ok=mode!='bad',
                          units_done=1 if mode!='bad' else 0,
                          fatal=mode=='fatal',detail='input error' if mode=='bad' else 'ok')),flush=True)
    if mode == 'fatal':
        break
