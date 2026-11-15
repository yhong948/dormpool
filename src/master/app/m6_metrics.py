"""Measured Job rates, independent benchmark baselines, and resource windows."""
from copy import deepcopy
import json
import logging
import os
from pathlib import Path
import threading
import time
import uuid


class MetricsCollector:
    def __init__(self, report_path=None):
        self._lock = threading.RLock()
        self._records = {}
        self._seen_tasks = set()
        self._node_events = []
        self._metadata = {}
        self._pool_jobs = {}
        self._samples = {}
        self._sample_keys = {}
        self._benchmarks = {}
        self._report_path = Path(report_path) if report_path else None
        if self._report_path and self._report_path.is_file():
            try:
                reports = json.loads(self._report_path.read_text(encoding='utf-8'))
                self._benchmarks = {r['benchmark_id']: r for r in reports
                                    if r.get('measurements') and r.get('p_pool') is not None}
            except (ValueError,KeyError,TypeError) as exc:
                logging.getLogger('metrics').warning('cannot load benchmark reports: %s',exc)

    def register_job(self, job_id, **metadata):
        with self._lock:
            self._metadata[job_id] = deepcopy(metadata)

    def record_attempt(self, worker_id, task_id, attempt_id, units_done, elapsed_s, **_):
        with self._lock:
            if units_done <= 0 or task_id in self._seen_tasks or attempt_id in self._records:
                return False
            self._seen_tasks.add(task_id)
            self._records[attempt_id] = dict(worker_id=worker_id,task_id=task_id,
                                             units_done=units_done,elapsed_s=elapsed_s)
            return True

    def record_node_event(self, worker_id, event):
        with self._lock:
            self._node_events.append(dict(worker_id=worker_id,event=event,ts=time.time()))

    def record_sample(self, worker_id, sample, received_at=None):
        if not sample or sample.get('sampled_at') is None:
            return
        with self._lock:
            series = self._samples.setdefault(worker_id, [])
            keys = self._sample_keys.setdefault(worker_id,set())
            if sample['sampled_at'] in keys:
                return  # heartbeats may carry the same 1Hz sample more than once
            keys.add(sample['sampled_at'])
            series.append({**deepcopy(sample),'received_at':received_at or time.time()})
            # Keep samples for up to one day, not unbounded process lifetime.
            cutoff = (received_at or time.time())-86400
            while series and series[0]['received_at'] < cutoff:
                keys.discard(series.pop(0)['sampled_at'])

    def resource_window(self, workers, start, end):
        result = {}
        with self._lock:
            samples = {wid:list(self._samples.get(wid,[])) for wid in workers}
        for wid, series in samples.items():
            values = {'cpu_pct':[], 'gpu_pct':[]}
            cursor = {'cpu_pct':start,'gpu_pct':start}
            for sample in sorted(series,key=lambda x:x['received_at']):
                # Master receive clock defines the window; never compare clocks
                # of independent PCs. Sampling/transport delay is reported.
                right = min(end,sample['received_at'])
                left = max(start,sample['received_at']-min(2.0,float(sample.get('interval_s',1))))
                for key in values:
                    weight = max(0,right-max(left,cursor[key]))
                    value = sample.get(key)
                    if weight and isinstance(value,(int,float)) and 0<=value<=100:
                        values[key].append((value,weight))
                        cursor[key] = right
            item = {}
            for key, pairs in values.items():
                coverage = sum(weight for _,weight in pairs)
                item['avg_'+key] = sum(v*w for v,w in pairs)/coverage if coverage else None
                item[key+'_coverage'] = min(1.0,coverage/max(end-start,1e-9))
                item[key+'_samples'] = len(pairs)
            result[wid] = item
        return result

    def record_pool_job(self, job_id, created_at, finished_at, units_total, **details):
        if not finished_at or finished_at <= created_at or units_total <= 0:
            return
        with self._lock:
            measurement = dict(self._metadata.get(job_id, {}), **details)
            measurement.update(job_id=job_id,created_at=created_at,finished_at=finished_at,
                               wall_s=finished_at-created_at,units_total=units_total,
                               p_pool=units_total/(finished_at-created_at))
            measurement['resources'] = self.resource_window(measurement.get('workers',[]),created_at,finished_at)
            self._pool_jobs[job_id] = deepcopy(measurement)

    def _save_benchmarks_locked(self):
        if not self._report_path:
            return
        self._report_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._report_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(list(self._benchmarks.values()),
                                        ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(temporary, self._report_path)

    @staticmethod
    def device_measurements(run):
        """Observed per-worker throughput; independent baseline rates stay separate."""
        result = {}
        for wid in run.get('workers', []):
            units = run.get('per_worker_units', {}).get(wid, 0)
            window = run.get('per_worker_windows', {}).get(wid, {})
            start, end = window.get('started_at'), window.get('finished_at')
            measured = start is not None and end is not None
            # Older reports did not retain per-worker timestamps. Use their
            # recorded Job window explicitly, never summed concurrent durations.
            elapsed = max(0, end - start) if measured else run['wall_s']
            result[wid] = dict(units_done=units, processing_s=elapsed,
                               p_i=units / elapsed if elapsed > 0 else None,
                               time_source='worker_window' if measured else 'job_window',
                               started_at=start if measured else run.get('created_at'),
                               finished_at=end if measured else run.get('finished_at'),
                               resources=run.get('resources', {}).get(wid, {}),
                               device=run.get('worker_devices', {}).get(wid, {}))
        return result

    def _automatic_report(self, run, benchmark_id=None, created_at=None):
        devices = self.device_measurements(run)
        rates = {wid: item['p_i'] for wid, item in devices.items()}
        complete = bool(rates) and all(value is not None for value in rates.values())
        p_sum = sum(rates.values()) if complete else None
        p_max = max(rates.values()) if complete else None
        warnings = []
        identities = [(d['device'].get('hostname'), d['device'].get('gpu_uuid'))
                      for d in devices.values()]
        known = [identity for identity in identities if any(identity)]
        if len(set(known)) < len(known):
            warnings.append('参与 Worker 共用物理设备；这是本地并发模拟。')
        if any(item['time_source'] == 'job_window' for item in devices.values()):
            warnings.append('历史记录缺少设备起止时间，处理时间采用完整 Job 窗口。')
        if run.get('failed_attempts'):
            warnings.append('处理时间包含重试，图片数仅计唯一成功结果。')
        if run.get('profile_changed'):
            warnings.append('运行期间设备配置发生变化。')
        return dict(benchmark_id=benchmark_id or 'auto-' + uuid.uuid4().hex[:8],
                    name='单机运行测评' if len(devices) == 1 else '多 Worker 运行测评',
                    created_at=time.time() if created_at is None else created_at,
                    status='needs_review' if warnings else 'recorded', warnings=warnings,
                    participants=list(devices), device_metrics=devices,
                    per_device_throughput=rates, p_pool=run['p_pool'],
                    eta_pool_pct=run['p_pool'] / p_sum * 100 if p_sum else None,
                    speedup_S=run['p_pool'] / p_max if p_max else None,
                    p_sum=p_sum, p_max=p_max, baseline_jobs={}, warmup_jobs={},
                    pool_job=run['job_id'], measurements={run['job_id']: deepcopy(run)},
                    unit='verified images/s', mode='automatic_run',
                    rate_source='observed_worker_window',
                    conditions={key: run.get(key) for key in
                                ('task_type', 'input_sha256', 'shard_size', 'correctness_version')})

    def record_automatic_benchmark(self, job_id):
        """Retain every Job's device timings and observed aggregate metrics."""
        with self._lock:
            run = self._pool_jobs.get(job_id)
            if not run or not run.get('workers'):
                return None
            existing = next((r for r in self._benchmarks.values()
                             if r.get('pool_job') == job_id
                             and r.get('mode', '').startswith('automatic_')), None)
            report = self._automatic_report(run, existing['benchmark_id'] if existing else None)
            self._benchmarks[report['benchmark_id']] = report
            self._save_benchmarks_locked()
            return deepcopy(report)

    def latest_pool_measurement(self):
        with self._lock:
            return deepcopy(max(self._pool_jobs.values(),key=lambda x:x['finished_at'])) if self._pool_jobs else None

    def auto_baselines(self):
        """Per-worker average throughput from solo (single-worker) completed jobs.

        A solo job is any completed pool_job whose workers list has exactly one entry.
        Averaging multiple solo runs gives a stable P_i baseline for each machine.
        Returns {worker_id: avg_P_imgs_per_s}.
        """
        with self._lock:
            jobs = list(self._pool_jobs.values())
        per_worker = {}
        for job in jobs:
            workers = job.get('workers', [])
            if len(workers) != 1:
                continue
            wid = workers[0]
            p = job.get('p_pool', 0.0)
            if p > 0:
                per_worker.setdefault(wid, []).append(p)
        return {wid: sum(vals) / len(vals) for wid, vals in per_worker.items()}

    def create_benchmark(self, participants, baseline_jobs, pool_job, warmup_jobs, name='', observed_jobs=()):
        with self._lock:
            if (not isinstance(participants,list) or not all(isinstance(w,str) and w for w in participants)
                    or not isinstance(baseline_jobs,dict) or not isinstance(warmup_jobs,dict)
                    or not isinstance(pool_job,str)):
                raise ValueError('participants must be Worker IDs; baseline_jobs/warmup_jobs must map IDs to Jobs')
            participants = sorted(set(participants))
            if not participants or set(baseline_jobs)!=set(participants) or set(warmup_jobs)!=set(participants):
                raise ValueError('every participant requires an independent baseline and warmup Job')
            ids = list(baseline_jobs.values())+[pool_job]+list(warmup_jobs.values())
            if len(set(ids)) != len(ids):
                raise ValueError('warmup, baseline and pool Jobs must be distinct runs')
            if any(jid not in self._pool_jobs for jid in ids):
                raise ValueError('all referenced Jobs must be completed with verified, downloadable results')
            runs = deepcopy(self._pool_jobs)
            pool = runs[pool_job]
            if set(pool.get('workers',[])) != set(participants):
                raise ValueError('pool participants must match the actual participating workers')
            conditions = ('task_type','input_sha256','shard_size','correctness_version')
            if not all(pool.get(key) is not None for key in conditions):
                raise ValueError('missing measurement provenance')
            warnings = []
            per_device = {}
            for wid in participants:
                base, warm = runs[baseline_jobs[wid]], runs[warmup_jobs[wid]]
                if base.get('workers') != [wid] or warm.get('workers') != [wid]:
                    raise ValueError(f'{wid}: baseline and warmup must run on this worker alone')
                if any(base.get(key)!=pool[key] for key in conditions):
                    raise ValueError('baseline and pool must use identical input, workload, shard and correctness standard')
                if warm.get('task_type') != pool['task_type']:
                    raise ValueError('warmup workload mismatch')
                if warm['finished_at']>base['created_at'] or base['finished_at']>pool['created_at']:
                    raise ValueError('warmup must precede baseline, and baselines must precede pool run')
                if not base.get('execution_profiles',{}).get(wid):
                    raise ValueError('missing execution profile; update and re-register Worker')
                if (base['execution_profiles'][wid]!=pool.get('execution_profiles',{}).get(wid)
                        or base['execution_profiles'][wid]!=warm.get('execution_profiles',{}).get(wid)):
                    raise ValueError('execution settings/model changed between runs')
                per_device[wid] = base['p_pool']
            for jid in ids:
                run = runs[jid]
                if run.get('profile_changed'):
                    raise ValueError('execution profile changed during measurement')
                if run.get('failed_attempts'):
                    warnings.append(f'{jid}: retries observed; retry time retained, failed/duplicate units excluded')
                if any(run['created_at']<=e['ts']<=run['finished_at'] and e['worker_id'] in run['workers']
                       and e['event'] in ('pause','resume','join','timeout','leave') for e in self._node_events):
                    raise ValueError('worker availability changed during a measurement')
            for jid in list(baseline_jobs.values())+[pool_job]:
                run = runs[jid]
                other_runs = list(runs.items()) + [(r['job_id'],r) for r in observed_jobs]
                for other_id, other in other_runs:
                    if other_id != jid and set(other['workers']) & set(run['workers']) and (
                            max(other['created_at'],run['created_at']) < min(other['finished_at'] or time.time(),run['finished_at'])):
                        raise ValueError('measurement overlaps another Job on the same worker')
            for jid in list(baseline_jobs.values())+[pool_job]:
                if runs[jid]['wall_s']<10:
                    warnings.append(f'{jid}: window shorter than 10s; verify steady load')
                if pool['task_type'] in ('convnext_infer','image_thumb_gpu') and any(
                        r.get('gpu_pct_coverage',0)<0.8 for r in runs[jid].get('resources',{}).values()):
                    warnings.append(f'{jid}: GPU sample coverage below 80%')
            devices = pool.get('worker_devices',{})
            physical = [str(devices.get(wid)) for wid in participants]
            if len(set(physical))<len(participants) or len({devices.get(w,{}).get('hostname') for w in participants})<len(participants):
                warnings.append('workers share a physical device; not a multi-device benchmark')
            signatures = [pool['execution_profiles'][wid] for wid in participants]
            if any((p.get('weights_sha256'),p.get('implementation_sha256')) !=
                   (signatures[0].get('weights_sha256'),signatures[0].get('implementation_sha256')) for p in signatures):
                raise ValueError('participants have different model weights or workload implementation')
            p_sum, p_max = sum(per_device.values()), max(per_device.values())
            eta, speedup = pool['p_pool']/p_sum*100, pool['p_pool']/p_max
            if eta>100:
                warnings.append('efficiency exceeds 100%; recheck baselines, batching, load and variance')
            report = dict(benchmark_id='bench-'+uuid.uuid4().hex[:8],name=name,created_at=time.time(),
                          status='needs_review' if warnings else 'ready',warnings=warnings,
                          participants=participants,per_device_throughput=per_device,p_pool=pool['p_pool'],
                          eta_pool_pct=eta,speedup_S=speedup,p_sum=p_sum,p_max=p_max,
                          baseline_jobs=baseline_jobs,warmup_jobs=warmup_jobs,pool_job=pool_job,
                          conditions={key:pool[key] for key in conditions},
                          measurements={jid:runs[jid] for jid in ids},unit='verified images/s',
                          mode='official')
            updated = {**self._benchmarks,report['benchmark_id']:report}
            self._benchmarks = updated
            self._save_benchmarks_locked()
            return deepcopy(report)

    def benchmarks(self):
        with self._lock:
            return deepcopy(list(self._benchmarks.values()))

    def summary(self, live=None, benchmark_id=None):
        with self._lock:
            ordered = sorted(self._benchmarks.values(),
                             key=lambda r: (r.get('created_at', 0), r['benchmark_id']), reverse=True)
            benchmark = deepcopy(self._benchmarks.get(benchmark_id)) if benchmark_id else (
                deepcopy(ordered[0]) if ordered else None)
            if benchmark and benchmark.get('mode', '').startswith('automatic_'):
                run = benchmark['measurements'][benchmark['pool_job']]
                benchmark = self._automatic_report(run, benchmark['benchmark_id'], benchmark['created_at'])
            # One latest record plus up to five older choices; retain full
            # reports on disk and through the benchmark history API.
            choices = [{'benchmark_id': r['benchmark_id'], 'name': r['name'],
                        'created_at': r.get('created_at')} for r in ordered[:6]]
            events = deepcopy(self._node_events[-20:])
            frozen = deepcopy(self._pool_jobs.get((live or {}).get('job_id')))
        now = time.time()
        live = dict(live or {})
        with self._lock:
            live.update(deepcopy(self._metadata.get(live.get('job_id'),{})))
        if frozen:
            live.update(frozen)
        elif live.get('status')=='completed':
            live.update(status='finalizing',finished_at=None)
        elapsed = max(0,(live.get('finished_at') or now)-live.get('created_at',now))
        live_rate = live.get('units_total',0)/elapsed if elapsed>0 else None
        contributions = {wid:units/elapsed for wid,units in live.get('per_worker_units',{}).items()} if elapsed>0 else {}
        resource = frozen.get('resources',{}) if frozen else self.resource_window(
            live.get('workers',[]),live.get('created_at',now),now)

        # Auto baselines: average P_i from solo completed jobs per worker.
        ab = self.auto_baselines()

        # Auto S and eta for the current job's worker set vs solo baselines.
        if frozen:
            current_p = frozen.get('p_pool')
            current_workers = frozen.get('workers', [])
        else:
            current_p = live_rate
            current_workers = list(live.get('per_worker_units', {}).keys()) or list(live.get('workers', []))

        auto_S = None
        auto_eta = None
        auto_p_sum = None
        auto_p_max = None
        if current_p and current_workers:
            covered = [w for w in current_workers if w in ab]
            if covered:
                auto_p_max = max(ab[w] for w in covered)
                auto_p_sum = sum(ab[w] for w in covered)
                if auto_p_max > 0:
                    auto_S = current_p / auto_p_max
                if auto_p_sum > 0:
                    auto_eta = current_p / auto_p_sum * 100

        return dict(
            benchmark_choices=choices,
            generated_at=now,
            live={**live,'elapsed_s':elapsed,'throughput':live_rate,'per_worker_rate':contributions,
                  'unit':'verified images/s','resources':resource},
            benchmark=benchmark,
            p_pool=frozen['p_pool'] if frozen else None,
            p_pool_source='pool_wall_clock' if frozen else ('live' if live_rate else 'no_completed_selected_job'),
            pool_measurement=frozen,
            per_device_throughput=contributions,
            auto_baselines=ab,
            auto_speedup_S=auto_S,
            auto_eta_pct=auto_eta,
            auto_p_sum=auto_p_sum,
            auto_p_max=auto_p_max,
            eta_pool_pct=benchmark['eta_pool_pct'] if benchmark else None,
            speedup_S=benchmark['speedup_S'] if benchmark else None,
            total_records=live.get('succeeded_attempts',0),
            node_events=events,
            avg_cpu_pct={wid:v['avg_cpu_pct'] for wid,v in resource.items()},
            avg_gpu_pct={wid:v['avg_gpu_pct'] for wid,v in resource.items()},
        )


metrics = MetricsCollector()
