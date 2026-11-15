// Exercise the actual console renderers using Node's built-in VM and a tiny DOM.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const html = fs.readFileSync('console/index.html','utf8');
const code = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements = {};
const document = {getElementById(id) {return elements[id] ||= {value:'',textContent:'',innerHTML:'',style:{},addEventListener(){}};},addEventListener(){}};
const sandbox = {document,window:{location:{search:'',origin:'http://localhost'}},localStorage:{getItem(){return null;}},URLSearchParams,Date,AbortController,setInterval(){},clearInterval(){},setTimeout,clearTimeout};
vm.runInNewContext(code.replace('  startAll();','  globalThis.renderTest={renderWorkers,renderMetrics};'),sandbox);
const view = sandbox.renderTest;
const worker = {worker_id:'w',conn_state:'online',gpu_name:'GPU',gpu_pct:0,resource_sample:{sampled_at:1,stale:false,status:'ok'}};
view.renderWorkers({workers:[worker]});
assert.match(elements.workersBody.innerHTML,/>0\.0%/);
view.renderWorkers({workers:[{...worker,gpu_pct:null,resource_sample:{sampled_at:1,error:'missing NVML'}}]});
assert.match(elements.workersBody.innerHTML,/不可用/);
view.renderWorkers({workers:[{...worker,resource_sample:{sampled_at:1,stale:true}}]});
assert.match(elements.workersBody.innerHTML,/采样过期/);
view.renderMetrics({live:{units_total:8,elapsed_s:2,throughput:4},generated_at:1});
assert.equal(elements.kS.textContent,'—');
assert.equal(elements.kEta.textContent,'—');
assert.equal(elements.liveRate.textContent,'4.00');
const run = {job_id:'base',wall_s:20,units_total:200,resources:{w:{avg_gpu_pct:0,gpu_pct_coverage:1}}};
view.renderMetrics({live:{},generated_at:1,benchmark:{status:'needs_review',warnings:['efficiency exceeds 100%; recheck baselines, batching, load and variance'],participants:['w'],p_pool:12,eta_pool_pct:120,speedup_S:1.2,per_device_throughput:{w:10},baseline_jobs:{w:'base'},pool_job:'pool',measurements:{base:run,pool:{...run,job_id:'pool',created_at:10,finished_at:30}}}});
assert.equal(elements.kS.textContent,'1.20×');
assert.equal(elements.kEta.textContent,'120.0%');
assert.match(elements.metricsBody.innerHTML,/base/);
assert.match(elements.metricsBody.innerHTML,/0\.0%/);
assert.match(elements.poolScope.textContent,/pool/);
assert.match(elements.benchmarkStatus.textContent,/待复核/);
view.renderMetrics({live:{},generated_at:1,benchmark:{mode:'automatic_run',status:'recorded',warnings:[],
  participants:['w','other'],p_pool:9,eta_pool_pct:75,speedup_S:1.125,
  per_device_throughput:{w:8,other:4},device_metrics:{
    w:{processing_s:12.5,units_done:100,time_source:'worker_window',resources:{avg_cpu_pct:10,avg_gpu_pct:0}},
    other:{processing_s:10,units_done:40,time_source:'worker_window',resources:{avg_cpu_pct:20,avg_gpu_pct:50}}},
  pool_job:'pool',measurements:{pool:{...run,job_id:'pool',created_at:10,finished_at:30}}}});
assert.match(elements.metricsBody.innerHTML,/12\.50 秒/);
assert.match(elements.metricsBody.innerHTML,/10\.00 秒/);
assert.match(elements.metricsBody.innerHTML,/P_i · other/);
assert.match(elements.metricsBody.innerHTML,/8\.000 张\/秒/);
assert.match(elements.metricsBody.innerHTML,/4\.000 张\/秒/);
assert.equal(elements.kEta.textContent,'75.0%');
assert.equal(elements.kS.textContent,'1.13×');
assert.match(elements.metricDefinition.textContent,/本次运行/);
assert.ok(html.indexOf('id="metricsBody"') < html.indexOf('id="kEta"'));
view.renderMetrics({live:{},generated_at:1,benchmark:{status:'needs_review',warnings:[],participants:[],
  p_pool:9,eta_pool_pct:null,speedup_S:null,per_device_throughput:{},pool_job:'pool',measurements:{pool:run}}});
assert.equal(elements.kS.textContent,'—');
assert.equal(elements.kEta.textContent,'—');
const dropdownPayload = {live:{},generated_at:1,benchmark_choices:
  [2,7,0,5,6,3,1,4].map(i => ({benchmark_id:'b'+i,name:'多 Worker 运行测评',created_at:1794672000+i}))};
view.renderMetrics(dropdownPayload);
const options = elements.benchmarkSelect.innerHTML;
assert.equal((options.match(/<option /g)||[]).length,6);
assert.match(options,/<option value="">最新记录<\/option>/);
assert.ok(!options.includes('value="b7"')); // The newest run already has the latest option.
assert.ok(options.indexOf('value="b6"') < options.indexOf('value="b5"'));
assert.ok(!options.includes('value="b1"'));
assert.match(options,/2026\/11\/15 00:00:06/); // Recorded time, explicitly Shanghai time.
elements.benchmarkSelect.value = 'b5';
view.renderMetrics(dropdownPayload);
assert.equal(elements.benchmarkSelect.value,'b5'); // Polls preserve the chosen history entry.
elements.benchmarkSelect.value = '';
console.log('Console metrics render gates passed');
