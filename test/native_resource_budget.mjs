/** Observed local request budget against the owned compiled direct-Cell fixture. */
import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
const origin=process.env.CELLD_FIXTURE_ORIGIN;
assert.ok(origin,'Set the owned native fixture origin');
let evidence={fixture:'bounded-adapter-validation-measurement',passed:true};
if(process.env.CELLD_MEASUREMENT_ONLY!=='1') {
const timings=[];const values=[];
for(let batch=0;batch<25;batch++){
 const records=await Promise.all(Array.from({length:4},async()=>{
  const start=performance.now();const response=await fetch(origin+'/rpc');
  assert.equal(response.status,200);return {value:Number(await response.text()),milliseconds:performance.now()-start};
 }));
 for(const record of records){values.push(record.value);timings.push(record.milliseconds);}
}
values.sort((a,b)=>a-b);
for(let index=1;index<values.length;index++)assert.equal(values[index],values[index-1]+1,'Concurrent effects execute exactly once');
timings.sort((a,b)=>a-b);
evidence={fixture:'local-concurrent-rpc-budget',passed:true,calls:100,concurrency:4,
 medianMilliseconds:timings[50],p95Milliseconds:timings[95],maxMilliseconds:timings[99],
 declaredBudget:{p95Milliseconds:100,maxMilliseconds:1000},scope:'This local native fixture and toolchain; no universal latency claim.'};
assert.ok(evidence.p95Milliseconds<100,'Local p95 budget exceeded');assert.ok(evidence.maxMilliseconds<1000,'Local maximum budget exceeded');
}
const operations=['baseline','validation','direct-call','adapter-call','options'];
async function operation(name) {
 const start=performance.now();const response=await fetch(origin+'/adapter-cost?operation='+name);
 assert.equal(response.status,200);const data=await response.json();const elapsed=performance.now()-start;
 assert.equal(data.iterations,20000);assert.equal(data.result,140000);assert.equal(data['fresh-option-object'],true);
 return {elapsed,data};
}
for(const name of operations)await operation(name);
const samples=[];
for(let round=0;round<5;round++) {
 const sample={};
 for(const name of operations)sample[name+'-ms']=(await operation(name)).elapsed;
 samples.push(sample);
}
const measurement={iterations:20000,rounds:5,clock:'Node performance.now (external monotonic)',samples,
 'validation-overhead-microseconds-per-call':samples.map(x=>(x['validation-ms']-x['baseline-ms'])*1000/20000),
 'adapter-overhead-microseconds-per-call':samples.map(x=>(x['adapter-call-ms']-x['direct-call-ms'])*1000/20000),
 'allocation-estimate':(await operation('options')).data['allocation-estimate'],
 methodology:'One warmup request per operation, then five paired rounds against the same compiled native Cell. Batch elapsed includes HTTP/RPC overhead; differences estimate incremental cached validation/adapter work and retain noise, including negatives. Celld clocks are frozen within a turn. Explicit JS-container counts are source estimates, corroborated by distinct projection identities; bytes, CLJS intermediates, V8 escape analysis and GC allocation are not measured.'};
evidence['adapter-validation-measurement']=measurement;
await writeFile('target/native-resource-evidence.json',JSON.stringify(evidence,null,2)+'\n');
console.log(evidence.fixture+' PASS');
