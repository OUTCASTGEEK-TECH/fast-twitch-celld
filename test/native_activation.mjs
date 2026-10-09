import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
const origin=process.env.CELLD_FIXTURE_ORIGIN;
const internal=process.env.CELLD_FIXTURE_INTERNAL_ORIGIN;
assert.ok(origin && internal, 'Use the owned native fixture and internal listener');
const snapshot=async()=>await (await fetch(origin+'/activation')).json();
const first=await snapshot();
assert.equal(first.ready,true);
const tokens=new Set([first.token]);
const times=[];
for(let cycle=0;cycle<10;cycle++) {
 const before=await snapshot();
 assert.equal(await (await fetch(origin+'/activation-add')).text(),String(before.count+1));
 const evict=await fetch(internal+'/evict/Counter:'+first.id,{method:'POST'});
 assert.equal(evict.status,200,await evict.text());
 const start=performance.now();
 const after=await snapshot();
 times.push(performance.now()-start);
 assert.equal(after.id,first.id);
 assert.equal(after.ready,true);
 assert.equal(after.count,0);
 assert.ok(!tokens.has(after.token),'Each activation must reconstruct its transient context');
 tokens.add(after.token);
}
const record={fixture:'repeat-activation-context-reconstruction',passed:true,cycles:10,
 'distinct-activation-tokens':tokens.size,'cold-activation-max-ms':Math.max(...times),
 'cold-activation-mean-ms':times.reduce((a,b)=>a+b,0)/times.length,
 'budget-max-ms':1000};
assert.ok(record['cold-activation-max-ms']<record['budget-max-ms']);
await writeFile('target/native-activation-evidence.json',JSON.stringify(record,null,2)+'\n');
console.log(record.fixture,'PASS');
