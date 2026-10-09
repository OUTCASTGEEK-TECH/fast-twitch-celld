import assert from 'node:assert/strict';
const origin=process.env.CELLD_FIXTURE_ORIGIN||'http://127.0.0.1:19015';
const baseline=Number(await (await fetch(origin+'/facet-state')).text())||0;
const ws=new WebSocket(origin.replace('http:','ws:')+'/facet-ws');ws.binaryType='arraybuffer';
await new Promise((resolve,reject)=>{ws.addEventListener('open',resolve,{once:true});ws.addEventListener('error',reject,{once:true});});
async function echo(value){const response=new Promise((resolve,reject)=>{ws.addEventListener('message',e=>resolve(e.data),{once:true});ws.addEventListener('error',reject,{once:true});});ws.send(value);return response;}
assert.equal(await echo('facet-owned'),'facet-owned');assert.deepEqual([...new Uint8Array(await echo(new Uint8Array([1,2,3])))],[1,2,3]);
const closed=new Promise(resolve=>ws.addEventListener('close',resolve,{once:true}));ws.close();await closed;
assert.equal(Number(await (await fetch(origin+'/facet-state')).text()),baseline+2);
assert.equal(await (await fetch(origin+'/facet-outbound')).text(),'true');
console.log('accepted-and-outbound-facet-websockets PASS');
