import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
import net from 'node:net';
import {randomBytes} from 'node:crypto';
const origin = process.env.CELLD_FIXTURE_ORIGIN || 'http://127.0.0.1:18996';
const internalOrigin = process.env.CELLD_FIXTURE_INTERNAL_ORIGIN;
const wsOrigin = origin.replace('http:', 'ws:');
const results = [];
async function checked(name, fn) {let timer,result;try{result=await Promise.race([fn(),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(name+' timed out')),7000);})]);}finally{clearTimeout(timer);}results.push({fixture:name,passed:true,...(result?{result:JSON.stringify(result)}:{})});await writeFile('target/native-transport-progress.json',JSON.stringify(results));console.log(name,'PASS');}
async function socket(path) {
 const ws=new WebSocket(wsOrigin+path);ws.binaryType='arraybuffer';
 await new Promise((resolve,reject)=>{ws.addEventListener('open',resolve,{once:true});ws.addEventListener('error',reject,{once:true});});
 return ws;
}
function message(ws,value) {const p=new Promise((resolve,reject)=>{ws.addEventListener('message',e=>resolve(e.data),{once:true});ws.addEventListener('error',reject,{once:true});});ws.send(value);return p;}
function close(ws) {const p=new Promise(r=>ws.addEventListener('close',r,{once:true}));ws.close(1000,'done');return p;}
const state=async()=>await (await fetch(origin+'/state')).json();
await checked('communication',async()=>{const r=await (await fetch(origin+'/communication')).json();assert.ok(Object.values(r).every(x=>x===true));return r;});
await checked('sse-serving',async()=>assert.match(await (await fetch(origin+'/sse')).text(),/data: ?served/));
await checked('native-rejection',async()=>assert.equal(await (await fetch(origin+'/forward-reject')).text(),'rejected'));
await checked('current-request-and-response-middleware',async()=>{const response=await fetch(origin+'/forward-edit');assert.equal(response.status,201);assert.equal(response.headers.get('x-cell'),'edited');assert.equal(response.headers.get('x-response'),'edited');assert.equal(await response.text(),'changed');});
await checked('resident-open-message-close',async()=>{const before=await state();const ws=await socket('/resident');assert.equal(await message(ws,'resident'),'resident');await close(ws);const s=await state();assert.equal(s['resident-open'],(before['resident-open']||0)+1);assert.equal(s['resident-close'],(before['resident-close']||0)+1);});
await checked('hibernation-binary-tags-auto-response',async()=>{const before=await state();const ws=await socket('/ws');assert.equal(await message(ws,'text'),'text');const bytes=new Uint8Array([99,1,2,3,99]);assert.deepEqual([...new Uint8Array(await message(ws,bytes.subarray(1,4)))],[1,2,3]);assert.equal(await message(ws,'ping'),'pong');const s=await state();assert.equal(s.open,1);assert.deepEqual(s.tags,[['fixture']]);assert.deepEqual(s.auto,{request:'ping',response:'pong'});
 assert.ok(internalOrigin, 'Set CELLD_FIXTURE_INTERNAL_ORIGIN to the owned local native listener');
 assert.ok(Number.isFinite(s['auto-timestamp']),'Native auto-response Date is visible after ping');
 const evict=await fetch(internalOrigin+'/evict/TransportCell:'+s.id,{method:'POST'});assert.equal(evict.status,200,await evict.text());assert.equal(await message(ws,'after-eviction'),'after-eviction');await close(ws);const after=await state();assert.equal(after['hibernated-close'],1000);assert.equal(after['hibernated-close-count'],(before['hibernated-close-count']||0)+1);assert.equal(s['attachment-round-trip'],true);return {'accepted-socket':true,'tags':true,'auto-response':true,'auto-timestamp':true,'message-after-eviction':true,'close-after-eviction':true,'attachment-round-trip':true};
});
for(const [name,path] of [['unknown-attachment-version-rejected','/bad-version'],['missing-attachment-rejected','/missing-attachment']])await checked(name,async()=>{const ws=await socket(path);const closed=new Promise(resolve=>ws.addEventListener('close',resolve,{once:true}));ws.send('reject');const event=await closed;assert.equal(event.code,4008);assert.equal(event.reason,'Invalid dispatch attachment');});
await checked('hibernated-native-error',async()=>{const before=await state();const url=new URL(origin);const socket=net.connect({host:url.hostname,port:Number(url.port)});socket.on('error',()=>{});await new Promise((resolve,reject)=>{socket.once('connect',()=>{socket.write('GET /ws HTTP/1.1\r\nHost: '+url.host+'\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: '+randomBytes(16).toString('base64')+'\r\n\r\n');});let headers='';socket.on('data',data=>{headers+=data.toString();if(headers.includes('\r\n\r\n')){try{assert.match(headers,/HTTP\/1\.1 101/);resolve();}catch(error){reject(error);}}});socket.once('error',reject);});socket.destroy();const deadline=Date.now()+3000;let after;do{after=await state();if(after['hibernated-error']===(before['hibernated-error']||0)+1 && after.open===0)break;await new Promise(resolve=>setTimeout(resolve,20));}while(Date.now()<deadline);assert.equal(after['hibernated-error'],(before['hibernated-error']||0)+1);assert.equal(after['hibernated-error-value'],true);assert.equal(after.open,0);return {'typed-error':true};});
await checked('repeated-resident-cleanup',async()=>{const before=await state();for(let i=0;i<10;i++){const ws=await socket('/resident');assert.equal(await message(ws,'cycle'),'cycle');await close(ws);}const after=await state();assert.equal(after['resident-open'],(before['resident-open']||0)+10);assert.equal(after['resident-close'],(before['resident-close']||0)+10);assert.equal(after.open,0);});
await writeFile('target/native-transport-evidence.json',JSON.stringify(results,null,2)+'\n');
