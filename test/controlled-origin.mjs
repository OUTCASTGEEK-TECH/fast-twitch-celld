/** Local HTTP/SSE/TCP endpoints used only by the native Celld qualification. */
import http from 'node:http';
import net from 'node:net';
import tls from 'node:tls';
import {readFileSync,existsSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
const httpServer=http.createServer((req,res)=>{
  if(req.url.startsWith('/sse')) {
    res.writeHead(200,{'content-type':'text/event-stream'});
    res.end('id: fixture\ndata: hello\n\n');
  } else if(req.url.startsWith('/slow')) {
    const timer=setTimeout(()=>res.end('late'),10000);req.on('close',()=>clearTimeout(timer));
  } else res.end('controlled');
});
const tcpServer=net.createServer({allowHalfOpen:true},socket=>{
  const chunks=[];socket.on('data',chunk=>chunks.push(chunk));
  socket.on('end',()=>socket.end('echo:'+Buffer.concat(chunks).toString()));
  socket.on('error',()=>{});
});
let tlsServer;
if(process.env.CELLD_TLS_FIXTURE==='1') {
 const key='target/local-tls.key',cert='target/local-tls.crt';
 if(!existsSync(key)||!existsSync(cert)) {
  const generated=spawnSync('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',key,'-out',cert,'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],{stdio:'ignore'});
  if(generated.status!==0)throw new Error('Owned TLS fixture certificate generation failed');
 }
 tlsServer=tls.createServer({key:readFileSync(key),cert:readFileSync(cert)},socket=>{socket.on('error',()=>{});socket.end('tls-ready');});
 tlsServer.on('tlsClientError',()=>{});
 await new Promise(resolve=>tlsServer.listen(18997,'127.0.0.1',resolve));
}
await Promise.all([new Promise(resolve=>httpServer.listen(18991,'127.0.0.1',resolve)),new Promise(resolve=>tcpServer.listen(18995,'127.0.0.1',resolve))]);
console.log('Controlled origin ready 18991/18995');
process.on('SIGINT',()=>{httpServer.close();tcpServer.close();tlsServer?.close();});
