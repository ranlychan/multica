import { fork } from 'node:child_process';
import { writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';
import { validateSource } from '../dist/index.js';

const privateJWT='eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZW5hbnQifQ.c2lnbmF0dXJl';

let raw=''; for await(const chunk of process.stdin) raw+=chunk;
const config=JSON.parse(raw);
const children=[];
const emit=value=>console.log(JSON.stringify(value));
const request=async(path,body)=>{
 const response=await fetch(config.api+path,{method:body?'POST':'GET',headers:{authorization:`Bearer ${config.serverToken}`,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});
 const text=await response.text(); return {status:response.status,data:JSON.parse(text)};
};
const until=async(label,fn,timeout=90000)=>{const end=Date.now()+timeout;while(Date.now()<end){const value=await fn();if(value)return value;await new Promise(r=>setTimeout(r,300));}throw Error(`timeout: ${label}`);};
const revision=async version=>{
 const source=`import {defineWorkflow} from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'first',retries:1,timeoutMs:180000,run:async ctx=>{await ctx.log('info','node proof r${version}');if(ctx.input.retry&&ctx.attempt===0)throw Error('planned');await ctx.sleep(ctx.input.delay);return {revision:${version},run:ctx.runId,attempt:ctx.attempt};}},{id:'second',parents:['first'],run:async ctx=>({parent:ctx.parents.first,revision:${version}})}]});`;
 const result=await validateSource(source);const path=`${config.root}/r${version}.ts`;await writeFile(path,source,{mode:0o600});
 return {...result,workflow_id:'c78d9029-23da-4463-91ab-891742f94b11',revision_id:`c78d9029-23da-4463-91ab-891742f94b1${version}`,version,source_path:path,engine_workflow_name:`multica-c78d9029-23da-4463-91ab-891742f94b11-v${version}-${result.hash.slice(0,12)}`};
};
const start=async(revisions,generation,tlsOverride={})=>{
 const child=fork(fileURLToPath(new URL('../dist/cli.js',import.meta.url)),[],{silent:true,env:{LANG:'C'},execArgv:['--import',fileURLToPath(new URL('./network-proof.mjs',import.meta.url))]});children.push(child);
 let output='';child.stdout.on('data',x=>output+=x);child.stderr.on('data',x=>output+=x);
 let network;
 const ready=new Promise((resolve,reject)=>{child.on('message',m=>{if(m.type==='network-proof')network=m;if(m.type==='ready')resolve(m);if(m.type==='error')reject(Error(m.code));});child.on('exit',code=>reject(Error(`worker exited ${code}`)));setTimeout(()=>reject(Error('worker readiness timeout')),20000).unref();});
 child.send({mode:'worker',descriptor:{protocol:1,workspace_id:'workspace-node-proof',tenant_id:config.tenant,generation,engine:{token:config.workerToken,host_port:config.grpc,tls:{strategy:'mtls',server_name:'cluster',ca_file:`${config.certs}/ca.cert`,cert_file:`${config.certs}/client-worker.pem`,key_file:`${config.certs}/client-worker.key`,...tlsOverride}},product:{origin:'http://127.0.0.1:1',runner_token:'unused-product-proof'},revisions,drain_timeout_ms:5000}});
 const evidence=await ready;assert(!output.includes(config.workerToken));assert(!JSON.stringify(evidence).includes(config.certs));assert(network.denied>=1);assert.equal(network.wire,0);child.on('message',m=>{if(m.type==='drained'){assert(network.denied>=1);assert.equal(network.wire,0);emit({event:'network-after-execution',...network});}});emit({event:'ready',generation,network,registrations:evidence.registrations});return child;
};
try {
 await assert.rejects(fetch('http://127.0.0.1:1/api/v1/tenants/'+config.tenant));
 const r1=await revision(1),r2=await revision(2);
 if(!config.authoredBoundaryOnly){
 await assert.rejects(start([r1],'node-wrong-name',{server_name:'wrong.invalid'}),/workflow_worker_failed/);
 await assert.rejects(start([r1],'node-no-client-cert',{strategy:'tls',cert_file:undefined,key_file:undefined}),/workflow_worker_failed/);
 emit({event:'mtls-negative-cases',wrongIdentityRejected:true,missingClientCertificateRejected:true});
 }
 const sensitiveSource=`import {defineWorkflow,WorkflowError} from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'sensitive',run:async ctx=>{await ctx.log('info','private /srv/workspace/private.pem ${privateJWT}',{runner_token:'opaque-secret'});if(ctx.input.fail==='descriptor')throw new Proxy(new WorkflowError('workflow_operation_conflict'),{getOwnPropertyDescriptor(){throw new Error('/srv/workspace/private.pem token=leaked');}});if(ctx.input.fail==='prototype')throw new Proxy(new WorkflowError('workflow_operation_conflict'),{getPrototypeOf(){throw new Error('/srv/workspace/private.pem token=leaked');}});if(ctx.input.fail)throw new WorkflowError('/Users/private token=leaked');return {path:'/srv/workspace/private.pem',raw:'${privateJWT}'};}}]});`;
 const checked=await validateSource(sensitiveSource);const sensitivePath=`${config.root}/sensitive.ts`;await writeFile(sensitivePath,sensitiveSource,{mode:0o600});
 const sensitive={...checked,workflow_id:r1.workflow_id,revision_id:'c78d9029-23da-4463-91ab-891742f94b13',version:3,source_path:sensitivePath,engine_workflow_name:`multica-${r1.workflow_id}-v3-${checked.hash.slice(0,12)}`};
 const sensitiveWorker=await start([sensitive],'node-redaction');
 for(const fail of ['descriptor','prototype',true,false]){
  const submitted=await request(`/api/v1/stable/tenants/${config.tenant}/workflow-runs/trigger`,{workflowName:sensitive.engine_workflow_name,input:{multica_run_id:`redaction-${fail}`,input:{fail}},return_only_id:true});assert.equal(submitted.status,200);
  const done=await until('authored redaction boundary',async()=>{const r=await request(`/api/v1/stable/workflow-runs/${submitted.data.run.metadata.id}`);if(r.status===404)return false;assert.equal(r.status,200);return ['FAILED','COMPLETED'].includes(r.data.run.status)?r.data:false;});
  if(fail){assert.equal(done.run.status,'FAILED');assert.deepEqual(JSON.parse(done.run.errorMessage),{message:'workflow_step_failed'});for(const task of done.tasks)assert.deepEqual(JSON.parse(task.errorMessage),{message:'workflow_step_failed'});}
  else {assert.equal(done.run.status,'COMPLETED');assert.deepEqual(done.run.output,{value:{path:'[redacted]',raw:'[redacted]'}});}
  const logs=await request(`/api/v1/stable/tasks/${done.tasks[0].metadata.id}/logs?limit=10`);assert.equal(logs.status,200);assert(logs.data.rows.length>0);for(const entry of logs.data.rows){const message=JSON.parse(entry.message);assert.equal(message.message,'private [redacted] [redacted]');assert.deepEqual(message.fields,{runner_token:'[redacted]'});}
 }
 emit({event:'authored-boundaries',hostileReflection:['descriptor','prototype'],error:'workflow_step_failed',resultAndLogs:'redacted'});
 if(config.authoredBoundaryOnly){
  const drained=new Promise((resolve,reject)=>{sensitiveWorker.on('message',m=>{if(m.type==='drained')resolve();if(m.type==='error')reject(Error(m.code));});});sensitiveWorker.send({mode:'drain'});await drained;emit({event:'authored-boundary-only',drained:true});
 }else{
 sensitiveWorker.kill('SIGKILL');
 const old=await start([r1],'node-old');
 const submit=async(rev,id,delay,retry=false)=>request(`/api/v1/stable/tenants/${config.tenant}/workflow-runs/trigger`,{workflowName:rev.engine_workflow_name,input:{multica_run_id:id,input:{delay,retry}},return_only_id:true});
 const runId='59586e83-c2f7-4499-adab-5ad8056d1211';
 const submitted=await submit(r1,runId,45000);assert.equal(submitted.status,200);const engineId=submitted.data.run.metadata.id;
 const duplicate=await submit(r1,runId,45000);assert(duplicate.status>=400);emit({event:'duplicate',status:duplicate.status});
 const run=async()=>request(`/api/v1/stable/workflow-runs/${engineId}`);
 let taskId;
 const pending=async()=>{const r=await run();taskId=r.data.tasks?.[0]?.metadata.id;if(!taskId||r.data.run.status!=='RUNNING')return false;const d=await request(`/api/v1/stable/tenants/${config.tenant}/durable-tasks/${taskId}`);return d.data.some(e=>e.taskExternalId===taskId&&e.kind==='WAIT_FOR'&&e.isSatisfied===false&&e.waitData?.some(w=>w.kind==='SLEEP'&&Date.parse(e.insertedAt)+w.sleepDurationMs>Date.now()+20000))?d.data:false;};
 await until('unsatisfied durable sleep',pending);
 emit({event:'durable-wait',engineId,taskId});
 const newOnly=await start([r2],'node-new-only');
 const before=(await run()).data;emit({event:'pre-crash',status:before.run.status,tasks:before.tasks.map(t=>({id:t.metadata.id,status:t.status,attempt:t.attempt,retryCount:t.retryCount,isEvicted:t.isEvicted})),wait:await pending()});
 await until('durable eviction acknowledged',async()=>{const r=await run();return r.data.tasks.some(t=>t.metadata.id===taskId&&t.isEvicted===true);},10000);
 assert(await pending());
 old.kill('SIGKILL');await new Promise(r=>old.once('exit',r));emit({event:'killed-old-worker'});
 const current=await start([r1,r2],'node-restarted');
 newOnly.kill('SIGKILL');
 const fresh=await submit(r2,'59586e83-c2f7-4499-adab-5ad8056d1212',1000);assert.equal(fresh.status,200);
 let last;
 const recovered=await until('old revision completion',async()=>{const r=await run();last=r.data;if(r.data.run.status==='FAILED')throw Error('old run failed: '+JSON.stringify(r.data));return r.data.run.status==='COMPLETED'?r.data:false;},150000).catch(async error=>{emit({event:'recovery-diagnostic',run:last,wait:(await request(`/api/v1/stable/tenants/${config.tenant}/durable-tasks/${taskId}`)).data});throw error;});
 assert.deepEqual(recovered.run.output,{value:{parent:{revision:1,run:runId,attempt:0},revision:1}});
 assert.deepEqual(recovered.workflowConfig.idempotency,{expression:'input.multica_run_id',method:'TTL',ttlMs:3600000});
 for(const task of recovered.tasks)assert.equal(task.actionId,`${r1.engine_workflow_name}:${task.displayName.split('-179')[0]}`);
 const freshDone=await until('new revision completion',async()=>{const r=await request(`/api/v1/stable/workflow-runs/${fresh.data.run.metadata.id}`);return r.data.run.status==='COMPLETED'?r.data:false;});assert(JSON.stringify(freshDone).includes('"revision":2'));
 const logs=await request(`/api/v1/stable/tasks/${taskId}/logs?limit=10`);assert.equal(logs.status,200);assert(JSON.stringify(logs.data).includes('node proof r1'));
 assert(logs.data.rows.length>=2);
 emit({event:'outage-recovered',oldRunId:engineId,output:recovered.run.output,replayLogCount:logs.data.rows.length});
 const retry=await submit(r2,'59586e83-c2f7-4499-adab-5ad8056d1213',1000,true);assert.equal(retry.status,200);
 let lastRetry;
 const retried=await until('engine retry identity',async()=>{const r=await request(`/api/v1/stable/workflow-runs/${retry.data.run.metadata.id}`);lastRetry=r;if(r.status===404)return false;assert.equal(r.status,200);if(r.data.run.status==='FAILED')throw Error('retry run failed '+JSON.stringify(r.data));return r.data.run.status==='COMPLETED'?r.data:false;}).catch(async error=>{emit({event:'retry-diagnostic',last:lastRetry});for(const task of lastRetry?.data.tasks??[])emit({event:'retry-durable-diagnostic',taskId:task.metadata.id,wait:(await request(`/api/v1/stable/tenants/${config.tenant}/durable-tasks/${task.metadata.id}`)).data});throw error;});assert.equal(retried.run.output.value.parent.attempt,1);
 emit({event:'completed',oldRunId:engineId,oldOutput:recovered.run.output,newOutput:freshDone.run.output,retryOutput:retried.run.output,idempotency:recovered.workflowConfig.idempotency,actions:recovered.tasks.map(t=>t.actionId),replayLogCount:logs.data.rows.length});
 const drained=new Promise((resolve,reject)=>{current.on('message',m=>{if(m.type==='drained')resolve();if(m.type==='error')reject(Error(m.code));});});current.send({mode:'drain'});await drained;emit({event:'drained'});
 }
} finally {for(const child of children)child.kill('SIGKILL');}
