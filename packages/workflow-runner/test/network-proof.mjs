// Test-only preload instruments the actual packaged SDK; no production hooks.
import { createRequire } from 'node:module';
import http from 'node:http';
import https from 'node:https';
const require=createRequire(import.meta.url);
const sdkRequire=createRequire(require.resolve('@hatchet-dev/typescript-sdk'));
const axios=sdkRequire('axios');
let denied=0,wire=0;
const create=axios.create;
axios.create=function(config){
 const adapter=config.adapter;
 if(config.baseURL==='http://127.0.0.1:1'){
  if(typeof adapter!=='function')throw Error('REST adapter missing');
  config={...config,adapter:async request=>{denied++;return adapter(request);}};
 }
 return create.call(this,config);
};
for(const module of [http,https])for(const name of ['request','get'])module[name]=()=>{wire++;throw Error('test forbids all HTTP REST wire traffic');};
const send=process.send?.bind(process);
process.send=function(message,...args){
 if(message?.type==='ready'||message?.type==='drained')send?.({type:'network-proof',denied,wire});
 return send?.(message,...args);
};
