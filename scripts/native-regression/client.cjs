const http = require('node:http');
const {randomUUID}=require('node:crypto');

function parseJSON(raw,context) {
  try {return JSON.parse(raw);}
  catch {const error=Error('Malformed '+context+' JSON');error.code='invalid_json';throw error;}
}

// Never execute a streamed call until the full message and its JSON input end.
class MessageSSE {
  constructor(onEvent) { this.onEvent=onEvent; this.buffer=''; this.message=null; this.blocks=new Map(); this.inputs=new Map(); this.closed=new Set(); this.stopped=false; }
  feed(chunk) {
    this.buffer+=chunk;
    let boundary;
    while((boundary=this.buffer.search(/\r?\n\r?\n/))>=0) {
      const separator=this.buffer.slice(boundary).match(/^\r?\n\r?\n/)[0];
      const frame=this.buffer.slice(0,boundary).replace(/\r\n/g,'\n'); this.buffer=this.buffer.slice(boundary+separator.length);
      const lines=frame.split('\n').filter(line=>line.startsWith('data:')).map(line=>line.slice(5).trimStart());
      if(!lines.length)continue;
      const event=parseJSON(lines.join('\n'),'SSE event');
      if(this.stopped)throw Error('Event after message_stop');
      switch(event.type) {
        case 'error': {
          const error=Error('SSE upstream error');
          if(/^[a-zA-Z0-9_-]{1,80}$/.test(event.error?.type||''))error.code=event.error.type;
          throw error;
        }
        case 'message_start':
          if(this.message)throw Error('Duplicate message_start');
          this.message={...event.message,content:[]};break;
        case 'content_block_start':
          if(this.blocks.has(event.index))throw Error('Duplicate content block');
          this.blocks.set(event.index,structuredClone(event.content_block));break;
        case 'content_block_delta': {
          const block=this.blocks.get(event.index);if(!block||this.closed.has(event.index))throw Error('Delta outside open block');
          const delta=event.delta;
          if(delta.type==='text_delta')block.text=(block.text||'')+delta.text;
          if(delta.type==='thinking_delta')block.thinking=(block.thinking||'')+delta.thinking;
          if(delta.type==='signature_delta')block.signature=(block.signature||'')+delta.signature;
          if(delta.type==='input_json_delta')this.inputs.set(event.index,(this.inputs.get(event.index)||'')+delta.partial_json);
          break;
        }
        case 'content_block_stop': {
          const block=this.blocks.get(event.index);if(!block||this.closed.has(event.index))throw Error('Invalid block end');
          if(this.inputs.has(event.index))block.input=parseJSON(this.inputs.get(event.index),'tool input');
          this.closed.add(event.index);break;
        }
        case 'message_delta':
          if(!this.message)throw Error('Missing message_start');
          Object.assign(this.message,event.delta);break;
        case 'message_stop': this.stopped=true;break;
      }
      if(this.onEvent)this.onEvent(event);
    }
  }
  finish() {
    if(!this.stopped||!this.message||this.closed.size!==this.blocks.size||this.buffer.trim())throw Error('Incomplete SSE message');
    this.message.content=[...this.blocks.entries()].sort((a,b)=>a[0]-b[0]).map(([,block])=>block);
    return this.message;
  }
}

function postOnce({endpoint,key,session,payload,signal,timeoutMs=180000,onHeaders,onEvent,requestId:sentId='regression-'+randomUUID()}) {
  const url=new URL(endpoint);
  if(url.protocol!=='http:'||!['127.0.0.1','localhost','[::1]'].includes(url.hostname))throw Error('Regression endpoint must be local HTTP');
  const body=JSON.stringify(payload),start=performance.now();
  return new Promise((resolve,reject)=>{
    let raw='',firstByteMs,status,shouldRetry,requestId=sentId,deadline,settled=false;
    const fail=error=>{
      if(settled)return;
      settled=true;clearTimeout(deadline);
      error.evidence={raw,status,shouldRetry,requestId,ms:Math.round(performance.now()-start),firstByteMs,code:error.code};
      reject(error);
    };
    const request=http.request(url,{method:'POST',signal,headers:{'content-type':'application/json','content-length':Buffer.byteLength(body),'x-api-key':key,'x-session-id':session,'x-request-id':sentId}},response=>{
      const parser=payload.stream?new MessageSSE(onEvent):null;
      status=response.statusCode;requestId=response.headers['request-id']||response.headers['x-request-id']||sentId;
      shouldRetry=response.headers['x-should-retry'];
      response.setEncoding('utf8');
      response.on('data',chunk=>{
        firstByteMs??=Math.round(performance.now()-start);raw+=chunk;
        if(parser&&response.statusCode===200){try{parser.feed(chunk);}catch(error){fail(error);request.destroy(error);}}
      });
      response.on('error',fail);
      response.on('end',()=>{
        if(settled)return;
        try {
          if(response.statusCode!==200)throw Error('HTTP '+response.statusCode);
          const reply=parser?parser.finish():parseJSON(raw,'response');
          if(!Array.isArray(reply.content)||!reply.stop_reason)throw Error('Incomplete API reply');
          settled=true;clearTimeout(deadline);
          resolve({reply,raw,requestId,ms:Math.round(performance.now()-start),firstByteMs,bytes:Buffer.byteLength(body),stream:!!payload.stream});
        }catch(error){fail(error);}
      });
      if(onHeaders) {try{onHeaders({status,requestId});}catch(error){fail(error);request.destroy(error);}}
    });
    deadline=setTimeout(()=>request.destroy(Error('Regression request deadline exceeded')),timeoutMs);
    request.setTimeout(timeoutMs,()=>request.destroy(Error('Regression request timeout')));
    request.on('error',fail);request.end(body);
  });
}

// Quick limits new upstream sessions in windows: a burst succeeds, then setup is
// closed with 1011 for about a minute. The proxy reports this before any user
// message is sent, so waiting and resending cannot run anything twice. Only that
// exact condition is resent; all other failures (including content filtering) are not.
const SETUP_REJECTED=/Quick connection setup (?:failed|timed out) at session_(?:connect|create) \(WebSocket close 1011\); no user message was sent/;
const sleepMs=ms=>new Promise(resolve=>setTimeout(resolve,ms));

async function post(options) {
  const {setupCooldownMs=90000,setupAttempts=4,sleep=sleepMs,...rest}=options;
  const baseId=options.requestId||'regression-'+randomUUID(),cooldowns=[];
  for(let attempt=1;;attempt++) {
    const requestId=attempt===1?baseId:baseId+'-setup'+attempt;
    try {
      const result=await postOnce({...rest,requestId});
      if(cooldowns.length)result.setupCooldowns=cooldowns;
      return result;
    } catch(error) {
      const rejected=SETUP_REJECTED.test(error.evidence?.raw||'');
      if(!rejected||attempt>=setupAttempts||options.signal?.aborted) {
        if(cooldowns.length&&error.evidence)error.evidence.setupCooldowns=cooldowns;
        throw error;
      }
      cooldowns.push({requestId,waitedMs:setupCooldownMs});
      await sleep(setupCooldownMs);
    }
  }
}
module.exports={MessageSSE,post};
