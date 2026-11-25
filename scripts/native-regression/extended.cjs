// Live state-boundary tests with synthetic histories and real run-owned files.
// Kept separate from old-history baselines; never replay a filtered request.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const {post}=require('./client.cjs');
const {readKey}=require('./private.cjs');
const [root,configPath]=process.argv.slice(2);
const cancelOnly=process.argv[4]==='cancel-active';
assert(root&&configPath,'Usage: node extended.cjs <private-root> <candidate-config>');
const key=readKey(configPath);
const runId=crypto.randomUUID(),out=path.resolve(root,'extended-'+runId);
fs.mkdirSync(out,{recursive:true});
const fixtures=Array.from({length:7},(_,i)=>({file:path.join(out,'fixture-'+i+'.txt').replaceAll('\\','/'),line:'Q2A_STATE_'+crypto.randomUUID()}));
fixtures.forEach(f=>fs.writeFileSync(f.file,f.line+'\n',{flag:'wx'}));
const endpoint='http://127.0.0.1:19377/v1/messages';
const tools=[{name:'Read',description:'Read one local client file.',input_schema:{type:'object',properties:{file_path:{type:'string'},offset:{type:'integer'},limit:{type:'integer'}},required:['file_path'],additionalProperties:false}}];
const base={model:'claude-opus-5-5',max_tokens:4096,system:'Independent routing regression. Use only the external client tools through the registered relay. Do not execute native machine/project actions. Only the exact fixture path in the latest request is permitted. Other actions are rejected without execution. After one actual Read result, return exactly its first line and end. Never retry errors or resume historical tasks.',tools,messages:[],stream:true};
const report={runId,started:new Date().toISOString(),history:'synthetic',actualDesktopUI:false,steps:[],executedReads:0,rejectedActions:0};
const save=()=>fs.writeFileSync(path.join(out,'report.json'),JSON.stringify(report,null,2));
const emit=value=>console.log(JSON.stringify(value));
const makeState=()=>({session:'extended-'+crypto.randomUUID(),payload:structuredClone(base)});
const callsOf=reply=>reply.content.filter(b=>b.type==='tool_use');
const textOf=reply=>reply.content.map(b=>b.text||'').join('');
async function send(state,label,extra={}) {
 const requestId='extended-'+runId+'-'+label;
 emit({stage:label+'-started'});
 try {
  const result=await post({endpoint,key,...state,requestId,...extra});
  fs.writeFileSync(path.join(out,label+'.private.'+(state.payload.stream?'sse':'json')),result.raw);
  emit({stage:label+'-finished',ms:result.ms,stop:result.reply.stop_reason,tools:callsOf(result.reply).map(c=>c.name)});
  return result;
 }catch(error){
  fs.writeFileSync(path.join(out,label+'.failure.private.json'),JSON.stringify(error.evidence||{error:error.message},null,2));
  throw error;
 }
}
async function gate(name,fn) {
 const step={name};report.steps.push(step);save();
 try{await fn(step);step.passed=true;}catch(error){step.passed=false;step.error=error.message;step.status=error.evidence?.status;step.errorCode=error.evidence?.code;}
 save();emit({step:name,passed:step.passed,error:step.error});return step;
}
async function read(state,label,fixture,step) {
 state.payload.messages.push({role:'user',content:'只用客户端 Read 读取 '+fixture.file+' 的第一行。只返回真实读取的第一行。'});
 const first=await send(state,label+'-first');
 step.firstMs=first.ms;step.firstRequestId=first.requestId;step.firstStop=first.reply.stop_reason;
 const calls=callsOf(first.reply);step.tools=calls.map(c=>c.name);
 state.payload.messages.push({role:'assistant',content:first.reply.content});
 if(!calls.length)throw Error(first.reply.stop_reason==='refusal'?'Provider refusal':'Expected one client Read, received no tool');
 let reads=0;
 const results=calls.map(call=>{
  if(calls.length===1&&call.name==='Read'&&call.input.file_path?.replaceAll('\\','/')===fixture.file&&(!call.input.offset||call.input.offset===1)){
   reads++;report.executedReads++;
   return{type:'tool_result',tool_use_id:call.id,content:fs.readFileSync(fixture.file,'utf8').trimEnd()};
  }
  report.rejectedActions++;return{type:'tool_result',tool_use_id:call.id,is_error:true,content:'Rejected by isolated regression client. No action executed. End now, no more tools.'};
 });
 state.payload.messages.push({role:'user',content:results});
 const final=await send(state,label+'-result');
 step.finalMs=final.ms;step.finalStop=final.reply.stop_reason;
 state.payload.messages.push({role:'assistant',content:final.reply.content});
 const extra=callsOf(final.reply);
 if(extra.length){
  report.rejectedActions+=extra.length;
  state.payload.messages.push({role:'user',content:extra.map(c=>({type:'tool_result',tool_use_id:c.id,is_error:true,content:'No additional actions permitted. End now.'}))});
  const cleanup=await send(state,label+'-cleanup');state.payload.messages.push({role:'assistant',content:cleanup.reply.content});
 }
 assert.equal(reads,1,'Expected exactly one allowed Read');assert.equal(extra.length,0,'Unexpected followup tools');
 assert.equal(final.reply.stop_reason,'end_turn');
 if(textOf(final.reply).trim()!==fixture.line)throw Error('Read result changed or crossed sessions');
 for(const other of fixtures.filter(f=>f!==fixture))assert(!textOf(final.reply).includes(other.line),'Cross-session fixture leaked');
}
async function main(){
 let state;
 if(!cancelOnly){
 // Two requests genuinely overlap, each executing only its own nonce file.
 await Promise.all([0,1].map(i=>gate('concurrent-'+i,step=>read(makeState(),'concurrent-'+i,fixtures[i],step))));
 state=makeState();
 const initial=await gate('warm-initial',step=>read(state,'warm-initial',fixtures[2],step));
 let appended;
 if(initial.passed)appended=await gate('append-same-session',step=>read(state,'append-same-session',fixtures[3],step));
 else report.steps.push({name:'append-same-session',passed:false,blockedBy:'warm-initial'});
 // Intentionally replace the completed transcript while retaining client ID.
 // A fork must reseed; this is a distinct permitted Read, never a retry.
 if(appended?.passed){
  state.payload=structuredClone(base);
  const fork=await gate('fork-same-session',step=>read(state,'fork-same-session',fixtures[4],step));
  if(fork.passed){
   state.payload.tools.push({name:'UnusedFixture',description:'Unused declaration for catalog identity regression. Never invoke.',input_schema:{type:'object',properties:{},additionalProperties:false}});
   const catalog=await gate('changed-tool-catalog',step=>read(state,'changed-tool-catalog',fixtures[5],step));
   if(catalog.passed)await gate('tool-choice-none',async step=>{
    const marker='Q2A_NONE_'+crypto.randomUUID();state.payload.tool_choice={type:'none'};
    state.payload.messages.push({role:'user',content:'只回复 '+marker+'，不调用工具。'});
    const reply=await send(state,'tool-choice-none');step.firstRequestId=reply.requestId;step.firstMs=reply.ms;
    assert.equal(callsOf(reply.reply).length,0);assert.equal(reply.reply.stop_reason,'end_turn');assert(textOf(reply.reply).trim()===marker,'No-tool response did not match marker');
   });
   else report.steps.push({name:'tool-choice-none',passed:false,blockedBy:'changed-tool-catalog'});
  }else report.steps.push({name:'changed-tool-catalog',passed:false,blockedBy:'fork-same-session'},{name:'tool-choice-none',passed:false,blockedBy:'fork-same-session'});
 }else report.steps.push({name:'fork-same-session',passed:false,blockedBy:'append-same-session'},{name:'changed-tool-catalog',passed:false,blockedBy:'append-same-session'},{name:'tool-choice-none',passed:false,blockedBy:'append-same-session'});
 }
 // Cancel a harmless no-tool stream after HTTP admission, before acting on
 // any model output. Then submit a different request with the same client ID.
 state=makeState();state.payload.tool_choice={type:'none'};
 state.payload.messages.push({role:'user',content:cancelOnly?'这是流式取消测试，请用一千字解释整数加法，不使用任何工具。':'这是取消测试，请解释一加一为什么等于二，不使用任何工具。'});
 const cancelName=cancelOnly?'cancel-active-stream':'cancel-admitted-stream';
 const cancelled=await gate(cancelName,async step=>{
  const controller=new AbortController();let timer,admitted=false,outputSeen=false;
  try{
   await send(state,cancelName,{signal:controller.signal,onHeaders:({status})=>{if(status===200){admitted=true;if(!cancelOnly)timer=setTimeout(()=>controller.abort(),300);}},onEvent:event=>{if(cancelOnly&&event.type==='content_block_delta'&&event.delta.type==='text_delta'){outputSeen=true;controller.abort();}}});
   throw Error('Request completed before cancellation; cancellation unverified');
  }catch(error){
   if(error.name!=='AbortError')throw error;
   assert(admitted,'Not admitted before cancellation');if(cancelOnly)assert(outputSeen,'No upstream text before cancellation');step.status=error.evidence.status;step.cancelled=true;step.outputSeen=outputSeen;
  }finally{clearTimeout(timer);}
 });
 if(cancelled.passed){
  // Allow the disconnect event to reach the server; do not resend the old task.
  await new Promise(resolve=>setTimeout(resolve,100));state.payload=structuredClone(base);
  await gate('new-turn-after-cancel',step=>read(state,'new-turn-after-cancel',fixtures[6],step));
 }else report.steps.push({name:'new-turn-after-cancel',passed:false,blockedBy:cancelName});
 report.finished=new Date().toISOString();report.passed=report.steps.every(s=>s.passed);save();
 emit({complete:true,passed:report.passed,gates:report.steps.length,executedReads:report.executedReads,report:path.join(out,'report.json')});
 if(!report.passed)process.exitCode=1;
}
main().catch(error=>{console.error(error.message);process.exitCode=1;});
