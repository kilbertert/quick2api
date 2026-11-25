// New synthetic contract scenarios, never a replay of a filtered history.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {post}=require('./client.cjs');
const {readKey}=require('./private.cjs');
const [root,config]=process.argv.slice(2);
if(!root||!config)throw Error('Usage: node contract.cjs <private-root> <candidate-config>');
const runId=crypto.randomUUID(),out=path.resolve(root,'contract-'+runId);
fs.mkdirSync(out,{recursive:true});
const file=path.join(out,'fixture.txt').replaceAll('\\','/'),nonce='Q2A_CONTRACT_'+crypto.randomUUID();
fs.writeFileSync(file,nonce+'\n',{flag:'wx'});
const key=readKey(config),session='contract-'+runId,endpoint='http://127.0.0.1:19377/v1/messages';
const tools=[
 {name:'Read',description:'Read a permitted client file.',input_schema:{type:'object',properties:{file_path:{type:'string'},offset:{type:'integer'},limit:{type:'integer'}},required:['file_path'],additionalProperties:false}},
 {name:'AskUserQuestion',description:'Ask the test client to select a fixture format. The returned value is a test fixture, not a human business decision.',input_schema:{type:'object',properties:{questions:{type:'array',minItems:1,maxItems:1,items:{type:'object',properties:{question:{type:'string'},header:{type:'string'},multiSelect:{type:'boolean'},options:{type:'array',minItems:2,maxItems:2,items:{type:'object',properties:{label:{type:'string'},description:{type:'string'}},required:['label','description'],additionalProperties:false}}},required:['question','header','options','multiSelect'],additionalProperties:false}}},required:['questions'],additionalProperties:false}}
];
const system='Independent client contract regression. Only the exact fixture Read and the requested fixture-format question are permitted. The test client returns actual file contents and a fixed test answer. Do not execute native machine or project actions. Use the declared client tools through the registered relay. Do not resume other work or repeat rejected actions.';
let payload={model:'claude-opus-5-5',max_tokens:4096,system,tools,messages:[{role:'user',content:'请只用客户端 Read 读取 '+file+' 的第一行；收到真实结果后只回复该行。'}],stream:true};
const original=structuredClone(payload);
const report={runId,history:'synthetic',actualDesktopUI:false,started:new Date().toISOString(),steps:[],executedReads:0,fixtureAnswers:0,rejectedActions:0};
const save=()=>fs.writeFileSync(path.join(out,'report.json'),JSON.stringify(report,null,2));
const check=(test,message)=>{if(!test)throw Error(message);};
const calls=reply=>reply.content.filter(b=>b.type==='tool_use');
const text=reply=>reply.content.map(b=>b.text||'').join('').trim();
function cacheHints(on){
 payload.system=on?[{type:'text',text:system,cache_control:{type:'ephemeral',ttl:'1h'}}]:system;
 payload.tools=structuredClone(tools);
 if(on)payload.tools.at(-1).cache_control={type:'ephemeral'};
}
async function send(label,step){
 const requestId='contract-'+runId+'-'+label;
 step.requests??=[];step.requests.push(requestId);save();
 console.log(JSON.stringify({stage:label+'-started'}));
 try{
  const result=await post({endpoint,key,session,payload,requestId});
  fs.writeFileSync(path.join(out,label+'.private.'+(payload.stream?'sse':'json')),result.raw);
  console.log(JSON.stringify({stage:label+'-finished',ms:result.ms,stop:result.reply.stop_reason,tools:calls(result.reply).map(c=>c.name)}));
  return result.reply;
 }catch(error){
  fs.writeFileSync(path.join(out,label+'.failure.private.json'),JSON.stringify(error.evidence||{error:error.message},null,2));
  throw error;
 }
}
async function step(name,fn){
 const current={name};report.steps.push(current);save();
 try{await fn(current);current.passed=true;}
 catch(error){current.passed=false;current.error=error.message;current.status=error.evidence?.status;throw error;}
 finally{save();}
}
async function rejectUnexpected(reply,label,current){
 const unexpected=calls(reply);if(!unexpected.length)return;
 report.rejectedActions+=unexpected.length;
 payload.messages.push({role:'assistant',content:reply.content},{role:'user',content:unexpected.map(c=>({type:'tool_result',tool_use_id:c.id,is_error:true,content:'Rejected by the isolated contract test client. No action executed. End now, without more tools.'}))});
 const final=await send(label+'-rejection',current);
 report.rejectedActions+=calls(final).length;
}
async function main(){
 await step('read-result-with-preamble-cache-hints',async current=>{
  const first=await send('read-first',current),cs=calls(first);
  if(!(cs.length===1&&cs[0].name==='Read'&&cs[0].input.file_path?.replaceAll('\\','/')===file&&(!cs[0].input.offset||cs[0].input.offset===1))){await rejectUnexpected(first,'read',current);throw Error('Expected exactly one permitted fixture Read');}
  const line=fs.readFileSync(file,'utf8').split(/\r?\n/)[0];report.executedReads++;
  payload.messages.push({role:'assistant',content:first.content.map(b=>({...b,cache_control:{type:'ephemeral'}}))},{role:'user',content:[{type:'tool_result',tool_use_id:cs[0].id,content:line}]});
  payload.messages[0].content=[{type:'text',text:original.messages[0].content,cache_control:{type:'ephemeral'}}];
  cacheHints(true);payload.stream=false;
  const final=await send('read-result',current);
  await rejectUnexpected(final,'read-result',current);
  check(calls(final).length===0&&final.stop_reason==='end_turn'&&text(final)===nonce,'Fixture result changed or failed to resume');
  payload.messages.push({role:'assistant',content:final.content});
 });
 await step('semantic-duplicate-is-rejected',async current=>{
  const completed=payload;payload=structuredClone(original);
  cacheHints(true);payload.stream=false;
  payload.messages[0].content=[{type:'text',text:original.messages[0].content,cache_control:{type:'ephemeral'}}];
  let rejected=false;
  try{
   const unexpected=await send('duplicate',current);
   await rejectUnexpected(unexpected,'duplicate',current);
  }catch(error){
   const e=error.evidence;
   let body;try{body=JSON.parse(e?.raw||'');}catch{}
   if(e?.status!==400||e.shouldRetry!=='false'||body?.error?.message!=='native turn is already active or was recently submitted; no upstream request was replayed')throw error;
   rejected=true;
  }
  check(rejected,'Equivalent completed request was replayed');
  payload=completed;
 });
 await step('question-after-warm-read-and-cache-move',async current=>{
  cacheHints(false);payload.stream=true;
  payload.messages.push({role:'user',content:'这是独立的问答协议测试。请用客户端 AskUserQuestion 提问“测试数据用哪种格式？”，仅提供 CSV 和 JSON 两个选项，multiSelect 为 false。收到测试客户端的回答后只回复所选格式，不执行其他操作。'});
  const question=await send('question-first',current),cs=calls(question),q=cs[0]?.input?.questions?.[0];
  if(!(cs.length===1&&cs[0].name==='AskUserQuestion'&&cs[0].input.questions.length===1&&q.multiSelect===false&&q.options?.length===2&&q.options.some(o=>o.label==='CSV')&&q.options.some(o=>o.label==='JSON'))){await rejectUnexpected(question,'question',current);throw Error('Expected one fixture-format question');}
  report.fixtureAnswers++;
  payload.messages.push({role:'assistant',content:question.content.map(b=>({...b,cache_control:{type:'ephemeral'}}))},{role:'user',content:[{type:'tool_result',tool_use_id:cs[0].id,content:JSON.stringify({questions:cs[0].input.questions,answers:{[q.question]:'CSV'}})}]},{role:'system',content:[{type:'text',text:'<system-reminder>isolated fixture environment unchanged</system-reminder>'}]});
  cacheHints(true);payload.stream=false;
  const final=await send('question-answer',current);
  await rejectUnexpected(final,'question-answer',current);
  check(calls(final).length===0&&final.stop_reason==='end_turn'&&text(final)==='CSV','Actual fixture answer not returned');
 });
}
main().catch(error=>{report.error=error.message;process.exitCode=1;}).finally(()=>{
 report.finished=new Date().toISOString();report.passed=report.steps.length===3&&report.steps.every(s=>s.passed);save();
 console.log(JSON.stringify({complete:true,passed:report.passed,steps:report.steps.map(s=>({name:s.name,passed:s.passed,error:s.error})),report:path.join(out,'report.json')}));
});
