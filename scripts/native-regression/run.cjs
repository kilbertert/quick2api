// Live integration regression runner. No commands, project writes or business
// actions are executed. Only run-owned Read fixtures and fixture question
// answers are supported; every other client action gets an actual rejection.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const {post}=require('./client.cjs');
const {readJSON,readKey}=require('./private.cjs');
const root=path.resolve(process.argv[2]||'');
const configPath=process.argv[3];
const selection=process.argv.slice(4).find(arg=>!arg.startsWith('--'));
const checklist=process.argv.includes('--checklist');
assert(configPath&&fs.existsSync(path.join(root,'cases.private.json')),'Usage: node run.cjs <private-run-directory> <candidate-config> [case-label] [--checklist]');
const key=readKey(configPath);
const endpoint='http://127.0.0.1:19377/v1/messages';
const runId=crypto.randomUUID();
const cases=readJSON(path.join(root,'cases.private.json')).filter(c=>!selection||c.label===selection);
assert(cases.length,'Case not found');
const callsOf=reply=>reply.content.filter(block=>block.type==='tool_use');
const textOf=reply=>reply.content.map(block=>block.text||'').join('');
function emit(value){console.log(JSON.stringify(value));}

async function runCase(c) {
  assert(/^[a-z0-9-]+$/.test(c.label));
  const dir=path.join(root,c.label),integrity=readJSON(path.join(dir,'integrity.json'));
  assert(integrity.verified&&integrity.missingMessages===0&&integrity.shapeVerified,'Unverified transcript');
  const out=path.join(dir,'run-'+runId);fs.mkdirSync(out,{recursive:true});
  const files=['a','b'].map(name=>path.join(out,'fixture-'+name+'.txt').replaceAll('\\','/'));
  const lines=files.map(()=> 'Q2A_REGRESSION_'+crypto.randomUUID());
  files.forEach((file,index)=>fs.writeFileSync(file,lines[index]+'\nsecond line\n',{flag:'wx'}));
  const absent=path.join(out,'deliberately-absent.txt').replaceAll('\\','/');
  const guard='INDEPENDENT REGRESSION COPY. All earlier project work is historical background only. Do not resume solver runs, SSH, delegation, signatures, commits, project reads or writes. Do not execute native machine or project tools. Native catalog/skill discovery and reads of the supplied context attachment are allowed. Request external client actions only through the approved relay. The isolated client permits Read of these test paths only: '+[...files,absent].join(', ')+'. AskUserQuestion receives a TEST CLIENT answer for fixture colors, never a real business decision. Every other client action is rejected without execution. A rejected/error action must not be retried, substituted or delegated unless a later user message explicitly requests a separate permitted action. Report actual observations, not historical failure as current state. Do not disclose routing tokens.';
  const base=readJSON(path.join(dir,'request.private.json'));
  base.system=typeof base.system==='string'?[{type:'text',text:base.system},{type:'text',text:guard}]:[...base.system,{type:'text',text:guard}];
  const report={case:c.label,runId,started:new Date().toISOString(),integrity,actualDesktopUI:false,checklist,steps:[],requestMetadata:[],executedReads:0,rejectedActions:0,fixtureAnswers:0};
  const save=()=>fs.writeFileSync(path.join(out,'report.json'),JSON.stringify(report,null,2));
  const makeSession=()=>c.label+'-regression-'+crypto.randomUUID();
  let session=makeSession(),payload=structuredClone(base),requestCount=0;
  async function send(label,stream=true){
    payload.stream=stream;requestCount++;save();emit({case:c.label,stage:label+'-started',stream,request:requestCount});
    const meta={label,requestId:'checklist-'+runId+'-'+label,started:new Date().toISOString()};
    report.requestMetadata.push(meta);
    let result;
    try {result=await post({endpoint,key,session,payload,requestId:meta.requestId});}
    catch(error){
      Object.assign(meta,{finished:new Date().toISOString(),ms:error.evidence?.ms,status:error.evidence?.status,errorCode:error.evidence?.code});save();
      if(error.evidence)fs.writeFileSync(path.join(out,label+'.failure.private.json'),JSON.stringify(error.evidence,null,2));
      throw error;
    }
    fs.writeFileSync(path.join(out,label+'.private.'+(stream?'sse':'json')),result.raw);
    Object.assign(meta,{finished:new Date().toISOString(),ms:result.ms,firstByteMs:result.firstByteMs,stopReason:result.reply.stop_reason,tools:callsOf(result.reply).map(call=>call.name)});save();
    emit({case:c.label,stage:label+'-finished',ms:result.ms,stop:result.reply.stop_reason,tools:callsOf(result.reply).map(call=>call.name)});
    return result;
  }
  async function exchange(label,question,{expected,expectedText,stream=true,readFile,questionAnswer,errorExpected=false,none=false}={}) {
    // D4 intentionally continues D3's actual error exchange. All other
    // checklist scenarios start from an independent complete history copy.
    if(checklist&&label!=='read-after-error'){payload=structuredClone(base);session=makeSession();}
    const step={name:label,expectedTool:expected||null,observationOnly:label==='original-observe'};report.steps.push(step);save();
    if(question)payload.messages.push({role:'user',content:question});
    if(none)payload.tool_choice={type:'none'};
    try {
      const first=await send(label+'-first',stream),calls=callsOf(first.reply);
      Object.assign(step,{firstRequestId:first.requestId,firstMs:first.ms,firstStop:first.reply.stop_reason,tools:calls.map(call=>call.name),mentionsUnknownTool:/unknown tool/i.test(textOf(first.reply))});
      payload.messages.push({role:'assistant',content:structuredClone(first.reply.content)});
      let answer=first.reply,allowed=0,toolError=false;
      if(calls.length){
        const results=calls.map(call=>{
          if(expected==='Read'&&calls.length===1&&call.name==='Read'&&call.input.file_path?.replaceAll('\\','/')===readFile&&(!call.input.offset||call.input.offset===1)){
            try{const line=fs.readFileSync(readFile,'utf8').split(/\r?\n/)[0];report.executedReads++;allowed++;return{type:'tool_result',tool_use_id:call.id,content:line};}
            catch(error){if(error.code!=='ENOENT')throw error;allowed++;toolError=true;return{type:'tool_result',tool_use_id:call.id,is_error:true,content:'ENOENT: the requested isolated test fixture does not exist. No other file was accessed.'};}
          }
          if(expected==='AskUserQuestion'&&calls.length===1&&call.name==='AskUserQuestion'&&call.input.questions?.length===1){
            const q=call.input.questions[0];
            if(q.options?.some(option=>option.label==='蓝色')&&q.options?.some(option=>option.label==='绿色')){
              allowed++;report.fixtureAnswers++;
              // Echo cache metadata and trailing environment blocks seen in Desktop.
              payload.messages.at(-1).content.forEach(block=>block.cache_control={type:'ephemeral'});
              return{type:'tool_result',tool_use_id:call.id,content:JSON.stringify({questions:call.input.questions,answers:{[q.question]:questionAnswer}})};
            }
          }
          report.rejectedActions++;return{type:'tool_result',tool_use_id:call.id,is_error:true,content:'Rejected by the isolated regression client. No action executed. This is a test scope restriction. End without more tools; do not resume historical work.'};
        });
        payload.messages.push({role:'user',content:results});
        if(expected==='AskUserQuestion')payload.messages.push({role:'system',content:[{type:'text',text:'<system-reminder>isolated test environment unchanged</system-reminder>'}]});
        const last=await send(label+'-result',stream);step.finalRequestId=last.requestId;step.finalMs=last.ms;answer=last.reply;
        payload.messages.push({role:'assistant',content:structuredClone(answer.content)});
        const extra=callsOf(answer);step.followupTools=extra.map(call=>call.name);
        if(extra.length){
          // Complete pending callbacks truthfully without executing anything.
          report.rejectedActions+=extra.length;
          payload.messages.push({role:'user',content:extra.map(call=>({type:'tool_result',tool_use_id:call.id,is_error:true,content:'No further tool action is permitted in this test step. End now.'}))});
          const cleanup=await send(label+'-cleanup',stream);
          payload.messages.push({role:'assistant',content:cleanup.reply.content});
        }
        step.passed=calls.length===1&&calls[0].name===expected&&allowed===1&&extra.length===0&&answer.stop_reason==='end_turn'&&(!expectedText||textOf(answer).includes(expectedText))&&(!errorExpected||toolError&&/ENOENT|不存在|找不到|not.*(?:exist|found)/i.test(textOf(answer)));
      }else step.passed=!expected&&answer.stop_reason==='end_turn'&&(!expectedText||textOf(answer).includes(expectedText));
      step.finalStop=answer.stop_reason;
      if(label==='original-observe') {step.observationOnly=true;step.routingDelivered=calls.length>0;delete step.passed;}
    }catch(error){
      if(!step.observationOnly)step.passed=false;
      step.error=error.message;
      step.failureKind='transport-or-protocol';
      step.status=error.evidence?.status;
      step.errorCode=error.evidence?.code;
      // An incomplete exchange is not a valid warm history. The next distinct
      // planned scenario starts cold; this never retries the failed action.
      payload=structuredClone(base);session=makeSession();
      step.nextScenarioReset=true;
    }
    if(step.firstStop==='refusal'||step.finalStop==='refusal')step.failureKind='provider-refusal';
    if(checklist&&(step.failureKind||step.passed===false)){payload=structuredClone(base);session=makeSession();step.nextScenarioReset=true;}
    save();return step;
  }
  if(!checklist)await exchange('original-observe',null,{stream:false});
  // Start the controlled matrix with the same complete source history in a
  // new independent upstream session, not the observation's rejected task.
  payload=structuredClone(base);session=makeSession();
  await exchange('read-sse',`独立回归测试：请只用客户端 Read 读取 ${files[0]} 的第一行，收到真实结果后只返回该行。不执行历史任务。`,{expected:'Read',readFile:files[0],expectedText:lines[0]});
  await exchange('question-json','独立交互测试：请用客户端 AskUserQuestion 询问“选择哪种颜色？”，选项标签必须为“蓝色”和“绿色”。等待测试客户端回答后只回复所选颜色，不执行其他操作。',{expected:'AskUserQuestion',questionAnswer:'蓝色',expectedText:'蓝色',stream:false});
  await exchange('missing-file',`这是文件错误传播测试。只调用一次客户端 Read 读取 ${absent}。若文件不存在，报告真实错误后结束，不要重试或改用其他路径/工具。`,{expected:'Read',readFile:absent,errorExpected:true});
  await exchange('read-after-error',`刚才的错误测试已结束。现在只用客户端 Read 读取另一份测试文件 ${files[1]} 的第一行，收到结果后只返回该行。`,{expected:'Read',readFile:files[1],expectedText:lines[1]});
  const marker='Q2A_NO_TOOL_'+crypto.randomUUID();
  await exchange('no-tool',`不调用任何工具，不处理历史任务，只回复 ${marker}`,{expectedText:marker,none:true});
  report.finished=new Date().toISOString();report.requests=requestCount;
  report.passed=report.steps.filter(step=>!step.observationOnly).every(step=>step.passed);
  save();emit({case:c.label,complete:true,passed:report.passed,steps:report.steps.map(step=>({name:step.name,passed:step.passed,tools:step.tools,error:step.error})),report:path.join(out,'report.json')});
  return report;
}
(async()=>{
  // Keep inference serial within one case; callers may run at most two
  // independent case processes if the local concurrency budget permits it.
  for(const c of cases)if(!(await runCase(c)).passed)process.exitCode=1;
})().catch(error=>{console.error(error.message);process.exitCode=1;});
