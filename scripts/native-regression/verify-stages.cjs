// Correlate functional extension results with owned-candidate stage metadata.
// Do not print log lines, transcript text, device IDs or route tokens.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {readJSON}=require('./private.cjs');
const [reportPath,logPath]=process.argv.slice(2);assert(reportPath&&logPath,'Usage: node verify-stages.cjs <extended-report> <candidate-log>');
const report=readJSON(reportPath),logs=fs.readFileSync(logPath,'utf8').split(/\r?\n/);
const results=[];
for(const step of report.steps){
 const id=step.firstRequestId||('extended-'+report.runId+'-'+step.name);
 const lines=logs.filter(line=>line.includes('request_id='+id+' '));
 const stages=lines.filter(l=>l.includes('msg="native preparation stage"')).map(l=>l.match(/ stage=([a-z_]+)/)?.[1]).filter(Boolean);
 if(step.name.startsWith('cancel-')){
  const active=step.name==='cancel-active-stream';
  const output=lines.some(l=>l.includes('msg="native first output"'));
  results.push({step:step.name,stages,scope:active?'cancel-after-upstream-output':'cancel-during-setup',passed:step.cancelled===true&&stages.includes('connection_started')&&(!active||stages.includes('user_message_sent')&&output)});
 }else if(step.firstRequestId){
  const fresh=step.name!=='append-same-session';
  results.push({step:step.name,stages,expectedConnection:fresh?'fresh':'reuse',passed:step.passed===true&&stages.includes('user_message_sent')&&stages.includes('relay_ready')&&(fresh?stages.includes('connection_started')&&stages.includes('connection_ready'):!stages.includes('connection_started')&&!stages.includes('connection_ready'))});
 }else results.push({step:step.name,passed:false,reason:'Functional step incomplete or blocked'});
}
const result={runId:report.runId,passed:results.every(r=>r.passed),scope:'Correlated setup stages; not a remote cancellation acknowledgement or proof of all internal state isolation',results};
fs.writeFileSync(path.join(path.dirname(reportPath),'stage-verification.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(result));if(!result.passed)process.exitCode=1;
