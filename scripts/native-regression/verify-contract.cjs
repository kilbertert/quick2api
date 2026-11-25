// Metadata only: correlate the independent contract run with its owned log.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {readJSON}=require('./private.cjs');
const [reportPath,logPath]=process.argv.slice(2);
assert(reportPath&&logPath,'Usage: node verify-contract.cjs <contract-report> <candidate-log>');
const report=readJSON(reportPath),logs=fs.readFileSync(logPath,'utf8').split(/\r?\n/);
assert(report.history==='synthetic'&&report.actualDesktopUI===false,'Expected an isolated synthetic contract report');
const expected=[
 ['read-result-with-preamble-cache-hints','read-first','fresh','tool_use'],
 ['read-result-with-preamble-cache-hints','read-result','same-job','end_turn'],
 ['semantic-duplicate-is-rejected','duplicate','rejected',null],
 ['question-after-warm-read-and-cache-move','question-first','warm','tool_use'],
 ['question-after-warm-read-and-cache-move','question-answer','fresh','end_turn'],
];
const results=expected.map(([stepName,label,scope,stop])=>{
 const id='contract-'+report.runId+'-'+label;
 const step=report.steps.find(s=>s.name===stepName);
 const lines=logs.filter(line=>line.includes('request_id='+id+' '));
 const stages=lines.filter(l=>l.includes('msg="native preparation stage"')).map(l=>l.match(/ stage=([a-z_]+)/)?.[1]).filter(Boolean);
 const delivered=lines.some(l=>l.includes('msg="native response delivered"')&&l.includes('stop_reason='+stop+' '));
 let passed=step?.passed===true&&step.requests?.includes(id)===true;
 if(scope==='rejected')passed&&=stages.length===0&&lines.some(l=>l.includes('msg="native client continuation rejected"'))&&!lines.some(l=>l.includes('msg="native response delivered"'));
 else if(scope==='same-job')passed&&=delivered&&stages.length===0;
 else passed&&=delivered&&stages.includes('relay_ready')&&stages.includes('user_message_sent')&&(scope==='fresh'?stages.includes('connection_started')&&stages.includes('connection_ready'):!stages.includes('connection_started')&&!stages.includes('connection_ready'));
 return {request:label,scope,stages,passed:!!passed};
});
const result={runId:report.runId,passed:report.passed===true&&results.every(r=>r.passed),scope:'Correlated API setup/delivery metadata; not Desktop UI acceptance or proof of all internal execution state',results};
fs.writeFileSync(path.join(path.dirname(reportPath),'stage-verification.json'),JSON.stringify(result,null,2));
console.log(JSON.stringify(result));if(!result.passed)process.exitCode=1;
