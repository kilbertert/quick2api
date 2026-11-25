// Metadata-only summary. Include every saved run; never pick only successes.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {readJSON}=require('./private.cjs');
const root=path.resolve(process.argv[2]||'');
assert(fs.existsSync(path.join(root,'cases.private.json')),'Usage: node summarize.cjs <private-root>');
const cases=readJSON(path.join(root,'cases.private.json'));
const summary={oldHistoryRuns:[],extensions:[],contracts:[],incomplete:[],gates:0,passed:0,observations:0,failures:{providerRefusal:0,transportOrProtocol:0,other:0}};
for(const c of cases){
 const dir=path.join(root,c.label);if(!fs.existsSync(dir))continue;
 for(const name of fs.readdirSync(dir).filter(n=>n.startsWith('run-'))){
  const file=path.join(dir,name,'report.json');if(!fs.existsSync(file))continue;
  const r=readJSON(file);
  if(!r.finished){summary.incomplete.push(c.label+'/'+name);continue;}
  // Older baseline reports only set observationOnly on the success path.
  const gates=r.steps.filter(s=>!s.observationOnly&&s.name!=='original-observe');
  const row={case:c.label,runId:r.runId,messages:r.integrity.messageCount??r.integrity.messages,tools:r.integrity.toolCount,requestBytes:r.integrity.requestBytes,passed:gates.filter(s=>s.passed).length,gates:gates.length,executedReads:r.executedReads,failures:[]};
  for(const s of gates.filter(s=>!s.passed)){
   const category=s.firstStop==='refusal'||s.finalStop==='refusal'?'providerRefusal':s.error?'transportOrProtocol':'other';
   summary.failures[category]++;row.failures.push({step:s.name,category});
  }
  summary.gates+=gates.length;summary.passed+=row.passed;summary.observations+=r.steps.length-gates.length;summary.oldHistoryRuns.push(row);
 }
}
for(const name of fs.readdirSync(root).filter(n=>n.startsWith('extended-'))){
 const file=path.join(root,name,'report.json');if(!fs.existsSync(file))continue;
 const r=readJSON(file);
 if(!r.finished){summary.incomplete.push(name);continue;}
 summary.extensions.push({runId:r.runId,history:r.history,passed:r.steps.filter(s=>s.passed).length,gates:r.steps.length,executedReads:r.executedReads,steps:r.steps.map(s=>({name:s.name,passed:s.passed,blockedBy:s.blockedBy}))});
}
for(const name of fs.readdirSync(root).filter(n=>n.startsWith('contract-'))){
 const file=path.join(root,name,'report.json');if(!fs.existsSync(file))continue;
 const r=readJSON(file);
 if(!r.finished){summary.incomplete.push(name);continue;}
 summary.contracts.push({runId:r.runId,history:r.history,actualDesktopUI:r.actualDesktopUI,passed:r.steps.filter(s=>s.passed).length,gates:r.steps.length,executedReads:r.executedReads,fixtureAnswers:r.fixtureAnswers,rejectedActions:r.rejectedActions,steps:r.steps.map(s=>({name:s.name,passed:s.passed,error:s.error,status:s.status}))});
}
fs.writeFileSync(path.join(root,'summary.json'),JSON.stringify(summary,null,2));console.log(JSON.stringify(summary));
