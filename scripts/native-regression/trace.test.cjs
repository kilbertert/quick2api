const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {recover,digest}=require('./trace.cjs');
function fixture(t,events){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'q2a-trace-test-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));const file=path.join(dir,'fixture.jsonl');fs.writeFileSync(file,events.map(event=>JSON.stringify(event)).join('\n')+'\n');return file;}
const shape={system:[{type:'text',text:'Synthetic reader test'}],tools:[]};
const shapeEvent={type:'api-request-shape',shape,shapeHash:digest(shape)};
function request(id,base,keep,messages){return{type:'api-request',id,base,keep,tail:messages.map(digest),shapeHash:digest(shape),params:{model:'test',max_tokens:10}};}
test('base references determine history independently of interleaved requests',t=>{
 const first={role:'user',content:[{type:'text',text:'first'}]},other={role:'user',content:'unrelated'},last={role:'user',content:'last'};
 const events=[shapeEvent,...[first,other,last].map(message=>({type:'api-request-blob',hash:digest(message),message})),request('a',undefined,0,[first]),request('unrelated',undefined,0,[other]),request('b','a',1,[last])];
 const result=recover(fixture(t,events),'b');assert.equal(result.report.verified,true);assert.deepEqual(result.payload.messages,[first,last]);
});
test('flattened blobs and split message records require an exact identity match',t=>{
 const environment={role:'system',content:[{type:'text',text:'fixture environment'}]},assistant={role:'assistant',content:[{type:'text',text:'first'},{type:'text',text:'second'}]};
 const events=[shapeEvent,{type:'api-request-blob',hash:digest(environment),message:{role:'system',content:'fixture environment'}},...assistant.content.map(block=>({type:'assistant',message:{id:'same-message',role:'assistant',content:[block]}})),request('a',undefined,0,[environment,assistant])];
 const result=recover(fixture(t,events),'a');assert.equal(result.report.verified,true);assert.deepEqual(result.payload.messages,[environment,assistant]);
});
test('missing data, modified shapes and invalid deltas fail closed',t=>{
 const missing={role:'user',content:'not recorded'};
 const result=recover(fixture(t,[shapeEvent,request('a',undefined,0,[missing])]),'a');assert.equal(result.report.missingMessages,1);assert.equal(result.payload,undefined);
 const badShape=recover(fixture(t,[{...shapeEvent,shape:{system:'modified',tools:[]}},request('b',undefined,0,[])]),'b');assert.equal(badShape.report.shapeVerified,false);assert.equal(badShape.payload,undefined);
 assert.throws(()=>recover(fixture(t,[shapeEvent,request('c',undefined,2,[])]),'c'),/Invalid history delta/);
});
