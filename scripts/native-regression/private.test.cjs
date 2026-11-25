const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {readJSON,readKey}=require('./private.cjs');
test('malformed private files never appear in reported parse errors',t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'q2a-private-test-'));
 const json=path.join(dir,'private.json'),config=path.join(dir,'private.yaml');
 t.after(()=>{fs.unlinkSync(json);fs.unlinkSync(config);fs.rmdirSync(dir);});
 fs.writeFileSync(json,'{"PRIVATE_BODY_SECRET": invalid}');fs.writeFileSync(config,'api_key: "PRIVATE_KEY_SECRET invalid');
 for(const [read,file] of [[readJSON,json],[readKey,config]])assert.throws(()=>read(file),error=>{assert.doesNotMatch(error.message,/PRIVATE_|SECRET/);assert.match(error.message,/Unable to read private/);return true;});
});
