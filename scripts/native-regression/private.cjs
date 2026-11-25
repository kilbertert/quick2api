const fs=require('node:fs');

// Never surface SyntaxError excerpts from private inputs (including keys).
function readJSON(file) {
 try{return JSON.parse(fs.readFileSync(file,'utf8'));}
 catch{throw Error('Unable to read private JSON input');}
}
function readKey(file) {
 try{
  const config=fs.readFileSync(file,'utf8');
  let key=config.match(/^\s*api_key:\s*(.+)$/m)?.[1]?.trim();
  if(!key)throw Error();
  if(key.startsWith('"'))key=JSON.parse(key);else if(key.startsWith("'"))key=key.slice(1,-1).replaceAll("''","'");
  if(typeof key!=='string'||!key.trim())throw Error();return key;
 }catch{throw Error('Unable to read private API credential');}
}
module.exports={readJSON,readKey};
