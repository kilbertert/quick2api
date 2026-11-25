// Recover a local Desktop request without inventing missing transcript data.
// Private source transcripts and recovered requests stay outside this repo.
const fs = require('node:fs');
const crypto = require('node:crypto');

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, stable(value[key])]));
}
const digest = value => crypto.createHash('sha256').update(JSON.stringify(stable(value))).digest('hex');

function recover(file, requestId) {
  const raw = fs.readFileSync(file);
  const requests = new Map(), shapes = new Map(), messages = new Map();
  const groups = new Map();
  let userRun = [], parseErrors = 0;
  function remember(message) { messages.set(digest(message), message); }
  for (const line of raw.toString('utf8').split('\n')) {
    if (!line.trim()) continue;
    let event; try { event = JSON.parse(line); } catch { parseErrors++; continue; }
    if (event.type === 'api-request') requests.set(event.id, event);
    if (event.type === 'api-request-shape') shapes.set(event.shapeHash, event.shape);
    if (event.type === 'api-request-blob') {
      let message = event.message;
      if (digest(message) !== event.hash && typeof message.content === 'string') {
        const expanded = {...message, content:[{type:'text', text:message.content}]};
        if (digest(expanded) === event.hash) message = expanded;
      }
      // A logged blob with an inconsistent identity cannot authorize recovery.
      if (digest(message) === event.hash) messages.set(event.hash, message);
    }
    if (!['user', 'assistant'].includes(event.type) || !event.message) continue;
    const message = {role:event.message.role, content:event.message.content};
    remember(message);
    if (typeof message.content === 'string') remember({...message, content:[{type:'text', text:message.content}]});
    if (event.type === 'assistant') {
      userRun = [];
      if (event.message.id && Array.isArray(message.content)) {
        const blocks = [...(groups.get(event.message.id) || []), ...message.content];
        groups.set(event.message.id, blocks);
        remember({role:'assistant', content:blocks});
      }
    } else {
      userRun.push(typeof message.content === 'string' ? [{type:'text', text:message.content}] : message.content);
      userRun = userRun.slice(-12);
      for (let i = 0; i < userRun.length; i++) remember({role:'user', content:userRun.slice(i).flat()});
    }
  }
  const memo = new Map();
  function resolve(id, visiting = new Set()) {
    if (memo.has(id)) return memo.get(id);
    if (visiting.has(id)) throw Error('Cyclic request base chain');
    const event = requests.get(id);
    if (!event) throw Error('Missing request/base in snapshot');
    visiting.add(id);
    const base = event.base ? resolve(event.base, visiting) : [];
    if (!Number.isInteger(event.keep) || event.keep < 0 || event.keep > base.length || !Array.isArray(event.tail)) throw Error('Invalid history delta');
    const hashes = [...base.slice(0, event.keep), ...event.tail];
    memo.set(id, hashes); return hashes;
  }
  const hashes = resolve(requestId), event = requests.get(requestId), shape = shapes.get(event.shapeHash);
  const missing = hashes.filter(hash => !messages.has(hash));
  const shapeVerified = !!shape && digest(shape) === event.shapeHash;
  const report = {sourceSHA256:crypto.createHash('sha256').update(raw).digest('hex'), requestId, timestamp:event.timestamp, messageCount:hashes.length, missingMessages:missing.length, shapeVerified, parseErrors, toolCount:shape?.tools?.length, requestDigestVerified:false};
  if (missing.length) report.missingDetails = hashes.flatMap((hash,index) => messages.has(hash) ? [] : [{index, firstUse:[...requests.values()].find(request => request.tail?.includes(hash))?.timestamp, neighbors:hashes.slice(Math.max(0,index-1),index+2).map(key => {const message=messages.get(key);return {role:message?.role,blocks:Array.isArray(message?.content)?message.content.map(block=>({type:block.type,name:block.name})):typeof message?.content};})}]);
  if (missing.length || !shapeVerified || parseErrors) return {report};
  const payload = structuredClone({...shape, ...event.params, messages:hashes.map(hash => messages.get(hash)), stream:false});
  for (const hint of event.cacheControl || []) {
    let block = payload;
    for (const key of hint.path) {
      if (!block || typeof block !== 'object' || !(key in block)) throw Error('Invalid recorded cache hint');
      block = block[key];
    }
    block.cache_control = hint.value;
  }
  report.requestBytes = Buffer.byteLength(JSON.stringify(payload));
  report.trailingRole = payload.messages.at(-1)?.role;
  report.questionTool = !!payload.tools?.some(tool => tool.name === 'AskUserQuestion');
  report.historicalUnknownToolMessages = payload.messages.filter(message => /unknown tool/i.test(JSON.stringify(message))).length;
  report.verified = true;
  return {report, payload};
}
module.exports = {recover, digest};
