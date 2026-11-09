/* Timestamp acceptance against the actual portal script, no browser/network. */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const elements=new Map();
const element=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',hidden:true,value:'',addEventListener(){}});return elements.get(id);};
const context=vm.createContext({document:{getElementById:element},setInterval(){},URLSearchParams,FormData:class {},
  fetch:async()=>({ok:true,json:async()=>({owner:null,csrf:'test'})})});
new vm.Script(fs.readFileSync(path.join(__dirname,'../controller/portal_static/portal.js'),'utf8')).runInContext(context);
const evaluate=code=>vm.runInContext(code,context);
for(const value of ['null','undefined','true','"1794268800"','NaN','Infinity','-Infinity','9e12'])assert.equal(evaluate(`stamp(${value})`),'—',value);
for(const value of [0,1794268800,1793511000,1793514600]){
  assert.equal(evaluate(`stamp(${value})`),evaluate(`new Date(${value}*1000).toLocaleString(undefined,{timeZoneName:'short'})`));
  assert.ok(!evaluate(`stamp(${value})`).includes('Invalid Date'));
}
if(process.env.TZ==='America/New_York')assert.notEqual(evaluate('stamp(1793511000)'),evaluate('stamp(1793514600)'),'DST repeated local hour requires offset/zone');
console.log('PASS timestamp zero, missing/nonfinite rejection, explicit local timezone and DST: '+process.env.TZ);
