/* Executes the existing portal script with a DOM/fetch harness, no network/npm.
 * This checks rendering/permission transitions, not browser layout or hardware. */
const assert=require('node:assert/strict');
const fs=require('node:fs');const path=require('node:path');const vm=require('node:vm');
async function run(){
  const elements=new Map();
  function element(id){
    if(!elements.has(id))elements.set(id,{value:'all',innerHTML:'',textContent:'',hidden:true,
      scrollTop:0,addEventListener(){},querySelector(){return null;},querySelectorAll(){return [];}});
    return elements.get(id);
  }
  const calls=[];let routes={'/api/session':{owner:null,csrf:'test'}};
  const context=vm.createContext({document:{getElementById:element},setInterval(){},URLSearchParams,FormData:class {},
    fetch:async url=>{calls.push(url);assert.ok(url in routes,url);return {ok:true,status:200,json:async()=>routes[url]};}});
  new vm.Script(fs.readFileSync(path.join(__dirname,'../controller/portal_static/portal.js'),'utf8')).runInContext(context);
  await new Promise(resolve=>setImmediate(resolve));const evaluate=code=>vm.runInContext(code,context);
  const unknown={disk_available_bytes:null,disk_total_bytes:null,verified_cache_bytes:null,cache_limit_bytes:null,
    dataset_coverage:null,inventory_age_seconds:null,last_seen_at:null,nfs:{status:'unknown',observed_at:null},transfer_history:[]};
  const view={recorded_at:1794268800,coverage_limit:20,coverage_truncated:false,transfer_history_limit:10,transfer_scan_limit:300,
    workers:[{...unknown,worker_id:'pi01',inventory_status:'not_reported'},
      {...unknown,worker_id:'pi02',inventory_status:'reported_fresh',verified_cache_bytes:100,disk_total_bytes:1000,
        disk_available_bytes:800,inventory_age_seconds:2,nfs:{status:'verified_readonly',observed_at:1794268800},
        dataset_coverage:{required_bytes:100,cached_bytes:100,complete_versions:1,dataset_versions:1},
        transfer_history:[{recorded_at:1794268800,cache_hit_bytes:0,transferred_bytes:100,copy_seconds:0.02,verification_seconds:0.01}]},
      {...unknown,worker_id:'pi03',inventory_status:'stale_or_untrusted'}]};
  context.view=view;let html=evaluate('renderAdminStorage(view)');
  for(const text of ['pi01','pi02','pi03','Unknown','Stale or untrusted','Verified read-only','I/O availability: untested','100 B','0.020 s'])assert.ok(html.includes(text),text);
  context.malicious={...view,workers:[{...unknown,worker_id:'<img src=x onerror=alert(1)>',inventory_status:'not_reported'}]};
  html=evaluate('renderAdminStorage(malicious)');assert.ok(html.includes('&lt;img'));assert.ok(!html.includes('<img'));
  routes['/api/examples']={regression:{name:'test',entrypoint:[],execution:{mode:'single',workers:1},
    resources_per_worker:{cpu_cores:1,memory_gb:1,gpus:0},environment:{requirements:'requirements.txt'},recovery:{mode:'restart',max_retries:1}}};
  routes['/api/overview']={jobs:[],nodes:[],dispatcher_running:true,timestamp:1,limits:{max_active_per_owner:1,max_pending_per_owner:2}};
  routes['/api/admin/storage']=view;
  await evaluate("enter({owner:'operator',csrf:'test',is_admin:true})");
  assert.equal(element('admin-storage').hidden,false);assert.ok(element('admin-storage').innerHTML.includes('pi02'));
  const count=calls.filter(u=>u==='/api/admin/storage').length;
  await evaluate("enter({owner:'alice',csrf:'test'})");
  assert.equal(calls.filter(u=>u==='/api/admin/storage').length,count);
  assert.equal(element('admin-storage').hidden,true);assert.equal(element('admin-storage').innerHTML,'');
  let release;
  context.fetch=async url=>{assert.equal(url,'/api/admin/storage');return new Promise(resolve=>{release=()=>resolve({ok:true,json:async()=>view});});};
  evaluate("isAdmin=true;owner='operator'");const pending=evaluate('loadAdminStorage()');
  evaluate('showLogin()');release();await pending;
  assert.equal(element('admin-storage').hidden,true);assert.equal(element('admin-storage').innerHTML,'');
  context.fetch=async()=>({ok:false,status:403,json:async()=>({error:'Administrator access required'})});
  evaluate("isAdmin=true;owner='operator'");element('admin-storage').innerHTML='previous sensitive data';
  await evaluate('loadAdminStorage()');assert.ok(!element('admin-storage').innerHTML.includes('sensitive'));
  return 'PASS: existing portal admin panel, all three workers, unknown/stale values, units, escaping, student exclusion, logout race and revoked access';
}
module.exports={run};if(require.main===module)run().then(console.log).catch(e=>{console.error(e);process.exitCode=1;});
