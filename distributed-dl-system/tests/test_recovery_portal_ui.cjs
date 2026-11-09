/* Actual portal JavaScript with DOM/fetch harness; offline, no npm dependencies. */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
async function run(){
  const elements=new Map();
  function element(id){
    if(!elements.has(id))elements.set(id,{value:'all',innerHTML:'',textContent:'',hidden:true,scrollTop:0,
      scrollHeight:0,clientHeight:0,addEventListener(){},querySelector(s){return s==='.timeline'?element('timeline'):null;},querySelectorAll(){return [];}});
    return elements.get(id);
  }
  const calls=[];let routes={'/api/session':{owner:null,csrf:'test'}};
  const context=vm.createContext({document:{getElementById:element},setInterval(){},URLSearchParams,FormData:class {},
    fetch:async url=>{calls.push(url);assert.ok(url in routes,url);return {ok:true,json:async()=>routes[url]};}});
  new vm.Script(fs.readFileSync(path.join(__dirname,'../controller/portal_static/portal.js'),'utf8')).runInContext(context);
  await new Promise(resolve=>setImmediate(resolve));const evaluate=code=>vm.runInContext(code,context);
  assert.equal(evaluate('renderRecovery(null)'),'');
  const recovery={schema_version:1,attempt:2,mode:'checkpoint',phase:'CONFIRMING',label:'Waiting for training-state restoration',
    total_ranks:3,inputs_ready_ranks:3,checkpoint_ready_ranks:3,launched_ranks:3,restored_ranks:0,restore_required:true,
    source:{state:'committed',global_step:5,size_bytes:1000,from_attempt:1},ranks_truncated:false,
    ranks:[0,1,2].map(rank=>({rank,inputs_ready:true,checkpoint_ready:true,launch_acknowledged:true,training_state_restored:false}))};
  const detail={job:{job_id:'my-job',status:'SCHEDULED',retry_count:1},spec:{name:'Own training',execution:{mode:'distributed',workers:3}},
    recovery,staging:{phase:'READY',label:'Inputs ready',ready_ranks:3,total_ranks:3,measured_ranks:0,metrics:null,ranks:[]},
    preparation:null,checkpoint:null,logs:[],attempts:[],ranks:[],events:[],leases:[]};
  routes['/api/jobs/my-job']=detail;routes['/api/jobs/my-job/logs?view=recovery&rank=0']={source:'Recovery progress',text:'Training state restored: 0/3.'};
  evaluate("selected='my-job';owner='alice'");await evaluate('loadDetail()');
  let html=element('detail').innerHTML;
  for(const text of ['Recovery progress','badge RECOVERING','step 5','0 / 3','value="@recovery" selected','data-action="cancel"'])assert.ok(html.includes(text),text);
  assert.ok(!calls.some(url=>url.includes('file=@recovery')));assert.equal(element('training-log').textContent,'Training state restored: 0/3.');
  context.detail=detail;evaluate('latest={jobs:[{...detail.job,name:detail.spec.name,execution:detail.spec.execution,staging:detail.staging,recovery:detail.recovery}]};renderJobs()');
  assert.ok(element('jobs').innerHTML.includes('badge RECOVERING'));assert.ok(element('jobs').innerHTML.includes(recovery.label));
  context.unknown={...recovery,inputs_ready_ranks:null,checkpoint_ready_ranks:null,restored_ranks:null,source:{state:'not_selected'},restore_required:null,ranks:[]};
  assert.ok(evaluate('renderRecovery(unknown)').includes('Not recorded'));
  context.unsafe={...recovery,label:'<img src=x onerror=alert(1)>',source:{state:'committed',global_step:'<script>',from_attempt:1}};
  html=evaluate('renderRecovery(unsafe)');assert.ok(html.includes('&lt;img'));assert.ok(html.includes('&lt;script&gt;'));assert.ok(!html.includes('<img'));
  context.restarted={...recovery,restore_required:false,source:{state:'not_required'}};
  assert.ok(evaluate('renderRecovery(restarted)').includes('Full restart'));assert.ok(evaluate('renderRecovery(restarted)').includes('Not required'));
  recovery.phase='RESUMED';recovery.label='Training resumed from checkpoint';recovery.restored_ranks=3;
  for(const rank of recovery.ranks)rank.training_state_restored=true;detail.job.status='RUNNING';
  await evaluate('loadDetail()');assert.ok(element('detail').innerHTML.includes('3 / 3'));assert.ok(element('detail').innerHTML.includes('badge RUNNING'));

  // Late successful owner-scoped responses must not enter a new session's DOM,
  // even if a selected ID is repeated. Exercise detail, logs AND overview.
  let release;
  context.fetch=async()=>new Promise(resolve=>{release=value=>resolve({ok:true,json:async()=>value});});
  const pendingDetail=evaluate('loadDetail()');evaluate("showLogin();owner='bob';selected='my-job'");release(detail);await pendingDetail;
  assert.equal(element('detail').innerHTML,'');
  evaluate("owner='alice';selected='my-job';logSelection='@recovery';rankSelection='0'");element('training-log').textContent='';
  const pendingLog=evaluate('loadLog()');evaluate("showLogin();owner='bob';selected='my-job';logSelection='@recovery'");
  release({text:'alice-private-progress',source:'Recovery'});await pendingLog;assert.equal(element('training-log').textContent,'');
  evaluate("owner='alice';busy=false");const pendingOverview=evaluate('refresh()');
  evaluate("showLogin();owner='bob'");release({jobs:[{name:'alice-private-job'}]});await pendingOverview;
  assert.equal(evaluate('latest'),null);assert.equal(element('jobs').innerHTML,'');
  return 'PASS: existing student recovery panel, staged/SDK milestones, truthful unknowns/restart, recovery log and controls, escaping, and late detail/log/overview session fencing';
}
module.exports={run};if(require.main===module)run().then(console.log).catch(e=>{console.error(e);process.exitCode=1;});
