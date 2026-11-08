/* No npm packages or browser/network required. Executes the actual portal script
 * with a small DOM/fetch harness; this is not a browser layout/accessibility test.
 * Run: node tests/test_staging_portal_ui.cjs
 */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

async function run() {
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) elements.set(id, {value:'all', innerHTML:'', textContent:'',
      scrollTop:0, scrollHeight:0, clientHeight:0, addEventListener(){},
      querySelector(selector){return selector==='.timeline' ? element('timeline') : null;},
      querySelectorAll(){return [];}});
    return elements.get(id);
  }
  let routes = {'/api/session':{owner:null,csrf:'test'}};
  const calls = [];
  const context = vm.createContext({document:{getElementById:element},
    setInterval(){}, URLSearchParams, FormData:class {},
    fetch:async url=>{calls.push(url);assert.ok(url in routes, url);return {ok:true,json:async()=>routes[url]};}});
  const source = fs.readFileSync(path.join(__dirname,'../controller/portal_static/portal.js'),'utf8');
  new vm.Script(source).runInContext(context);
  await new Promise(resolve=>setImmediate(resolve));
  const evaluate = code=>vm.runInContext(code, context);
  assert.equal(evaluate('renderStaging(null)'), '');
  assert.equal(evaluate("inputBadge({status:'SCHEDULED'})"), 'SCHEDULED');
  assert.equal(evaluate("inputBadge({status:'SCHEDULED',staging:{phase:'READY'}})"), 'STAGING');
  assert.equal(evaluate("inputBadge({status:'RUNNING',staging:{phase:'STARTED'}})"), 'RUNNING');
  assert.equal(evaluate("inputBadge({status:'CANCELLED',staging:{phase:'TERMINAL'}})"), 'CANCELLED');

  const metrics={cache_hit_bytes:100,staged_bytes:0,verification_ns:1000000,copy_ns:2000000};
  const staging={label:'Preparing <inputs>',phase:'STAGING',ready_ranks:1,total_ranks:3,
    measured_ranks:1,metrics,ranks:[{rank:0,ready:true,metrics},{rank:1,ready:false,metrics:null},
                                {rank:2,ready:false,metrics:null}]};
  const detail={job:{job_id:'fixture-job',status:'SCHEDULED',retry_count:0},
    spec:{name:'Fixture job',execution:{mode:'distributed',workers:3}},staging,
    preparation:null,checkpoint:null,logs:[],attempts:[],ranks:[],events:[],leases:[]};
  routes={'/api/jobs/fixture-job':detail,
          '/api/jobs/fixture-job/logs?view=staging&rank=0':{text:'Preparing inputs.',source:'Input staging · controller'}};
  evaluate("selected='fixture-job';owner='alice'");
  await evaluate('loadDetail()');
  const html=element('detail').innerHTML;
  assert.ok(html.includes('Preparing &lt;inputs&gt;'));
  assert.ok(html.includes('1/3 worker inputs ready'));
  assert.ok(html.includes('Not recorded'));
  assert.ok(html.includes('0 B'));
  assert.ok(html.includes('0.001 s'));
  assert.ok(html.includes('value="@staging" selected'));
  assert.ok(html.includes('data-action="cancel"'));
  assert.equal(element('training-log').textContent,'Preparing inputs.');
  assert.ok(!calls.some(url=>url.includes('file=@staging')));

  context.fixture=detail;
  evaluate("latest={jobs:[{...fixture.job,name:fixture.spec.name,execution:fixture.spec.execution,staging:fixture.staging}]};renderJobs()");
  assert.ok(element('jobs').innerHTML.includes('badge STAGING'));
  assert.ok(element('jobs').innerHTML.includes('Preparing &lt;inputs&gt;'));
  delete detail.staging;detail.job.status='RUNNING';
  evaluate("logSelection=''");
  routes['/api/jobs/fixture-job/logs?rank=0']={text:'epoch 1',source:'training'};
  await evaluate('loadDetail()');
  assert.ok(!element('detail').innerHTML.includes('Input preparation'));
  assert.ok(!element('detail').innerHTML.includes('value="@staging"'));
  assert.equal(element('training-log').textContent,'epoch 1');
  return 'PASS: actual portal script syntax, legacy display, staging badge, partial/unknown metrics, escaping, log selection and cancel control';
}

module.exports={run};
if(require.main===module)run().then(console.log).catch(error=>{console.error(error);process.exitCode=1;});
