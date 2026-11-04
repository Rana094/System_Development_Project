/* The browser only requests operations; scheduler and ownership decisions stay on the controller. */
const $ = id => document.getElementById(id);
let csrf = '', owner = null, examples = {}, selected = null, latest = null, busy = false, detailData = null;
let logSelection = '', rankSelection = '0', specExtras = {};
const terminal = ['COMPLETED','FAILED','CANCELLED'];
const esc = value => String(value ?? '—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const badge = value => `<span class="badge ${/^[A-Z_]+$/.test(value)?value:''}">${esc(value)}</span>`;
const stamp = value => value ? new Date(value*1000).toLocaleString() : '—';
function notice(message) {
  $('notice').textContent=message; $('notice').hidden=!message;
  $('submit-error').textContent=message; $('submit-error').hidden=!message || !$('submit-dialog').open;
}
async function api(path, options={}) {
  const headers = {...options.headers};
  if (options.method && options.method!=='GET') headers['X-CSRF-Token']=csrf;
  if (options.body && !(options.body instanceof FormData)) { headers['Content-Type']='application/json'; options.body=JSON.stringify(options.body); }
  const response=await fetch('/api'+path,{...options,headers});
  const data=await response.json();
  if (!response.ok) { if(response.status===401 && path!='/login') showLogin(); throw new Error(data.error || 'Request failed'); }
  return data;
}
function showLogin() { owner=null; selected=null; $('workspace').hidden=true; $('account').hidden=true; $('login-screen').hidden=false; }
async function enter(data) {
  csrf=data.csrf; owner=data.owner;
  if(!owner) return showLogin();
  $('owner').textContent=owner; $('workspace').hidden=false; $('account').hidden=false; $('login-screen').hidden=true;
  examples=await api('/examples'); fillSpec(examples.regression); await refresh();
}
$('login-form').addEventListener('submit',async e=>{
  e.preventDefault(); const button=e.submitter; button.disabled=true;
  try { await enter(await api('/login',{method:'POST',body:{owner:$('login-owner').value,password:$('login-password').value}})); $('login-password').value=''; notice(''); }
  catch(error){notice(error.message);} finally{button.disabled=false;}
});
$('logout').onclick=async()=>{try{await api('/logout',{method:'POST'}); showLogin(); csrf=(await api('/session')).csrf;}catch(e){notice(e.message);}};
function syncMode(){
  const singleExample=['regression','binary','cnn'].includes($('source').value);
  $('mode').disabled=singleExample;
  if(singleExample) $('mode').value='single';
  const single=$('mode').value==='single';
  $('workers').disabled=single;
  if(single) $('workers').value=1;
}
function fillSpec(spec){
  specExtras=spec.environment.python ? {python:spec.environment.python} : {};
  $('job-name').value=spec.name; $('command').value=JSON.stringify(spec.entrypoint);
  $('mode').value=spec.execution.mode; $('workers').value=spec.execution.workers;
  $('cpu').value=spec.resources_per_worker.cpu_cores; $('ram').value=spec.resources_per_worker.memory_gb; $('gpus').value=spec.resources_per_worker.gpus;
  $('requirements').value=spec.environment.requirements; $('recovery').value=spec.recovery.mode;
  $('interval').value=spec.recovery.interval_seconds || 5; $('retries').value=spec.recovery.max_retries ?? 2;
  syncMode();
}
$('source').onchange=()=>{
  const key=$('source').value, upload=key==='upload'; $('upload-fields').hidden=!upload; $('example-help').hidden=upload; $('project-file').required=upload;
  if(!upload){ fillSpec(examples[key]); $('sample-download').href='/api/examples/'+key+'/download'; }
  syncMode();
};
$('mode').onchange=()=>{$('workers').value=$('mode').value==='single'?1:2;syncMode();};
$('spec-file').onchange=async()=>{try{fillSpec(JSON.parse(await $('spec-file').files[0].text()));}catch(e){notice('Invalid job definition: '+e.message);}};
$('new-job').onclick=()=>{$('submit-error').hidden=true;$('submit-dialog').showModal();};
$('close-submit').onclick=()=>{$('submit-dialog').close();};
$('submit-form').addEventListener('submit',async e=>{
  e.preventDefault(); const button=$('submit-button'); button.disabled=true; button.textContent='Uploading and validating…';
  try{
    const recovery={mode:$('recovery').value,max_retries:Number($('retries').value)};
    if(recovery.mode==='checkpoint') recovery.interval_seconds=Number($('interval').value);
    const spec={schema_version:1,name:$('job-name').value,project:'project.zip',dataset:null,entrypoint:JSON.parse($('command').value),
      execution:{mode:$('mode').value,workers:Number($('workers').value)},resources_per_worker:{cpu_cores:Number($('cpu').value),memory_gb:Number($('ram').value),gpus:Number($('gpus').value)},
      environment:{requirements:$('requirements').value,...specExtras},recovery};
    const form=new FormData(); form.set('spec',JSON.stringify(spec)); form.set('profile',$('profile').value);
    if($('source').value==='upload'){
      form.set('project',$('project-file').files[0]); if($('dataset-file').files[0])form.set('dataset',$('dataset-file').files[0]);
    }else form.set('example',$('source').value);
    const result=await api('/jobs',{method:'POST',body:form}); selected=result.job_id; logSelection='preparation.log'; rankSelection='0';
    $('filter').value='all'; $('submit-dialog').close(); notice('Submission received. Environment preparation will run automatically.'); await refresh();
  }catch(error){notice(error.message);}finally{button.disabled=false; button.textContent='Submit & prepare';}
});
function renderJobs(){
  const filter=$('filter').value;
  const jobs=latest.jobs.filter(j=>filter==='all'||((filter==='finished')===terminal.includes(j.status)));
  const scroll=$('jobs').scrollTop;
  $('jobs').innerHTML=jobs.length?jobs.map(j=>{
    const preparing=['PENDING','PREPARING'].includes(j.preparation?.state);
    return `<button class="job ${j.job_id===selected?'selected':''}" data-job="${esc(j.job_id)}"><div class="job-head"><strong>${esc(j.name)}</strong>${badge(preparing?'PREPARING':j.status)}</div><p>${esc(j.execution.mode)} · ${esc(j.execution.workers)} worker(s) · ${esc(stamp(j.created_at))}</p><p>${j.position?`Queue #${j.position} · `:''}${esc(j.queue_reason || (preparing?'Preparing worker environments':j.node_id || j.preparation?.error || 'Waiting for preparation'))}</p></button>`;
  }).join(''):'<div class="empty"><h2>No jobs here yet</h2><p>Submit a training example or upload your project.</p></div>';
  $('jobs').scrollTop=scroll;
  $('jobs').querySelectorAll('[data-job]').forEach(button=>button.onclick=async()=>{selected=button.dataset.job;logSelection='';rankSelection='0';renderJobs();try{await loadDetail();}catch(e){notice(e.message);}});
}
$('filter').onchange=()=>{if(latest)renderJobs();};
async function refresh(){
  if(!owner||busy)return; busy=true;
  try{
    latest=await api('/overview');
    $('dispatcher-warning').hidden=latest.dispatcher_running;
    $('refresh-state').textContent='Live cluster · updated '+new Date(latest.timestamp*1000).toLocaleTimeString();
    $('quota').textContent=`Up to ${latest.limits.max_active_per_owner} active jobs · ${latest.limits.max_pending_per_owner} pending submissions`;
    $('nodes').innerHTML=latest.nodes.map(n=>`<article class="node panel"><div class="section-title"><h2>${esc(n.id)}</h2>${badge(n.state)}</div><div class="metrics"><div><small>CPU USED</small>${esc(n.cpu?.usage_percent ?? '—')}${n.cpu?'%':''}</div><div><small>RAM FREE</small>${esc(n.memory?.available_gb ?? '—')} GB</div><div><small>SLOTS FREE</small>${esc(n.slots?.available ?? '—')}</div></div></article>`).join('');
    renderJobs(); if(selected)await loadDetail();
  }catch(error){notice(error.message); $('refresh-state').textContent='Connection interrupted · retrying…';}finally{busy=false;}
}
function table(head,body){return `<div class="table-wrap"><table><thead><tr>${head.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${body.join('')}</tbody></table></div>`;}
async function loadDetail(){
  const id=selected; const data=await api('/jobs/'+encodeURIComponent(id)); if(id!==selected)return;
  detailData=data;
  if(['log-source','rank-source'].includes(document.activeElement?.id)){await loadLog();return;}
  const disclosure=$('detail').querySelector('details')?.open || false;
  const timelineScroll=$('detail').querySelector('.timeline')?.scrollTop || 0;
  const j=data.job, prep=data.preparation, ck=data.checkpoint?.details;
  const currentRanks=data.ranks.filter(r=>r.retry_count===j.retry_count);
  const controls=[];
  if(['STAGED','READY'].includes(j.status)&&!['PENDING','PREPARING'].includes(prep?.state))controls.push(['prepare','Retry preparation']);
  if(j.status==='QUEUED')controls.push(['hold','Hold']); if(j.status==='HELD')controls.push(['release','Release']);
  if(!terminal.includes(j.status)&&j.status!=='CANCELLING')controls.push(['cancel','Cancel job']);
  // Preserve the user's log selection and scroll position while data refreshes.
  const previous=$('training-log'); const scroll=previous?.scrollTop ?? 0, follow=!previous || previous.scrollHeight-previous.scrollTop-previous.clientHeight<30;
  if(['PENDING','PREPARING'].includes(prep?.state))logSelection='preparation.log';
  else if(logSelection==='preparation.log'&&['RUNNING','SCHEDULED'].includes(j.status))logSelection='';
  if(terminal.includes(j.status)&&!logSelection)logSelection=data.logs.findLast(n=>n.startsWith('attempt-')) || data.logs[0] || '';
  const sourceOptions=[['','Live training'],...data.logs.map(name=>[name,name])];
  if(logSelection&&!sourceOptions.some(([v])=>v===logSelection))sourceOptions.push([logSelection,logSelection]);
  $('detail').innerHTML=`<div class="section-title"><h2>${esc(data.spec.name)}</h2>${badge(j.status)}</div><p class="id">${esc(j.job_id)}</p>
    <div class="facts"><div><small>EXECUTION</small><strong>${esc(data.spec.execution.mode)} · ${data.spec.execution.workers} workers</strong></div><div><small>ATTEMPT</small><strong>${j.retry_count+1}</strong></div><div><small>CENTRAL CHECKPOINT</small><strong>${ck?.global_step!==undefined?'Step '+esc(ck.global_step):'Not yet saved'}</strong></div></div>
    <div class="actions">${controls.map(([a,label])=>`<button data-action="${a}" class="${a==='cancel'?'danger':'quiet'}">${label}</button>`).join('')}${terminal.includes(j.status)?`<a class="button" href="/api/jobs/${encodeURIComponent(id)}/results">Download results ZIP</a>`:''}</div>
    ${j.last_error||j.error||prep?.error?`<p class="warning">${esc(j.last_error || j.error || prep.error)}</p>`:''}<p class="muted small">${esc(j.queue_reason || (prep?.state==='PREPARING'?'Preparing compatible environments…':''))}</p>
    ${data.timing?'<h3>Training time</h3>'+table(['Measurement','Seconds'],[
      ['Training loop · final attempt',data.timing.final_attempt_training_loop_seconds],
      ['Training program · final attempt',data.timing.final_attempt_training_program_seconds],
      ['Full training · including recovery',data.timing.full_training_wall_seconds],
      ['Submission to completion',data.timing.submission_to_completion_seconds]
    ].map(([label,value])=>`<tr><td>${esc(label)}</td><td>${Number.isFinite(value)?value.toFixed(3):'Not recorded'}</td></tr>`))+'<p class="muted small">DDP uses the slowest rank. Loop time includes checkpointing and configured delays. Download results for the full timing breakdown.</p>':''}
    <h3>Training logs</h3><div class="log-controls"><select id="log-source" aria-label="Log source">${sourceOptions.map(([v,l])=>`<option value="${esc(v)}" ${v===logSelection?'selected':''}>${esc(l)}</option>`).join('')}</select><select id="rank-source" aria-label="Training rank">${(currentRanks.length?currentRanks:[{rank:0,node_id:j.node_id}]).map(r=>`<option value="${r.rank}" ${String(r.rank)===rankSelection?'selected':''}>Rank ${r.rank} · ${esc(r.node_id)}</option>`).join('')}</select></div><p id="log-caption" class="muted small">Most recent 64 KiB · refreshes every 3 seconds</p><pre id="training-log" class="log">Loading log…</pre>
    <h3>Attempts & workers</h3>${table(['Attempt','Worker','Status','Error'],data.attempts.map(a=>`<tr><td>${a.attempt_number}</td><td>${esc(a.node_id)}</td><td>${badge(a.status)}</td><td>${esc(a.error)}</td></tr>`))}
    ${data.ranks.length?'<h3>Distributed ranks</h3>'+table(['Attempt','Rank','Worker','Status'],data.ranks.map(r=>`<tr><td>${r.retry_count+1}</td><td>${r.rank}</td><td>${esc(r.node_id)}</td><td>${badge(r.status)}</td></tr>`)):''}
    <h3>Recovery history</h3><div class="timeline">${data.events.filter(e=>!['CHECKPOINT_SYNC','DDP_CHECKPOINT_COMMITTED'].includes(e.event_type)).map(e=>`<div class="event"><strong>${esc(e.event_type.replaceAll('_',' '))}</strong><small>${esc(stamp(e.event_time))} · ${esc(e.node_id)}</small><pre>${esc(JSON.stringify(e.details,null,2))}</pre></div>`).join('')||'<p class="muted small">No recovery events recorded yet.</p>'}</div>
    <details ${disclosure?'open':''}><summary>Checkpoint and execution ownership</summary><pre>${esc(JSON.stringify({checkpoint:ck || null,leases:data.leases.map(l=>({attempt:l.retry_count+1,node:l.node_id,generation:l.generation,expires:stamp(l.expires_at),renewal_blocked:!!l.renewal_blocked,fenced:!!l.revoked}))},null,2))}</pre></details>`;
  $('detail').querySelector('.timeline').scrollTop=timelineScroll;
  $('detail').querySelectorAll('[data-action]').forEach(button=>button.onclick=async()=>{
    button.disabled=true;
    try{await api('/jobs/'+encodeURIComponent(id)+'/'+button.dataset.action,{method:'POST',body:{profile:prep?.profile || 'pytorch-cpu'}});await refresh();}catch(e){notice(e.message);button.disabled=false;}
  });
  $('log-source').onchange=()=>{logSelection=$('log-source').value;loadLog().catch(e=>notice(e.message));};
  $('rank-source').onchange=()=>{rankSelection=$('rank-source').value;loadLog().catch(e=>notice(e.message));};
  await loadLog(); if(selected===id){$('training-log').scrollTop=follow?$('training-log').scrollHeight:scroll;}
}
async function loadLog(){
  const id=selected, source=logSelection, rank=rankSelection;
  let data;
  try{data=await api('/jobs/'+encodeURIComponent(id)+'/logs?'+new URLSearchParams({...(source?{file:source}:{}),rank}));}
  catch(error){data={text:error.message==='Log not collected yet'?'Waiting for the first log output…':error.message,source:source||'Live training'};}
  if(id!==selected||source!==logSelection||rank!==rankSelection)return;
  $('training-log').textContent=data.text; $('log-caption').textContent=data.source+' · most recent 64 KiB';
}
api('/session').then(enter).catch(e=>notice(e.message));
setInterval(refresh,3000);
