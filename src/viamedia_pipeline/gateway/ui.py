"""Single-page unified UI served at GET /. Talks to the gateway's own API.

Brand: Poppins/Raleway, BF Navy background, Viamedia purple + LocalFactor yellow.
"""

UI_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>viamedia.ai — Data Lake</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@500;600;700&family=Raleway:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root{--navy:#2f3342;--navy2:#272b38;--card:#343a4d;--purple:#864797;--purple2:#B190C1;
        --yellow:#F2DA00;--white:#fff;--gray:#bfbfbf;--line:#3c4154;--green:#2fae66;}
  *{box-sizing:border-box}
  body{margin:0;min-height:100vh;background:var(--navy);color:var(--white);
       font-family:Raleway,system-ui,-apple-system,Segoe UI,sans-serif;}
  header{padding:14px 22px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  header h1{font-family:Poppins;font-weight:700;font-size:18px;margin:0}
  header h1 .ai{color:var(--yellow)}
  nav{display:flex;gap:6px}
  nav button{font-family:Poppins;font-weight:500;background:transparent;border:0;color:var(--gray);
             padding:8px 14px;border-radius:8px;cursor:pointer;font-size:14px}
  nav button.active{background:var(--purple);color:#fff}
  nav button:hover:not(.active){background:var(--navy2);color:#fff}
  main{padding:20px 28px;width:100%}
  h2{font-family:Poppins;font-weight:600;font-size:15px;color:var(--purple2);margin:0 0 12px;text-transform:uppercase;letter-spacing:.5px}
  h3{font-family:Poppins;font-weight:600;font-size:13px;margin:14px 0 6px}
  .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin-bottom:16px}
  label{display:block;font-size:12px;color:var(--gray);margin:8px 0 3px}
  input,select,textarea{width:100%;background:var(--navy2);border:1px solid var(--line);border-radius:7px;color:#fff;padding:8px 10px;font-size:13px;font-family:inherit}
  textarea{font-family:ui-monospace,Menlo,Consolas,monospace;min-height:130px;resize:vertical}
  .row{display:flex;gap:12px;flex-wrap:wrap}
  .row>div{flex:1;min-width:150px}
  button.btn{font-family:Poppins;font-weight:600;background:var(--purple);color:#fff;border:0;border-radius:8px;padding:9px 18px;cursor:pointer;font-size:13px}
  button.btn:hover{background:#74407f}
  button.btn.alt{background:transparent;border:1px solid var(--purple2);color:var(--purple2)}
  button.btn.danger{background:#7a3550}
  button.btn:disabled{opacity:.5;cursor:default}
  table{border-collapse:collapse;width:100%;font-size:13px}
  th{font-family:Poppins;font-weight:600;text-align:left;padding:8px 10px;border-bottom:2px solid var(--yellow);white-space:nowrap}
  th .coltype{font-family:Raleway;font-weight:400;font-size:11px;color:var(--yellow)}
  td{padding:6px 10px;border-bottom:1px solid var(--line);white-space:nowrap;max-width:340px;overflow:hidden;text-overflow:ellipsis}
  td.act{max-width:none;overflow:visible}
  .muted{color:var(--gray)} .ok{color:var(--green)} .bad{color:#ff9db1} .yell{color:var(--yellow)}
  .pill{display:inline-block;padding:1px 7px;border-radius:20px;font-size:11px;border:1px solid var(--line);color:var(--gray);margin-left:6px}
  .pill.inc{border-color:var(--purple);color:var(--purple2)}
  .msg{font-size:12px;min-height:16px;margin-top:8px}
  .checklist{max-height:300px;overflow:auto;border:1px solid var(--line);border-radius:8px;padding:6px}
  .checklist label{display:flex;align-items:center;gap:8px;color:#fff;font-size:13px;margin:0;padding:4px 6px;border-radius:6px}
  .checklist label:hover{background:var(--navy2)}
  .checklist input{width:auto}
  .results{overflow:auto;border:1px solid var(--line);border-radius:8px;margin-top:10px;max-height:55vh}
  .topbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:10px}
  .grow{flex:1}
  .kinds{display:flex;gap:14px;align-items:center}
  .kinds label{display:flex;gap:6px;align-items:center;font-size:13px;color:#fff;margin:0}
  .kinds input{width:auto}
  .tokwrap{margin-left:auto}.tokwrap input{width:200px}
  .saved{background:var(--navy2);border:1px solid var(--line);border-radius:8px;padding:10px}
  .saved .pill{margin:3px}
  .guide h3{color:var(--yellow)} .guide p{font-size:13px;line-height:1.6;color:#e7e7ea;margin:4px 0 12px}
  .guide code{background:var(--navy2);padding:1px 5px;border-radius:4px}
  details{margin-bottom:8px} summary{cursor:pointer;font-family:Poppins;font-weight:600;font-size:13px}
</style>
</head>
<body>
<header>
  <h1>viamedia<span class="ai">.ai</span></h1>
  <nav>
    <button data-tab="connections" class="active">Connections</button>
    <button data-tab="sync">Sync</button>
    <button data-tab="query">Query</button>
    <button data-tab="runs">Runs</button>
    <button data-tab="settings">Settings</button>
    <button data-tab="guide">Guide</button>
  </nav>
  <span class="tokwrap"><input id="token" type="password" placeholder="Bearer token (prod)"/></span>
</header>
<main>
  <section id="tab-connections"></section>
  <section id="tab-sync" style="display:none"></section>
  <section id="tab-query" style="display:none"></section>
  <section id="tab-runs" style="display:none"></section>
  <section id="tab-settings" style="display:none"></section>
  <section id="tab-guide" style="display:none"></section>
</main>
<script>
const $=s=>document.querySelector(s);
const el=(t,a={},...k)=>{const e=document.createElement(t);for(const[x,v]of Object.entries(a)){if(x==="class")e.className=v;else if(x==="html")e.innerHTML=v;else if(x.startsWith("on"))e.addEventListener(x.slice(2),v);else if(v===true)e.setAttribute(x,x);else if(v!==false&&v!=null)e.setAttribute(x,v);}k.forEach(c=>{if(c!=null)e.append(c);});return e;};
function headers(){const h={"Content-Type":"application/json"};const t=$("#token").value.trim();if(t)h["Authorization"]="Bearer "+t;return h;}
async function api(method,path,body){const r=await fetch(path,{method,headers:headers(),body:body?JSON.stringify(body):undefined});const txt=await r.text();let d;try{d=txt?JSON.parse(txt):{};}catch{d={detail:txt};}if(!r.ok)throw new Error(typeof d.detail==="string"?d.detail:JSON.stringify(d));return d;}
const KIND_LABEL={TABLE:"Table",VIEW:"View",MATERIALIZED_VIEW:"Materialized View"};
let RUNS=[];
let CONNS=[];
async function loadConns(){CONNS=(await api("GET","/connections")).connections;return CONNS;}

document.querySelectorAll("nav button").forEach(b=>b.onclick=()=>{
  document.querySelectorAll("nav button").forEach(x=>x.classList.toggle("active",x===b));
  ["connections","sync","query","runs","settings","guide"].forEach(t=>$("#tab-"+t).style.display=t===b.dataset.tab?"":"none");
  ({connections:renderConnections,sync:renderSync,query:renderQuery,runs:renderRuns,settings:renderSettings,guide:renderGuide})[b.dataset.tab]();
});

/* ---------- Connections ---------- */
const SSLMODES=["disable","allow","prefer","require","verify-ca","verify-full"];
async function renderConnections(){
  const root=$("#tab-connections");root.innerHTML="";
  root.append(el("h2",{},"Source database connections"));
  const listCard=el("div",{class:"card"},el("div",{class:"muted"},"loading…"));root.append(listCard);
  root.append(connectionForm());
  try{await loadConns();listCard.innerHTML="";
    if(!CONNS.length){listCard.append(el("div",{class:"muted"},"No connections yet — add one below."));}
    else{const t=el("table");t.append(el("tr",{},...["Name","Host","Database","Namespace","Bucket","On",""].map(h=>el("th",{},h))));
      CONNS.forEach(c=>t.append(el("tr",{},el("td",{},c.name),el("td",{},c.db_host+":"+c.db_port),el("td",{},c.db_name),
        el("td",{},c.iceberg_namespace),el("td",{},c.lake_bucket),
        el("td",{html:c.enabled?'<span class="ok">yes</span>':'<span class="muted">no</span>'}),
        el("td",{class:"act"},el("button",{class:"btn alt",onclick:()=>fillForm(c)},"Edit"),
                  el("button",{class:"btn danger",style:"margin-left:6px",onclick:()=>wipeConn(c)},"Wipe data"),
                  el("button",{class:"btn danger",style:"margin-left:6px",onclick:()=>delConn(c.id)},"Delete")))));
      listCard.append(t);}
  }catch(e){listCard.innerHTML="";listCard.append(el("div",{class:"bad"},String(e)));}
}
function field(id,ph,type){return el("div",{},el("label",{},ph),el("input",{id:"f_"+id,placeholder:ph,type:type||"text"}));}
function connectionForm(){
  const f=el("div",{class:"card"});f.id="connForm";
  f.append(el("h2",{},"Add / edit connection"),el("input",{id:"f_id",type:"hidden"}));
  f.append(el("div",{class:"row"},field("name","connection name"),
    el("div",{},el("label",{},"enabled"),el("select",{id:"f_enabled"},el("option",{value:"true"},"yes"),el("option",{value:"false"},"no")))));
  f.append(el("h3",{},"Source database"));
  const ssl=el("select",{id:"f_db_sslmode"});SSLMODES.forEach(m=>ssl.append(el("option",{value:m},m)));
  f.append(el("div",{class:"row"},field("db_host","host"),field("db_port","port"),field("db_name","database name")));
  f.append(el("div",{class:"row"},field("db_user","username"),field("db_password","password (blank = keep)","password"),
    el("div",{},el("label",{},"sslmode"),ssl)));
  f.append(el("h3",{},"Target (AWS S3 — single bucket per database)"));
  f.append(el("div",{class:"row"},field("aws_access_key","AWS access key (blank = keep)"),field("aws_secret_key","AWS secret key (blank = keep)","password"),field("aws_region","aws region")));
  f.append(el("div",{class:"row"},field("lake_bucket","lake bucket (holds raw/ curated/ gateway-results/)"),field("iceberg_namespace","iceberg namespace")));
  f.append(el("div",{class:"topbar"},
    el("button",{class:"btn alt",onclick:testConn},"Test connection"),
    el("button",{class:"btn",onclick:saveConn},"Save (tests first)"),
    el("button",{class:"btn alt",onclick:()=>$("#connForm").replaceWith(connectionForm())},"Clear")));
  f.append(el("div",{class:"msg",id:"connMsg"}));
  setTimeout(()=>{if(!$("#f_db_port").value)$("#f_db_port").value="5432";if(!$("#f_aws_region").value)$("#f_aws_region").value="us-east-1";$("#f_db_sslmode").value="require";},0);
  return f;
}
function fillForm(c){renderConnections().then(()=>setTimeout(()=>{
  $("#f_id").value=c.id;$("#f_name").value=c.name;$("#f_enabled").value=String(c.enabled);
  $("#f_db_host").value=c.db_host;$("#f_db_port").value=c.db_port;$("#f_db_name").value=c.db_name;
  $("#f_db_user").value=c.db_user;$("#f_db_sslmode").value=c.db_sslmode;
  $("#f_aws_region").value=c.aws_region;$("#f_lake_bucket").value=c.lake_bucket;$("#f_iceberg_namespace").value=c.iceberg_namespace;
  $("#connMsg").innerHTML='<span class="yell">Editing \''+c.name+'\'</span> — leave password / AWS keys blank to keep existing.';
},40));}
function formBody(){return{name:$("#f_name").value.trim(),enabled:$("#f_enabled").value==="true",
  db_host:$("#f_db_host").value.trim(),db_port:Number($("#f_db_port").value||5432),db_name:$("#f_db_name").value.trim(),
  db_user:$("#f_db_user").value.trim(),db_password:$("#f_db_password").value||null,db_sslmode:$("#f_db_sslmode").value,
  aws_access_key:$("#f_aws_access_key").value.trim()||null,aws_secret_key:$("#f_aws_secret_key").value.trim()||null,
  aws_region:$("#f_aws_region").value.trim()||"us-east-1",lake_bucket:$("#f_lake_bucket").value.trim(),iceberg_namespace:$("#f_iceberg_namespace").value.trim()};}
async function testConn(){const m=$("#connMsg");m.textContent="testing database + AWS…";
  try{const d=await api("POST","/connections/test",formBody());
    const dbp=d.db.ok?'<span class="ok">DB OK</span>':'<span class="bad">DB: '+d.db.error+'</span>';
    const awsp=d.aws.ok?'<span class="ok">AWS OK</span>':'<span class="bad">AWS: '+d.aws.error+'</span>';
    m.innerHTML=dbp+' &nbsp; '+awsp;}catch(e){m.innerHTML='<span class="bad">'+e+'</span>';}}
async function saveConn(){const id=$("#f_id").value;const m=$("#connMsg");m.textContent="testing then saving…";
  try{if(id)await api("PUT","/connections/"+id,formBody());else await api("POST","/connections",formBody());
    m.innerHTML='<span class="ok">saved</span>';renderConnections();}catch(e){m.innerHTML='<span class="bad">not saved — '+e+'</span>';}}
async function delConn(id){if(!confirm("Delete connection "+id+"? (does NOT delete S3 data)"))return;try{await api("DELETE","/connections/"+id);renderConnections();}catch(e){alert(e);}}
async function wipeConn(c){
  if(!confirm("WIPE ALL DATA for '"+c.name+"'?\n\nThis DROPS every Iceberg table, DELETES the S3 folders (raw/ curated/ gateway-results/) in bucket '"+c.lake_bucket+"', clears watermarks + chunk state, and deletes all Dagster runs for this connection.\n\nThe connection details and your object selection are KEPT, so you can re-sync from scratch.\n\nThis cannot be undone."))return;
  if(prompt("Type the connection name to confirm wipe:")!==c.name){alert("Name did not match — wipe cancelled.");return;}
  try{const r=await api("POST","/connections/"+c.id+"/wipe",{});
    alert("Wiped '"+c.name+"':\n  tables dropped: "+r.tables_dropped+"\n  S3 objects deleted: "+r.s3_objects_deleted+"\n  runs deleted: "+r.runs_deleted);
    renderConnections();}catch(e){alert("Wipe failed: "+e);}}

/* ---------- Sync ---------- */
async function renderSync(){
  const root=$("#tab-sync");root.innerHTML="";root.append(el("h2",{},"Select objects to sync"));
  const bar=el("div",{class:"card"});root.append(bar);
  const sel=el("select",{id:"syncConn",onchange:loadObjects});bar.append(el("label",{},"Connection"),sel);
  const kinds=el("div",{class:"kinds",style:"margin-top:10px"},el("span",{class:"muted"},"Show kinds:"));
  ["TABLE","VIEW","MATERIALIZED_VIEW"].forEach(k=>{const cb=el("input",{type:"checkbox",id:"kind_"+k});cb.checked=true;cb.onchange=applyKindFilter;
    kinds.append(el("label",{},cb,KIND_LABEL[k]+"s"));});
  bar.append(kinds);
  root.append(el("div",{class:"card",id:"syncBox"},el("div",{class:"muted"},"choose a connection")));
  try{await loadConns();if(!CONNS.length){bar.append(el("div",{class:"muted"},"Add a connection first."));return;}
    CONNS.forEach(c=>sel.append(el("option",{value:c.id},c.name+"  ("+c.iceberg_namespace+")")));loadObjects();
  }catch(e){bar.append(el("div",{class:"bad"},String(e)));}
}
async function loadObjects(){
  const id=$("#syncConn").value;const box=$("#syncBox");box.innerHTML="";box.append(el("div",{class:"muted"},"discovering objects + loading saved selection…"));
  try{const [objsR,selR]=await Promise.all([api("GET","/connections/"+id+"/objects"),api("GET","/connections/"+id+"/selection")]);
    box.innerHTML="";box._objs=objsR.objects;
    // saved panel
    const savedWrap=el("div",{class:"saved"});savedWrap.append(el("h3",{style:"margin-top:0"},"Currently saved & syncing"));
    const savedEnabled=selR.selection.filter(s=>s.enabled);
    if(!savedEnabled.length)savedWrap.append(el("span",{class:"muted"},"nothing saved yet — pick objects below and Save selection"));
    else savedEnabled.forEach(s=>savedWrap.append(el("span",{class:"pill"},s.schema+"."+s.name+" · "+(KIND_LABEL[s.kind]||s.kind))));
    box.append(savedWrap);
    const savedSet=new Set(savedEnabled.map(s=>s.schema+"."+s.name));
    box.append(el("h3",{},"Available objects (check to include)"));
    const list=el("div",{class:"checklist",id:"objList"});box._list=list;
    objsR.objects.forEach(o=>{const cb=el("input",{type:"checkbox"});cb.checked=savedSet.size?savedSet.has(o.schema+"."+o.name):o.selected;cb._o=o;
      const lab=el("label",{"data-kind":o.kind},cb,el("span",{},o.schema+"."+o.name),el("span",{class:"pill"},KIND_LABEL[o.kind]||o.kind));
      if(o.incremental)lab.append(el("span",{class:"pill inc"},"incremental"));list.append(lab);});
    box.append(list);box.append(el("div",{class:"msg",id:"syncMsg"}));
    box.append(el("div",{class:"topbar"},
      el("button",{class:"btn",onclick:()=>saveSelection(id)},"Save selection"),
      el("button",{class:"btn alt",onclick:()=>toggleAll(true)},"Check all"),
      el("button",{class:"btn alt",onclick:()=>toggleAll(false)},"Uncheck all"),
      el("span",{class:"grow"}),
      el("button",{class:"btn",onclick:()=>syncNow(id,"bootstrap")},"Bootstrap checked"),
      el("button",{class:"btn alt",onclick:()=>syncNow(id,"incremental")},"Incremental checked"),
      el("button",{class:"btn alt",onclick:()=>syncNow(id,"reconcile")},"Reconcile checked")));
    applyKindFilter();
  }catch(e){box.innerHTML="";box.append(el("div",{class:"bad"},String(e)));}
}
function applyKindFilter(){const show={};["TABLE","VIEW","MATERIALIZED_VIEW"].forEach(k=>show[k]=$("#kind_"+k)&&$("#kind_"+k).checked);
  const list=$("#objList");if(!list)return;[...list.children].forEach(lab=>{lab.style.display=show[lab.getAttribute("data-kind")]?"":"none";});}
function rows(){return [...($("#objList")?.children||[])];}
function toggleAll(v){rows().forEach(lab=>{if(lab.style.display!=="none")lab.querySelector("input").checked=v;});}
function checkedObjs(){return rows().map(l=>l.querySelector("input")).filter(cb=>cb.checked).map(cb=>cb._o);}
async function saveSelection(id){const items=rows().map(l=>{const cb=l.querySelector("input");return{schema:cb._o.schema,name:cb._o.name,kind:cb._o.kind,enabled:cb.checked};});
  $("#syncMsg").textContent="saving…";try{const d=await api("POST","/connections/"+id+"/selection",items);$("#syncMsg").innerHTML='<span class="ok">saved '+d.saved+' objects</span>';loadObjects();}catch(e){$("#syncMsg").innerHTML='<span class="bad">'+e+'</span>';}}
async function syncNow(id,mode){const objs=checkedObjs();if(!objs.length){$("#syncMsg").textContent="nothing checked";return;}
  $("#syncMsg").textContent="launching "+objs.length+" "+mode+" run(s)…";
  for(const o of objs){try{const r=await api("POST","/runs",{connection_id:Number(id),table_fqn:o.schema+"."+o.name,mode});
    RUNS.unshift({run_id:r.runId,status:r.status,table:o.schema+"."+o.name,mode,connection_id:Number(id)});}catch(e){RUNS.unshift({run_id:"(failed)",status:String(e),table:o.schema+"."+o.name,mode,connection_id:Number(id)});}}
  $("#syncMsg").innerHTML='<span class="ok">launched — see the Runs tab</span>';}

/* ---------- Query ---------- */
async function renderQuery(){
  const root=$("#tab-query");root.innerHTML="";root.append(el("h2",{},"Query"));
  const wrap=el("div",{class:"card"});root.append(wrap);
  const sel=el("select",{id:"qConn",onchange:loadQueryTables});
  wrap.append(el("label",{},"Connection"),sel);
  const ta=el("textarea",{id:"sql",spellcheck:"false"},"SELECT 1;");
  const side=el("div",{class:"muted"},"select a connection");
  wrap.append(el("div",{class:"row",style:"margin-top:10px"},el("div",{style:"max-width:300px"},el("label",{},"Tables"),side),
    el("div",{style:"flex:3"},el("label",{},"SQL (Ctrl/Cmd+Enter to run)"),ta)));
  wrap.append(el("div",{class:"topbar"},el("button",{class:"btn",onclick:runQuery},"Run query"),el("span",{class:"msg",id:"qmsg"})));
  root.append(el("div",{class:"results",id:"qres"}));
  ta.addEventListener("keydown",e=>{if((e.metaKey||e.ctrlKey)&&e.key==="Enter")runQuery();});
  wrap._side=side;
  try{await loadConns();CONNS.forEach(c=>sel.append(el("option",{value:c.id},c.name+"  ("+c.iceberg_namespace+")")));
    if(CONNS.length)loadQueryTables();else side.textContent="add a connection first";}catch(e){side.textContent=String(e);}
}
async function loadQueryTables(){
  const id=Number($("#qConn").value);const c=CONNS.find(x=>x.id===id);const side=$("#tab-query .card")._side;side.innerHTML="loading…";
  try{const d=await api("GET","/tables");const grp=d.connections.find(x=>x.id===id);side.innerHTML="";
    const tables=(grp&&grp.tables)||[];
    if(!tables.length)side.append(el("div",{class:"muted"},"no synced tables yet for this connection"));
    tables.forEach(t=>side.append(el("button",{class:"btn alt",style:"display:block;width:100%;text-align:left;margin:3px 0",
      onclick:()=>{$("#sql").value='SELECT *\nFROM "'+c.iceberg_namespace+'"."'+t+'"\nLIMIT 100;';runQuery();}},t)));
  }catch(e){side.innerHTML="";side.append(el("div",{class:"bad"},String(e)));}
}
async function runQuery(){const sql=$("#sql").value.trim();if(!sql)return;$("#qmsg").textContent="running…";
  try{const d=await api("POST","/query",{sql,limit:1000});const res=$("#qres");res.innerHTML="";
    const types=d.column_types||[];
    const t=el("table");t.append(el("tr",{},...d.columns.map((c,i)=>el("th",{},
      el("div",{},c),el("div",{class:"coltype"},types[i]||"")))));
    d.rows.forEach(r=>t.append(el("tr",{},...r.map(v=>el("td",{html:v===null?'<span class="muted">NULL</span>':String(v)})))));
    res.append(t);$("#qmsg").innerHTML='<span class="ok">'+d.row_count+'</span> rows'+(d.truncated?' (truncated)':'');}
  catch(e){$("#qmsg").innerHTML='<span class="bad">'+e+'</span>';}}

/* ---------- Runs ---------- */
async function renderRuns(){
  const root=$("#tab-runs");root.innerHTML="";root.append(el("h2",{},"Runs"));
  const card=el("div",{class:"card"});root.append(card);
  const sel=el("select",{id:"runConn",onchange:loadRuns});card.append(el("label",{},"Connection"),sel);
  card.append(el("div",{class:"topbar"},el("button",{class:"btn alt",onclick:loadRuns},"Refresh")));
  card.append(el("div",{id:"runsBox"},el("div",{class:"muted"},"select a connection")));
  try{await loadConns();CONNS.forEach(c=>sel.append(el("option",{value:c.id},c.name)));if(CONNS.length)loadRuns();}catch(e){card.append(el("div",{class:"bad"},String(e)));}
}
async function loadRuns(){
  const id=Number($("#runConn").value);const box=$("#runsBox");box.innerHTML="loading…";
  let server=[];try{server=(await api("GET","/runs?connection_id="+id)).runs;}catch(e){/* ignore */}
  const session=RUNS.filter(r=>r.connection_id===id);
  const all=[...session.map(s=>({runId:s.run_id,status:s.status,table:s.table,kind:s.mode})),...server];
  const seen=new Set();const uniq=all.filter(r=>{const k=(r.runId||"")+r.table;if(seen.has(k))return false;seen.add(k);return true;});
  box.innerHTML="";const t=el("table");t.append(el("tr",{},...["Run","Table","Kind","Status"].map(h=>el("th",{},h))));
  if(!uniq.length)t.append(el("tr",{},el("td",{class:"muted",colspan:"4"},"no runs for this connection yet")));
  uniq.forEach(r=>t.append(el("tr",{},el("td",{},(r.runId||"").slice(0,8)),el("td",{},r.table||""),el("td",{},r.kind||""),el("td",{html:statusHtml(r.status)}))));
  box.append(t);
}
function statusHtml(s){if(s==="SUCCESS")return '<span class="ok">SUCCESS</span>';if(s&&String(s).includes("FAIL"))return '<span class="bad">'+s+'</span>';return '<span class="muted">'+(s||"?")+'</span>';}

/* ---------- Settings ---------- */
const SETTING_HELP={extract_rows_per_chunk:"Rows per parallel extract chunk. LOWER this to use less memory (e.g. 500000 or 250000).",
  extract_row_group_rows:"Rows buffered before writing a parquet row group. Lower = less memory (e.g. 100000).",
  parquet_compression:"zstd / snappy / gzip",parquet_compression_level:"zstd level 1-22",
  incremental_overlap_seconds:"Seconds of overlap re-scanned each incremental run.",
  incremental_safety_gap_seconds:"Skip rows newer than now-this (avoids in-flight txns).",
  gateway_query_timeout_seconds:"Query timeout (advisory).",gateway_view_refresh_seconds:"How often query views refresh.",
  gateway_default_row_limit:"LIMIT auto-applied when a query has none. Set -1 for UNLIMITED (returns the whole table — can be huge).",
  gateway_max_row_limit:"Largest explicit LIMIT allowed. Set -1 to allow any LIMIT (no ceiling)."};
async function saveSettings(inputs){const body={};Object.entries(inputs).forEach(([k,i])=>body[k]=i.value);$("#smsg").textContent="saving…";
  try{await api("POST","/settings",body);$("#smsg").innerHTML='<span class="ok">saved — applies within ~30s (re-run a sync to use new chunk sizes)</span>';}catch(e){$("#smsg").innerHTML='<span class="bad">'+e+'</span>';}}
async function renderSettings(){
  const root=$("#tab-settings");root.innerHTML="";root.append(el("h2",{},"Settings — extractor tuning & gateway"));
  const card=el("div",{class:"card"},el("div",{class:"muted"},"loading…"));root.append(card);
  try{const d=await api("GET","/settings");card.innerHTML="";const inputs={};
    Object.entries(d).forEach(([k,v])=>{card.append(el("label",{},k+(SETTING_HELP[k]?"  —  "+SETTING_HELP[k]:"")));
      const i=el("input",{value:v==null?"":String(v)});inputs[k]=i;card.append(i);});
    card.append(el("div",{class:"topbar"},el("button",{class:"btn",onclick:()=>saveSettings(inputs)},"Save settings"),el("span",{class:"msg",id:"smsg"})));
  }catch(e){card.innerHTML="";card.append(el("div",{class:"bad"},String(e)));}
}

/* ---------- Guide ---------- */
function renderGuide(){
  const root=$("#tab-guide");root.innerHTML="";root.append(el("h2",{},"Guide — what each part does"));
  root.append(el("div",{class:"guide card",html:`
    <h3>This app (localhost:3000)</h3>
    <p>This is your control panel for moving data out of your PostgreSQL databases into your AWS S3 data lake, and for querying it. You manage everything here — no config files.</p>
    <h3>Connections tab</h3>
    <p>A <b>connection</b> = one source database. Enter its host, port, database name, username, password and SSL mode, plus the AWS keys, region, the single S3 <b>bucket</b> for this database, and an Iceberg <b>namespace</b> (a label that groups this database's tables in the lake). Click <b>Test connection</b> to check the database and AWS are reachable — a connection can only be <b>Saved</b> if both tests pass. Each database gets its own bucket; inside it the app creates folders <code>raw/</code>, <code>curated/</code> and <code>gateway-results/</code> automatically.</p>
    <h3>Sync tab</h3>
    <p>Pick a connection, then tick the <b>tables / views / materialized views</b> you want copied to the lake (use the "Show kinds" filter to narrow the list). Click <b>Save selection</b> to remember your choice — the box at the top always shows what is currently saved and syncing. <b>Bootstrap checked</b> does a full first-time copy; <b>Incremental checked</b> pulls only new/changed rows since last time. Un-ticking an object and saving <b>stops</b> its sync (your existing data in S3 is <b>not</b> deleted).</p>
    <h3>Query tab</h3>
    <p>Pick a connection, click a table on the left (or type SQL), and run read-only <code>SELECT</code> queries directly against the lake. Results show inline. This is the alternative to PowerBI for quick checks.</p>
    <h3>Runs tab</h3>
    <p>Pick a connection to see the status of its sync runs (queued / running / success / failed), including ones you launched from the Sync tab.</p>
    <h3>The engine (localhost:3001)</h3>
    <p>Behind the scenes the actual copy jobs run on <b>Dagster</b>, the scheduling engine, reachable at <code>http://localhost:3001</code>. You normally don't need it — this app triggers and monitors everything for you. Open it only for deep debugging: detailed step logs, run timelines, retries, and the 5-minute incremental schedule. Think of <code>:3000</code> as the dashboard and <code>:3001</code> as the engine room.</p>
  `}));
}

renderConnections();
</script>
</body>
</html>
"""
