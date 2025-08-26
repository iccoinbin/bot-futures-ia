import os, subprocess, secrets
from typing import List, Dict
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI(title="Bot Futures Panel", version="0.3.1")
security = HTTPBasic()

PANEL_USER = os.getenv("PANEL_USER", "admin")
PANEL_PASS = os.getenv("PANEL_PASS", "changeme")
ALLOWED = [s.strip() for s in os.getenv("PANEL_ALLOWED_SERVICES","").split(",") if s.strip()]

def auth(credentials: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(credentials.username, PANEL_USER)
    ok_pass = secrets.compare_digest(credentials.password, PANEL_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})
    return True

def run(cmd: List[str], use_sudo: bool=False) -> str:
    if use_sudo:
        cmd = ["sudo"] + cmd
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True).strip()
    except subprocess.CalledProcessError as e:
        msg = e.output[-800:] if e.output else str(e)
        raise HTTPException(status_code=400, detail=msg)

def check_allowed(svc: str) -> str:
    if not svc.endswith(".service") and not svc.endswith(".timer"):
        svc = f"{svc}.service"
    if svc not in ALLOWED:
        raise HTTPException(status_code=400, detail=f"Service '{svc}' não permitido")
    return svc

@app.get("/health")
def health(_: bool = Depends(auth)):
    return {"ok": True, "services": ALLOWED}

@app.get("/", response_class=HTMLResponse)
def index(_: bool = Depends(auth)):
    return """
<!doctype html>
<html lang="pt-br"><head><meta charset="utf-8"><title>Bot Futures — Painel</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{font-family:system-ui,Segoe UI,Roboto,Arial;margin:24px}
h1{margin-top:0}
.card{border:1px solid #ddd;border-radius:12px;padding:16px;margin-bottom:16px}
.btn{padding:8px 12px;border:1px solid #ccc;border-radius:10px;background:#f7f7f7;cursor:pointer;margin-right:8px}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;border:1px solid #ccc;margin-left:8px}
pre{background:#0b0b0b;color:#eaeaea;padding:12px;border-radius:10px;overflow:auto;max-height:45vh}
.small{opacity:.8;font-size:.9rem}
</style></head><body>
<h1>Bot Futures — Painel</h1>

<div class="card">
  <div class="small">Serviços permitidos: <span id="allowed"></span></div>
</div>

<div class="card">
  <h3>Controle</h3>
  <div id="services"></div>
</div>

<div class="card">
  <h3>Logs</h3>
  <select id="svcSel"></select>
  <input id="lines" type="number" value="200" min="50" step="50" style="width:6rem">
  <button class="btn" id="btnLoadLogs">Carregar logs</button>
  <pre id="logs">Selecione um serviço e clique em Carregar logs…</pre>
</div>

<script>
async function j(u){ const r=await fetch(u,{credentials:'include'}); if(r.status===401){location.reload(); return null} if(!r.ok){alert('Erro '+r.status); return null} return r.json(); }
async function t(u){ const r=await fetch(u,{credentials:'include'}); if(r.status===401){location.reload(); return ''} return r.text(); }

function esc(s){ return String(s).replace(/[&<>"']/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m])); }

async function refresh(){
  const h = await j('/health'); if(!h) return;
  document.getElementById('allowed').textContent = h.services.join(', ');
  const data = await j('/api/services'); if(!data) return;

  const c = document.getElementById('services'); const sSel = document.getElementById('svcSel');
  c.innerHTML=''; sSel.innerHTML='';

  data.forEach(row=>{
    const card=document.createElement('div'); card.className='card';

    const title=document.createElement('b'); title.textContent=row.name;
    const badge=document.createElement('span'); badge.className='badge'; badge.textContent = row.active ? 'ativo' : 'inativo';
    card.appendChild(title); card.appendChild(document.createTextNode(' ')); card.appendChild(badge);

    if(row.error){ const small=document.createElement('span'); small.className='small'; small.textContent=' ('+row.error+')'; card.appendChild(document.createTextNode(' ')); card.appendChild(small); }

    const actions=document.createElement('div'); actions.style.marginTop='8px';

    const mkBtn=(label,action)=>{ const b=document.createElement('button'); b.className='btn'; b.textContent=label; b.addEventListener('click', async ()=>{ const r=await fetch('/api/'+action+'/'+encodeURIComponent(row.name), {method:'POST',credentials:'include'}); if(!r.ok){alert('Falhou: '+(await r.text()));} else { refresh(); } }); return b; };

    actions.appendChild(mkBtn('Start','start'));
    actions.appendChild(mkBtn('Stop','stop'));
    actions.appendChild(mkBtn('Restart','restart'));

    const bLogs=document.createElement('button'); bLogs.className='btn'; bLogs.textContent='Ver logs';
    bLogs.addEventListener('click', ()=>{ document.getElementById('svcSel').value = row.name; loadLogs(); });
    actions.appendChild(bLogs);

    card.appendChild(actions);
    c.appendChild(card);

    const opt=document.createElement('option'); opt.value=row.name; opt.textContent=row.name; sSel.appendChild(opt);
  });
}

async function loadLogs(){
  const name = document.getElementById('svcSel').value;
  const n = document.getElementById('lines').value;
  const text = await t('/api/logs/'+encodeURIComponent(name)+'?n='+n);
  document.getElementById('logs').textContent = text || '(vazio)';
}

document.getElementById('btnLoadLogs').addEventListener('click', loadLogs);

refresh();
</script>
</body></html>
"""

@app.get("/api/services")
def services(_: bool = Depends(auth)) -> List[Dict]:
    out = []
    for svc in ALLOWED:
        try:
            state = run(["sudo -n systemctl","is-active",svc])
            out.append({"name": svc, "active": (state.strip()=="active")})
        except Exception as e:
            out.append({"name": svc, "active": False, "error": str(e)})
    return out

@app.get("/api/logs/{service_name}", response_class=PlainTextResponse)
def svc_logs(service_name: str, n: int = Query(200, ge=50, le=2000), _: bool = Depends(auth)):
    svc = check_allowed(service_name)
    return run(["/usr/bin/journalctl","-u",svc,"--no-pager","-n",str(n),"--output","short-iso"], use_sudo=True)

@app.post("/api/start/{service_name}")
def svc_start(service_name: str, _: bool = Depends(auth)):
    svc = check_allowed(service_name)
    return {"ok": True, "out": run(["/usr/bin/sudo -n systemctl","start",svc], use_sudo=True)}

@app.post("/api/stop/{service_name}")
def svc_stop(service_name: str, _: bool = Depends(auth)):
    svc = check_allowed(service_name)
    return {"ok": True, "out": run(["/usr/bin/sudo -n systemctl","stop",svc], use_sudo=True)}

@app.post("/api/restart/{service_name}")
def svc_restart(service_name: str, _: bool = Depends(auth)):
    svc = check_allowed(service_name)
    return {"ok": True, "out": run(["/usr/bin/sudo -n systemctl","restart",svc], use_sudo=True)}
