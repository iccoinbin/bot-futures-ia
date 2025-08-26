from services import list_services, vm_status, list_services, service_start, service_stop
import shlex
import subprocess
import os
from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from .services import (
    check_db, fe_is_running, fe_start, fe_stop, logs_feature_engine,
    co_is_running, co_start, co_stop, logs_collector, recent_errors
)

app = FastAPI(title="bot-futures-ia | Web UI")
app.mount("/static", StaticFiles(directory="src/webui/static"), name="static")
templates = Jinja2Templates(directory="src/webui/templates")

security = HTTPBasic()
UI_USER = os.getenv("UI_USER", "admin")
UI_PASS = os.getenv("UI_PASS", "botfutures")

def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    if not (credentials.username == UI_USER and credentials.password == UI_PASS):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

@app.get("/", response_class=HTMLResponse, dependencies=[Depends(require_auth)])
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/status", dependencies=[Depends(require_auth)])
def api_status():
    db_ok, db_msg = check_db()
    return {
        "db": {"ok": db_ok, "info": db_msg},
        "feature_engine": {"running": fe_is_running(), "session": "fe_v1"},
        "collector": {"running": co_is_running(), "session": "collector_candles"},
    }

@app.post("/api/start/feature_engine", dependencies=[Depends(require_auth)])
def api_start_fe(): ok, msg = fe_start(); return {"ok": ok, "message": msg}

@app.post("/api/stop/feature_engine", dependencies=[Depends(require_auth)])
def api_stop_fe(): ok, msg = fe_stop(); return {"ok": ok, "message": msg}

@app.post("/api/start/collector", dependencies=[Depends(require_auth)])
def api_start_co(): ok, msg = co_start(); return {"ok": ok, "message": msg}

@app.post("/api/stop/collector", dependencies=[Depends(require_auth)])
def api_stop_co(): ok, msg = co_stop(); return {"ok": ok, "message": msg}

@app.get("/api/logs/feature_engine", dependencies=[Depends(require_auth)])
def api_logs_fe(n: int = 120): return logs_feature_engine(n)

@app.get("/api/logs/collector", dependencies=[Depends(require_auth)])
def api_logs_co(n: int = 120): return logs_collector(n)

@app.get("/api/errors", dependencies=[Depends(require_auth)])
def api_errors(): return recent_errors()


# ===== Rota de status da VM =====
from fastapi.responses import JSONResponse
try:
from services import vm_status, list_services, service_start, service_stop
    @app.get("/api/vm_status")
    def api_vm_status():
        return JSONResponse(vm_status())
except Exception as _e:
    pass
# =================================



# -- replaced /api/services --

@app.post("/api/service/{name}/{action}", dependencies=[Depends(require_auth)])
def api_service_action(name: str, action: str):
    if action not in ("start","stop"):
        raise HTTPException(status_code=400, detail="Ação inválida")
    try:
        ok,msg = (service_start(name) if action=="start" else service_stop(name))
        return {"ok": bool(ok), "message": msg}
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)

@app.get("/api/health", dependencies=[Depends(require_auth)])
def api_health():
    db_ok, _ = check_db()
    return {
        "db_ok": bool(db_ok),
        "feature_engine_running": fe_is_running(),
        "collector_running": co_is_running()
    }

@app.post("/api/services/{name}/start", dependencies=[Depends(require_auth)])
def api_service_start(name: str):
    ok, msg = service_start(name)
    return JSONResponse({"ok": ok, "message": msg})

@app.post("/api/services/{name}/stop", dependencies=[Depends(require_auth)])
def api_service_stop(name: str):
    ok, msg = service_stop(name)
    return JSONResponse({"ok": ok, "message": msg})

@app.get("/api/services", dependencies=[Depends(require_auth)])
def api_services():
    """
    Lista serviços descobertos (tmux/systemd) com proteção de erro.
    """
    try:
        from services import list_services as _list
    except Exception as e:
        return JSONResponse({"error": f"import failed: {type(e).__name__}: {e}"}, status_code=500)
    try:
        data = _list()
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=500)

