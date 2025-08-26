
import os, shlex, subprocess, time, shutil
try:
    import psutil  # opcional
except Exception:
    psutil = None

def tmux_has(name: str) -> bool:
    try:
        r = subprocess.run(
            ["bash","-lc", f"tmux has-session -t {shlex.quote(name)}"],
            capture_output=True
        )
        return r.returncode == 0
    except Exception:
        return False

def _systemd_items():
    items = []
    try:
        cmd = "sudo -n systemctl list-units --type=service --all --no-legend --plain"
        r = subprocess.run(["bash","-lc", cmd], capture_output=True, text=True, timeout=5)
        for ln in (r.stdout or "").splitlines():
            parts = ln.split()
            if not parts:
                continue
            name = parts[0]
            active = ("running" in ln) or ("active (running)" in ln)
            if any(k in name for k in ("webui", "feature", "collector", "botfutures")):
                items.append({"name": name, "running": active, "kind": "systemd", "description": ""})
    except Exception:
        pass
    return items

def vm_status():
    try:
        boot = psutil.boot_time() if (psutil and hasattr(psutil, "boot_time")) else None
    except Exception:
        boot = None
    now = time.time()
    uptime = int(now - (boot or now))

    try:
        l1, l5, l15 = os.getloadavg()
    except Exception:
        l1 = l5 = l15 = 0.0

    try:
        cpu = float(psutil.cpu_percent(interval=0.1)) if psutil else 0.0
    except Exception:
        cpu = 0.0

    try:
        if psutil and hasattr(psutil, "virtual_memory"):
            mem = psutil.virtual_memory()
            memory = {"total": int(mem.total), "used": int(mem.used), "free": int(mem.available), "percent": float(mem.percent)}
        else:
            memory = {"total": 0, "used": 0, "free": 0, "percent": 0.0}
    except Exception:
        memory = {"total": 0, "used": 0, "free": 0, "percent": 0.0}

    try:
        du = shutil.disk_usage("/")
        disk = {"total": int(du.total), "used": int(du.used), "free": int(du.free),
                "percent": round((du.used/du.total)*100, 1) if du.total else 0.0, "mount": "/"}
    except Exception:
        disk = {"total": 0, "used": 0, "free": 0, "percent": 0.0, "mount": "/"}

    return {"uptime": uptime, "load": {"1": l1, "5": l5, "15": l15}, "cpu_percent": cpu, "memory": memory, "disk": disk}

def list_services():
    items = []
    # tmux conhecidos
    known_tmux = ("fe_v1", "feature_engine", "feature-engine", "collector_candles", "collector", "webui")
    for n in known_tmux:
        if tmux_has(n):
            items.append({"name": n, "running": True, "kind": "tmux", "description": ""})
    # systemd relevantes
    exist = {i["name"] for i in items}
    for it in _systemd_items():
        if it["name"] not in exist:
            items.append(it)
    return {"items": items, "vm_status": vm_status()}

def service_start(name: str):
    if tmux_has(name):
        return True, "tmux: já em execução"
    try:
        r = subprocess.run(["bash","-lc", f"sudo sudo -n systemctl start {shlex.quote(name)}"], capture_output=True, text=True, timeout=10)
        return (r.returncode == 0), (r.stdout.strip() or r.stderr.strip() or "systemd start executado")
    except Exception as e:
        return False, str(e)

def service_stop(name: str):
    try:
        r = subprocess.run(["bash","-lc", f"sudo sudo -n systemctl stop {shlex.quote(name)}"], capture_output=True, text=True, timeout=10)
        return (r.returncode == 0), (r.stdout.strip() or r.stderr.strip() or "systemd stop executado")
    except Exception as e:
        return False, str(e)
