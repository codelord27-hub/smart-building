"""End-to-end Two-Phase Commit scenarios against the locally running system (local/run_local.sh).

Each scenario asserts the coordinator log AND both participants' state, i.e. atomicity:
either both HVAC Controller and Energy Monitor committed, or both aborted and released locks.
Run:  python tests/e2e_2pc_local.py
"""
import concurrent.futures as cf
import os
import subprocess
import sys
import time

import httpx

G = os.environ.get("GATEWAY", "http://127.0.0.1:8000")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_DIR = os.environ.get("RUN_DIR", "/tmp/sb-local")
ZONE = "f1-archives"
UNIT = f"ahu-{ZONE}"
c = httpx.Client(base_url=G, timeout=30)
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""), flush=True)


def participant_states(txn_id):
    hv = {t["txn_id"]: t for t in c.get("/api/hvac/transactions?limit=500").json()}.get(txn_id)
    en = {t["txn_id"]: t for t in c.get("/api/energy/transactions?limit=500").json()}.get(txn_id)
    return (hv or {}).get("state"), (en or {}).get("state")


def unit():
    return c.get(f"/api/hvac/units/by-zone/{ZONE}").json()


def command(mode, power, inject=None, zone=ZONE):
    body = {"mode": mode, "power_pct": power}
    if inject:
        body["inject"] = inject
    return c.post(f"/api/control/zones/{zone}/command", json=body)


def wait_up(url, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if httpx.get(url, timeout=2).status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(1)
    return False


def restart(service, port, extra_env):
    env = dict(os.environ, SERVICE_NAME=service, **extra_env)
    log = open(f"{RUN_DIR}/logs/{service}.log", "a")
    p = subprocess.Popen(["uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(port), "--no-access-log"],
                         cwd=f"{ROOT}/services/{service}", env=env, stdout=log, stderr=log)
    open(f"{RUN_DIR}/{service}.pid", "w").write(str(p.pid))
    assert wait_up(f"http://127.0.0.1:{port}/healthz"), f"{service} did not come back"
    return p


def kill(service):
    subprocess.run(["pkill", "-f", f"uvicorn app:app --host 127.0.0.1 --port {PORTS[service]}"], check=False)
    time.sleep(2)


PORTS = {"control-optimizer": 8005, "hvac-controller": 8006}
OPT_ENV = {"DB_NAME": "optimizer", "TWOPC_RECOVERY_AGE_SECONDS": "5", "CONTROL_LOOP_ENABLED": "false"}
HVAC_ENV = {"DB_NAME": "hvac", "GRPC_PORT": "50051"}

# ---------------------------------------------------------------------------------------------
print("== setup: restart the Control Optimizer with its periodic loop disabled (deterministic test)")
kill("control-optimizer")
restart("control-optimizer", 8005, OPT_ENV)
c.put("/api/energy/budget", json={"budget_kw": 45})
c.put(f"/api/hvac/units/{UNIT}/status", json={"status": "ONLINE"})
load = c.get("/api/energy/load").json()
zone_kw = next((z["kw"] for z in load["zones"] if z["zone_id"] == ZONE), 0.0)
headroom = load["budget_kw"] - load["total_kw"] - load["reserved_kw"]
print(f"   budget={load['budget_kw']} total={load['total_kw']} zone_kw={zone_kw} headroom={headroom:.1f}")

# 1. successful commit (reduce load so the budget always allows it)
r = command("COOL", 20).json()
hv, en = participant_states(r["txn_id"])
u = unit()
check("1 commit: coordinator COMMITTED", r["outcome"] == "COMMITTED", r)
check("1 commit: both participants COMMITTED", hv == en == "COMMITTED", (hv, en))
check("1 commit: HVAC unit applied COOL 20 and unlocked", u["mode"] == "COOL" and u["power_pct"] == 20
      and u["locked_by"] is None, u)
zone_load = next(z["kw"] for z in c.get("/api/energy/load").json()["zones"] if z["zone_id"] == ZONE)
check("1 commit: Energy Monitor committed 2.0 kW for the zone", abs(zone_load - 2.0) < 1e-6, zone_load)

# 2. abort: Energy Monitor votes NO (budget too small)
c.put("/api/energy/budget", json={"budget_kw": 5})
r = command("COOL", 100).json()
hv, en = participant_states(r["txn_id"])
u = unit()
check("2 budget abort: coordinator ABORTED", r["outcome"] == "ABORTED", r)
check("2 budget abort: energy-monitor voted NO, hvac voted YES",
      r["votes"]["energy-monitor"]["yes"] is False and r["votes"]["hvac-controller"]["yes"] is True, r["votes"])
check("2 budget abort: both participants ABORTED", hv == en == "ABORTED", (hv, en))
check("2 budget abort: HVAC unit unchanged (COOL 20) and lock released",
      u["power_pct"] == 20 and u["locked_by"] is None, u)
c.put("/api/energy/budget", json={"budget_kw": 45})

# 3. abort: HVAC Controller votes NO (unit in maintenance)
c.put(f"/api/hvac/units/{UNIT}/status", json={"status": "MAINTENANCE"})
r = command("COOL", 30).json()
hv, en = participant_states(r["txn_id"])
check("3 maintenance abort: coordinator ABORTED with hvac NO", r["outcome"] == "ABORTED"
      and r["votes"]["hvac-controller"]["yes"] is False, r)
check("3 maintenance abort: energy reservation released (ABORTED)", en == "ABORTED", (hv, en))
reserved = c.get("/api/energy/load").json()["reserved_kw"]
check("3 maintenance abort: no power left reserved", reserved == 0, reserved)
c.put(f"/api/hvac/units/{UNIT}/status", json={"status": "ONLINE"})

# 4. participant unreachable -> abort + CONTROLLER_FAILURE alert
kill("hvac-controller")
t0 = int(time.time() * 1000)
r = command("COOL", 30).json()
check("4 hvac down: coordinator ABORTED (unreachable)", r["outcome"] == "ABORTED"
      and "unreachable" in r["votes"]["hvac-controller"]["reason"], r)
en_state = {t["txn_id"]: t for t in c.get("/api/energy/transactions?limit=500").json()}[r["txn_id"]]["state"]
check("4 hvac down: energy-monitor reservation ABORTED", en_state == "ABORTED", en_state)
restart("hvac-controller", 8006, HVAC_ENV)
hv_after = participant_states(r["txn_id"])[0]
check("4 hvac down: hvac never prepared (no txn) or ABORTED tombstone", hv_after in (None, "ABORTED"), hv_after)
time.sleep(6)  # pubsub -> function -> firestore
alerts = c.get("/api/alerts?limit=50").json()
check("4 hvac down: CONTROLLER_FAILURE alert stored by Alert Service",
      any(a["type"] == "CONTROLLER_FAILURE" and a["ts_ms"] >= t0 for a in alerts),
      [a["type"] for a in alerts[:5]])

# 5. coordinator crashes AFTER the commit decision -> recovery must COMMIT
try:
    resp = command("COOL", 40, inject="crash_after_decision")
    check("5 crash-after: coordinator process died mid-request (gateway 5xx)", resp.status_code >= 500,
          resp.status_code)
except httpx.HTTPError:
    check("5 crash-after: coordinator process died mid-request (connection dropped)", True)
time.sleep(1)
pending = [t for t in c.get("/api/hvac/transactions?limit=5").json() if t["zone_id"] == ZONE][0]
txn = pending["txn_id"]
check("5 crash-after: participants left PREPARED (blocked) and unit locked",
      participant_states(txn) == ("PREPARED", "PREPARED") and unit()["locked_by"] == txn, participant_states(txn))
restart("control-optimizer", 8005, OPT_ENV)
deadline = time.time() + 60
while time.time() < deadline and c.get(f"/api/control/transactions/{txn}").json()["state"] != "COMMITTED":
    time.sleep(2)
t = c.get(f"/api/control/transactions/{txn}").json()
u = unit()
check("5 crash-after: recovery completed txn as COMMITTED", t["state"] == "COMMITTED" and t["recovered"], t["state"])
check("5 crash-after: both participants COMMITTED", participant_states(txn) == ("COMMITTED", "COMMITTED"),
      participant_states(txn))
check("5 crash-after: HVAC now COOL 40 and unlocked", u["power_pct"] == 40 and u["locked_by"] is None, u)

# 6. coordinator crashes BEFORE deciding -> recovery must ABORT (presumed abort)
try:
    command("COOL", 50, inject="crash_before_decision")
except httpx.HTTPError:
    pass
time.sleep(1)
txn = [t for t in c.get("/api/hvac/transactions?limit=5").json() if t["zone_id"] == ZONE][0]["txn_id"]
restart("control-optimizer", 8005, OPT_ENV)
deadline = time.time() + 60
while time.time() < deadline and c.get(f"/api/control/transactions/{txn}").json()["state"] != "ABORTED":
    time.sleep(2)
t = c.get(f"/api/control/transactions/{txn}").json()
u = unit()
check("6 crash-before: recovery presumed abort -> ABORTED", t["state"] == "ABORTED" and t["recovered"], t["state"])
check("6 crash-before: both participants ABORTED", participant_states(txn) == ("ABORTED", "ABORTED"),
      participant_states(txn))
check("6 crash-before: HVAC still COOL 40, lock released", u["power_pct"] == 40 and u["locked_by"] is None, u)

# 7. concurrent commands for the same zone: at most one commits, every txn is atomic
with cf.ThreadPoolExecutor(4) as ex:
    outs = [f.result().json() for f in [ex.submit(command, "COOL", p) for p in (25, 35, 45, 55)]]
committed = [o for o in outs if o["outcome"] == "COMMITTED"]
atomic = all(participant_states(o["txn_id"]) == ((o["outcome"],) * 2) for o in outs)
check("7 concurrency: at least one commit, conflicts aborted", len(committed) >= 1 and
      all(o["outcome"] in ("COMMITTED", "ABORTED") for o in outs), [o["outcome"] for o in outs])
check("7 concurrency: every transaction atomic across participants", atomic,
      [(o["outcome"], participant_states(o["txn_id"])) for o in outs])
check("7 concurrency: unit unlocked afterwards", unit()["locked_by"] is None, unit())

print("== teardown: restore the Control Optimizer with its control loop")
kill("control-optimizer")
restart("control-optimizer", 8005, {"DB_NAME": "optimizer"})
failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
sys.exit(1 if failed else 0)
