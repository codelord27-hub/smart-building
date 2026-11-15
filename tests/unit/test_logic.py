"""Fast unit tests (no infrastructure needed): thermal model / MPC, fair-share budget allocation,
upload parsing and alert normalisation. Run: pytest tests/unit -q"""
import base64
import importlib.util
import json
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "common"))

from sbcommon import thermal  # noqa: E402


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_thermal_step_directions():
    assert thermal.step(22, 32, 0, "OFF", 0) > 22          # warms towards outside air
    assert thermal.step(26, 32, 0, "COOL", 100) < 26       # cooling wins at full power
    assert thermal.step(18, 10, 0, "HEAT", 100) > 18


def test_mpc_cools_hot_zone_and_idles_in_band():
    hot = thermal.mpc_choose(27, 22, 0.5, 10, 32, 10)
    assert hot["mode"] == "COOL" and hot["power_pct"] >= 50
    cold_winter = thermal.mpc_choose(16, 22, 0.5, 5, 5, 10)
    assert cold_winter["mode"] == "HEAT"
    assert 0 <= hot["power_kw"] <= 10


def test_fair_share_scales_requests_to_budget(monkeypatch):
    monkeypatch.setenv("CONTROL_LOOP_ENABLED", "false")
    sys.path.insert(0, str(ROOT / "services" / "control-optimizer"))
    app = _load("optimizer_app", ROOT / "services" / "control-optimizer" / "app.py")
    plans = [{"rated_kw": 10.0, "best": {"mode": "COOL", "power_pct": 100, "power_kw": 10.0}} for _ in range(6)]
    app._fair_share(plans, budget_kw=45, fixed_kw=0)
    total = sum(p["best"]["power_kw"] for p in plans)
    assert total <= 45 * app.BUDGET_USE + 1e-9
    assert all(p["best"]["power_pct"] == plans[0]["best"]["power_pct"] for p in plans)   # equal share
    small = [{"rated_kw": 10.0, "best": {"mode": "COOL", "power_pct": 30, "power_kw": 3.0}}]
    app._fair_share(small, budget_kw=45, fixed_kw=0)
    assert small[0]["best"]["power_pct"] == 30                                             # untouched


def test_upload_parser_accepts_csv_and_ndjson():
    ing = _load("ingestion_app", ROOT / "services" / "sensor-ingestion" / "app.py")
    csv_bytes = b"sensor_id,zone_id,temperature_c,occupancy_count\ns1,f0-hall,22.5,3\nbad,BAD ZONE,1,1\n"
    ok, bad = ing._parse_upload(csv_bytes, "a.csv")
    assert len(ok) == 1 and bad == 1 and ok[0].temperature_c == 22.5
    nd = b'{"sensor_id":"s1","zone_id":"f0-hall","occupancy_count":4}\n\n'
    ok, bad = ing._parse_upload(nd, "a.ndjson")
    assert len(ok) == 1 and bad == 0 and ok[0].occupancy_count == 4


def test_alert_normalise():
    fn = _load("alert_fn", ROOT / "functions" / "alert-service" / "main.py")
    a = fn.normalise({"type": "abnormal_temperature", "severity": "loud", "message": "x" * 2000, "ts_ms": 5},
                     "m-1", {"source": "occupancy-service"})
    assert a["type"] == "ABNORMAL_TEMPERATURE" and a["severity"] == "WARNING"
    assert len(a["message"]) == 1000 and a["source"] == "occupancy-service" and a["message_id"] == "m-1"
    assert json.loads(base64.b64decode(base64.b64encode(json.dumps(a).encode())))["ts_ms"] == 5


def test_twopc_proto_is_valid():
    # the proto must compile (same command the Dockerfile runs)
    from grpc_tools import protoc
    out = ROOT / "build" / "proto-test"
    out.mkdir(parents=True, exist_ok=True)
    rc = protoc.main(["protoc", f"-I{ROOT / 'proto'}", f"--python_out={out}", f"--grpc_python_out={out}",
                      str(ROOT / "proto" / "sbproto" / "twopc.proto")])
    assert rc == 0 and (out / "sbproto" / "twopc_pb2_grpc.py").exists()

