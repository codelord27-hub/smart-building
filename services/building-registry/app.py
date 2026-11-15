"""Building Registry - records buildings, floors and zones (Cloud SQL / Postgres).

Public entry point of the system: it serves the operator dashboard at `/` and the
REST API for the building model. Also exposes recent alerts (written to Firestore by
the Alert Service Cloud Function).
"""
import logging
import pathlib
from contextlib import asynccontextmanager

from fastapi import HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sbcommon.db import create_pool, migrate
from sbcommon.logs import setup_logging
from sbcommon.web import create_app

log = setup_logging()

DDL = """
CREATE TABLE IF NOT EXISTS buildings (
    building_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    address     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS floors (
    floor_id    TEXT PRIMARY KEY,
    building_id TEXT NOT NULL REFERENCES buildings(building_id) ON DELETE CASCADE,
    level       INT  NOT NULL,
    name        TEXT NOT NULL,
    UNIQUE (building_id, level)
);
CREATE TABLE IF NOT EXISTS zones (
    zone_id       TEXT PRIMARY KEY,
    floor_id      TEXT NOT NULL REFERENCES floors(floor_id) ON DELETE CASCADE,
    building_id   TEXT NOT NULL REFERENCES buildings(building_id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    area_m2       DOUBLE PRECISION NOT NULL DEFAULT 50,
    target_temp_c DOUBLE PRECISION NOT NULL DEFAULT 22,
    min_temp_c    DOUBLE PRECISION NOT NULL DEFAULT 16,
    max_temp_c    DOUBLE PRECISION NOT NULL DEFAULT 28,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (min_temp_c < max_temp_c)
);
CREATE INDEX IF NOT EXISTS zones_building_idx ON zones(building_id);
"""

pool = None


@asynccontextmanager
async def lifespan(_app):
    global pool
    pool = create_pool()
    migrate(pool, DDL)
    log.info("building registry ready")
    yield
    pool.close()


app = create_app("Building Registry", "/api/registry", lifespan=lifespan)


class BuildingIn(BaseModel):
    building_id: str = Field(pattern=r"^[a-z0-9-]{3,40}$")
    name: str = Field(min_length=1, max_length=120)
    address: str | None = None


class FloorIn(BaseModel):
    floor_id: str = Field(pattern=r"^[a-z0-9-]{3,60}$")
    level: int = Field(ge=-5, le=200)
    name: str = Field(min_length=1, max_length=120)


class ZoneIn(BaseModel):
    zone_id: str = Field(pattern=r"^[a-z0-9-]{3,60}$")
    name: str = Field(min_length=1, max_length=120)
    area_m2: float = Field(default=50, gt=0, le=100000)
    target_temp_c: float = Field(default=22, ge=10, le=35)
    min_temp_c: float = Field(default=16, ge=-10, le=40)
    max_temp_c: float = Field(default=28, ge=0, le=50)


class ZonePatch(BaseModel):
    name: str | None = None
    target_temp_c: float | None = Field(default=None, ge=10, le=35)
    min_temp_c: float | None = Field(default=None, ge=-10, le=40)
    max_temp_c: float | None = Field(default=None, ge=0, le=50)


def _q(sql, params=(), one=False, commit=False):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else None
        if commit:
            conn.commit()
    if rows is None:
        return None
    return (rows[0] if rows else None) if one else rows


@app.post("/api/buildings", status_code=201)
def create_building(b: BuildingIn):
    row = _q("INSERT INTO buildings(building_id,name,address) VALUES (%s,%s,%s) "
             "ON CONFLICT (building_id) DO NOTHING RETURNING *", (b.building_id, b.name, b.address),
             one=True, commit=True)
    if row is None:
        raise HTTPException(409, "building already exists")
    log.info("building created", extra={"building_id": b.building_id})
    return row


@app.get("/api/buildings")
def list_buildings():
    return _q("SELECT b.*, (SELECT count(*) FROM zones z WHERE z.building_id=b.building_id) AS zone_count "
              "FROM buildings b ORDER BY building_id")


@app.get("/api/buildings/{building_id}")
def get_building(building_id: str):
    b = _q("SELECT * FROM buildings WHERE building_id=%s", (building_id,), one=True)
    if not b:
        raise HTTPException(404, "building not found")
    floors = _q("SELECT * FROM floors WHERE building_id=%s ORDER BY level", (building_id,))
    zones = _q("SELECT * FROM zones WHERE building_id=%s ORDER BY zone_id", (building_id,))
    for f in floors:
        f["zones"] = [z for z in zones if z["floor_id"] == f["floor_id"]]
    b["floors"] = floors
    return b


@app.post("/api/buildings/{building_id}/floors", status_code=201)
def create_floor(building_id: str, f: FloorIn):
    if not _q("SELECT 1 FROM buildings WHERE building_id=%s", (building_id,), one=True):
        raise HTTPException(404, "building not found")
    row = _q("INSERT INTO floors(floor_id,building_id,level,name) VALUES (%s,%s,%s,%s) "
             "ON CONFLICT DO NOTHING RETURNING *", (f.floor_id, building_id, f.level, f.name),
             one=True, commit=True)
    if row is None:
        raise HTTPException(409, "floor already exists")
    return row


@app.post("/api/floors/{floor_id}/zones", status_code=201)
def create_zone(floor_id: str, z: ZoneIn):
    if z.min_temp_c >= z.max_temp_c:
        raise HTTPException(422, "min_temp_c must be lower than max_temp_c")
    floor = _q("SELECT * FROM floors WHERE floor_id=%s", (floor_id,), one=True)
    if not floor:
        raise HTTPException(404, "floor not found")
    row = _q("INSERT INTO zones(zone_id,floor_id,building_id,name,area_m2,target_temp_c,min_temp_c,max_temp_c) "
             "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING *",
             (z.zone_id, floor_id, floor["building_id"], z.name, z.area_m2, z.target_temp_c,
              z.min_temp_c, z.max_temp_c), one=True, commit=True)
    if row is None:
        raise HTTPException(409, "zone already exists")
    log.info("zone created", extra={"zone_id": z.zone_id})
    return row


@app.get("/api/zones")
def list_zones(building_id: str | None = None):
    if building_id:
        return _q("SELECT * FROM zones WHERE building_id=%s ORDER BY zone_id", (building_id,))
    return _q("SELECT * FROM zones ORDER BY zone_id")


@app.get("/api/zones/{zone_id}")
def get_zone(zone_id: str):
    z = _q("SELECT * FROM zones WHERE zone_id=%s", (zone_id,), one=True)
    if not z:
        raise HTTPException(404, "zone not found")
    return z


@app.patch("/api/zones/{zone_id}")
def patch_zone(zone_id: str, p: ZonePatch):
    z = get_zone(zone_id)
    merged = {**z, **{k: v for k, v in p.model_dump().items() if v is not None}}
    if merged["min_temp_c"] >= merged["max_temp_c"]:
        raise HTTPException(422, "min_temp_c must be lower than max_temp_c")
    row = _q("UPDATE zones SET name=%s,target_temp_c=%s,min_temp_c=%s,max_temp_c=%s WHERE zone_id=%s RETURNING *",
             (merged["name"], merged["target_temp_c"], merged["min_temp_c"], merged["max_temp_c"], zone_id),
             one=True, commit=True)
    log.info("zone updated", extra={"zone_id": zone_id, "target_temp_c": row["target_temp_c"]})
    return row


DEMO = {
    "building_id": "bits-library",
    "name": "BITS Pilani - Central Library",
    "address": "Vidya Vihar, Pilani, Rajasthan",
    "floors": [
        (0, "Ground Floor", [("reading-hall", "Reading Hall", 22), ("reception", "Reception", 23),
                             ("server-room", "Server Room", 20)]),
        (1, "First Floor", [("study-pods", "Study Pods", 22), ("seminar-room", "Seminar Room", 22),
                            ("archives", "Archives", 21)]),
    ],
}


@app.post("/api/admin/seed")
def seed_demo_building():
    """Idempotently creates the demo building (2 floors x 3 zones)."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO buildings(building_id,name,address) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                        (DEMO["building_id"], DEMO["name"], DEMO["address"]))
            for level, fname, zones in DEMO["floors"]:
                floor_id = f"{DEMO['building_id']}-f{level}"
                cur.execute("INSERT INTO floors(floor_id,building_id,level,name) VALUES (%s,%s,%s,%s) "
                            "ON CONFLICT DO NOTHING", (floor_id, DEMO["building_id"], level, fname))
                for slug, zname, target in zones:
                    cur.execute("INSERT INTO zones(zone_id,floor_id,building_id,name,target_temp_c) "
                                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                (f"f{level}-{slug}", floor_id, DEMO["building_id"], zname, target))
        conn.commit()
    return get_building(DEMO["building_id"])


@app.get("/api/alerts")
def recent_alerts(limit: int = Query(30, ge=1, le=200)):
    """Alerts written to Firestore by the Alert Service (serverless function)."""
    from google.cloud import firestore

    from sbcommon.gcp import firestore_client

    docs = (firestore_client().collection("alerts")
            .order_by("ts_ms", direction=firestore.Query.DESCENDING).limit(limit).stream())
    out = []
    for d in docs:
        item = d.to_dict()
        item["id"] = d.id
        item.pop("received_at", None)
        out.append(item)
    return out


_DASHBOARD = pathlib.Path(__file__).with_name("dashboard.html")


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    return HTMLResponse(_DASHBOARD.read_text())
