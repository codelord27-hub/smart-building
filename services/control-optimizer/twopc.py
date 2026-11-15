"""Two-Phase Commit coordinator.

Durable coordinator log (Cloud SQL table `twopc_log`) states:

    PREPARING --all YES--> COMMITTING --all acks--> COMMITTED
        |                                     (decision is durable once COMMITTING is written)
        +--any NO / timeout / error--> ABORTING --all acks--> ABORTED

Recovery (after a coordinator crash / pod restart):
    PREPARING  -> presumed abort: send Abort to all participants -> ABORTED
    COMMITTING -> decision already taken: re-send Commit until acked -> COMMITTED
    ABORTING   -> re-send Abort -> ABORTED
Participant RPCs are idempotent, so re-sending is always safe.
"""
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc
from prometheus_client import Counter, Histogram

from sbcommon.config import POD_NAME
from sbproto import twopc_pb2, twopc_pb2_grpc

log = logging.getLogger("twopc")

TXNS = Counter("twopc_transactions_total", "2PC transactions by final outcome", ["outcome"])
VOTES = Counter("twopc_votes_total", "Votes received in the prepare phase", ["participant", "vote"])
DURATION = Histogram("twopc_duration_seconds", "End-to-end 2PC transaction latency",
                     buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
RECOVERED = Counter("twopc_recovered_total", "Transactions completed by the recovery process", ["from_state"])

DDL = """
CREATE TABLE IF NOT EXISTS twopc_log (
    txn_id        TEXT PRIMARY KEY,
    zone_id       TEXT NOT NULL,
    command       JSONB NOT NULL,
    state         TEXT NOT NULL,
    votes         JSONB NOT NULL DEFAULT '{}'::jsonb,
    acks          JSONB NOT NULL DEFAULT '{}'::jsonb,
    reason        TEXT,
    initiator     TEXT,
    coordinator   TEXT,
    recovered     BOOLEAN NOT NULL DEFAULT false,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS twopc_log_state_idx ON twopc_log(state);
CREATE INDEX IF NOT EXISTS twopc_log_created_idx ON twopc_log(created_at DESC);
"""

TERMINAL = ("COMMITTED", "ABORTED")


class Coordinator:
    def __init__(self, pool, targets: dict[str, str], alerts=None, prepare_timeout_s: float = 3.0,
                 decision_timeout_s: float = 3.0):
        self.pool = pool
        self.alerts = alerts
        self.prepare_timeout_s = prepare_timeout_s
        self.decision_timeout_s = decision_timeout_s
        self._channels = {name: grpc.insecure_channel(t) for name, t in targets.items()}
        self.stubs = {name: twopc_pb2_grpc.ParticipantStub(ch) for name, ch in self._channels.items()}
        self._executor = ThreadPoolExecutor(max_workers=8 * max(1, len(targets)))

    # ---------------------------------------------------------------- log helpers
    def _write(self, sql, params):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone() if cur.description else None
            conn.commit()
        return row

    def _set_state(self, txn_id, state, **fields):
        sets = ["state=%s", "updated_at=now()"]
        params = [state]
        for k, v in fields.items():
            sets.append(f"{k}=%s")
            params.append(json.dumps(v) if k in ("votes", "acks") else v)
        if state in TERMINAL:
            sets.append("finished_at=now()")
        params.append(txn_id)
        self._write(f"UPDATE twopc_log SET {', '.join(sets)} WHERE txn_id=%s", params)

    def _cas(self, txn_id, from_state, to_state, **fields) -> bool:
        """Compare-and-set state transition. Guarantees a single decision per transaction even if
        the recovery process and a (slow) live coordinator race on the same transaction."""
        sets = ["state=%s", "updated_at=now()"]
        params = [to_state]
        for k, v in fields.items():
            sets.append(f"{k}=%s")
            params.append(json.dumps(v) if k in ("votes", "acks") else v)
        if to_state in TERMINAL:
            sets.append("finished_at=now()")
        params += [txn_id, from_state]
        row = self._write(f"UPDATE twopc_log SET {', '.join(sets)} WHERE txn_id=%s AND state=%s RETURNING txn_id",
                          params)
        return row is not None

    def get(self, txn_id):
        return self._write("SELECT * FROM twopc_log WHERE txn_id=%s", (txn_id,))

    # ---------------------------------------------------------------- protocol
    def _prepare_one(self, name, req):
        try:
            vote = self.stubs[name].Prepare(req, timeout=self.prepare_timeout_s)
            return name, {"yes": vote.yes, "reason": vote.reason}
        except grpc.RpcError as exc:
            return name, {"yes": False, "reason": f"unreachable: {exc.code().name}", "unreachable": True}

    def _decide_one(self, name, txn_id, commit: bool, attempts: int = 3):
        req = twopc_pb2.DecisionRequest(txn_id=txn_id)
        last = None
        for attempt in range(attempts):
            try:
                rpc = self.stubs[name].Commit if commit else self.stubs[name].Abort
                ack = rpc(req, timeout=self.decision_timeout_s)
                return name, ack.state
            except grpc.RpcError as exc:
                last = exc.code().name
                time.sleep(0.2 * (attempt + 1))
        return name, f"FAILED:{last}"

    def _broadcast_decision(self, txn_id, commit: bool, attempts: int = 3) -> tuple[bool, dict]:
        futures = [self._executor.submit(self._decide_one, n, txn_id, commit, attempts) for n in self.stubs]
        acks = dict(f.result() for f in futures)
        expected = "COMMITTED" if commit else "ABORTED"
        return all(v == expected for v in acks.values()), acks

    def execute(self, zone_id: str, mode: str, power_pct: int, power_kw: float, initiator: str,
                inject: str | None = None) -> dict:
        txn_id = f"txn-{uuid.uuid4().hex[:16]}"
        started = time.perf_counter()
        command = {"mode": mode, "power_pct": int(power_pct), "power_kw": float(power_kw)}
        self._write("INSERT INTO twopc_log(txn_id,zone_id,command,state,initiator,coordinator) "
                    "VALUES (%s,%s,%s,'PREPARING',%s,%s)",
                    (txn_id, zone_id, json.dumps(command), initiator, POD_NAME))
        log.info("2PC begin", extra={"txn_id": txn_id, "zone_id": zone_id, **command, "initiator": initiator})

        # ---- phase 1: PREPARE (parallel)
        req = twopc_pb2.PrepareRequest(txn_id=txn_id, zone_id=zone_id, mode=mode, power_pct=int(power_pct),
                                       power_kw=float(power_kw), requested_at_ms=int(time.time() * 1000),
                                       coordinator=POD_NAME)
        futures = [self._executor.submit(self._prepare_one, n, req) for n in self.stubs]
        votes = dict(f.result() for f in futures)
        for name, v in votes.items():
            VOTES.labels(name, "yes" if v["yes"] else "no").inc()
        all_yes = all(v["yes"] for v in votes.values())

        if inject == "crash_before_decision":
            self._set_state(txn_id, "PREPARING", votes=votes)
            log.critical("FAULT INJECTION: coordinator crashing before decision", extra={"txn_id": txn_id})
            os._exit(1)

        # ---- decision (durable before phase 2). If recovery already presumed-aborted this
        # transaction (coordinator too slow), the CAS fails and we must abort as well.
        if all_yes and not self._cas(txn_id, "PREPARING", "COMMITTING", votes=votes):
            all_yes = False
            votes["coordinator"] = {"yes": False, "reason": "transaction already aborted by recovery"}
        if all_yes:
            if inject == "crash_after_decision":
                log.critical("FAULT INJECTION: coordinator crashing after COMMIT decision", extra={"txn_id": txn_id})
                os._exit(1)
            ok, acks = self._broadcast_decision(txn_id, commit=True)
            outcome = "COMMITTED"   # the decision is final once COMMITTING is durable
            if ok:
                state = "COMMITTED"
                self._set_state(txn_id, state, acks=acks)
            else:  # stays COMMITTING; the recovery loop keeps re-sending Commit until acknowledged
                state = "COMMITTING"
                self._set_state(txn_id, state, acks=acks, reason="commit not yet acknowledged by all participants")
            reason = None
        else:
            reason = "; ".join(f"{n}: {v['reason']}" for n, v in votes.items() if not v["yes"])
            if not self._cas(txn_id, "PREPARING", "ABORTING", votes=votes, reason=reason):
                self._set_state(txn_id, "ABORTING", votes=votes, reason=reason)
            ok, acks = self._broadcast_decision(txn_id, commit=False)
            outcome = "ABORTED"
            state = "ABORTED" if ok else "ABORTING"   # ABORTING: recovery re-sends Abort to the unreachable one
            self._set_state(txn_id, state, acks=acks, reason=reason)
            self._raise_alerts(txn_id, zone_id, votes, reason)

        elapsed = time.perf_counter() - started
        DURATION.observe(elapsed)
        TXNS.labels(outcome).inc()
        log.info("2PC end", extra={"txn_id": txn_id, "zone_id": zone_id, "outcome": outcome, "state": state,
                                   "duration_ms": round(elapsed * 1000, 1), "reason": reason})
        return {"txn_id": txn_id, "zone_id": zone_id, "command": command, "outcome": outcome, "state": state,
                "votes": votes, "acks": acks, "reason": reason, "duration_ms": round(elapsed * 1000, 1)}

    def _raise_alerts(self, txn_id, zone_id, votes, reason):
        if not self.alerts:
            return
        failed = [n for n, v in votes.items() if v.get("unreachable") or "FAULT" in v.get("reason", "")]
        if failed:
            self.alerts.publish("CONTROLLER_FAILURE", "CRITICAL",
                                f"Participant(s) {', '.join(failed)} failed during 2PC for zone {zone_id}",
                                zone_id=zone_id, details={"txn_id": txn_id, "votes": votes},
                                dedupe_key=f"CONTROLLER_FAILURE:{zone_id}:{','.join(failed)}")
        else:
            self.alerts.publish("CONTROL_TXN_ABORTED", "WARNING",
                                f"Setpoint change for zone {zone_id} aborted: {reason}",
                                zone_id=zone_id, details={"txn_id": txn_id, "votes": votes})

    # ---------------------------------------------------------------- recovery
    def recover(self, min_age_s: int = 20) -> list[dict]:
        """Complete transactions left unfinished by a crashed coordinator."""
        done = []
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT txn_id, state FROM twopc_log WHERE state IN ('PREPARING','COMMITTING','ABORTING') "
                            "AND updated_at < now() - make_interval(secs => %s) ORDER BY created_at LIMIT 50",
                            (min_age_s,))
                pending = cur.fetchall()
            conn.commit()
        for t in pending:
            txn_id, state = t["txn_id"], t["state"]
            if state == "PREPARING":
                # presumed abort: atomically take the ABORT decision (fails if coordinator decided meanwhile)
                if not self._cas(txn_id, "PREPARING", "ABORTING", recovered=True,
                                 reason="recovered: presumed abort (coordinator failed before decision)"):
                    continue
                state_now = "ABORTING"
            else:
                state_now = state
            commit = state_now == "COMMITTING"
            ok, acks = self._broadcast_decision(txn_id, commit=commit, attempts=2)
            final = ("COMMITTED" if commit else "ABORTED") if ok else state_now
            self._cas(txn_id, state_now, final, acks=acks, recovered=True)
            if ok:
                RECOVERED.labels(state).inc()
                if state == "PREPARING":      # live coordinator never reached a decision for this one
                    TXNS.labels(final).inc()
            log.warning("2PC recovery", extra={"txn_id": txn_id, "from_state": state, "to_state": final,
                                                "acks": acks})
            done.append({"txn_id": txn_id, "from": state, "to": final})
        return done
