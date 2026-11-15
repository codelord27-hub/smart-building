// k6 load test - sustained traffic that makes the HPAs of sensor-ingestion and control-optimizer
// scale out, then back in. Run from Cloud Shell:
//   docker run --rm -i -e BASE_URL=http://<INGRESS_IP> grafana/k6 run - < loadtest/hpa-load.js
// Watch scaling:  kubectl get hpa -n smart-building -w      (and the Grafana dashboard)
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const ZONES = ["f0-reading-hall", "f0-reception", "f0-server-room", "f1-study-pods", "f1-seminar-room", "f1-archives"];

const stages = [
  { duration: "1m", target: 20 },   // warm up
  { duration: "2m", target: 80 },   // ramp
  { duration: "5m", target: 80 },   // sustained load -> HPA scales out
  { duration: "1m", target: 0 },    // ramp down -> HPA scales in after the stabilisation window
];

export const options = {
  scenarios: {
    ingest: {                     // sensor gateways posting batches of readings
      executor: "ramping-arrival-rate",
      exec: "ingest",
      startRate: 5, timeUnit: "1s", preAllocatedVUs: 50, maxVUs: 300,
      stages,
    },
    optimize: {                   // what-if requests to the model-predictive controller (CPU heavy)
      executor: "ramping-arrival-rate",
      exec: "optimize",
      startRate: 5, timeUnit: "1s", preAllocatedVUs: 50, maxVUs: 300,
      stages,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.02"],
    "http_req_duration{scenario:ingest}": ["p(95)<2000"],
    "http_req_duration{scenario:optimize}": ["p(95)<3000"],
  },
};

const json = { headers: { "Content-Type": "application/json" } };

export function ingest() {
  const readings = [];
  for (let i = 0; i < 25; i++) {
    const zone = ZONES[Math.floor(Math.random() * ZONES.length)];
    readings.push({
      sensor_id: `${zone}-load-${i % 5}`,
      zone_id: zone,
      building_id: "bits-library",
      temperature_c: 20 + Math.random() * 6,
      occupancy_count: Math.floor(Math.random() * 30),
    });
  }
  const res = http.post(`${BASE}/api/readings`, JSON.stringify({ readings }), json);
  check(res, { "ingest 202": (r) => r.status === 202 });
}

export function optimize() {
  const body = {
    temperature_c: 18 + Math.random() * 12,
    target_c: 22,
    occupancy: Math.floor(Math.random() * 40),
    outside_c: 25 + Math.random() * 15,
    horizon_min: 120,
  };
  const res = http.post(`${BASE}/api/control/recommend`, JSON.stringify(body), json);
  check(res, { "recommend 200": (r) => r.status === 200 });
}
