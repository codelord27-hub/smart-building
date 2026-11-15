"""Local stand-in for Eventarc: pulls the Pub/Sub emulator subscription and delivers each message
to the Alert Service function as a CloudEvent (same envelope GCP uses), acking on HTTP 2xx."""
import base64
import os
import time
import uuid

import httpx
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

PROJECT = os.environ.get("GCP_PROJECT", "demo-sb")
TOPIC = os.environ.get("PUBSUB_ALERTS_TOPIC", "building-alerts")
FUNCTION_URL = os.environ.get("FUNCTION_URL", "http://127.0.0.1:8090")

pub = pubsub_v1.PublisherClient()
sub = pubsub_v1.SubscriberClient()
topic_path = pub.topic_path(PROJECT, TOPIC)
sub_path = sub.subscription_path(PROJECT, "alert-function-local")
for fn, args in ((pub.create_topic, {"name": topic_path}),
                 (sub.create_subscription, {"name": sub_path, "topic": topic_path})):
    try:
        fn(request=args)
    except AlreadyExists:
        pass
print("bridge ready", sub_path, flush=True)


def deliver(msg):
    body = {"message": {"data": base64.b64encode(msg.message.data).decode(), "messageId": msg.message.message_id,
                        "attributes": dict(msg.message.attributes),
                        "publishTime": msg.message.publish_time.isoformat()},
            "subscription": sub_path}
    headers = {"ce-id": str(uuid.uuid4()), "ce-specversion": "1.0",
               "ce-type": "google.cloud.pubsub.topic.v1.messagePublished",
               "ce-source": f"//pubsub.googleapis.com/{topic_path}", "Content-Type": "application/json"}
    r = httpx.post(FUNCTION_URL, json=body, headers=headers, timeout=30)
    return r.status_code < 300


while True:
    try:
        resp = sub.pull(request={"subscription": sub_path, "max_messages": 50}, timeout=10)
    except Exception as exc:  # noqa: BLE001
        print("pull failed", exc, flush=True)
        time.sleep(2)
        continue
    acks = [m.ack_id for m in resp.received_messages if deliver(m)]
    if acks:
        sub.acknowledge(request={"subscription": sub_path, "ack_ids": acks})
        print("delivered", len(acks), flush=True)
    if not resp.received_messages:
        time.sleep(1)
