import os
import json
import hashlib
import logging
from typing import Dict, Any

from flask import Request, abort, make_response
from google.cloud import pubsub_v1

# ---- Config via env vars ----
PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GCP_PROJECT")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "receipts.new")
API_KEY = os.getenv("API_KEY")  # Inject from Secret Manager at deploy time
if API_KEY:
    API_KEY = API_KEY.strip()

# Pub/Sub client at module import (reused across invocations for speed)
publisher = pubsub_v1.PublisherClient()
_topic_path = None
if PROJECT_ID and TOPIC_ID:
    _topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

REQUIRED_FIELDS = ("fileId", "name", "mimeType", "createdTime", "folderId")

def _bad_request(msg: str, code: int = 400):
    logging.warning("Bad request: %s", msg)
    return make_response({"error": msg}, code)

def _validate_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(body, dict):
        abort(_bad_request("JSON body must be an object"))

    missing = [k for k in REQUIRED_FIELDS if k not in body]
    if missing:
        abort(_bad_request(f"Missing required fields: {', '.join(missing)}"))

    # Light sanity checks
    if not str(body["fileId"]).strip():
        abort(_bad_request("fileId is empty"))
    if "application/" not in body["mimeType"] and "image/" not in body["mimeType"]:
        abort(_bad_request("mimeType looks invalid"))

    return body

def ingress(request: Request):
    """HTTP entrypoint for Apps Script.
    - Verifies X-API-Key
    - Validates payload
    - Publishes to Pub/Sub 'receipts.new'
    """
    if request.method != "POST":
        return _bad_request("Use POST", 405)

    # Shared-secret check
    incoming_key = request.headers.get("X-API-Key")
    if incoming_key:
        incoming_key = incoming_key.strip()
    
    if not API_KEY or incoming_key != API_KEY:
        return _bad_request("Unauthorized", 401)

    # Parse and validate JSON
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        return _bad_request(f"Invalid JSON: {e}")

    payload = _validate_payload(body)

    # Build idempotency key from fileId + createdTime
    idem_src = f"{payload['fileId']}:{payload['createdTime']}".encode("utf-8")
    idempotency_key = hashlib.sha256(idem_src).hexdigest()

    event = {
        "fileId": payload["fileId"],
        "name": payload["name"],
        "mimeType": payload["mimeType"],
        "createdTime": payload["createdTime"],
        "folderId": payload["folderId"],
        "idempotencyKey": idempotency_key,
    }

    if not _topic_path:
        return _bad_request("Server misconfigured: topic path not set", 500)

    # Publish JSON as the message data; also include attributes for easy filtering
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(
        _topic_path,
        data=data,
        idempotencyKey=idempotency_key,
        fileId=payload["fileId"],
    )
    message_id = future.result()  # wait for publish confirmation

    logging.info("Published %s to %s", message_id, _topic_path)
    return make_response({"status": "ok", "messageId": message_id}, 200)