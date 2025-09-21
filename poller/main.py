import os, json, datetime, base64, time, sys
from typing import List, Dict, Any

import google.auth
from googleapiclient.discovery import build
from google.cloud import storage, pubsub_v1

# ---- Config (env) ----
PROJECT_ID = os.getenv("PROJECT_ID", "") or os.getenv("GCP_PROJECT", "")
FOLDER_ID = os.getenv("TARGET_FOLDER_ID", "")         # the Drive folder you scan
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "5"))
STATE_BUCKET = os.getenv("STATE_BUCKET")              # GCS bucket to store watermark
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "receipts.new")
SEEN_TTL_MINUTES = int(os.getenv("SEEN_TTL_MINUTES", str(max(LOOKBACK_MINUTES * 6, 30))))  # default ~30 min
SEEN_MAX = int(os.getenv("SEEN_MAX", "5000"))  # cap memory to prevent unbounded growth
SEVERITIES = {"DEFAULT","DEBUG","INFO","NOTICE","WARNING","ERROR","CRITICAL","ALERT","EMERGENCY"}

# ADC with Drive readonly + Storage
creds, _ = google.auth.default(scopes=[
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/devstorage.read_write",
    "https://www.googleapis.com/auth/pubsub"
])
drive = build("drive", "v3", credentials=creds, cache_discovery=False)
storage_client = storage.Client(credentials=creds)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

STATE_BLOB = f"state/drive_poller/{FOLDER_ID}.json"

def _now_utc():
    return datetime.datetime.now(datetime.timezone.utc)

def _read_state() -> dict:
    """
    State file layout:
    {
      "last_createdTime": "2025-09-21T02:03:21Z",
      "seen": { "<fileId>": "2025-09-21T02:03:21Z", ... }   # small LRU-like map
    }
    """
    if not STATE_BUCKET:
        return {"seen": {}}
    b = storage_client.bucket(STATE_BUCKET).blob(STATE_BLOB)
    if not b.exists():
        return {"seen": {}}
    try:
        state = json.loads(b.download_as_text())
        state.setdefault("seen", {})
        return state
    except Exception:
        return {"seen": {}}

def _write_state(obj: dict):
    if not STATE_BUCKET:
        return
    # keep only top-level keys we care about
    out = {
        "last_createdTime": obj.get("last_createdTime"),
        "seen": obj.get("seen", {})
    }
    b = storage_client.bucket(STATE_BUCKET).blob(STATE_BLOB)
    b.upload_from_string(json.dumps(out), content_type="application/json")
    
def _prune_seen(seen: dict) -> dict:
    """Drop old/overflowed entries from seen map."""
    if not seen:
        return {}
    # prune by time (TTL) then by size (LRU-ish based on createdTime)
    try:
        cutoff = _now_utc() - datetime.timedelta(minutes=SEEN_TTL_MINUTES)
        keep = {fid: ts for fid, ts in seen.items()
                if ts and datetime.datetime.fromisoformat(ts.replace("Z","+00:00")) >= cutoff}
    except Exception:
        keep = dict(seen)

    if len(keep) > SEEN_MAX:
        # sort by timestamp ascending and keep the most recent
        items = sorted(keep.items(), key=lambda kv: kv[1])
        keep = dict(items[-SEEN_MAX:])
    return keep

def _iso(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def _list_new_files(folder_id: str, since_iso: str) -> List[Dict[str, Any]]:
    query = f"'{folder_id}' in parents and trashed = false and createdTime > '{since_iso}'"
    files: List[Dict[str, Any]] = []
    page_token = None
    while True:
        res = drive.files().list(
            q=query,
            orderBy="createdTime",
            pageToken=page_token,
            pageSize=200,
            fields="files(id,name,mimeType,createdTime,parents),nextPageToken",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return files

def _publish_new(files: List[Dict[str, Any]], state: dict):
    """
    Publish only files not in state['seen'].
    Updates the in-memory state['seen'] as it publishes.
    """
    seen = state.setdefault("seen", {})
    published = 0
    for f in files:
        fid = f["id"]
        if fid in seen:
            continue  # already sent before; skip
        evt = {
            "fileId": fid,
            "name": f.get("name"),
            "mimeType": f.get("mimeType"),
            "createdTime": f.get("createdTime"),
            "folderId": FOLDER_ID,
        }
        data = json.dumps(evt).encode("utf-8")
        publisher.publish(topic_path, data=data, fileId=fid).result()
        # mark when we PUBLISHED it, not when it was created
        seen[fid] = _iso(_now_utc())
        published += 1
    # prune memory so state doesn't grow forever
    state["seen"] = _prune_seen(seen)
    return published

def log(severity="INFO", message="", **fields):
    """Usage: log("SEVERITY-LEVEL", your-message)"""
    sev = severity.upper() if severity else "DEFAULT"
    if sev not in SEVERITIES:
        sev = "DEFAULT"
    record = {
        "severity": sev, # <-- Cloud Logging parses this
        "message": message, # human-readable
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **fields, # any extra structured fields you want
    }
    # Send errors+ to stderr, others to stdout (helps with some runtimes)
    stream = sys.stderr if sev in {"ERROR","CRITICAL","ALERT","EMERGENCY"} else sys.stdout
    print(json.dumps(record, ensure_ascii=False), file=stream, flush=True)

def drive_poller(request):
    """HTTP trigger (called by Cloud Scheduler). Polls Drive for new files and publishes to Pub/Sub."""
    if request and request.method not in ("GET", "POST"):
        return ("Method Not Allowed", 405)

    state = _read_state()
    last_iso = state.get("last_createdTime")
    overlap_start = _now_utc() - datetime.timedelta(minutes=LOOKBACK_MINUTES)
    
    if last_iso:
        try:
            last_dt = datetime.datetime.fromisoformat(last_iso.replace("Z","+00:00"))
        except Exception:
            last_dt = overlap_start
    else:
        last_dt = overlap_start

    # scan from the LATER of (watermark, overlap window)
    since_dt = max(last_dt, overlap_start)
    since_iso = _iso(since_dt)
    log("INFO", f"Polling folder {FOLDER_ID} since {since_iso}")

    files = _list_new_files(FOLDER_ID, since_iso)
    print(files, flush=True)
    if files:
        published = _publish_new(files, state)
        # watermark = newest createdTime among *all* seen this run
        newest_dt = max(datetime.datetime.fromisoformat(f["createdTime"].replace("Z","+00:00")) for f in files)
        state["last_createdTime"] = _iso(newest_dt)
        _write_state(state)
        log("INFO", f"Found {len(files)} files; published {published}; watermark={state["last_createdTime"]}")
    else:
        # still persist pruned seen set occasionally
        state["seen"] = _prune_seen(state.get("seen", {}))
        _write_state(state)
        log("INFO", f"No new files since {since_iso}")

    return ("ok", 200)
