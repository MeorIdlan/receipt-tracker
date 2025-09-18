import os, json, logging, datetime, base64
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

def _read_state():
    if not STATE_BUCKET: return None
    b = storage_client.bucket(STATE_BUCKET).blob(STATE_BLOB)
    if not b.exists(): return None
    try:
        return json.loads(b.download_as_text())
    except Exception:
        return None

def _write_state(obj: dict):
    if not STATE_BUCKET: return
    b = storage_client.bucket(STATE_BUCKET).blob(STATE_BLOB)
    b.upload_from_string(json.dumps(obj), content_type="application/json")

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

def _publish_new(files: List[Dict[str, Any]]):
    for f in files:
        evt = {
            "fileId": f["id"],
            "name": f.get("name"),
            "mimeType": f.get("mimeType"),
            "createdTime": f.get("createdTime"),
            "folderId": FOLDER_ID,
            # idempotencyKey = sha256(fileId:createdTime) — let downstream compute if needed
        }
        data = json.dumps(evt).encode("utf-8")
        publisher.publish(topic_path, data=data, fileId=f["id"]).result()

def drive_poller(request):
    """HTTP trigger (called by Cloud Scheduler). Polls Drive for new files and publishes to Pub/Sub."""
    if request and request.method not in ("GET", "POST"):
        return ("Method Not Allowed", 405)

    # Load last watermark; overlap lookback to be safe
    state = _read_state() or {}
    last_iso = state.get("last_createdTime")
    now = _now_utc()
    overlap = now - datetime.timedelta(minutes=LOOKBACK_MINUTES)

    # Start from last watermark or overlap (whichever earlier)
    if last_iso:
        try:
            last_dt = datetime.datetime.fromisoformat(last_iso.replace("Z","+00:00"))
        except Exception:
            last_dt = overlap
    else:
        last_dt = overlap

    since_iso = _iso(min(last_dt, overlap))
    logging.info("Polling folder %s since %s", FOLDER_ID, since_iso)

    files = _list_new_files(FOLDER_ID, since_iso)
    if files:
        _publish_new(files)
        # update watermark to newest createdTime we saw
        newest = max(datetime.datetime.fromisoformat(f["createdTime"].replace("Z","+00:00")) for f in files)
        _write_state({"last_createdTime": _iso(newest)})
        logging.info("Published %d new files; watermark=%s", len(files), _iso(newest))
    else:
        logging.info("No new files since %s", since_iso)

    return ("ok", 200)
