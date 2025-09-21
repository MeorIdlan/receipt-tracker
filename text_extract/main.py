import base64
import hashlib
import io
import json
import os
import time, sys
from typing import Any

import google.auth
from google.cloud import pubsub_v1, storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pdfminer.high_level import extract_text as pdf_extract_text

from google.cloud import vision

# ---------- Config ----------
PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GCP_PROJECT")
if not PROJECT_ID:
    PROJECT_ID = ""
ARTIFACTS_BUCKET = os.getenv("ARTIFACTS_BUCKET")  # optional but required for Vision-on-PDF
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "receipts.text")

PDF_TEXT_MIN_CHARS = int(os.getenv("PDF_TEXT_MIN_CHARS", "120"))
VISION_PAGES_LIMIT = int(os.getenv("VISION_PAGES_LIMIT", "2"))  # OCR only first N pages for cost

SEVERITIES = {"DEFAULT","DEBUG","INFO","NOTICE","WARNING","ERROR","CRITICAL","ALERT","EMERGENCY"}

# ---------- Clients (reused across invocations) ----------
# Auth with required scopes for Drive read
creds, _ = google.auth.default(scopes=[
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/devstorage.read_write",
    "https://www.googleapis.com/auth/cloud-platform"
])

drive = build("drive", "v3", credentials=creds, cache_discovery=False)
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client(credentials=creds)
vision_client = vision.ImageAnnotatorClient(credentials=creds)

topic_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC)

# ---------- Helpers ----------
def _download_drive_file_bytes(file_id: str) -> bytes:
    """Downloads a Drive file into memory."""
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=8 * 1024 * 1024)
    done = False
    
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()

def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _try_pdf_text_layer(pdf_bytes: bytes) -> str:
    try:
        return pdf_extract_text(io.BytesIO(pdf_bytes)) or ""
    except Exception as e:
        log("WARNING", f"pdfminer extraction failed: {e}")
        return ""

def _vision_ocr_image(image_bytes: bytes) -> tuple[str, float, int]:
    client = vision.ImageAnnotatorClient(credentials=creds)
    image = vision.Image(content=image_bytes)
    features = [vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)]
    request = vision.AnnotateImageRequest(image=image, features=features)

    resp = client.batch_annotate_images(requests=[request])
    r = resp.responses[0]
    text = (r.full_text_annotation.text or "") if r.full_text_annotation else ""
    
    # Approximate confidence by averaging block confidences if present
    conf_vals = []
    pages = 0
    if r.full_text_annotation:
        for p in r.full_text_annotation.pages:
            pages += 1
            for b in p.blocks:
                if b.confidence:
                    conf_vals.append(b.confidence)
    confidence = (sum(conf_vals) / len(conf_vals)) if conf_vals else 0.0
    return text, confidence, max(1, pages)

def _vision_ocr_pdf_via_gcs_async(file_id: str, pdf_bytes: bytes) -> tuple[str, float, int]:
    if not ARTIFACTS_BUCKET:
        raise RuntimeError("ARTIFACTS_BUCKET not set; required for Vision PDF OCR.")

    # Upload source PDF to GCS
    in_blob = storage_client.bucket(ARTIFACTS_BUCKET).blob(f"raw/{file_id}.pdf")
    if not in_blob.exists():
        in_blob.upload_from_string(pdf_bytes, content_type="application/pdf")

    gcs_source_uri = f"gs://{ARTIFACTS_BUCKET}/raw/{file_id}.pdf"
    gcs_dest_uri   = f"gs://{ARTIFACTS_BUCKET}/vision/{file_id}/"

    client = vision.ImageAnnotatorClient(credentials=creds)

    features = [vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)]
    input_cfg  = vision.InputConfig(gcs_source=vision.GcsSource(uri=gcs_source_uri),
                                    mime_type="application/pdf")
    output_cfg = vision.OutputConfig(gcs_destination=vision.GcsDestination(uri=gcs_dest_uri),
                                     batch_size=1)

    req = vision.AsyncAnnotateFileRequest(
        features=features,
        input_config=input_cfg,
        output_config=output_cfg
    )

    op = client.async_batch_annotate_files(requests=[req])
    op.result(timeout=180)

    # Read Vision’s output JSON(s) from GCS
    out_bucket = storage_client.bucket(ARTIFACTS_BUCKET)
    blobs = list(out_bucket.list_blobs(prefix=f"vision/{file_id}/"))
    full_texts, conf_vals, pages = [], [], 0
    for b in blobs:
        if b.name.endswith(".json"):
            data = json.loads(b.download_as_text())
            for r in data.get("responses", []):
                fta = r.get("fullTextAnnotation")
                if not fta:
                    continue
                full_texts.append(fta.get("text", ""))
                pages += len(fta.get("pages", [])) if "pages" in fta else 0
                for p in fta.get("pages", []):
                    for blk in p.get("blocks", []):
                        c = blk.get("confidence")
                        if c is not None:
                            conf_vals.append(c)
    text = "\n".join(t for t in full_texts if t).strip()
    confidence = (sum(conf_vals) / len(conf_vals)) if conf_vals else 0.0
    return text, confidence, max(1, pages)

def _publish_output(msg: dict[str, Any]) -> None:
    data = json.dumps(msg).encode("utf-8")
    publisher.publish(
        topic_path,
        data=data,
        fileId=msg["fileId"],
        engine=msg["ocr_meta"]["engine"]
    ).result()

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

# ---------- Entrypoint ----------
def text_extraction(event, context):
    """
    Pub/Sub trigger (topic: receipts.new).
    Downloads file from Drive, extracts text (PDF text layer or Vision OCR),
    then publishes to receipts.text.
    """
    del context  # unused

    raw = base64.b64decode(event["data"]).decode("utf-8")
    payload = json.loads(raw)

    file_id = payload["fileId"]
    mime_type = payload["mimeType"]
    name = payload["name"]
    created_time = payload["createdTime"]

    log("INFO", f"Processing fileId={file_id} name={name} mime={mime_type}")

    # 1) Download bytes + hash (for dedupe later)
    content = _download_drive_file_bytes(file_id)
    image_hash = _sha256_hex(content)

    text = ""
    engine = ""
    confidence = 0.0
    pages = 1

    try:
        if mime_type == "application/pdf":
            # 2) Try text layer
            text = _try_pdf_text_layer(content)
            if len(text.strip()) >= PDF_TEXT_MIN_CHARS:
                engine = "pdf_text"
                confidence = 1.0  # we trust embedded text more than OCR
                # (pages unknown here; keep 1 or compute via a fast PDF metadata read)
            else:
                # 3) Fallback to Vision PDF OCR (async via GCS)
                text, confidence, pages = _vision_ocr_pdf_via_gcs_async(file_id, content)
                engine = "vision_pdf_async"
        elif mime_type.startswith("image/"):
            text, confidence, pages = _vision_ocr_image(content)
            engine = "vision_image"
        else:
            # As a defensive default, try Vision image OCR anyway
            text, confidence, pages = _vision_ocr_image(content)
            engine = "vision_image"
    except Exception as e:
        log("ERROR", f"OCR failed for fileId={file_id}: {e}")
        # Publish a minimal message with empty text; downstream can route to review if desired
        text = ""

    out: dict[str, Any] = {
        "fileId": file_id,
        "name": name,
        "createdTime": created_time,
        "image_hash": f"sha256:{image_hash}",
        "text": text,
        "ocr_meta": {
            "engine": engine,
            "confidence": float(confidence),
            "pages": int(pages),
        },
    }

    _publish_output(out)
    log("INFO", f"Published text for fileId={file_id} ({engine}, conf={confidence:.3f}, chars={len(text)})")