import base64, json, logging, os, re, math
from typing import Any
from datetime import datetime, date
from zoneinfo import ZoneInfo

from dateutil import parser as dtparse
from google.cloud import pubsub_v1, storage

# ---------- Config ----------
PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GCP_PROJECT")
if not PROJECT_ID:
    PROJECT_ID = ""
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "receipts.valid")
REVIEW_TOPIC = os.getenv("REVIEW_TOPIC", "receipts.review")
DUP_TOPIC = os.getenv("DUP_TOPIC", "receipts.duplicate")
TZ = ZoneInfo(os.getenv("TIMEZONE", "Asia/Kuala_Lumpur"))
CURRENCY_DEFAULT = os.getenv("CURRENCY_DEFAULT", "MYR").upper()
EPSILON = float(os.getenv("TOTALS_EPSILON", "0.05"))  # allowed rounding diff
DEDUPE_BUCKET = os.getenv("DEDUPE_BUCKET")  # optional: GCS bucket for atomic dedupe

publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client() if DEDUPE_BUCKET else None

topic_valid = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC)
topic_review = publisher.topic_path(PROJECT_ID, REVIEW_TOPIC)
topic_dup = publisher.topic_path(PROJECT_ID, DUP_TOPIC)

HEADER = [
    "date","vendor","item","qty","unit_price","line_total",
    "subtotal","tax","total","currency","payment_method","receipt_id","image_hash"
]

# ---------- Helpers ----------
def _pub(topic_path: str, payload: dict[str, Any], **attrs):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    publisher.publish(topic_path, data=data, **{k:str(v) for k,v in attrs.items()}).result()

def _parse_date(s: str | None) -> str | None:
    if not s or not str(s).strip():
        return None
    try:
        # parse with best-effort, assume local tz, return YYYY-MM-DD
        dt = dtparse.parse(str(s), dayfirst=False)  # most MY receipts are YYYY-MM-DD or DD/MM/YY
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        else:
            dt = dt.astimezone(TZ)
        return dt.date().isoformat()
    except Exception:
        return None

def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    # strip currency symbols/commas
    s = str(x).strip()
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return None

def _norm_currency(c: str | None) -> str:
    if not c: return CURRENCY_DEFAULT
    return str(c).strip().upper()

def _round2(x: float | None) -> float | None:
    if x is None: return None
    return round(float(x) + 1e-12, 2)

def _flatten_rows(data: dict[str, Any]) -> tuple[str, list[list[Any]]]:
    """Return month_key and rows for Sheets (one row per item)."""
    d = data
    month_key = (d.get("purchase_date") or "")[:7] or datetime.now(TZ).date().isoformat()[:7]
    rows = []
    for it in d.get("items", []) or []:
        rows.append([
            d.get("purchase_date"),
            d.get("vendor"),
            it.get("description"),
            it.get("quantity"),
            it.get("unit_price"),
            it.get("line_total"),
            d.get("subtotal"),
            d.get("tax"),
            d.get("total"),
            d.get("currency"),
            d.get("payment_method"),
            d.get("receipt_id"),
            d.get("source_image_hash"),
        ])
    return month_key, rows

def _dedupe_key(d: dict[str, Any]) -> str | None:
    if d.get("source_image_hash"):
        return d["source_image_hash"]
    v, dt_iso, tot = d.get("vendor"), d.get("purchase_date"), d.get("total")
    if v and dt_iso and isinstance(tot, (int, float)):
        return f"{v}|{dt_iso}|{tot:.2f}"
    return None

def _gcs_dedupe_mark(key: str) -> bool:
    """
    Poor-man's atomic dedupe using GCS:
    try to create gs://DEDUPE_BUCKET/dedupe/<key> with if_generation_match=0.
    Returns True if created (first time), False if already exists.
    """
    if not storage_client or not DEDUPE_BUCKET:
        return True  # if not configured, don't block
    bucket = storage_client.bucket(DEDUPE_BUCKET)
    blob = bucket.blob(f"dedupe/{key}")
    try:
        blob.upload_from_string(b"", if_generation_match=0)
        return True
    except Exception as e:
        # PreconditionFailed means it already exists
        if "PreconditionFailed" in str(e) or "412" in str(e):
            return False
        logging.warning("Dedupe mark error for key=%s: %s", key, e)
        # fail-open so we don't drop data
        return True

# ---------- Validation / Normalization ----------
def _normalize(data: dict[str, Any]) -> tuple[dict[str, Any], list[str], bool]:
    """
    Returns (normalized_data, notes, needs_review)
    """
    notes: list[str] = []
    needs_review = False

    # Required-ish fields
    vendor = (data.get("vendor") or None)
    if vendor is None or not str(vendor).strip():
        needs_review = True
        notes.append("vendor missing")

    purchase_date = _parse_date(data.get("purchase_date"))
    if not purchase_date:
        needs_review = True
        notes.append("purchase_date missing/invalid")

    currency = _norm_currency(data.get("currency"))
    payment_method = data.get("payment_method") or None
    receipt_id = data.get("receipt_id") or None
    image_hash = data.get("source_image_hash") or None

    # Items
    items_in = data.get("items") or []
    norm_items = []
    if not isinstance(items_in, list) or len(items_in) == 0:
        needs_review = True
        notes.append("no items")
        items_in = []

    for it in items_in:
        desc = (it.get("description") if isinstance(it, dict) else None) or None
        qty = _to_float((it.get("quantity") if isinstance(it, dict) else None))
        if qty is None or qty <= 0: qty = 1.0
        unit_price = _to_float((it.get("unit_price") if isinstance(it, dict) else None))
        line_total = _to_float((it.get("line_total") if isinstance(it, dict) else None))

        # derive missing fields
        if line_total is None and unit_price is not None:
            line_total = qty * unit_price
        if unit_price is None and line_total is not None and qty:
            unit_price = line_total / qty

        unit_price = _round2(unit_price)
        line_total = _round2(line_total)

        if not desc or line_total is None:
            needs_review = True
            notes.append("item missing description or line_total")
        norm_items.append({
            "description": desc,
            "quantity": _round2(qty),
            "unit_price": unit_price,
            "line_total": line_total
        })

    # Totals
    subtotal = _to_float(data.get("subtotal"))
    tax = _to_float(data.get("tax"))
    total = _to_float(data.get("total"))

    sum_lines = _round2(sum([it.get("line_total") or 0.0 for it in norm_items]))
    if subtotal is None:
        subtotal = sum_lines
    if total is None and subtotal is not None and tax is not None:
        total = subtotal + tax
    if (tax is None) and (subtotal is not None) and (total is not None):
        tax = _round2(total - subtotal)

    subtotal = _round2(subtotal)
    tax = _round2(tax)
    total = _round2(total)

    # Sanity checks
    if total is None or total <= 0:
        needs_review = True
        notes.append("total missing/invalid")
    if subtotal is None:
        needs_review = True
        notes.append("subtotal missing/invalid")

    if total is not None and sum_lines is not None:
        if abs((total or 0) - (sum_lines or 0)) > EPSILON:
            # tolerate small rounding; flag bigger gaps
            needs_review = True
            notes.append(f"sum(items) {sum_lines} != total {total}")

    norm = {
        "vendor": vendor,
        "purchase_date": purchase_date,
        "currency": currency,
        "subtotal": subtotal,
        "tax": tax,
        "total": total,
        "payment_method": payment_method,
        "items": norm_items,
        "receipt_id": receipt_id,
        "source_image_hash": image_hash
    }
    return norm, notes, needs_review

# ---------- Entrypoint ----------
def validator(event, context):
    """
    Pub/Sub trigger (topic: receipts.parsed).
    Validates & normalizes LLM JSON. Emits:
      - receipts.valid (rows ready for Sheets)
      - receipts.review (needs human attention)
      - receipts.duplicate (seen already)
    """
    del context  # unused
    payload = json.loads(base64.b64decode(event["data"]).decode("utf-8"))

    file_id = payload.get("fileId")
    image_hash = payload.get("image_hash")
    data = payload.get("data")

    if not data:
        _pub(topic_review, {
            "fileId": file_id,
            "reason": "empty_or_invalid_llm_data",
            "image_hash": image_hash
        }, fileId=file_id or "")
        logging.warning("validator: no data for fileId=%s", file_id)
        return

    norm, notes, needs_review = _normalize(data)

    # Dedupe
    dedupe = _dedupe_key(norm)
    if dedupe:
        created_first_time = _gcs_dedupe_mark(dedupe)
        if not created_first_time:
            _pub(topic_dup, {
                "fileId": file_id,
                "dedupe_key": dedupe,
                "norm": norm
            }, fileId=file_id or "", dedupe="hit")
            logging.info("validator: duplicate %s fileId=%s", dedupe, file_id)
            return

    # Prepare rows for Sheets
    month_key, rows = _flatten_rows(norm)
    if not rows:
        needs_review = True
        notes.append("no rows produced from items")

    out_common = {
        "fileId": file_id,
        "month_key": month_key,
        "header": HEADER,
        "norm": norm,
        "notes": notes
    }

    if needs_review:
        _pub(topic_review, {**out_common, "reason": "needs_review"}, fileId=file_id or "", status="review")
        logging.info("validator: sent to review fileId=%s notes=%s", file_id, notes)
        return

    _pub(topic_valid, {**out_common, "rows": rows}, fileId=file_id or "", status="valid")
    logging.info("validator: ok fileId=%s rows=%d", file_id, len(rows))
