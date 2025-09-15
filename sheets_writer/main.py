import base64
import json
import logging
import os
from typing import Any
from datetime import datetime
from zoneinfo import ZoneInfo

import google.auth
from googleapiclient.discovery import build

# -------- Config --------
PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GCP_PROJECT")
SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
# If you deploy two identical functions (one per topic), no extra config is needed.
# If you deploy ONE function twice and want to force a default status, you can set:
DEFAULT_STATUS = os.getenv("DEFAULT_STATUS")  # "OK" or "NEEDS REVIEW" or empty

EXPECTED_BASE_HEADER = [
    "date","vendor","item","qty","unit_price","line_total",
    "subtotal","tax","total","currency","payment_method","receipt_id","image_hash"
]
EXTRA_COLS = ["status","notes","file_link"]
EXPECTED_HEADER = EXPECTED_BASE_HEADER + EXTRA_COLS

TOTALS_SHEET_NAME = "MONTHLY TOTAL"
TOTALS_HEADER = [
    "month",                # e.g., 2025-09
    "receipts_ok",          # unique receipts (OK)
    "receipts_all",         # unique receipts (all)
    "qty_ok",               # sum of qty (OK rows)
    "qty_all",              # sum of qty (all rows)
    "total_ok",             # sum of unique receipt totals (OK)
    "total_all",            # sum of unique receipt totals (all)
    "avg_per_receipt_ok",   # total_ok / receipts_ok
    "avg_per_day_ok",       # total_ok / distinct purchase days (OK)
    "reviewed_count",       # receipts flagged NEEDS REVIEW
    "reviewed_pct",         # reviewed_count / receipts_all
    "distinct_days_ok",     # number of purchase days counted in avg
    "last_updated"          # ISO timestamp
]

TZ = ZoneInfo(os.getenv("TIMEZONE", "Asia/Kuala_Lumpur"))

# Auth with Sheets scope
creds, _ = google.auth.default(scopes=[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly"
])
sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)

# -------- Helpers --------
def _col_letter(idx_zero_based: int) -> str:
    """0 -> A, 1 -> B, ..."""
    idx = idx_zero_based + 1
    letters = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def _ensure_month_tab(spreadsheet_id: str, title: str) -> int:
    """Ensure a sheet named `title` exists; return its sheetId (int). Also ensure header and formatting."""
    # Get spreadsheet metadata
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = { s["properties"]["title"]: s for s in meta.get("sheets", []) }
    if title in existing:
        sheet_id = existing[title]["properties"]["sheetId"]
        # Ensure header exists/matches; if empty, write it and set formatting once.
        _ensure_header_and_format(spreadsheet_id, title, sheet_id)
        return sheet_id

    # Create the sheet
    add_req = {
        "addSheet": {
            "properties": {
                "title": title,
                "gridProperties": {"frozenRowCount": 1}
            }
        }
    }
    resp = sheets.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests":[add_req]}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    # Set header + conditional formatting
    _ensure_header_and_format(spreadsheet_id, title, sheet_id, fresh=True)
    return sheet_id

def _ensure_header_and_format(spreadsheet_id: str, title: str, sheet_id: int, fresh: bool=False):
    # Read first row to see if header already present
    vr = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{title}!1:1"
    ).execute()
    header = vr.get("values", [[]])
    header = header[0] if header else []

    reqs = []

    # If header absent or mismatch, write it
    if header != EXPECTED_HEADER:
        reqs.append({
            "updateCells": {
                "start": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": 0},
                "rows": [{
                    "values": [{"userEnteredValue":{"stringValue": h}, "userEnteredFormat":{"textFormat":{"bold": True}}}
                               for h in EXPECTED_HEADER]
                }],
                "fields": "userEnteredValue,userEnteredFormat.textFormat"
            }
        })

    # Freeze header row (in case existing sheet lacked it)
    reqs.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }
    })

    # Conditional formatting: entire data range (A2:…)
    status_col_idx = EXPECTED_HEADER.index("status")
    status_col_letter = _col_letter(status_col_idx)
    # Apply rule to a generous range: A2 through the width of header, down many rows
    range_req = {
        "sheetId": sheet_id,
        "startRowIndex": 1,              # from row 2
        "startColumnIndex": 0,
        "endColumnIndex": len(EXPECTED_HEADER)
    }
    cond_req = {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [range_req],
                "booleanRule": {
                    "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [{"userEnteredValue": f'=${status_col_letter}2="NEEDS REVIEW"'}]
                    },
                    "format": {
                        "backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8},
                        "textFormat": {"bold": True}
                    }
                }
            },
            "index": 0
        }
    }
    reqs.append(cond_req)

    # Auto-resize columns for readability (optional)
    reqs.append({
        "autoResizeDimensions": {
            "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": len(EXPECTED_HEADER)}
        }
    })

    if reqs:
        sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
        
    # Add data validation rule for status column
    _add_status_dropdown(spreadsheet_id, sheet_id, status_col_idx)
    
    # protect header row (warning-only)
    _ensure_header_protection(spreadsheet_id, sheet_id, width=len(EXPECTED_HEADER))

def _append_rows(spreadsheet_id: str, title: str, rows: list[list[Any]]):
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()
    
def _add_status_dropdown(spreadsheet_id: str, sheet_id: int, status_col_idx: int):
    req = {
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,  # from row 2 down
                "startColumnIndex": status_col_idx,
                "endColumnIndex": status_col_idx + 1
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [
                        {"userEnteredValue": "OK"},
                        {"userEnteredValue": "NEEDS REVIEW"}
                    ]
                },
                "strict": True,
                "inputMessage": "Choose OK or NEEDS REVIEW"
            }
        }
    }
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": [req]}
    ).execute()

def _ensure_header_protection(spreadsheet_id: str, sheet_id: int, width: int):
    """
    Protect row 1 (warning only). Idempotent: skips if an equivalent protection exists.
    """
    meta = sheets.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId),protectedRanges)"
    ).execute()

    # Find current sheet's protections
    pranges = []
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("sheetId") == sheet_id:
            pranges = s.get("protectedRanges", []) or []
            break

    # See if a warningOnly protection for row 1 already exists
    for pr in pranges:
        rng = pr.get("range", {})
        if (pr.get("warningOnly") is True and
            rng.get("sheetId") == sheet_id and
            rng.get("startRowIndex") == 0 and rng.get("endRowIndex") == 1 and
            (rng.get("startColumnIndex", 0) == 0) and
            (rng.get("endColumnIndex") in (None, width))):
            return  # already protected

    # Add protection for A1:… (first row, through current header width)
    req = {
        "addProtectedRange": {
            "protectedRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": width
                },
                "warningOnly": True,
                "description": "Header row protected (warning only)."
            }
        }
    }
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [req]}
    ).execute()
    
def _ensure_totals_sheet(spreadsheet_id: str) -> int:
    """Create 'MONTHLY TOTAL' sheet if missing, set header, freeze row, number formats."""
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    by_title = {s["properties"]["title"]: s for s in meta.get("sheets", [])}
    if TOTALS_SHEET_NAME in by_title:
        sheet_id = by_title[TOTALS_SHEET_NAME]["properties"]["sheetId"]
    else:
        resp = sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title": TOTALS_SHEET_NAME, "gridProperties":{"frozenRowCount":1}}}}]}
        ).execute()
        sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    # Ensure header row matches TOTALS_HEADER
    vr = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{TOTALS_SHEET_NAME}!1:1"
    ).execute()
    header = (vr.get("values") or [[]])
    header = header[0] if header else []
    if header != TOTALS_HEADER:
        sheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{TOTALS_SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            body={"values":[TOTALS_HEADER]}
        ).execute()

    # Freeze header + apply basic number formats (currency & percent)
    reqs = [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties":{"frozenRowCount":1}},
                "fields": "gridProperties.frozenRowCount"
            }
        },
        # Percent format for reviewed_pct (column K = index 10)
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex":1, "startColumnIndex":10, "endColumnIndex":11},
                "cell": {"userEnteredFormat":{"numberFormat":{"type":"PERCENT", "pattern":"0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        # Currency-ish number format for totals (F,G,H) = indices 5,6,7 and also I (avg/day) idx=8
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex":1, "startColumnIndex":5, "endColumnIndex":9},
                "cell": {"userEnteredFormat":{"numberFormat":{"type":"NUMBER", "pattern":"0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        }
    ]
    sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
    return sheet_id


def _safe_float(x):
    try:
        return float(str(x).replace(",","").strip())
    except Exception:
        return None

def _compute_month_metrics(spreadsheet_id: str, month_key: str) -> list:
    """
    Reads the month sheet rows and computes metrics:
    - dedupe by image_hash for receipt-level totals
    - OK means status != 'NEEDS REVIEW'
    """
    # Expect columns A..P (0..15) as per EXPECTED_HEADER (+ status/notes/file_link)
    rng = f"{month_key}!A2:P"
    vr = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    rows = vr.get("values", [])

    # Indexes based on EXPECTED_HEADER order
    IDX_DATE = 0
    IDX_QTY = 3
    IDX_TOTAL = 8
    IDX_IMG = 12
    IDX_STATUS = 13

    receipts = {}  # image_hash -> {"status": "OK"/"NEEDS REVIEW", "total": float, "dates": set()}
    qty_ok = 0.0
    qty_all = 0.0
    days_ok = set()

    for r in rows:
        # pad row defensively
        r = (r + [""]*16)[:16]
        status = (r[IDX_STATUS] or "").strip().upper()
        img = (r[IDX_IMG] or "").strip()
        total = _safe_float(r[IDX_TOTAL])
        qty = _safe_float(r[IDX_QTY]) or 0.0
        date_str = (r[IDX_DATE] or "").strip()

        # items-level counts
        qty_all += qty
        if status != "NEEDS REVIEW":
            qty_ok += qty
            if date_str:
                days_ok.add(date_str)

        # receipt-level map (dedupe by image_hash; fall back key if missing)
        key = img or f"{r[IDX_DATE]}|{total}"
        if key not in receipts:
            receipts[key] = {"status": status or "OK", "total": total or 0.0, "dates": set()}
        # keep worst status if mixed (shouldn't happen, but be safe)
        if status == "NEEDS REVIEW":
            receipts[key]["status"] = "NEEDS REVIEW"
        if date_str:
            receipts[key]["dates"].add(date_str)
        # prefer a non-null total if we encounter one later
        if total is not None:
            receipts[key]["total"] = total

    receipts_all = len(receipts)
    reviewed_count = sum(1 for v in receipts.values() if v["status"] == "NEEDS REVIEW")
    receipts_ok = receipts_all - reviewed_count

    total_ok = sum(v["total"] for v in receipts.values() if v["status"] != "NEEDS REVIEW")
    total_all = sum(v["total"] for v in receipts.values())

    avg_per_receipt_ok = (total_ok / receipts_ok) if receipts_ok else 0.0
    distinct_days_ok = len(days_ok)
    avg_per_day_ok = (total_ok / distinct_days_ok) if distinct_days_ok else 0.0
    reviewed_pct = (reviewed_count / receipts_all) if receipts_all else 0.0

    return [
        month_key,
        receipts_ok,
        receipts_all,
        round(qty_ok, 2),
        round(qty_all, 2),
        round(total_ok, 2),
        round(total_all, 2),
        round(avg_per_receipt_ok, 2),
        round(avg_per_day_ok, 2),
        reviewed_count,
        reviewed_pct,
        distinct_days_ok,
        datetime.now(TZ).isoformat(timespec="seconds")
    ]


def _upsert_month_total_row(spreadsheet_id: str, totals_sheet_id: int, metrics_row: list):
    """Insert or update the row in MONTHLY TOTAL where column A == month."""
    month_key = metrics_row[0]
    # Find existing month row
    colA = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{TOTALS_SHEET_NAME}!A:A"
    ).execute().get("values", [])
    found_row = None
    for i, vals in enumerate(colA, start=1):
        if vals and str(vals[0]).strip() == month_key:
            found_row = i
            break

    if found_row is None:
        # Append new
        sheets.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{TOTALS_SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values":[metrics_row]}
        ).execute()
    else:
        # Update in place
        end_col_letter = _col_letter(len(TOTALS_HEADER)-1)
        rng = f"{TOTALS_SHEET_NAME}!A{found_row}:{end_col_letter}{found_row}"
        sheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            body={"values":[metrics_row]}
        ).execute()


def _upsert_monthly_total(spreadsheet_id: str, month_key: str):
    """Public helper to ensure totals sheet + compute & upsert metrics for month_key."""
    totals_sheet_id = _ensure_totals_sheet(spreadsheet_id)
    metrics = _compute_month_metrics(spreadsheet_id, month_key)
    _upsert_month_total_row(spreadsheet_id, totals_sheet_id, metrics)

# -------- Entrypoint --------
def sheets_writer(event, context):
    """
    Pub/Sub trigger (topic: receipts.valid OR receipts.review).
    Appends rows to the month tab, creating it if needed, and flags review rows.
    """
    del context  # unused
    if not SPREADSHEET_ID:
        raise RuntimeError("SHEETS_SPREADSHEET_ID env var is required")

    payload: dict = json.loads(base64.b64decode(event["data"]).decode("utf-8"))

    month_key = payload["month_key"]
    base_rows = payload.get("rows", [])
    notes_list = payload.get("notes", []) or []
    file_id = payload.get("fileId")
    incoming_status = payload.get("status")

    # Infer status if not supplied
    status = incoming_status or DEFAULT_STATUS or "OK"

    # Ensure monthly tab exists and is formatted
    sheet_id = _ensure_month_tab(SPREADSHEET_ID, month_key)

    # Build augmented rows with status, notes, and file link
    notes_text = "; ".join(notes_list) if notes_list else ""
    link_formula = f'=HYPERLINK("https://drive.google.com/file/d/{file_id}/view","open")' if file_id else ""

    augmented: list[list[Any]] = []
    for r in base_rows:
        # Pad row to expected base header length (defensive)
        r = (r + [None] * len(EXPECTED_BASE_HEADER))[:len(EXPECTED_BASE_HEADER)]
        augmented.append(r + [status, notes_text, link_formula])

    if not augmented:
        logging.warning("sheets_writer: no rows to append for month=%s fileId=%s", month_key, file_id)
        return

    _append_rows(SPREADSHEET_ID, month_key, augmented)
    logging.info("sheets_writer: appended %d row(s) to %s (status=%s)", len(augmented), month_key, status)
    
    _upsert_monthly_total(SPREADSHEET_ID, month_key)