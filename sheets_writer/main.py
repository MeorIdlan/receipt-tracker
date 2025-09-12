import base64
import json
import logging
import os
from typing import Any

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

def _append_rows(spreadsheet_id: str, title: str, rows: list[list[Any]]):
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

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