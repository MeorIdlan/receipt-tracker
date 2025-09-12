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

FOOTER_LABEL = "MONTH TOTAL"

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

def _get_last_used_row(spreadsheet_id: str, title: str) -> int:
    vr = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{title}!A:A"
    ).execute()
    values = vr.get("values", [])
    return len(values)  # 1-based; 1 means header only

def _delete_row(spreadsheet_id: str, sheet_id: int, row_1_based: int):
    req = {
        "deleteDimension": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS",
                      "startIndex": row_1_based - 1, "endIndex": row_1_based}
        }
    }
    sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests":[req]}).execute()

def _format_footer_row(spreadsheet_id: str, sheet_id: int, row_1_based: int, num_cols: int):
    req = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row_1_based - 1,
                "endRowIndex": row_1_based,
                "startColumnIndex": 0,
                "endColumnIndex": num_cols
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}
                }
            },
            "fields": "userEnteredFormat(textFormat,backgroundColor)"
        }
    }
    sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests":[req]}).execute()

def _upsert_month_footer(spreadsheet_id: str, title: str, sheet_id: int, header: list):
    # 1) If the last row is an existing footer, delete it
    last = _get_last_used_row(spreadsheet_id, title)
    if last >= 2:
        check = sheets.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{title}!A{last}:A{last}"
        ).execute().get("values", [[]])
        if check and check[0] and str(check[0][0]).strip().upper() == FOOTER_LABEL:
            _delete_row(spreadsheet_id, sheet_id, last)
            last -= 1

    # 2) Append fresh footer row
    # Build a blank row with the same width as header; label col A, formula in col 'total'
    width = len(header)
    row = [""] * width
    row[0] = FOOTER_LABEL
    total_col_idx = header.index("total")  # 0-based
    row[total_col_idx] = "=IFERROR(SUM(QUERY(UNIQUE(FILTER({M2:M, I2:I}, N2:N<>\"NEEDS REVIEW\")), \"select sum(Col2) where Col1 <> ''\", 0)), 0)"

    # Status/notes/file_link remain blank on the footer row
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()

    # 3) Make the new last row bold & shaded
    new_last = _get_last_used_row(spreadsheet_id, title)
    _format_footer_row(spreadsheet_id, sheet_id, new_last, width)
    
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
    
    _upsert_month_footer(SPREADSHEET_ID, month_key, sheet_id, EXPECTED_HEADER)