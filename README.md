# Receipt-to-Sheets Pipeline (Serverless · Python 3.13 · Google Cloud)

End-to-end pipeline to turn scanned receipt images/PDFs into structured rows in Google Sheets—using **Drive Scan → OCR → LLM parse → Validate → Append to Sheets**. “Needs review” rows are flagged in red, and a **running monthly total** is maintained at the bottom of each month tab.

---

## TL;DR

* Scan receipt with **Google Drive app** into a specific folder.
* **Apps Script poller** spots new files and notifies an **Ingress** function.
* Functions (Python 3.13, Cloud Run Functions) do:

  1. **Text Extraction** → PDF text layer first, fallback **Cloud Vision**
  2. **DeepSeek Parser** → OCR text → strict JSON
  3. **Validator/Normalizer** → schema, totals, dedupe
  4. **Sheets Writer** → append rows, flag reviews, update monthly footer
* Pub/Sub topics glue each step.

---

## Architecture

```
[Drive mobile scan] → [Drive Folder]
        │
        ▼  (every minute)
[Apps Script Poller] —HTTP→ [Ingress (Cloud Run Function)]
        │ Pub/Sub: receipts.new
        ▼
[Text Extraction]
  ├─ try PDF text layer
  └─ fallback: Vision OCR
        │ Pub/Sub: receipts.text
        ▼
[DeepSeek Parser]
        │ Pub/Sub: receipts.parsed
        ▼
[Validator/Normalizer]
  ├─ if OK → Pub/Sub: receipts.valid
  ├─ if issues → Pub/Sub: receipts.review
  └─ if duplicate → Pub/Sub: receipts.duplicate
        │
        ├───────────────┐
        ▼               ▼
[Sheets Writer (valid)] [Sheets Writer (review)]
        │
        ▼
[Google Sheets: YYYY-MM tab]
- “NEEDS REVIEW” rows highlighted red
- Footer row “MONTH TOTAL” (unique receipts, OK only)
```

---

## Components

### 0) Apps Script Poller (Drive → HTTP)

* **Purpose:** Find new files in a target Drive folder and POST their metadata to the Ingress endpoint.
* **Why Apps Script:** free/cheap, no server to host webhooks.

### 1) Ingress (HTTP → Pub/Sub)

* **Purpose:** Authenticate (`X-API-Key`), validate payload, add an idempotency key, publish to `receipts.new`.
* **Runtime:** Cloud Run Functions, Python 3.13.

### 2) Text Extraction (Drive → OCR)

* **Purpose:** Download file from Drive, try **PDF text layer**; if insufficient, run **Cloud Vision** (images or async PDF OCR via GCS).
* **Publishes:** `fileId`, `image_hash`, `text`, `ocr_meta` → `receipts.text`.

### 3) DeepSeek Parser (LLM)

* **Purpose:** Convert OCR text to strict JSON (MY receipts) using **DeepSeek** in JSON mode.
* **Publishes:** `data` (receipt JSON) → `receipts.parsed`. Falls back with `data: null` on issues.

### 4) Validator/Normalizer

* **Purpose:** Validate schema, normalize dates (Asia/Kuala\_Lumpur), coerce numbers, reconcile totals, **dedupe** (image hash or vendor+date+total).
* **Publishes:**

  * **OK:** `receipts.valid` (with `rows`)
  * **Needs review:** `receipts.review` (also includes `rows`), status=`NEEDS REVIEW`
  * **Duplicate:** `receipts.duplicate` (no sheet write)

### 5) Sheets Writer

* **Purpose:** Append rows to monthly tab `YYYY-MM`, create tab if missing, ensure header and **conditional format** (red for “NEEDS REVIEW”).
* **Footer:** Maintains last row **MONTH TOTAL** that sums **unique receipts** (by `image_hash`) and **excludes “NEEDS REVIEW”** by default.

---

## Prerequisites

* **Google Cloud project** with billing enabled.
* **gcloud** CLI installed and authenticated.
* **Service Account** to run functions, with least-privilege roles (see IAM below).
* **A Google Sheet** (create once), and share it with the service account.
* **A Drive Folder** for receipts; share the folder with the service account (viewer).

---

## Enable APIs (once)

```bash
gcloud services enable \
  run.googleapis.com cloudfunctions.googleapis.com \
  pubsub.googleapis.com vision.googleapis.com \
  drive.googleapis.com storage.googleapis.com \
  sheets.googleapis.com secretmanager.googleapis.com \
  artifactregistry.googleapis.com cloudbuild.googleapis.com \
  eventarc.googleapis.com logging.googleapis.com \
  iam.googleapis.com
```

---

## IAM (service account & roles)

```bash
PROJECT_ID=$(gcloud config get-value project)
SA="receipts-functions-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create receipts-functions-sa \
  --display-name="Receipts Functions SA"

# Pub/Sub publish & subscribe
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/pubsub.publisher"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/pubsub.subscriber"

# Cloud Vision
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/visionai.serviceAgent"

# Storage (for Vision PDF outputs + dedupe markers)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/storage.objectAdmin"

# Secret Manager
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor"

# Sheets (via API) & Drive read are OAuth scopes; also SHARE resources with SA:
# - Share the Drive FOLDER with $SA (Viewer)
# - Share the Spreadsheet with $SA (Editor)
```

---

## Pub/Sub Topics

```bash
gcloud pubsub topics create receipts.new
gcloud pubsub topics create receipts.text
gcloud pubsub topics create receipts.parsed
gcloud pubsub topics create receipts.valid
gcloud pubsub topics create receipts.review
gcloud pubsub topics create receipts.duplicate
```

---

## Buckets

* For Vision-on-PDF & artifacts:
  `gsutil mb -l asia-southeast1 gs://YOUR-ARTIFACTS-BUCKET`
* For dedupe markers (optional):
  `gsutil mb -l asia-southeast1 gs://YOUR-DEDUPE-BUCKET`

Add lifecycle rules if you want auto-delete after N days.

---

## Secrets

```bash
# Ingress shared key
python3 - <<'PY'
import secrets; print(secrets.token_urlsafe(48))
PY
# Copy output as API key:
echo -n "<API_KEY>" | gcloud secrets create RECEIPTS_INGRESS_API_KEY --data-file=-

# DeepSeek API key
echo -n "<DEEPSEEK_API_KEY>" | gcloud secrets create DEEPSEEK_API_KEY --data-file=-
```

You’ll reference secrets on deploy with `--set-secrets`.

---

## Environment Files (YAML)

> Use `--env-vars-file` with these; keep secrets in Secret Manager.

`.env.ingress.yaml`

```yaml
PROJECT_ID: your-project-id
PUBSUB_TOPIC_ID: receipts.new
```

`.env.text_extraction.yaml`

```yaml
PROJECT_ID: your-project-id
OUTPUT_TOPIC: receipts.text
ARTIFACTS_BUCKET: your-artifacts-bucket
PDF_TEXT_MIN_CHARS: "120"
VISION_PAGES_LIMIT: "2"
```

`.env.deepseek_parser.yaml`

```yaml
PROJECT_ID: your-project-id
OUTPUT_TOPIC: receipts.parsed
MODEL: deepseek-chat
MAX_TOKENS: "1000"
TEMPERATURE: "0.1"
```

`.env.validator.yaml`

```yaml
PROJECT_ID: your-project-id
OUTPUT_TOPIC: receipts.valid
REVIEW_TOPIC: receipts.review
DUP_TOPIC: receipts.duplicate
TIMEZONE: Asia/Kuala_Lumpur
TOTALS_EPSILON: "0.05"
CURRENCY_DEFAULT: MYR
DEDUPE_BUCKET: your-dedupe-bucket
```

`.env.sheets_writer.yaml`

```yaml
SHEETS_SPREADSHEET_ID: your-spreadsheet-id
```

---

## Deploy Commands (Python 3.13)

In each relevant subdirectories are deploy scripts named `deploy.sh` that you may run to deploy each function to Google Cloud. Make sure to change the appropriate values in the YAML files before running the scripts.

---

## Apps Script Poller (set up once)

1. Create Drive folder “Receipts Inbox” → copy **Folder ID**.
2. Go to **script.google.com** → New project → *Receipts Poller*.
3. **Services (puzzle icon)** → add **Drive API** (Advanced Google Services).
4. **Project Settings → Script properties**: add:

   * `FOLDER_ID`, `INGRESS_URL`, `API_KEY`, `LOOKBACK_MINUTES` (e.g., 5)
5. Paste the code from `poller.js` and save.

6. **Triggers (clock icon)** → Add Trigger → `pollFolder` → Time-driven → Every 1 minute.
7. Authorize (Drive read, external requests).

---

## Sheets Format

**Header (created by writer):**

```
date | vendor | item | qty | unit_price | line_total | subtotal | tax | total | currency | payment_method | receipt_id | image_hash | status | notes | file_link
```

**Conditional formatting:** whole row turns **red** if `status = "NEEDS REVIEW"` (writer adds rule).

**Footer row:** last row labeled `MONTH TOTAL` with a formula in **total** column that:

* Sums **unique `(image_hash, total)`** pairs to avoid double-counting multi-item receipts.
* Excludes rows with `status = "NEEDS REVIEW"`.

Formula used by the writer (placed in the **total** column):

```gs
=IFERROR(
  SUM(
    QUERY(
      UNIQUE( FILTER({M2:M, I2:I}, N2:N<>"NEEDS REVIEW") ),
      "select sum(Col2) where Col1 <> ''",
      0
    )
  ),
  0
)
```

Columns: `I=total`, `M=image_hash`, `N=status`.
To include *all* rows (even reviews), remove the `N2:N<>"NEEDS REVIEW"` filter.

---

## Message Contracts (Pub/Sub)

**`receipts.new`**

```json
{ "fileId": "...", "name": "...", "mimeType": "application/pdf", "createdTime": "...", "folderId": "...", "idempotencyKey": "..." }
```

**`receipts.text`**

```json
{ "fileId":"...", "name":"...", "createdTime":"...", "image_hash":"sha256:...", "text":"...", "ocr_meta":{"engine":"pdf_text|vision_image|vision_pdf","confidence":0.0,"pages":1} }
```

**`receipts.parsed`**

```json
{ "fileId":"...", "image_hash":"sha256:...", "data":{... or null}, "llm_meta":{...} }
```

**`receipts.valid` / `receipts.review`**

```json
{
  "fileId":"...", "month_key":"YYYY-MM",
  "header":[ "date",..., "image_hash" ],
  "norm": { ...normalized receipt JSON... },
  "notes":[ "vendor missing", "sum(items)!=total" ],
  "rows":[ ["2025-09-10","7-Eleven","Milk 1L",1,6.5,6.5,21.8,1.2,23.0,"MYR","Card",null,"sha256:..."] ],
  "status":"OK" | "NEEDS REVIEW"
}
```

**`receipts.duplicate`**

```json
{ "fileId":"...", "dedupe_key":"sha256:...", "norm":{...} }
```

---

## Testing

**Dry-publish into a topic:**

```bash
gcloud pubsub topics publish receipts.new --message='{"fileId":"<REAL_FILE_ID>","name":"scan.pdf","mimeType":"application/pdf","createdTime":"2025-09-12T03:00:00Z","folderId":"<FOLDER_ID>","idempotencyKey":"x"}'
```

**Peek outputs:**

```bash
gcloud pubsub subscriptions create receipts.text.peek --topic receipts.text
gcloud pubsub subscriptions pull receipts.text.peek --auto-ack --limit=1
```

**Logs:**

```bash
gcloud logs read "run.googleapis.com%2Ffunctions" --region=asia-southeast1 --limit=100
```

---

## Handling “Needs Review”

* The **validator** sends rows to `receipts.review` with `status="NEEDS REVIEW"` and `notes`.
* The **writer** appends them to the same month tab, highlights red.
* You fix the cells in Sheets and manually change `status` to `"OK"`.
* The footer total excludes review rows by default.

*(Optional: add a filter view that shows only `status="NEEDS REVIEW"`.)*

---

## Security & Cost

* **Secrets** in Secret Manager; never in env files.
* **Least privilege** service account roles.
* **Drive/Sheet sharing** limited to the service account.
* **Costs**: Vision OCR per page; Pub/Sub and Functions invocations are tiny; Apps Script free/cheap.

---

## Troubleshooting

* **Ingress 401** → `X-API-Key` mismatch or missing; redeploy secret & set Apps Script property.
* **Drive 403** → Share the folder with the service account.
* **Sheets write fails** → Share the spreadsheet with the service account (Editor).
* **OCR empty** → Try rescan; validator will flag review.
* **Duplicated rows** → Check dedupe bucket configured and image hashes present.

---

## JSON Schema (target shape)

```json
{
  "vendor": "AEON Big Wangsa Maju",
  "purchase_date": "YYYY-MM-DD",
  "currency": "MYR",
  "subtotal": 0.00,
  "tax": 0.00,
  "total": 0.00,
  "payment_method": "Card|null",
  "items": [
    {"description":"Milk 1L","quantity":1,"unit_price":6.50,"line_total":6.50}
  ],
  "receipt_id": "string|null",
  "source_image_hash": "sha256:..."
}
```

---