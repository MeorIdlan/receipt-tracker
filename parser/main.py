import base64
import json
import logging
import os
from typing import Any

from google.cloud import pubsub_v1
from openai import OpenAI
from openai.types.chat import (
    ChatCompletionMessageParam,
    ChatCompletionSystemMessageParam,
    ChatCompletionUserMessageParam,
)

# ---------- Config ----------
PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GCP_PROJECT")
if not PROJECT_ID:
    PROJECT_ID = ""
INPUT_ATTR_TEXT_KEY = "text"  # field in incoming message
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "receipts.parsed")
MODEL = os.getenv("MODEL", "deepseek-chat")  # or deepseek-reasoner
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "1000"))
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.1"))

# DeepSeek API: use OpenAI SDK with base_url -> DeepSeek endpoint
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
if DEEPSEEK_API_KEY:
    DEEPSEEK_API_KEY = DEEPSEEK_API_KEY.strip()
client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC)

# ---------- Prompt pieces ----------
RECEIPT_SCHEMA_EXAMPLE = {
    "vendor": "string",
    "purchase_date": "YYYY-MM-DD",
    "currency": "MYR",
    "subtotal": 0.0,
    "tax": 0.0,
    "total": 0.0,
    "payment_method": "string|null",
    "items": [
        {"description": "string", "quantity": 1, "unit_price": 0.0, "line_total": 0.0}
    ],
    "receipt_id": "string|null",
    "source_image_hash": "sha256:…"
}

SYSTEM_INSTR = (
    "You are an extraction function. Return ONLY valid JSON that matches the schema. "
    "Do not include explanations or extra keys."
)

USER_RULES = """\
Extract purchase information from the OCR text.

Rules:
- Output strictly valid JSON (no markdown, no comments).
- Enforce schema keys: vendor, purchase_date (YYYY-MM-DD), currency (ISO 4217), subtotal, tax, total,
  payment_method (string or null), items[{description, quantity, unit_price, line_total}],
  receipt_id (string or null), source_image_hash.
- Default currency to 'MYR' unless another currency is explicitly shown.
- If subtotal/tax missing, set subtotal = total and tax = 0.
- Coerce numeric fields to numbers (no currency symbols).
- If quantity missing, default to 1.
- If uncertain about a field, set it to null (not 'N/A').
"""


def _publish(msg: dict[str, Any]) -> None:
    data = json.dumps(msg).encode("utf-8")
    publisher.publish(
        topic_path,
        data=data,
        fileId=msg.get("fileId", ""),
    ).result()


def _to_json_or_none(s: str | None) -> dict[str, Any] | None:
    logging.info(f"s: {s}")
    try:
        if s:
            return json.loads(s)
        return None
    except Exception as e:
        logging.warning(f"Error encountered during json.loads: {e}. Returning none")
        return None


def deepseek_parser(event, context):
    """Pub/Sub trigger (topic: receipts.text) → DeepSeek JSON → receipts.parsed"""
    del context  # unused

    raw = base64.b64decode(event["data"]).decode("utf-8")
    payload = json.loads(raw)

    file_id = payload["fileId"]
    image_hash = payload.get("image_hash")
    created_time = payload.get("createdTime")
    ocr_text = payload.get("text", "") or ""

    logging.info("DeepSeek parse start fileId=%s chars=%d", file_id, len(ocr_text))

    # If we have no OCR text, publish a 'null' data for downstream review
    if not ocr_text.strip():
        _publish({
            "fileId": file_id,
            "image_hash": image_hash,
            "data": None,
            "llm_meta": {"model": MODEL, "reason": "empty_ocr_text"},
        })
        logging.warning("Empty OCR text for fileId=%s", file_id)
        return

    # Build messages
    sys_msg: ChatCompletionSystemMessageParam = {
        "role": "system",
        "content": SYSTEM_INSTR,
    }
    rules_msg: ChatCompletionUserMessageParam = {
        "role": "user",
        "content": f"{USER_RULES}\n\nSchema example:\n{json.dumps(RECEIPT_SCHEMA_EXAMPLE, ensure_ascii=False)}",
    }
    ocr_msg: ChatCompletionUserMessageParam = {
        "role": "user",
        "content": f"OCR text:\n{ocr_text}\n\nsource_image_hash: {image_hash}",
    }
    messages: list[ChatCompletionMessageParam] = [sys_msg, rules_msg, ocr_msg]

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
            response_format={"type": "json_object"},  # DeepSeek JSON mode
        )
        content = resp.choices[0].message.content
        data = _to_json_or_none(content)

        if not data or not isinstance(data, dict):
            # Fallback: one retry with a sharper instruction
            retry_messages = messages + [
                {"role": "system", "content": "Your previous output was invalid. Reply with JSON ONLY."}
            ]
            resp2 = client.chat.completions.create(
                model=MODEL,
                messages=retry_messages,
                temperature=0.0,
                max_tokens=MAX_TOKENS,
                response_format={"type": "json_object"},
            )
            content = resp2.choices[0].message.content
            data = _to_json_or_none(content)

        # If still bad, publish a null payload (validator can route to review)
        if not data or not isinstance(data, dict):
            logging.error("DeepSeek returned non-JSON for fileId=%s", file_id)
            _publish({
                "fileId": file_id,
                "image_hash": image_hash,
                "data": data,
                "llm_meta": {"model": MODEL, "reason": "non_json_output"},
            })
            return

        # Attach the image hash if model didn't echo it
        data.setdefault("source_image_hash", image_hash)

        out = {
            "fileId": file_id,
            "image_hash": image_hash,
            "data": data,
            "llm_meta": {
                "model": MODEL,
                "temperature": TEMPERATURE,
                "max_tokens": MAX_TOKENS,
                "usage": getattr(resp, "usage", None) and resp.usage.model_dump() if resp.usage else None,
            },
        }
        _publish(out)
        logging.info("DeepSeek parse ok fileId=%s", file_id)

    except Exception as e:
        logging.exception("DeepSeek call failed for fileId=%s: %s", file_id, e)
        _publish({
            "fileId": file_id,
            "image_hash": image_hash,
            "data": None,
            "llm_meta": {"model": MODEL, "reason": "api_error", "error": str(e)},
        })
