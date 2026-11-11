"""Pure transformation functions used by the Apache Beam pipeline."""
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

from src.config import REQUIRED_TRANSACTION_FIELDS, USD_RATES


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and normalize it to UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_transaction(
    raw: Dict[str, str],
    customers: Dict[str, Dict[str, str]],
    ingested_at: Optional[str] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """Normalize, validate and enrich one transaction.

    Returns (record, None) for valid rows or (None, rejection_reason).
    """
    missing = [field for field in REQUIRED_TRANSACTION_FIELDS
               if raw.get(field) in (None, "")]
    if missing:
        return None, "missing_required_fields:" + ",".join(missing)

    try:
        amount = float(raw["amount"])
    except (TypeError, ValueError):
        return None, "invalid_amount"

    if amount < 0:
        return None, "negative_amount"

    try:
        timestamp = parse_timestamp(raw["transaction_ts"])
    except (TypeError, ValueError):
        return None, "invalid_transaction_ts"

    currency = raw["currency"].strip().upper()
    if currency not in USD_RATES:
        return None, "unsupported_currency"

    customer = customers.get(raw["customer_id"], {})
    rate = USD_RATES[currency]

    record = {
        "transaction_id": raw["transaction_id"].strip(),
        "customer_id": raw["customer_id"].strip(),
        "transaction_ts": timestamp.isoformat(),
        "amount": round(amount, 2),
        "currency": currency,
        "country": raw["country"].strip().upper(),
        "category": raw["category"].strip().lower(),
        "customer_segment": customer.get("segment", "unknown"),
        "amount_usd": round(amount * rate, 2),
        "event_date": timestamp.date().isoformat(),
        "ingested_at": ingested_at or datetime.now(timezone.utc).isoformat(),
    }
    return record, None
