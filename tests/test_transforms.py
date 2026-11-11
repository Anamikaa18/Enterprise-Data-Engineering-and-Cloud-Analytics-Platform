import unittest

from src.transforms import normalize_transaction


class NormalizeTransactionTests(unittest.TestCase):
    def setUp(self):
        self.customers = {"C001": {"segment": "enterprise"}}
        self.valid_row = {
            "transaction_id": "T001",
            "customer_id": "C001",
            "transaction_ts": "2026-10-20T10:30:00Z",
            "amount": "100",
            "currency": "USD",
            "country": "in",
            "category": "Software",
        }

    def test_valid_row_is_normalized_and_enriched(self):
        record, reason = normalize_transaction(
            self.valid_row, self.customers, "2026-10-20T11:00:00+00:00"
        )
        self.assertIsNone(reason)
        self.assertEqual(record["amount_usd"], 100.0)
        self.assertEqual(record["customer_segment"], "enterprise")
        self.assertEqual(record["country"], "IN")
        self.assertEqual(record["category"], "software")

    def test_negative_amount_is_rejected(self):
        row = dict(self.valid_row, amount="-2")
        record, reason = normalize_transaction(row, self.customers)
        self.assertIsNone(record)
        self.assertEqual(reason, "negative_amount")

    def test_invalid_timestamp_is_rejected(self):
        row = dict(self.valid_row, transaction_ts="not-a-date")
        record, reason = normalize_transaction(row, self.customers)
        self.assertIsNone(record)
        self.assertEqual(reason, "invalid_transaction_ts")

    def test_missing_field_is_rejected(self):
        row = dict(self.valid_row)
        row["customer_id"] = ""
        record, reason = normalize_transaction(row, self.customers)
        self.assertIsNone(record)
        self.assertTrue(reason.startswith("missing_required_fields:"))

    def test_unknown_customer_gets_unknown_segment(self):
        row = dict(self.valid_row, customer_id="C999")
        record, reason = normalize_transaction(row, self.customers)
        self.assertIsNone(reason)
        self.assertEqual(record["customer_segment"], "unknown")


if __name__ == "__main__":
    unittest.main()
