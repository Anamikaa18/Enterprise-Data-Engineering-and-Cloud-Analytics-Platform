"""Configuration constants for the data pipeline."""

REQUIRED_TRANSACTION_FIELDS = (
    "transaction_id",
    "customer_id",
    "transaction_ts",
    "amount",
    "currency",
    "country",
    "category",
)

USD_RATES = {
    "USD": 1.0,
    "INR": 0.012,
    "EUR": 1.08,
    "GBP": 1.27,
}
