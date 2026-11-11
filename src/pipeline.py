"""Apache Beam batch ETL pipeline.

Local:
  python -m src.pipeline --transactions data/transactions.csv \\
    --customers data/customers.csv --output output/curated.jsonl \\
    --rejects output/rejected.jsonl

Dataflow:
  Add --runner DataflowRunner --project ... --region ... and GCS paths.
"""
import argparse
import csv
import json
import logging
from typing import Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from src.transforms import normalize_transaction


def load_customers(path: str) -> Dict[str, dict]:
    """Load the small customer dimension used for demo enrichment."""
    customers = {}
    with beam.io.filesystems.FileSystems.open(path) as handle:
        text = handle.read().decode("utf-8")
    for row in csv.DictReader(text.splitlines()):
        customers[row["customer_id"]] = row
    return customers


class NormalizeAndEnrich(beam.DoFn):
    def __init__(self, customers):
        self.customers = customers

    def process(self, raw):
        record, reason = normalize_transaction(raw, self.customers)
        if record:
            yield beam.pvalue.TaggedOutput("valid", record)
        else:
            yield beam.pvalue.TaggedOutput(
                "rejected",
                {"raw_record": raw, "rejection_reason": reason},
            )


def parse_args():
    parser = argparse.ArgumentParser(description="Enterprise data engineering ETL")
    parser.add_argument("--transactions", required=True)
    parser.add_argument("--customers", required=True)
    parser.add_argument("--output", required=True, help="Output prefix/path")
    parser.add_argument("--rejects", required=True, help="Rejected output prefix/path")
    known, pipeline_args = parser.parse_known_args()
    return known, pipeline_args


def run():
    args, pipeline_args = parse_args()
    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)

    customers = load_customers(args.customers)
    with beam.Pipeline(options=options) as pipeline:
        rows = (
            pipeline
            | "Read transactions" >> beam.io.ReadFromText(args.transactions, skip_header_lines=1)
            | "Parse CSV rows" >> beam.Map(lambda line: next(csv.reader([line])))
            | "Map CSV fields" >> beam.Map(lambda cols: {
                "transaction_id": cols[0],
                "customer_id": cols[1],
                "transaction_ts": cols[2],
                "amount": cols[3],
                "currency": cols[4],
                "country": cols[5],
                "category": cols[6],
            })
        )

        tagged = (
            rows
            | "Normalize and enrich" >> beam.ParDo(
                NormalizeAndEnrich(customers)
            ).with_outputs("valid", "rejected")
        )

        unique = (
            tagged.valid
            | "Key by transaction ID" >> beam.Map(
                lambda row: (row["transaction_id"], row)
            )
            | "Remove duplicate IDs" >> beam.GroupByKey()
            | "Keep one record per ID" >> beam.Map(
                lambda item: next(iter(item[1]))
            )
        )

        unique | "Serialize curated records" >> beam.Map(
            lambda row: json.dumps(row, sort_keys=True)
        ) | "Write curated output" >> beam.io.WriteToText(
            args.output, file_name_suffix=".jsonl"
        )

        tagged.rejected | "Serialize rejected records" >> beam.Map(
            lambda row: json.dumps(row, sort_keys=True)
        ) | "Write rejected output" >> beam.io.WriteToText(
            args.rejects, file_name_suffix=".jsonl"
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
