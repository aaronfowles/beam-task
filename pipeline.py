from datetime import date
from decimal import Decimal
import simplejson as json

import apache_beam as beam
from dateutil.parser import parse


def to_transaction(element):
    raw_timestamp, raw_origin, raw_destination, raw_transaction_amount = element.split(
        ","
    )
    return beam.Row(
        timestamp=parse(raw_timestamp),
        origin=raw_origin,
        destination=raw_destination,
        transaction_amount=Decimal(raw_transaction_amount),
        date=parse(raw_timestamp).date(),
    )


def to_json(row: beam.Row):
    return json.dumps({"date": row.date.isoformat(), "total_amount": row.total_amount})


class SumTransactionsByDate(beam.PTransform):
    def expand(self, input_pcoll):
        transform = (
            input_pcoll
            | beam.Map(to_transaction)
            | beam.Filter(lambda row: row.transaction_amount > 20)
            | beam.Filter(lambda row: row.date > date(2010, 1, 1))
            | beam.GroupBy("date").aggregate_field(
                "transaction_amount", sum, "total_amount"
            )
            | beam.Map(to_json)
        )
        return transform


def main():
    with beam.Pipeline("DirectRunner") as p:
        (
            p
            | beam.io.ReadFromText(
                "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",
                skip_header_lines=1,
            )
            | SumTransactionsByDate()
            | beam.io.WriteToText(
                "output/results.jsonl.gz",
                num_shards=1,
                compression_type=beam.io.filesystem.CompressionTypes.GZIP,
                shard_name_template="",
            )
        )
        p.run()


if __name__ == "__main__":
    main()
