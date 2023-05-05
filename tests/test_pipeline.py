import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipeline import SumTransactionsByDate


class TestCompositeTransform(unittest.TestCase):
    def test_composite_transform(self):
        with TestPipeline() as p:
            input = p | beam.Create(
                [
                    "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
                    "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
                    "2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
                    "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030",
                    "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08",
                    "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12",
                    "2018-02-27 16:04:12 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,21.00",  # adding an extra row to test case to test summing works
                ]
            )
            output = input | SumTransactionsByDate()

            assert_that(
                output,
                equal_to(
                    [
                        '{"date": "2017-03-18", "total_amount": 2102.22}',
                        '{"date": "2017-08-31", "total_amount": 13700000023.08}',
                        '{"date": "2018-02-27", "total_amount": 150.12}',
                    ]
                ),
            )
