from chispa.dataframe_comparer import *
from ..jobs.hosts_cumulated_full_join import do_hosts_cumulated_full_transformation
from collections import namedtuple
from datetime import date
Events = namedtuple("Events", "host event_time")
HostActivity = namedtuple("HostActivity", "host host_activity_datelist")


def test_scd_generation(spark):
    source_data = [
        Events("mysite", '2001-01-01'),
        Events("mysite", '2001-02-01'),
        Events("mysite-ok", '2023-01-01'),
        Events("mysite-ok", '2023-01-01'),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_hosts_cumulated_full_transformation(spark, source_df)
    expected_data = [
        HostActivity(
            "mysite",
            [date(2001, 1 ,1), date(2001, 2 ,1)]
        ),
        HostActivity(
            "mysite-ok",
            [date(2023, 1 ,1)]
        )
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)