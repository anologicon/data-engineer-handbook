from chispa.dataframe_comparer import *
from ..jobs.actors_history_scd_full_load import do_actors_history_scd_full_load_transformation
from collections import namedtuple
from datetime import date
Actor = namedtuple("Actor", "actorid actor quality_class is_active year")
ActorScd = namedtuple("ActorScd", "actorid actor quality_class is_active start_date end_date")


def test_scd_generation(spark):
    source_data = [
        Actor("1", "Zeca", "A", True, 2001),
        Actor("1", "Zeca", "A", False, 2002),
        Actor("2", "Vilson", "B", True, 2025),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_history_scd_full_load_transformation(spark, source_df)
    expected_data = [
        ActorScd("1", "Zeca", "A", True, 2001, 2002),
        ActorScd("1", "Zeca", "A", False, 2002, 9999),
        ActorScd("2", "Vilson", "B", True, 2025, 9999),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)