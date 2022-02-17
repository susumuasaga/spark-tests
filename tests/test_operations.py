from pyspark.sql import Row
import datetime as dt

from tests import operations
from spark_tests.sql import FakeSparkSession, FAKE_DF_WRITER
from spark_tests.datetime import FakeDate


def test_write_silver_table(fake_spark: FakeSparkSession,
                            mock_date: FakeDate) -> None:
    FAKE_DF_WRITER.clear()
    operations.write_silver_table(fake_spark)
    assert FAKE_DF_WRITER.name == "my_db.health_tracker_silver"
    assert FAKE_DF_WRITER.path is None
    assert FAKE_DF_WRITER.save_format == "delta"
    assert FAKE_DF_WRITER.save_mode == "append"
    assert FAKE_DF_WRITER.partition_by == ("p_eventdate",)
    assert FAKE_DF_WRITER.is_saved
    expected_source = [Row(eventtime=dt.datetime(2021, 3, 19, 6),
                           name="Armando Clemente", steps=1245,
                           p_eventdate=dt.date(2021, 3, 19)),
                       Row(eventtime=dt.datetime(2021, 3, 19, 6),
                           name="Meallan O'Conarain", steps=510,
                           p_eventdate=dt.date(2021, 3, 19))]
    assert FAKE_DF_WRITER.source.collect() == expected_source
