"""spark-tests.sql module.

Define test classes for Spark SQL:

* Queries on data frames are run regularly;

* Modification statements are just logged and does not modify data.
"""
from typing import List, Optional, Dict, Union

from pyspark.sql import SparkSession, DataFrame, DataFrameWriter, GroupedData
from pyspark.sql.types import StructType


class FakeSparkSession(SparkSession):
    """`SparkSession` proxy.

    * Queries on data frames are run regularly;

    * Modification statements are just logged and does not modify data.

    Attributes:
        real: real `SparkSession`.
        sql_queries: List of modification statements sent.
    """

    def __init__(self, real: SparkSession) -> None:
        self.real = real
        self.sql_queries: List[str] = []

    def clear(self) -> None:
        """Clear `sql_queries` list"""
        self.__init__(self.real)

    def table(self, table_name: str) -> "FakeDataFrame":
        """Returns specified table as `FakeDataFrame`.

        Delegates to `self.real`.
        Result is returned as a `FakeDataFrame`.
        This behavior may be changed by subclasses.
        """
        return FakeDataFrame(self.real.table(table_name))

    def sql(self, sql_statement: str) -> "FakeDataFrame":
        """Logs a `sql_statement`.

        Just appends `sql_statement` into `self.sql_queries`
        with no change to data.

        Returns: empty `FakeDataFrame`.

        Args:
            sql_statement
        """
        self.sql_queries.append(sql_statement)

        return self.createDataFrame((), StructType([]))

    def createDataFrame(self, data, schema=None) -> 'FakeDataFrame':
        """Creates a `FakeDataFrame`.

        Delegates creation to `self.real`

        Returns created `DataFrame` as a `FakeDataFrame`

        """
        return FakeDataFrame(self.real.createDataFrame(data, schema))


class FakeDataFrame(DataFrame):
    """`DataFrame` proxy.

    Attributes:
        real: real `DataFrame`.
    """

    def __init__(self, real: DataFrame) -> None:
        self.real = real

    def __getattribute__(self, item):
        """Get attribute or method.

        if item == `write`:

            return `FAKE_DF_WRITER`

        elif item is a `DataFrame`returning method:

            delegate method execution to `self.real`

            return the result as a `FakeDataFrame`

        else:

            delegate attribute getting to `self.real`
        """

        def wrap_fake_grouped(*args, **kwargs) -> FakeGroupedData:
            return FakeGroupedData(self.real.groupBy(*args, **kwargs))

        def wrap_fake_df(*args, **kwargs) -> FakeDataFrame:
            return FakeDataFrame(getattr(self.real, item)(*args, **kwargs))

        if item in {"real", "write"}:
            return super().__getattribute__(item)
        elif item in {"groupBy", "groupby"}:
            return wrap_fake_grouped
        elif item in {"alias",
                      "filter",
                      "drop",
                      "join",
                      "distinct",
                      "withColumn",
                      "select",
                      "orderBy", "sort",
                      "subtract",
                      "union",
                      "fillna"}:
            return wrap_fake_df
        else:
            attrib = getattr(self.real, item)
            return attrib

    @property
    def write(self) -> "FakeDFWriter":
        """Returns FAKE_DF_WRITER.

        Set `self` as the source `DataFrame`.
        """
        FAKE_DF_WRITER.input_df = self
        return FAKE_DF_WRITER


class FakeGroupedData(GroupedData):
    """`GroupedData` proxy.

    Attributes:
        real: real `GroupedData`.
    """

    def __init__(self, real: GroupedData):
        self.real = real

    def agg(self, *exprs) -> FakeDataFrame:
        """Compute aggregates.

        Delegate to `self.real`

        Return result as a `FakeDataFrame`

        """
        result = FakeDataFrame(self.real.agg(*exprs))
        return result

    def pivot(
        self, pivot_col: str, values: Optional[List[str]] = None
    ) -> GroupedData:
        """Pivots a column of the current `DataFrame`.

        Delegates to `self.real`

        Return result as a `FakeGroupedData`
        """
        result = FakeGroupedData(self.real.pivot(pivot_col, values))
        return result

    def sum(self, *cols: str) -> FakeDataFrame:
        """Compute the sum for each numeric columns for each group.

        Delegates to `self.real`

        Return result as `FakeDataFrame`
        """
        result = FakeDataFrame(self.real.sum(*cols))
        return result


class FakeDFWriter(DataFrameWriter):
    """Mock `DataFrameWriter`.

    Logs the rows written instead of actually writing the data.

    Singleton assumes that for each test case there is only one writer.

    Attributes:
        path:
            case writing to a file, the file path
        name:
            case writing to a table, the table name
        save_format:
            the format used to save.
        save_mode:
            specifies the behavior of the save operation: "error",
            "errorifexists", "append", "overwrite", "ignore"
        partition_by:
            names of partitioning columns
        **save_options:
            all other of partitioning columns.
        output:
            list of `Rows` "written".
    """

    def __init__(self):
        self.path = None
        self.name = None
        self.save_format = "parquet"
        self.save_mode = "errorifexists"
        self.partition_by = ()
        self.save_options: Dict[str, str] = {}
        self.input_df: Optional[DataFrame] = None
        self.output: List = []

    def clear(self):
        """Clear `self` to default values.

        `self.save_format` = "parquet"

        `self.save_mode` = "errorifexists"
        """
        self.__init__()

    def format(self, format: str) -> 'FakeDFWriter':
        """Logs format."""
        self.save_format = format
        return self

    def mode(self, mode: str) -> 'FakeDFWriter':
        """Logs mode."""
        self.save_mode = mode
        return self

    def partitionBy(self, *cols: str) -> 'FakeDFWriter':
        """Logs partition columns."""
        self.partition_by = cols
        return self

    def option(self, key: str, value: str) -> 'FakeDFWriter':
        """Logs a configuration option."""
        self.save_options[key] = value
        return self

    def options(self, **options: str) -> 'FakeDFWriter':
        """Logs configuration options."""
        self.save_options = options
        return self

    def _save(self, format, mode, partitionBy, **options):
        """Logs current `DataFrame` rows to `self.output`."""
        if format:
            self.save_format = format
        if mode:
            self.save_mode = mode
        if partitionBy:
            self.partition_by = partitionBy
        if options:
            self.save_options = options
        input_list = self.input_df.collect()
        self.output = self.output + input_list

    def save(self, path: Optional[str] = None,
             format: Optional[str] = None,
             mode: Optional[str] = None,
             partitionBy: Optional[List[str]] = None,
             **options: str) -> None:
        """Logs current `DataFrame` rows that would be written to a file."""
        self.path = path
        self.name = None
        self._save(format, mode, partitionBy, **options)

    def saveAsTable(
        self,
        name: str,
        format: Optional[str] = None,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        **options: str
    ) -> None:
        """Logs current `DataFrame` rows that would be written to a table."""
        self.path = None
        self.name = name
        self._save(format, mode, partitionBy, **options)


#: `FakeDFWriter` singleton instance.
FAKE_DF_WRITER = FakeDFWriter()
