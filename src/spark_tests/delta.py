"""Delta table test classes.

Merges will be just logged with no change to the data.
"""
from delta.tables import DeltaTable, DeltaMergeBuilder
from typing import Optional, Mapping
from pyspark.sql import SparkSession, DataFrame


class FakeDeltaMergeBuilder(DeltaMergeBuilder):
    """Builds a fake `sql_query` to perform a Merge.

    `self.sql_query` is just for testing purposes
    and will error with a real `DeltaTable`.
    Therefore `target` table must be a `FakeDeltaTable`
    The source `DataFrame` will not be validated,
    being referred as source src in `self.sql_query`.
    Fake `sql_query` will be just logged on `execute`.

    Attributes:
        spark: `FakeSparkSession` injected
        sql_query: sql query being built
    """
    def __init__(self, target: 'FakeDeltaTable', source: DataFrame,
                 condition: str) -> None:
        self.spark = target.spark
        self.sql_query = f"MERGE INTO {target.name} "
        if target.alias_name:
            self.sql_query += f"{target.alias_name} "
        self.sql_query += f"USING source src "
        self.sql_query += f"ON {condition} "

    def whenMatchedUpdate(
            self, condition: Optional[str] = None, set: Mapping[str, str] = None
    ) -> 'FakeDeltaMergeBuilder':
        """Update a matched table row based on the rules defined by `set`.
        """
        self.sql_query += "WHEN MATCHED "
        if condition:
            self.sql_query += f"AND {condition} "
        self.sql_query += f"THEN "
        for k in set:
            self.sql_query += f"{k} = {set[k]}, "
        self.sql_query = self.sql_query[:-2] + " "
        return self

    def execute(self) -> None:
        """Log `sql_query`"""
        self.spark.sql(self.sql_query)


class FakeDeltaTable(DeltaTable):
    """`FakeDeltaTable`

    Logs merge operations with no change in the data.

    Attributes:
        spark: a `FakeSparkSession`
        name: name or path of the table
        alias_name: alias name of the table
    """
    def __init__(self, spark: SparkSession, name: Optional[str] = None) -> None:
        self.spark = spark
        self.name = name
        self.alias_name = None

    @classmethod
    def forPath(cls, spark: SparkSession, path: str) -> 'FakeDeltaTable':
        """Creates a `FakeDeltaTable` for given `path`."""
        result = cls(spark, name=path)
        return result

    @classmethod
    def forName(cls, spark: SparkSession, name: str) -> 'FakeDeltaTable':
        """Creates a `FakeDeltaTable` for given `name`."""
        result = cls(spark, name=name)
        return result

    def alias(self, alias_name: str) -> 'FakeDeltaTable':
        """Sets alias for `self`."""
        self.alias_name = alias_name
        return self

    def merge(self, source: DataFrame, condition: str) -> DeltaMergeBuilder:
        """Logs merging operation from `source` `DataFrame`
        on the given merge `condition`."""
        result = FakeDeltaMergeBuilder(self, source, condition)
        return result
