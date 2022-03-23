"""Delta table test classes.

Merges operations will be just logged with no change to the database.
"""
from delta.tables import DeltaTable, DeltaMergeBuilder
from typing import Optional, Dict

from spark_tests.sql import FakeDataFrame, FakeSparkSession


class FakeDeltaTable(DeltaTable):
    """Logs merge operations with no change in the data.

    Attributes:
        spark: `FakeSparkSession`
        name: Optional name of the table
        path: Optional path of the table
        alias_name: alias name of the table
        source: `DataFrame` to merge
    """
    def __init__(self, spark: FakeSparkSession) -> None:
        self.spark = spark
        self.name = None
        self.path = None
        self.alias_name = None
        self.source = None

    @classmethod
    def forPath(cls, spark: FakeSparkSession, path: str) -> 'FakeDeltaTable':
        """Creates a `FakeDeltaTable` for given `path`."""
        result = cls(spark)
        result.path = path
        return result

    @classmethod
    def forName(cls, spark: FakeSparkSession, name: str) -> 'FakeDeltaTable':
        """Creates a `FakeDeltaTable` for given `name`."""
        result = cls(spark)
        result.name = name
        return result

    def alias(self, alias_name: str) -> 'FakeDeltaTable':
        """Sets alias for `self`."""
        self.alias_name = alias_name
        return self

    def merge(self, source: FakeDataFrame,
              condition: str) -> 'FakeDeltaMergeBuilder':
        """Start building `FakeDeltaMerge` from `source` `DataFrame`
        on the given merge `condition`."""
        result = FakeDeltaMergeBuilder(self, source, condition)
        return result


class FakeDeltaMerge:
    """Log of a merge operation.

    Logs merge parameters to check later in a test case,
    but does not execute the merge.

    Attributes:
        target:
            Target `DeltaTable`
        source: `DataFrame`
            Source `DataFrame`
        condition:
            Condition to match source rows with the target rows
        matched_action: {"UPDATE", "DELETE"}
            Optional action to be done on matched rows
        matched_condition:
            Optional condition to perform not_matched_action
        matched_update:
            Defines the rules of setting the values of columns
            that need to be updated.
        not_matched_action: {"INSERT"}
            Optional action to be done on not matched rows
        not_matched_condition:
            Optional condition to perform not_matched_action
        not_matched_insert:
            Defines the rules of setting the values of columns
            that need to be inserted.
        is_executed:
            Flag of execution.

    """
    def __init__(self) -> None:
        self.target: FakeDeltaTable = None
        self.source: FakeDataFrame = None
        self.condition: str = None
        self.matched_action: Optional[str] = None
        self.matched_condition: Optional[str] = None
        self.matched_update: Dict[str, str] = {}
        self.not_matched_action: Optional[str] = None
        self.not_matched_condition: Optional[str] = None
        self.not_matched_insert: Dict[str, str] = {}
        self.is_executed = False

    def clear(self) -> None:
        """Clear `self`."""
        self.__init__()


#: `FakeDeltaMerge` singleton instance.
FAKE_DELTA_MERGE = FakeDeltaMerge()


class FakeDeltaMergeBuilder(DeltaMergeBuilder):
    """Builds a `FakeDeltaMerge`.
    """
    def __init__(self, target: FakeDeltaTable, source: FakeDataFrame,
                 condition: str) -> None:
        FAKE_DELTA_MERGE.target = target
        FAKE_DELTA_MERGE.source = source
        FAKE_DELTA_MERGE.condition = condition

    def whenMatchedUpdate(
            self, condition: Optional[str] = None,
            set: Optional[Dict[str, str]] = None
    ) -> 'FakeDeltaMergeBuilder':
        FAKE_DELTA_MERGE.matched_action = "UPDATE"
        FAKE_DELTA_MERGE.matched_condition = condition
        FAKE_DELTA_MERGE.matched_update = set
        return self

    def whenMatchedDelete(
            self, condition: Optional[str] = None
    ) -> 'FakeDeltaMergeBuilder':
        FAKE_DELTA_MERGE.matched_action = "DELETE"
        FAKE_DELTA_MERGE.matched_condition = condition
        return self

    def whenNotMatchedInsert(
            self, condition: Optional[str] = None,
            values: Optional[Dict[str, str]] = None
    ) -> 'FakeDeltaMergeBuilder':
        FAKE_DELTA_MERGE.not_matched_action = "INSERT"
        FAKE_DELTA_MERGE.not_matched_condition = condition
        FAKE_DELTA_MERGE.not_matched_insert = values
        return self

    def execute(self) -> None:
        FAKE_DELTA_MERGE.is_executed = True
