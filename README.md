# spark-tests

A test framework, that defines several test doubles, to facilitate Python Spark application development.

## spark_tests.sql module

Defines the following test doubles:

* `FakeSparkSession`
    * Stubs  `sql(sql_query)`  method to only log the `sql_queries` , not sending them to database for execution;
    * `table(table_mame)` and `createDataFrame(data[, schema, samplingRatio, verifySchema])`  methods delegate execution to the real `SparkSession`, but returns a `FakeDataFrame` instead of a `DataFrame`;
    * `table(table_name)` is often overridden in a subclass to return a table from a fake test database.
* `FakeDataFrame`
    *  `write` returns a `FakeDFWriter`;
    * Other methods work just like a real `DataFrame`, but return `FakeDataFrame`s instead of `DataFrame`s;
* `FakeDFWriter`
    * Stubs a `DataFrameWriter` to only log `Row`s written, not writing them at all.

## spark_tests.delta module

Defines `FakeDeltaTable`, that stubs `merge(source, condition)` to only log the merge operation, changing no data.

## spark_tests.datetime module

Defines the following test doubles:

* `FakeDatetime`
    * Stubs `now()` method to return always a predefined `datetime`.
* `FakeDate`
    * Stubs `today()` method to return always a predefined `date`.