from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import datetime as dt

bronze_table_name = "my_db.health_tracker_bronze"
silver_table_name = "my_db.health_tracker_silver"


def write_silver_table(spark: SparkSession):
    """Write rows ingested yesterday into Silver table

    * Select rows ingested yesterday from Bronze table
    * Extract and transform the raw string to columns
    * Load data into Silver table
    """
    yesterday = dt.date.today() - dt.timedelta(1)
    bronze_df = (spark.table(bronze_table_name)
                 .filter(f"p_ingestdate = '{yesterday}'"))
    json_schema = "time TIMESTAMP, name STRING, steps INTEGER"
    bronze_augmented_df = (bronze_df
                           .withColumn("nested_json",
                                       f.from_json("value", json_schema)))
    silver_df = bronze_augmented_df.select(
        f.col("nested_json.time").alias("eventtime"),
        f.col("nested_json.name").alias("name"),
        f.col("nested_json.steps").alias("steps"),
        f.col("nested_json.time").cast("DATE").alias("p_eventdate"),
    )
    (silver_df.write.format("delta").mode("append").partitionBy("p_eventdate")
     .saveAsTable(silver_table_name))
