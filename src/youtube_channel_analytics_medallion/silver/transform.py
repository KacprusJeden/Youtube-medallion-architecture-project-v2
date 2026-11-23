from pyspark.sql import DataFrame, SparkSession, Window, Column
from youtube_channel_analytics_medallion.metadata import watermark as wtm
from youtube_channel_analytics_medallion.silver import functions as fs
from youtube_channel_analytics_medallion.bronze import functions as fb
from youtube_channel_analytics_medallion.silver.schemas import (
    HUB_CHANNEL_SCHEMA,
    SAT_CHANNEL_SCHEMA,
    HUB_VIDEO_SCHEMA,
    SAT_VIDEO_SCHEMA,
    LINK_VIDEO_SCHEMA,
    HUB_COMMENT_SCHEMA,
    SAT_COMMENT_SCHEMA, 
    LINK_COMMENT_SCHEMA
)

from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    col, 
    current_timestamp,
    date_sub,
    lead,
    lit,
    when,
)

from delta.tables import DeltaTable
from typing import Literal, List
from functools import reduce
from operator import and_
import argparse


_SILVER = "silver"


def truncate_table(spark: SparkSession, table_name: str) -> None:
    if fs.ensure_table_exists(spark, table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")


def load_satellite_incremental(
    source_df: DataFrame,
    key_columns: str | List[str],
    date_of_data: Column,
) -> None:
    try:
    
        silver_table = DeltaTable.forName(spark, sat_tab)

        keys = [key_columns] if isinstance(key_columns, str) else key_columns

        if not keys:
            raise ValueError("At least one key column is required.")

        source_df = (
            source_df
            .withColumn("valid_from", date_of_data)
            .withColumn("valid_to", lit(None).cast("date"))
            .withColumn("is_current", lit(True))
        )

        current_df = (
            silver_table.toDF()
            .filter(col("is_current") == True | col("valid_to").isNull())
            .select(
                *keys,
                col("hash_diff").alias("target_hash_diff"),
                lit(True).alias("_target_exists"),
            )
        )

        comparison_df = (
            source_df.alias("src")
            .join(current_df.alias("tgt"), on=keys, how="left")
        )

        changed_df = (
            comparison_df
            .filter(
                col("tgt._target_exists").isNull()
                | ~col("src.hash_diff").eqNullSafe(col("tgt.target_hash_diff"))
            )
            .select("src.*")
        )

        if changed_df.isEmpty():
            return


        staged_expire = changed_df.withColumn(
            "_merge_action", lit("expire")
        )

        staged_insert = changed_df.withColumn(
            "_merge_action", lit("insert")
        )

        staged_df = staged_expire.unionByName(staged_insert)

        merge_condition = " AND ".join(
            f"t.`{key}` = s.`{key}`"
            for key in keys
        )

        merge_condition += (
            " AND t.is_current = true"
            " AND s._merge_action = 'expire'"
        )

        target_columns = silver_table.toDF().columns

        insert_values = {
            column: f"s.`{column}`"
            for column in target_columns
        }

        (
            silver_table.alias("t")
            .merge(
                staged_df.alias("s"),
                merge_condition,
            )
            .whenMatchedUpdate(
                set={
                    "valid_to": "date_sub(s.valid_from, 1)",
                    "is_current": "false",
                }
            )
            .whenNotMatchedInsert(
                condition="s._merge_action = 'insert'",
                values=insert_values,
            )
            .execute()
        )

    except Exception as e:
        raise e


def load_satellite_reload_date(
    source_df: DataFrame,
    dataset_type: str,
    key_columns: str | List[str],
    date_of_data: Column,
    schema: StructType,
) -> None:
    try:
        silver_table = DeltaTable.forName(spark, sat_tab)
        
        if source_df.isEmpty():
            return

        keys = [key_columns] if isinstance(key_columns, str) else key_columns

        source_df = (
            source_df
            .withColumn("valid_from", date_of_data)
            .withColumn("valid_to", lit(None).cast("date"))
            .withColumn("is_current", lit(True))
        )

        scd2_closes = fs.get_scd2_closes(
            dataset_type,
            silver_table,
            date_of_data,
        )

        reload_df = (
            source_df
            .join(scd2_closes, on=keys, how="left")
            .withColumns({
                "valid_to": date_sub(col("next_val"), 1),
                "is_current": col("next_val").isNull(),
            })
            .drop("next_val")
            .select(schema.fieldNames())
        )

        fs.assert_schema(reload_df, schema)

        join_condition = reduce(
            and_, 
            [col(f"t.{key}") == col(f"r.{key}") for key in keys]
        )


        (
            silver_table.alias("t")
            .merge(
                source=reload_df.alias("r"),
                condition=(
                    join_condition
                    & (col("t.valid_from") <= date_of_data)
                    & (col("t.valid_to").isNull() | (col("t.valid_to") >= date_of_data))
                )
            )
            .whenMatchedUpdate(
                set={
                    "valid_to": "r.valid_to",
                    "is_current": "r.valid_to IS NULL",
                    "hash_diff": "r.hash_diff"
                }
            )
            .execute()
        )

        silver_table.delete(col("valid_from") == date_of_data)

        reload_df.write.format("delta").mode("append").saveAsTable(sat_tab)

    except Exception as e:
        raise e


def transform_silver_satelite(
    mode: Literal["full", "reload", "reload_date", "incremental"], 
    dataset_type: str, 
    bronze_df: DataFrame, 
    silver_df: DataFrame, 
    date_of_data: Column | None = None
) -> None:
    try:
        hash_diff_column = "hash_diff"

        match dataset_type:
            case "channels":
                columns = {
                    hash_diff_column: 
                        fs.get_hash_values(
                        col("title"), col("view_count").cast("string"), 
                        col("subscriber_count").cast("string"), col("video_count").cast("string"), 
                        col("hidden_subscriber_count").cast("string").cast("string")
                    ),
                    "hk_channel": fs.get_hash_values(col("channel_id"))
                }
                schema = SAT_CHANNEL_SCHEMA
                key_partitioning = ["hk_channel"]

            case "videos":
                columns = {
                    hash_diff_column: 
                        fs.get_hash_values(
                            col("title"), col("view_count").cast("string"), 
                            col("like_count").cast("string"), col("comment_count").cast("string"), 
                            col("favorite_count").cast("string").cast("string")
                        ),
                    "hk_channel": fs.get_hash_values(col("channel_id")),
                    "hk_video": fs.get_hash_values(col("video_id"))
                }
                schema = SAT_VIDEO_SCHEMA
                key_partitioning = ["hk_channel", "hk_video"]

            case "comments":
                columns = {
                    hash_diff_column: 
                        fs.get_hash_values(
                            col("comment_text"), col("comment_update_date").cast("string"), 
                            col("comment_like_count").cast("string"), col("comment_reply_count").cast("string"),  
                        ),
                    "hk_channel": fs.get_hash_values(col("channel_id")),
                    "hk_video": fs.get_hash_values(col("video_id")),
                    "hk_comment": fs.get_hash_values(col("comment_id"))
                }
                schema = SAT_COMMENT_SCHEMA
                key_partitioning = ["hk_channel", "hk_video", "hk_comment"]

            case _:
                raise ValueError(f"Unknown dataset_type: {dataset_type}")

        bronze_df = bronze_df.withColumns(columns)

        if mode in ["reload_date", "incremental"] and date_of_data is not None:
            bronze_df = bronze_df.filter(col("date_of_data") == date_of_data)

        
        if mode in ["full", "reload"]:
            wnd = Window.partitionBy(*key_partitioning).orderBy(col("date_of_data").asc_nulls_first())
            
            scd2_condition = {
                "valid_from": col("date_of_data"),
                "valid_to": lead(col("date_of_data") - 1).over(wnd),
                "is_current": (
                    when(lead(col("date_of_data") - 1).over(wnd).isNull(), lit(True))
                    .otherwise(lit(False))
                )
            }

            bronze_df = (
                bronze_df
                .withColumns(scd2_condition)
                .select(schema.fieldNames())
            )

            fs.assert_schema(bronze_df, schema)
            bronze_df.write.format("delta").mode("overwrite").saveAsTable(sat_tab)

        elif mode == "incremental":
            load_satellite_incremental(
                source_df=bronze_df,
                key_columns=key_partitioning,
                date_of_data=date_of_data,
            )

        elif mode == "reload_date":
            load_satellite_reload_date(
                source_df=bronze_df,
                dataset_type=dataset_type,
                key_columns=key_partitioning,
                date_of_data=date_of_data,
                schema=schema
            )

    except Exception as e:
        raise e
    

def transform_silver_link(dataset_type: str, bronze_df: DataFrame, silver_df: DataFrame) -> None:
    if dataset_type == "channels":
        return
    
    match dataset_type:
        case "videos":
            hash_link_alias = "hk_link_video"
            columns = [
                fs.get_hash_values(col("channel_id"), col("video_id")).alias(hash_link_alias),
                fs.get_hash_values(col("channel_id")).alias("hk_channel"),
                fs.get_hash_values(col("video_id")).alias("hk_video")
            ]
            schema = LINK_VIDEO_SCHEMA

        case "comments":
            hash_link_alias = "hk_link_comment"
            columns = [
                fs.get_hash_values(col("video_id"), col("comment_id")).alias(hash_link_alias),
                fs.get_hash_values(col("video_id")).alias("hk_video"),
                fs.get_hash_values(col("comment_id")).alias("hk_comment")
            ]
            schema = LINK_COMMENT_SCHEMA

        case _:
            raise ValueError(f"Unknown dataset_type: {dataset_type}")


    link_bronze = bronze_df.select(
        *columns,
        current_timestamp().alias("load_date"),
        col("source").alias("record_source")
    ).dropDuplicates([hash_link_alias])

    fs.assert_schema(link_bronze, schema)

    link_bronze = link_bronze.join(
        silver_df, on=hash_link_alias, how="left_anti"
    )
    
    if not link_bronze.isEmpty():
        (
            link_bronze.write
            .format("delta")
            .mode("append")
            .saveAsTable(link_tab)
        )


def transform_silver_hub(dataset_type: str, bronze_df: DataFrame, silver_df: DataFrame) -> None:
    match dataset_type:
        case "channels":
            column = "channel_id"
            hash_alias = "hk_channel"
            schema = HUB_CHANNEL_SCHEMA
        case "videos":
            column = "video_id"
            hash_alias = "hk_video"
            schema = HUB_VIDEO_SCHEMA
        case "comments":
            column = "comment_id"
            hash_alias = "hk_comment"
            schema = HUB_COMMENT_SCHEMA
        case _:
            raise ValueError(f"Unknown dataset_type: {dataset_type}")
    
    
    hub_bronze = bronze_df.select(
        fs.get_hash_values(col(column)).alias(hash_alias),
        col(column),
        current_timestamp().alias("load_date"),
        col("source").alias("record_source") 
    ).dropDuplicates([hash_alias])

    fs.assert_schema(hub_bronze, schema)

    hub_bronze = hub_bronze.join(
        silver_df, on=hash_alias, how="left_anti"
    )

    if hub_bronze.take(1):
        (
            hub_bronze.write
            .format("delta")
            .mode("append")
            .saveAsTable(hub_tab)
        )

def main():
    from youtube_channel_analytics_medallion.config.app_config import get_config
    from youtube_channel_analytics_medallion.metadata import log

    global _date_of_data, _run_id, _mode, _dataset_type
    global hub_tab, sat_tab, link_tab
    global spark

    spark = (
        SparkSession.builder.appName("YouTube-Analytics-Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoOptimize.autoCompact", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.executor.instances", "2") # dataset are not too large; 2 executors - enough
        .getOrCreate()
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--date_of_data", type=fb.parse_date, default="")
    parser.add_argument("--max_workers", type=int, default=0,
                        help="parallel channel workers; 0 = one per channel")
    parser.add_argument("--mode", choices=["full", "incremental", "reload", "reload_date"], default=None)
    parser.add_argument("--dataset_type", choices=["channels", "videos", "comments"], default=None)
    
    args, _ = parser.parse_known_args()

    task_name = None

    _date_of_data = args.date_of_data
    _run_id = get_config().run_id
    _dataset_type = args.dataset_type

    _mode = args.mode

    if _dataset_type:
        task_name = f"silver_{_dataset_type}"

    _date_of_data_ = fs.get_date_of_data(_date_of_data) if _mode not in ["full", "reload"] else None


    log.log_start_step(task_name)

    bronze = fs.get_current_bronze(spark, _dataset_type, _date_of_data_)
    hub, sat, link = fs.get_current_silver(spark, _dataset_type)

    hub_tab, sat_tab, link_tab, _ = wtm.get_data_dir_pattern(_SILVER, _dataset_type)
    slv_tabs = [hub_tab, sat_tab, link_tab]

    try:
        if _mode in ["reload", "full"]:
            for t in slv_tabs:
                truncate_table(spark, t)

        transform_silver_hub(_dataset_type, bronze, hub)
        transform_silver_link(_dataset_type, bronze, link)
        transform_silver_satelite(_mode, _dataset_type, bronze, sat, _date_of_data_)

        log.log_end_step(task_name, "success")

    except Exception as e:
        log.log_end_step(task_name, "failed", e)
        raise e
    

if __name__ == "__main__":
    main()