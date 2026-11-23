
from pyspark.sql import DataFrame, Window, SparkSession
from youtube_channel_analytics_medallion.bronze import functions as f
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    row_number,
    to_timestamp,
)
from youtube_channel_analytics_medallion.metadata import watermark as wtm
from youtube_channel_analytics_medallion.bronze.schemas import COMMENT_SCHEMA
import argparse

_RAW = "raw"
_BRONZE = "bronze"
_DATASET_TYPE = "comments"
_TARGET_TABLE = "bronze.yt.comments"
_PARTITION_BY = ["date_of_data", "channel_id"]
_KEY_PARTITIONING = ["date_of_data", "channel_id", "video_id", "comment_id"]


def _checkpoint_path() -> str:
    base = wtm.get_checkpoint_path(_RAW, _DATASET_TYPE)
    return base.rstrip("/") + "/" + _date_of_data + "/"


def main():
    from youtube_channel_analytics_medallion.config.app_config import get_config
    from youtube_channel_analytics_medallion.metadata import log
    from youtube_channel_analytics_medallion.metadata.mode import get_mode

    global _date_of_data, _run_id, _channels, _mode, _reset_checkpoint
    global raw_valid, bronze_valid, bronze_err

    # =====================================================
    # SPARK SESSION BULIDER
    # =====================================================
    spark = (
        SparkSession.builder.appName("YouTube-Analytics-Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoOptimize.autoCompact", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    pipeline_name = "load_youtube_data"
    task_name = "bronze_comment"

    parser = argparse.ArgumentParser()
    parser.add_argument("--date_of_data", type=f.parse_date, default="")
    parser.add_argument("--max_workers", type=int, default=0,
                        help="parallel channel workers; 0 = one per channel")
    parser.add_argument("--reset_checkpoint", type=f.str_to_bool, default=False)
    parser.add_argument("--mode", choices=["full", "incremental", "reload", "reload_date"], default=None)
    parser.add_argument("--start_layer", choices=["raw", "bronze", "silver", "gold"], default="raw")
    parser.add_argument("--end_layer", choices=["raw", "bronze", "silver", "gold"], default="gold")
    args, _ = parser.parse_known_args()

    _date_of_data = args.date_of_data
    _run_id = get_config().run_id

    _start_layer = args.start_layer
    _end_layer = args.end_layer

    if _start_layer == "bronze" and _end_layer == "raw":
        raise f.LayerError(f"{_start_layer} can not be run before {_end_layer}")

    _run_id_ = "*" if (_start_layer in ("raw", "bronze") or _mode == "reload_date") else _run_id

    _reset_checkpoint = args.reset_checkpoint

    _mode = args.mode
    if _mode is None:
        # get_mode reads the watermark table, so it has to exist first
        wtm.ensure_watermark_table()
        _mode = get_mode(_date_of_data)

    _date_of_data_ = "*" if _mode in ("full", "reload") else _date_of_data

    if _reset_checkpoint or _mode in ("full", "reload"):
        wtm.reset_checkpoints((_RAW, _BRONZE), (_DATASET_TYPE,))

    
    log.log_start_step(task_name)


    # =====================================================
    # TRANSFORM BATCH
    # =====================================================
    def _transform_batch(batch_df: DataFrame, batch_id: int):
        for key in [
            "spark.sql.shuffle.partitions",
            "spark.sql.adaptive.enabled",
            "spark.sql.adaptive.coalescePartitions.enabled",
        ]:
            try:
                value = batch_df.sparkSession.conf.get(key)
            except Exception as e:
                value = f"NIE USTAWIONO / BŁĄD: {e}"

            print(f"{key}: {value}")

        batch_df = batch_df.select(
            f.get_date_of_data_column(_mode, _date_of_data),
            col("run_id").cast("string").alias("run_id"),
            col("comment_id").cast("string").alias("comment_id"),
            col("video_id").cast("string").alias("video_id"),
            col("channel_id").cast("string").alias("channel_id"),
            col("comment_author").cast("string").alias("comment_author"),
            col("comment_text").cast("string").alias("comment_text"),
            coalesce(col("comment_like_count").cast("int"), lit(0)).alias("comment_like_count"),
            to_timestamp(col("comment_publish_date")).alias("comment_publish_date"),
            to_timestamp(col("comment_update_date")).alias("comment_update_date"),
            f.parse_bool_column("comment_is_public", "comment_is_public"),
            f.parse_bool_column("comment_can_reply", "comment_can_reply"),
            coalesce(col("comment_reply_count").cast("int"), lit(0)).alias("comment_reply_count"),
            to_timestamp(col("watermark_date")).alias("watermark_date"),
            to_timestamp(col("etl_metadata.load_date")).alias("load_date"),
            col("etl_metadata.source").cast("string").alias("source")
        )

        f.assert_schema(batch_df, COMMENT_SCHEMA)
            
        latest = Window.partitionBy(*_KEY_PARTITIONING).orderBy(col("load_date").desc_nulls_last())
        batch_df = (
            batch_df.withColumn("row_num", row_number().over(latest))
                .filter(col("row_num") == 1)
                .drop("row_num")
        )

        if _mode == "reload_date" or batch_id == 0:
            writer = (
                batch_df.write
                .format("delta")
                .mode("overwrite")
                .option("partitionOverwriteMode", "dynamic")
                .option("mergeSchema", "true")
            )

            if not f.ensure_table_exists(spark, _TARGET_TABLE):
                (
                    writer
                    .partitionBy(*_PARTITION_BY)
                    .saveAsTable(_TARGET_TABLE)
                )
            else:
                writer.saveAsTable(_TARGET_TABLE)

        else:
            (
                batch_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(_TARGET_TABLE)
            )

    try:
        if _mode in ["full", "reload"] and f.ensure_table_exists(spark, _TARGET_TABLE):
            spark.sql(f"TRUNCATE TABLE {_TARGET_TABLE}")

        
        raw_valid, _ = wtm.get_data_dir_pattern(
            layer=_RAW,
            dataset_type=_DATASET_TYPE,
            date_of_data=_date_of_data_,
            run_id=_run_id_,
        )

        cloud_file = dict(ignoreCorruptFiles=True)
        cloud_file["cloudFiles.format"] = "json"
        cloud_file["pathGlobFilter"] = "*.jsonl"
        cloud_file["cloudFiles.inferColumnTypes"] = "true"
        cloud_file["cloudFiles.schemaLocation"] = _checkpoint_path()

        df_raw_files = (
            spark.readStream
            .format("cloudFiles")
            .options(**cloud_file)
            .load(raw_valid)
            .withColumn("run_id", lit(_run_id))
            .withColumnRenamed("_rescued_data", "rescued_data")
        )


        # ========================================================
        # STREAMING
        # ========================================================

        query = (
            df_raw_files
            .writeStream
            .foreachBatch(_transform_batch)
            .option("checkpointLocation", _checkpoint_path())
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
     
        log.log_end_step(task_name, "success")
        log.flush_logs_to_adls(task_name)
        spark.stop()
    except Exception as error:
        log.log_end_step(task_name, "failed", error)
        log.log_end_pipeline(pipeline_name, "failed", error)
        log.flush_logs_to_adls(task_name)
        spark.stop()
        raise error

if __name__ == "__main__":
    main()
