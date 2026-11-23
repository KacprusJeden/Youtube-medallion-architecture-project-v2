
from pyspark.sql import DataFrame, Window, SparkSession
from youtube_channel_analytics_medallion.bronze import functions as f
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    lower,
    row_number,
    to_timestamp
)
from youtube_channel_analytics_medallion.metadata import watermark as wtm
from youtube_channel_analytics_medallion.bronze.schemas import VIDEO_SCHEMA
import argparse

_RAW = "raw"
_BRONZE = "bronze"
_DATASET_TYPE = "videos"
_TARGET_TABLE = "bronze.yt.videos"
_PARTITION_BY = ["date_of_data", "channel_id"]
_KEY_PARTITIONING = ["date_of_data", "video_id", "channel_id"]


def _checkpoint_path() -> str:
    base = wtm.get_checkpoint_path(_RAW, _DATASET_TYPE)
    return base.rstrip("/") + "/" + _date_of_data + "/"


def main():
    from youtube_channel_analytics_medallion.config.app_config import get_config
    from youtube_channel_analytics_medallion.metadata import log
    from youtube_channel_analytics_medallion.metadata.mode import get_mode

    # =====================================================
    # SPARK SESSION BULIDER
    # =====================================================
    spark = (
        SparkSession.builder.appName("YouTube-Analytics-Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoOptimize.autoCompact", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )

    global _date_of_data, _run_id, _channels, _mode, _reset_checkpoint
    global raw_valid, bronze_valid, bronze_err

    pipeline_name = "load_youtube_data"
    task_name = "bronze_video"

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
        batch_df = batch_df.select(
            f.get_date_of_data_column(_mode, _date_of_data),
            col("run_id").cast("string").alias("run_id"),
            col("kind").cast("string").alias("kind"),
            col("etag").cast("string").alias("etag"),
            col("id").cast("string").alias("video_id"),
            to_timestamp(col("snippet.publishedAt")).alias("published_at"),
            col("snippet.channelId").cast("string").alias("channel_id"),
            col("snippet.title").cast("string").alias("title"),
            col("snippet.description").cast("string").alias("description"),
            col("snippet.thumbnails.default.url").cast("string").alias("default_url"),
            f.parse_thumbnails_dims("default"),
            col("snippet.thumbnails.medium.url").cast("string").alias("medium_url"),
            f.parse_thumbnails_dims("medium"),
            col("snippet.thumbnails.high.url").cast("string").alias("high_url"),
            f.parse_thumbnails_dims("high"),
            col("snippet.thumbnails.standard.url").cast("string").alias("standard_url"),
            f.parse_thumbnails_dims("standard"),
            col("snippet.thumbnails.maxres.url").cast("string").alias("maxres_url"),
            f.parse_thumbnails_dims("maxres"),
            col("snippet.channelTitle").cast("string").alias("channel_title"),
            col("snippet.categoryId").cast("int").alias("category_id"),
            col("snippet.liveBroadcastContent").cast("string").alias("live_broadcast_content"),
            coalesce(lower(col("snippet.defaultLanguage")).cast("string"), lit("en")).alias("default_language"),
            coalesce(lower(col("snippet.defaultAudioLanguage")).cast("string"), lit("en")).alias("default_audio_language"),
            f.parse_iso8601_duration("contentDetails.duration", "duration"),
            lower(col("contentDetails.dimension")).cast("string").alias("dimension"),
            lower(col("contentDetails.definition")).cast("string").alias("definition"),
            f.parse_bool_column("contentDetails.caption", "caption"),
            f.parse_bool_column("contentDetails.licensedContent", "licensed_content"),
            col("contentDetails.projection").cast("string").alias("projection"),
            coalesce(col("statistics.viewCount").cast("int"), lit(0)).alias("view_count"),
            coalesce(col("statistics.likeCount").cast("int"), lit(0)).alias("like_count"),
            coalesce(col("statistics.favoriteCount").cast("int"), lit(0)).alias("favorite_count"),
            coalesce(col("statistics.commentCount").cast("int"), lit(0)).alias("comment_count"),
            to_timestamp(col("etl_metadata.load_date")).alias("load_date"),
            col("etl_metadata.source").cast("string").alias("source")
        )

        f.assert_schema(batch_df, VIDEO_SCHEMA)
            
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
