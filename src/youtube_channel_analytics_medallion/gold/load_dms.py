from youtube_channel_analytics_medallion.bronze import functions as fb
from youtube_channel_analytics_medallion.silver import functions as fs
from youtube_channel_analytics_medallion.gold import functions as fg

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from delta.tables import DeltaTable
from typing import Literal
from functools import reduce
from operator import and_


STATISTICS_TABLE = "gold.yt.youtube_statistics_obt"
NUM_STATISTICS_TABLE = "gold.yt.youtube_num_statistics_obt"
ModeType = Literal["full", "reload", "reload_date", "incremental"]


def truncate_table(spark: SparkSession, table_name: str) -> None:
    if fg.ensure_table_exists(spark, table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")



def get_stg_silver_current(channels: DataFrame, videos: DataFrame, comments: DataFrame):

    def _intervals_overlap(start_a: Column, end_a: Column, start_b: Column, end_b: Column) -> Column:
        inf_date = F.lit("9999-12-31").cast("date")
        a_start = F.coalesce(start_a, F.lit("1900-01-01").cast("date"))
        a_end = F.coalesce(end_a, inf_date)
        b_start = F.coalesce(start_b, F.lit("1900-01-01").cast("date"))
        b_end = F.coalesce(end_b, inf_date)
        return (a_start <= b_end) & (b_start <= a_end)

    ch = channels.alias("ch")
    vid = videos.alias("vid")
    com = comments.alias("com")

    stage = (
        ch.join(
            vid,
            how="left",
            on=(
                F.col("ch.hk_channel").eqNullSafe(F.col("vid.hk_channel"))
                & _intervals_overlap(
                    F.col("ch.ch_valid_from"),
                    F.col("ch.ch_valid_to"),
                    F.col("vid.vid_valid_from"),
                    F.col("vid.vid_valid_to"),
                )
            ),
        )
        .join(
            com,
            how="left",
            on=(
                F.col("vid.hk_video").eqNullSafe(F.col("com.hk_video"))
                & _intervals_overlap(
                    F.col("vid.vid_valid_from"),
                    F.col("vid.vid_valid_to"),
                    F.col("com.com_valid_from"),
                    F.col("com.com_valid_to"),
                )
            ),
        )
        .select(
            F.col("ch.hk_channel"),
            F.col("vid.hk_video"),
            F.col("com.hk_comment"),
            F.col("ch.channel_name"),
            F.col("ch.custom_url"),
            F.col("ch.description"),
            F.col("ch.channel_publish_date"),
            F.col("vid.video_publish_date"),
            F.col("vid.video_title"),
            F.col("vid.video_description"),
            F.col("vid.video_category_id"),
            F.col("vid.video_duration"),
            F.col("vid.video_caption"),
            F.col("vid.video_dflt_language"),
            F.col("vid.video_dflt_audio_language"),
            F.substring(F.col("com.comment_text"), 1, 2000).alias("comment_text"),
            F.col("com.comment_author"),
            F.col("com.comment_publish_date"),
            F.col("com.comment_update_date"),
            F.col("com.is_public"),
            F.col("com.can_reply"),
            F.col("ch.channel_view_count"),
            F.col("ch.channel_subscrb_count"),
            F.col("ch.channel_hidden_subscrb_count"),
            F.col("ch.video_count"),
            F.col("vid.video_view_count"),
            F.col("vid.video_like_count"),
            F.col("vid.video_favorite_count"),
            F.col("vid.video_comment_count"),
            F.col("com.comment_like_count"),
            F.col("com.comment_reply_count"),
            F.greatest(
                F.col("ch.ch_valid_from"),
                F.col("vid.vid_valid_from"),
                F.col("com.com_valid_from"),
            ).alias("valid_from"),
            F.least(
                F.col("ch.ch_valid_to"),
                F.col("vid.vid_valid_to"),
                F.col("com.com_valid_to"),
            ).alias("valid_to"),
            F.least(
                F.col("ch.ch_valid_to"),
                F.col("vid.vid_valid_to"),
                F.col("com.com_valid_to"),
            )
            .isNull()
            .alias("is_current"),
            fs.get_hash_values(
                F.coalesce(F.trim(F.upper(F.col("ch.channel_name"))), F.lit("")),
                F.coalesce(F.trim(F.upper(F.col("vid.video_title"))), F.lit("")),
                F.coalesce(F.trim(F.upper(F.col("com.comment_text"))), F.lit("")),
            ).alias("hash_diff"),
        )
        .filter(F.col("valid_to").isNull() | (F.col("valid_from") <= F.col("valid_to")))
    )

    return stage


def load_youtube_statistics_incremental(stage: DataFrame, target_table: str):
    try:
        gold_table = DeltaTable.forName(spark, target_table)

        keys = ["hk_channel", "hk_video", "hk_comment"]

        if not keys:
            raise ValueError("At least one key column is required.")

        current_df = (
            gold_table.toDF()
            .filter(F.col("is_current") == True | F.col("valid_to").isNull())
            .select(
                *keys,
                F.col("hash_diff").alias("target_hash_diff"),
                F.lit(True).alias("_target_exists"),
            )
        )

        comparison_df = (
            stage.alias("src")
            .join(current_df.alias("tgt"), on=keys, how="left")
        )

        changed_df = (
            comparison_df
            .filter(
                F.col("tgt._target_exists").isNull()
                | ~F.col("src.hash_diff").eqNullSafe(F.col("tgt.target_hash_diff"))
            )
            .select("src.*")
        )

        if changed_df.isEmpty():
            return


        staged_expire = changed_df.withColumn(
            "_merge_action", F.lit("expire")
        )

        staged_insert = changed_df.withColumn(
            "_merge_action", F.lit("insert")
        )

        staged_df = staged_expire.unionByName(staged_insert)

        merge_condition = " AND ".join(
            f"t.`{key}` IS NOT DISTINCT FROM s.`{key}`"
            for key in keys
        )

        merge_condition += (
            " AND t.is_current = true"
            " AND s._merge_action = 'expire'"
        )

        target_columns = gold_table.toDF().columns

        insert_values = {
            column: f"s.`{column}`"
            for column in target_columns
        }

        (
            gold_table.alias("t")
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


def load_youtube_statistics_reload_date(stage: DataFrame, target_table: str, date_of_data: Column):
    gold_table = DeltaTable.forName(spark, target_table)
            
    if stage.isEmpty():
        return

    keys = ["hk_channel", "hk_video", "hk_comment"]

    scd2_closes = fg.get_scd2_closes(gold_table, date_of_data)

    reload_df = (
        stage.alias("s")
        .join(scd2_closes.alias("g"), on=keys, how="left")
        .withColumns({
            "valid_to": F.date_sub(F.col("g.next_val"), 1),
            "is_current": F.col("g.next_val").isNull(),
        })
        .drop("next_val")
        .select(
            F.col("valid_from"), F.col("valid_to"), F.col("run_id"),
            F.col("hk_channel"), F.col("hk_video"), F.col("hk_comment"),
            F.col("channel_name"), F.col("custom_url"), F.col("description"), F.col("channel_publish_date"),
            F.col("video_publish_date"), F.col("video_title"), F.col("video_description"),
            F.col("video_category_id"), F.col("video_duration"), F.col("video_caption"), F.col("video_dflt_language"),
            F.col("video_dflt_audio_language"), F.col("comment_text"),
            F.col("comment_author"), F.col("comment_publish_date"), F.col("comment_update_date"),
            F.col("is_public"), F.col("can_reply"), F.col("is_current"), F.col("hash_diff")
        )
    )

    join_condition = reduce(
        and_, [F.col(f"t.{key}").eqNullSafe(F.col(f"r.{key}")) for key in keys]
    )

    (
        gold_table.alias("t")
        .merge(
            source=reload_df.alias("r"),
            condition=(
                join_condition
                & (F.col("t.valid_from") <= date_of_data)
                & (F.col("t.valid_to").isNull() | (F.col("t.valid_to") >= date_of_data))
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

    (
        reload_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"valid_from = CAST('{_date_of_data}' AS DATE)")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def load_youtube_statistics(stage: DataFrame, target_table: str, mode: Literal["full", "reload", "reload_date", "incremental"],
    date_of_data: Column, run_id: str):

    stage = (
        stage.select(
            F.col("valid_from"), F.col("valid_to"), F.lit(run_id).alias("run_id"),
            F.col("hk_channel"), F.col("hk_video"), F.col("hk_comment"),
            F.col("channel_name"), F.col("custom_url"), F.col("description"), F.col("channel_publish_date"),
            F.col("video_publish_date"), F.col("video_title"), F.col("video_description"),
            F.col("video_category_id"), F.col("video_duration"), F.col("video_caption"), F.col("video_dflt_language"),
            F.col("video_dflt_audio_language"), F.col("comment_text"),
            F.col("comment_author"), F.col("comment_publish_date"), F.col("comment_update_date"),
            F.col("is_public"), F.col("can_reply"), F.col("is_current"), F.col("hash_diff")
        )
    )

    if mode in ("full", "reload"):
        (
            stage
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

    elif mode == "reload_date":
        load_youtube_statistics_reload_date(stage, target_table, date_of_data)

    else:
        load_youtube_statistics_incremental(stage, target_table)


def load_youtube_num_statistics(stage: DataFrame, target_table: str, mode: Literal["full", "reload", "reload_date", "incremental"],
    date_of_data: Column, run_id: str):

    if mode in ("full", "reload"):
        date_of_data = F.col("valid_from")

    num_stats = (
        stage.select(
            date_of_data.alias("date_of_data"), F.lit(run_id).alias("run_id"), 
            F.col("hk_channel"), F.col("hk_video"), F.col("hk_comment"),
            F.col("channel_view_count"), F.col("channel_subscrb_count"), F.col("channel_hidden_subscrb_count"), 
            F.col("video_count"), F.col("video_view_count"), F.col("video_like_count"), F.col("video_favorite_count"), 
            F.col("video_comment_count"), F.col("comment_like_count"), F.col("comment_reply_count")
        )
    )

    (
        num_stats.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"date_of_data = CAST('{_date_of_data}' AS DATE)")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def main():
    from youtube_channel_analytics_medallion.config.app_config import get_config
    from youtube_channel_analytics_medallion.metadata import log
    import argparse

    global _date_of_data, _run_id, _mode, _dataset_type
    global hub_tab, sat_tab, link_tab
    global spark

    spark = (
        SparkSession.builder.appName("YouTube-Analytics-Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoOptimize.autoCompact", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.executor.instances", "2") # dataset are not too large; 2 executors - enough
        .getOrCreate()
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--date_of_data", type=fb.parse_date, default="")
    parser.add_argument("--max_workers", type=int, default=0,
                        help="parallel channel workers; 0 = one per channel")
    parser.add_argument("--mode", choices=["full", "incremental", "reload", "reload_date"], default=None)

    args, _ = parser.parse_known_args()

    task_name = None

    _date_of_data = args.date_of_data
    _run_id = get_config().run_id

    _mode = args.mode

    task_name = f"gold_yt_statistics"

    _date_of_data_ = fg.get_date_of_data(_date_of_data) if _mode not in ["full", "reload"] else None


    log.log_start_step(task_name)


    _, sat_ch, _ = fs.get_current_silver(spark, "channels")
    _, sat_v, link_v = fs.get_current_silver(spark, "videos")
    _, sat_cm, link_cm = fs.get_current_silver(spark, "comments")

    gld_tabs = [STATISTICS_TABLE, NUM_STATISTICS_TABLE]

    try:
        if _mode in ["reload", "full"]:
            for t in gld_tabs:
                truncate_table(spark, t)

        channels = fg.get_channels(sat_ch, _mode, _date_of_data_)
        videos = fg.get_videos(sat_v, link_v, _mode, _date_of_data_)
        comments = fg.get_comments(sat_cm, link_cm, _mode, _date_of_data_)

        stg_silver_current = get_stg_silver_current(channels, videos, comments)

        load_youtube_statistics(stg_silver_current, STATISTICS_TABLE, _mode, _date_of_data_, _run_id)
        load_youtube_num_statistics(stg_silver_current, NUM_STATISTICS_TABLE, _mode, _date_of_data_, _run_id)

        log.log_end_step(task_name, "success")

    except Exception as e:
        log.log_end_step(task_name, "failed", e)
        raise e
    

if __name__ == "__main__":
    main()