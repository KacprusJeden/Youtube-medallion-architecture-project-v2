from pyspark.sql.functions import (
    broadcast,
    col,
    lit,
    min,
    expr,
    to_date
)
from pyspark.sql import Column, SparkSession, DataFrame
from delta.tables import DeltaTable
from typing import Literal

def get_date_of_data(date_of_data: str | None) -> Column:
    if date_of_data:
        return to_date(lit(date_of_data), "yyyy-MM-dd")
    else:
        return expr("current_date()")


def ensure_table_exists(spark: SparkSession, table_name: str):
    try:
        return spark.catalog.tableExists(table_name)
    except:
        return False


def get_channels(satelite: DataFrame, mode: Literal["full", "reload", "reload_date", "incremental"],
        date_of_data: Column | None = None) -> DataFrame:

    ch = (
        satelite
        .select(
            col("valid_from").alias("ch_valid_from"), col("valid_to").alias("ch_valid_to"),
            col("hk_channel"), col("title").alias("channel_name"), col("custom_url"), 
            col("description"), col("published_at").alias("channel_publish_date"), 
            col("view_count").alias("channel_view_count"), col("subscriber_count").alias("channel_subscrb_count"),
            col("hidden_subscriber_count").alias("channel_hidden_subscrb_count"), col("video_count")
        )
    )

    if mode == "reload_date":
        ch = ch.filter(
            (col("ch_valid_from") <= date_of_data)
            & (
                col("ch_valid_to").isNull()
                | (col("ch_valid_to") >= date_of_data)
            )
        )

    elif mode == "incremental":
        ch = ch.filter(
            col("ch_valid_to").isNull()
        )


    else:
        pass


    return broadcast(ch)


def get_videos(satelite: DataFrame, link: DataFrame, mode: Literal["full", "reload", "reload_date", "incremental"],
        date_of_data: Column | None = None) -> DataFrame:
    
    vid = (
        satelite
        .select(
            col("valid_from").alias("vid_valid_from"), col("valid_to").alias("vid_valid_to"),
            col("hk_video"), col("hk_channel"), col("published_at").alias("video_publish_date"), 
            col("title").alias("video_title"), col("description").alias("video_description"), 
            col("category_id").alias("video_category_id"),col("duration").alias("video_duration"), 
            col("caption").alias("video_caption"), col("default_language").alias("video_dflt_language"), 
            col("default_audio_language").alias("video_dflt_audio_language"),
            col("view_count").alias("video_view_count"), col("like_count").alias("video_like_count"),
            col("favorite_count").alias("video_favorite_count"), col("comment_count").alias("video_comment_count")
        )
    )

    if mode == "reload_date":
        vid = vid.filter(
            (col("vid_valid_from") <= date_of_data)
            & (
                col("vid_valid_to").isNull()
                | (col("vid_valid_to") >= date_of_data)
            )
        )

    elif mode == "incremental":
        vid = vid.filter(
            col("vid_valid_to").isNull()
        )


    else:
        pass

    return (
        vid.alias("s").join(
            link.alias("l"),
            how="semi",
            on=col("s.hk_video") == col("l.hk_video")
        )
    )


def get_comments(satelite: DataFrame, link: DataFrame, mode: Literal["full", "reload", "reload_date", "incremental"],
        date_of_data: Column | None = None) -> DataFrame:
    cm = (
        satelite
        .select(
            col("valid_from").alias("com_valid_from"), col("valid_to").alias("com_valid_to"),
            col("hk_comment"), col("hk_video"), col("comment_text"), col("comment_author"),  
            col("comment_publish_date"), col("comment_update_date"), col("comment_is_public").alias("is_public"), 
            col("comment_can_reply").alias("can_reply"),  col("comment_like_count"), 
            col("comment_reply_count"))
    )

    if mode == "reload_date":
        cm = cm.filter(
            (col("com_valid_from") <= date_of_data)
            & (
                col("com_valid_to").isNull()
                | (col("com_valid_to") >= date_of_data)
            )
        )

    elif mode == "incremental":
        cm = cm.filter(
            col("com_valid_to").isNull()
        )


    else:
        pass

    return (
        cm.alias("s").join(
            link.alias("l"),
            how="semi",
            on=(col("s.hk_comment") == col("l.hk_comment")) & (col("s.hk_video") == col("l.hk_video"))
        )
    )


def get_scd2_closes(gold_table: DeltaTable, date_of_data: Column) -> DataFrame:
    cols = ["hk_comment", "hk_video", "hk_channel"]
    
    try:
        closes_df = (
            gold_table.toDF()
            .filter(col("valid_from") >= date_of_data)
            .groupBy(*cols)
            .agg(min("valid_from").alias("next_val"))
            .cache()
        )
    except Exception as e:
        raise e

    return closes_df