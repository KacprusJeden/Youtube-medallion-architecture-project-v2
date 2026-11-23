from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)




HUB_CHANNEL_SCHEMA = StructType([
    StructField("hk_channel", StringType(), False),
    StructField("channel_id", StringType(), False),
    StructField("load_date", TimestampType(), False),
    StructField("record_source", StringType(), True),
])


SAT_CHANNEL_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("kind", StringType(), False),
    StructField("etag", StringType(), False),
    StructField("hk_channel", StringType(), False),
    StructField("title", StringType(), False),
    StructField("description", StringType(), False),
    StructField("custom_url", StringType(), False),
    StructField("published_at", TimestampType(), False),
    StructField("likes", StringType(), True),
    StructField("uploads", StringType(), False),
    StructField("default_url", StringType(), True),
    StructField("default_dims", StringType(), True),
    StructField("medium_dims", StringType(), True),
    StructField("high_dims", StringType(), True),
    StructField("country", StringType(), True),
    StructField("view_count", LongType(), True),
    StructField("subscriber_count", LongType(), True),
    StructField("hidden_subscriber_count", IntegerType(), True),
    StructField("video_count", IntegerType(), True),
    StructField("channel", StringType(), True),
    StructField("valid_from", DateType(), True),
    StructField("valid_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("hash_diff", StringType(), True)
])




HUB_VIDEO_SCHEMA = StructType([
    StructField("hk_video", StringType(), False),
    StructField("video_id", StringType(), False),
    StructField("load_date", TimestampType(), False),
    StructField("record_source", StringType(), True),
])


SAT_VIDEO_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("kind", StringType(), False),
    StructField("etag", StringType(), False),
    StructField("hk_video", StringType(), False),
    StructField("hk_channel", StringType(), False),
    StructField("published_at", TimestampType(), False),
    StructField("title", StringType(), False),
    StructField("description", StringType(), True),
    StructField("default_url", StringType(), False),
    StructField("default_dims", StringType(), False),
    StructField("medium_url", StringType(), False),
    StructField("medium_dims", StringType(), False),
    StructField("high_url", StringType(), False),
    StructField("high_dims", StringType(), False),
    StructField("standard_url", StringType(), False),
    StructField("standard_dims", StringType(), False),
    StructField("maxres_url", StringType(), False),
    StructField("maxres_dims", StringType(), False),
    StructField("channel_title", StringType(), False),
    StructField("category_id", IntegerType(), True),
    StructField("live_broadcast_content", StringType(), True),
    StructField("default_language", StringType(), False),
    StructField("default_audio_language", StringType(), False),
    StructField("duration", StringType(), False),
    StructField("dimension", StringType(), False),
    StructField("definition", StringType(), True),
    StructField("caption", BooleanType(), True),
    StructField("licensed_content", BooleanType(), False),
    StructField("projection", StringType(), False),
    StructField("view_count", IntegerType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("favorite_count", IntegerType(), False),
    StructField("comment_count", IntegerType(), False),
    StructField("valid_from", DateType(), True),
    StructField("valid_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("hash_diff", StringType(), True),
])


LINK_VIDEO_SCHEMA = StructType([
    StructField("hk_link_video", StringType(), False),
    StructField("hk_channel", StringType(), False),
    StructField("hk_video", StringType(), False),
    StructField("load_date", TimestampType(), False),
    StructField("record_source", StringType(), True),
])




HUB_COMMENT_SCHEMA = StructType([
    StructField("hk_comment", StringType(), False),
    StructField("comment_id", StringType(), False),
    StructField("load_date", TimestampType(), False),
    StructField("record_source", StringType(), True),
])


SAT_COMMENT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("hk_comment", StringType(), False),
    StructField("hk_video", StringType(), False),
    StructField("hk_channel", StringType(), False),
    StructField("comment_author", StringType(), False),
    StructField("comment_text", StringType(), False),
    StructField("comment_like_count", IntegerType(), False),
    StructField("comment_publish_date", TimestampType(), False),
    StructField("comment_update_date", TimestampType(), False),
    StructField("comment_is_public", BooleanType(), False),
    StructField("comment_can_reply", BooleanType(), False),
    StructField("comment_reply_count", IntegerType(), False),
    StructField("watermark_date", TimestampType(), False),
    StructField("valid_from", DateType(), True),
    StructField("valid_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("hash_diff", StringType(), True),
])


LINK_COMMENT_SCHEMA = StructType([
    StructField("hk_link_comment", StringType(), False),
    StructField("hk_video", StringType(), False),
    StructField("hk_comment", StringType(), False),
    StructField("load_date", TimestampType(), False),
    StructField("record_source", StringType(), True),
])