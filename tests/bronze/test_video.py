from unittest.mock import patch, MagicMock
from youtube_channel_analytics_medallion.bronze import video as videos
from types import SimpleNamespace
import pytest

def test_spark_session(mock_spark):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("YouTube-Analytics-Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    assert spark is mock_spark


def test_checkpoint_path():
    dataset_type = "videos"
    expected_base = f"abfss://container/checkpoints/raw/{dataset_type}/"

    with patch.object(
        videos.wtm,
        "get_checkpoint_path",
        return_value=expected_base
    ) as mock_get_checkpoint_path:

        videos._date_of_data = "2026-11-02"

        result = videos._checkpoint_path()

    mock_get_checkpoint_path.assert_called_once_with("raw", dataset_type)

    assert result == f"abfss://container/checkpoints/raw/{dataset_type}/2026-11-02/"


def test_read_from_autoloader(mock_spark):
    args = SimpleNamespace(
        date_of_data="2026-11-02",
        run_id="test_run_001",
        max_workers=0,
        reset_checkpoint=False,
        mode="incremental",
        start_layer="raw",
        end_layer="gold",
    )

    # Mock CLI arguments
    parser_mock = MagicMock()
    parser_mock.parse_known_args.return_value = (args, [])

    # Mock DataFrame
    mock_df = MagicMock(name="raw_dataframe")

    # Mock Auto Loader
    mock_spark.readStream.format.return_value = mock_spark.readStream
    mock_spark.readStream.options.return_value = mock_spark.readStream
    mock_spark.readStream.load.return_value = mock_df

    mock_df.withColumn.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df

    # Mock streaming query
    mock_query = MagicMock(name="streaming_query")
    mock_df.writeStream.foreachBatch.return_value = mock_df.writeStream
    mock_df.writeStream.option.return_value = mock_df.writeStream
    mock_df.writeStream.trigger.return_value = mock_df.writeStream
    mock_df.writeStream.start.return_value = mock_query

    raw_path = "abfss://container/raw/videos/2026-11-02/"
    checkpoint_path = "abfss://container/checkpoints/raw/videos/2026-11-02/"
    log_path = "abfss://metadata/logs/youtube-api/test_run_001.log"

    with (
        patch.object(videos.argparse, "ArgumentParser") as mock_parser,
        patch.object(videos, "spark", mock_spark, create=True),
        patch.object(videos.wtm, "get_data_dir_pattern",
                     return_value=(raw_path, None)) as mock_get_data_dir,
        patch.object(videos, "_checkpoint_path",
                     return_value=checkpoint_path),
        patch(
            "youtube_channel_analytics_medallion.config.app_config.get_config",
            return_value=SimpleNamespace(run_id="test_run_001", log_path=log_path),
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.mode.get_mode",
            return_value="incremental",
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.log.log_start_step"
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.log.log_end_step"
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.log.flush_logs_to_adls",
            return_value=SimpleNamespace(run_id="test_run_001",log_path=log_path)
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.log.log_end_pipeline"
        ),
    ):
        mock_parser.return_value = parser_mock

        videos.main()


    mock_get_data_dir.assert_called_once_with(
        layer="raw",
        dataset_type="videos",
        date_of_data="2026-11-02",
        run_id="test_run_001",
    )

    mock_spark.readStream.format.assert_called_once_with("cloudFiles")

    mock_spark.readStream.options.assert_called_once_with(
        ignoreCorruptFiles=True,
        **{
            "cloudFiles.format": "json",
            "pathGlobFilter": "*.jsonl",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaLocation": checkpoint_path,
        },
    )

    mock_spark.readStream.load.assert_called_once_with(raw_path)

    mock_df.withColumn.assert_called_once()

    mock_df.withColumnRenamed.assert_called_once_with(
        "_rescued_data",
        "rescued_data",
    )

    mock_query.awaitTermination.assert_called_once()


def test_load_video_data(mock_spark):
    # Mock DataFrame
    mock_df = MagicMock(name="raw_dataframe")

    # Mock Auto Loader
    mock_spark.readStream.format.return_value = mock_spark.readStream
    mock_spark.readStream.options.return_value = mock_spark.readStream
    mock_spark.readStream.load.return_value = mock_df

    mock_df.withColumn.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df

    # Mock streaming query
    mock_query = MagicMock(name="streaming_query")
    mock_df.writeStream.foreachBatch.return_value = mock_df.writeStream
    mock_df.writeStream.option.return_value = mock_df.writeStream
    mock_df.writeStream.trigger.return_value = mock_df.writeStream
    mock_df.writeStream.start.return_value = mock_query


@pytest.mark.parametrize(
    "capture_pipeline_callback",
    [{"pipeline": "video", "mode": "incremental"}],
    indirect=True
)
def test_transform_creates_table(capture_pipeline_callback):
    ctx = capture_pipeline_callback

    with (
        patch.object(videos.f, "assert_schema") as mock_schema,
        patch.object(
            videos.f,
            "ensure_table_exists",
            return_value=False,
        ) as mock_exists,
    ):
        ctx["callback"](ctx["source_df"], 0)

    mock_schema.assert_called_once()
    mock_exists.assert_called_once()

    ctx["writer"].partitionBy.assert_called_once_with(
        "date_of_data",
        "channel_id",
    )
    ctx["writer"].saveAsTable.assert_called_once_with(
        videos._TARGET_TABLE
    )


@pytest.mark.parametrize(
    "capture_pipeline_callback",
    [{"pipeline": "video", "mode": "incremental"}],
    indirect=True
)
def test_transform_existing_table(capture_pipeline_callback):
    ctx = capture_pipeline_callback

    with (
        patch.object(videos.f, "assert_schema") as mock_schema,
        patch.object(
            videos.f,
            "ensure_table_exists",
            return_value=True,
        ) as mock_exists,
    ):

        ctx["callback"](ctx["source_df"], 0)

    mock_schema.assert_called_once()
    mock_exists.assert_called_once()

    ctx["writer"].mode.assert_called_once_with("overwrite")
    ctx["writer"].partitionBy.assert_not_called()

    ctx["writer"].saveAsTable.assert_called_once_with(
        videos._TARGET_TABLE,
    )


@pytest.mark.parametrize(
    "batch_id, expected_mode",
    [
        (0, "overwrite"),
        (1, "append"),
        (2, "append"),
    ],
)
def test_incremental_batch_write_mode(
    capture_pipeline_callback,
    batch_id,
    expected_mode,
):
    ctx = capture_pipeline_callback

    with (
        patch.object(videos.f, "assert_schema") as mock_schema,
        patch.object(
            videos.f,
            "ensure_table_exists",
            return_value=True,
        ) as mock_exists,
    ):
        ctx["callback"](ctx["source_df"], batch_id)

    ctx["writer"].mode.assert_called_once_with(expected_mode)


@pytest.mark.parametrize(
    "capture_pipeline_callback",
    [{"pipeline": "video", "mode": "reload_date"}],
    indirect=True,
)
@pytest.mark.parametrize("batch_id", [0, 1, 2, 10])
def test_reload_date_always_overwrites(
    capture_pipeline_callback,
    batch_id,
):
    ctx = capture_pipeline_callback

    assert ctx["args"].mode == "reload_date"

    with (
        patch.object(videos.f, "assert_schema"),
        patch.object(
            videos.f,
            "ensure_table_exists",
            return_value=True,
        ),
    ):
        ctx["callback"](ctx["source_df"], batch_id)

    ctx["writer"].mode.assert_called_once_with("overwrite")


def test_transform_invalid_schema(capture_pipeline_callback):
    ctx = capture_pipeline_callback
    pipeline = ctx["pipeline"]

    with (
        patch.object(
            pipeline.f,
            "assert_schema",
            side_effect=ValueError("Schema mismatch"),
        ),
        patch.object(
            pipeline.f,
            "ensure_table_exists",
        ) as mock_exists,
        pytest.raises(ValueError, match="Schema mismatch"),
    ):
        ctx["callback"](ctx["source_df"], 0)

    mock_exists.assert_not_called()
    ctx["writer"].saveAsTable.assert_not_called()


def test_transform_select_error(capture_pipeline_callback):
    ctx = capture_pipeline_callback

    ctx["source_df"].select.side_effect = RuntimeError(
        "Transformation failed"
    )

    with pytest.raises(RuntimeError, match="Transformation failed"):
        ctx["callback"](ctx["source_df"], 0)

    ctx["writer"].saveAsTable.assert_not_called()