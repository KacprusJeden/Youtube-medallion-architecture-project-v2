import pytest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from youtube_channel_analytics_medallion.bronze import (
    channel,
    video,
    comment,
)


@pytest.fixture
def mock_spark():
    spark_session = MagicMock(name="spark_session")
    spark_builder = MagicMock(name="spark_builder")

    spark_builder.appName.return_value = spark_builder
    spark_builder.config.return_value = spark_builder
    spark_builder.getOrCreate.return_value = spark_session

    with patch(
        "pyspark.sql.SparkSession.builder",
        new=spark_builder
    ):
        yield spark_session


@pytest.fixture
def pipeline_context():
    source_df = MagicMock(name="source_df")
    transformed_df = MagicMock(name="transformed_df")
    writer = MagicMock(name="delta_writer")
    stream_writer = source_df.writeStream
    query = MagicMock(name="streaming_query")

    # Auto Loader DataFrame
    source_df.withColumn.return_value = source_df
    source_df.withColumnRenamed.return_value = source_df
    source_df.select.return_value = transformed_df

    # Transformed DataFrame
    transformed_df.withColumn.return_value = transformed_df
    transformed_df.filter.return_value = transformed_df
    transformed_df.drop.return_value = transformed_df
    transformed_df.write = writer

    # Delta writer
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer

    # Streaming writer
    stream_writer.foreachBatch.return_value = stream_writer
    stream_writer.option.return_value = stream_writer
    stream_writer.trigger.return_value = stream_writer
    stream_writer.start.return_value = query

    return {
        "source_df": source_df,
        "transformed_df": transformed_df,
        "writer": writer,
        "stream_writer": stream_writer,
        "query": query
    }


@pytest.fixture
def capture_pipeline_callback(request, mock_spark, pipeline_context):
    """
    Captures the nested _transform_batch callback from any Bronze pipeline.

    Supported pipelines:
        - channel
        - video
        - comment

    Supports parametrization of:
        - pipeline
        - mode
    """

    ctx = pipeline_context

    # Parametry fixture
    params = getattr(request, "param", {})

    pipeline_name = params.get("pipeline", "channel")
    mode = params.get("mode", "incremental")

    pipelines = {
        "channel": channel,
        "video": video,
        "comment": comment,
    }

    if pipeline_name not in pipelines:
        raise ValueError(
            f"Unsupported pipeline: {pipeline_name}"
        )

    pipeline = pipelines[pipeline_name]

    # Argumenty przekazywane do main()
    args = SimpleNamespace(
        date_of_data="2026-11-02",
        max_workers=0,
        reset_checkpoint=False,
        mode=mode,
        start_layer="raw",
        end_layer="gold",
    )

    parser_mock = MagicMock()
    parser_mock.parse_known_args.return_value = (args, [])

    # Mock Auto Loader
    mock_spark.readStream.format.return_value = mock_spark.readStream
    mock_spark.readStream.options.return_value = mock_spark.readStream
    mock_spark.readStream.load.return_value = ctx["source_df"]

    # Capture nested callback
    callback_box = {}

    def capture_callback(func):
        callback_box["callback"] = func
        return ctx["stream_writer"]

    ctx["stream_writer"].foreachBatch.side_effect = capture_callback

    with (
        patch.object(
            pipeline.argparse,
            "ArgumentParser",
            return_value=parser_mock,
        ),
        patch.object(
            pipeline.wtm,
            "get_data_dir_pattern",
            return_value=("raw_path", None),
        ),
        patch.object(
            pipeline,
            "_checkpoint_path",
            return_value="checkpoint_path",
        ),
        patch(
            "youtube_channel_analytics_medallion.config.app_config.get_config",
            return_value=SimpleNamespace(
                run_id="test_run_001",
                log_path="log_path",
            ),
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.log.log_start_step"
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.log.log_end_step"
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.log.log_end_pipeline"
        ),
        patch(
            "youtube_channel_analytics_medallion.metadata.log.flush_logs_to_adls"
        ),
    ):
        pipeline.main()

    assert "callback" in callback_box

    return {
        **ctx,
        "callback": callback_box["callback"],
        "spark": mock_spark,
        "args": args,
        "pipeline": pipeline,
        "pipeline_name": pipeline_name,
    }