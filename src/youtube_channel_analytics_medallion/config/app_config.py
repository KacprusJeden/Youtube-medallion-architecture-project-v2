import argparse
import uuid
from dataclasses import dataclass
from functools import lru_cache


def _get_spark_conf(name: str) -> str | None:
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return None

        for key in (
            name,
            f"spark.databricks.pipelines.{name}",
            f"youtube_channel_analytics_medallion.{name}",
        ):
            value = spark.conf.get(key, None)
            if value:
                return value
    except Exception:
        return None

    return None


@dataclass(frozen=True)
class AppConfig:
    data_base_path: str
    watermark_table: str
    error_path: str
    checkpoint_path: str
    log_path: str
    run_id: str
    task_key: str


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="")
    parser.add_argument("--task_key", default="")
    parser.add_argument("--data_base_path", default="")
    parser.add_argument("--watermark_table", default="")
    parser.add_argument("--error_path", default="")
    parser.add_argument("--checkpoint_path", default="")
    parser.add_argument("--log_path", default="")
    args, _ = parser.parse_known_args()

    data_base_path = args.data_base_path or _get_spark_conf("data_base_path")
    watermark_table = args.watermark_table or _get_spark_conf("watermark_table")
    error_path = args.error_path or _get_spark_conf("error_path")
    checkpoint_path = args.checkpoint_path or _get_spark_conf("checkpoint_path")
    log_path = args.log_path or _get_spark_conf("log_path")
    run_id = args.run_id or _get_spark_conf("run_id") or uuid.uuid4().hex
    task_key = args.task_key or _get_spark_conf("task_key") or f"task-{uuid.uuid4().hex}"

    missing = [
        name
        for name, value in {
            "data_base_path": data_base_path,
            "watermark_table": watermark_table,
            "error_path": error_path,
            "checkpoint_path": checkpoint_path,
            "log_path": log_path,
        }.items()
        if not value
    ]

    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Missing required configuration values: {joined}")

    return AppConfig(
        data_base_path=data_base_path,
        watermark_table=watermark_table,
        error_path=error_path,
        checkpoint_path=checkpoint_path,
        log_path=log_path,
        run_id=run_id,
        task_key=task_key,
    )
