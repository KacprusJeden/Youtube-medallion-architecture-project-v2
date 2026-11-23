from typing import List, Literal, Optional, Tuple
import threading
from datetime import datetime
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType
from youtube_channel_analytics_medallion.metadata import log
from youtube_channel_analytics_medallion.config import app_config as ac
import traceback


def _spark() -> SparkSession:
    """Lazy SparkSession accessor; safe inside foreachBatch workers."""
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _dbutils():
    """Lazy dbutils accessor; imported only when filesystem access is really needed."""
    from databricks.sdk.runtime import dbutils
    return dbutils


_WATERMARK_TABLE_LOCK = threading.RLock()


WATERMARK_SCHEMA = StructType([
    StructField(name, StringType(), True)
    for name in [
        "dataset_type", "channel", "run_id", "layer", "date_of_data", "mode", "status",
        "watermark_column", "watermark_value", "load_timestamp", "data_dir",
        "hub_dir", "satelite_dir", "link_dir", "invalid_dir", "error", "traceback",
    ]
])


def _join(base: str, *parts: str) -> str:
    return base.rstrip("/") + "/" + "/".join(str(p).strip("/") for p in parts) + "/"


# =========================================================
# WATERMARK/CHECKPOINT
# =========================================================
def get_watermark_table() -> str:
    return ac.get_config().watermark_table


def get_checkpoint_path(layer: Literal["bronze", "silver", "gold"], dataset_type: str) -> str:
    return _join(ac.get_config().checkpoint_path.replace("LAYER", layer), dataset_type)


def get_layer_root(layer: Literal["raw", "bronze", "silver", "gold"]) -> str:
    return ac.get_config().data_base_path.replace("LAYER", layer)


def get_error_root(layer: Literal["raw", "bronze", "silver", "gold"]) -> str:
    return ac.get_config().error_path.replace("LAYER", layer)


def get_table_path(layer: Literal["bronze", "silver", "gold"], dataset_type: str) -> str:
    """Fixed ADLS location of a layer's external Delta table (one per dataset, not per run)."""
    return _join(ac.get_config().data_base_path.replace("LAYER", layer), "tables", dataset_type)


def reset_checkpoints(
    layers: Tuple[str, ...] = ("raw", "bronze", "silver", "gold"),
    dataset_types: Tuple[str, ...] = ("channels", "videos", "comments"),
) -> None:
    """Delete streaming checkpoints of the given datasets."""

    dbutils = _dbutils()

    for layer in layers:
        for dataset_type in dataset_types:
            path = None

            try:
                path = get_checkpoint_path(layer, dataset_type)

                if not path:
                    raise ValueError("Invalid checkpoint path")

                print(f"[CHECKPOINT RESET] Target: {path}")

                try:
                    dbutils.fs.ls(path)
                    exists = True
                except Exception as e:
                    exists = False
                    print(f"[CHECKPOINT RESET] Cannot list target: {e}")

                if not exists:
                    print(f"[CHECKPOINT RESET] Path does not exist: {path}")
                    continue

                deleted = dbutils.fs.rm(path, True)

                print(f"[CHECKPOINT RESET] rm() returned: {deleted}")

                if not deleted:
                    raise RuntimeError(
                        f"dbutils.fs.rm returned False for: {path}"
                    )

                try:
                    dbutils.fs.ls(path)
                    raise RuntimeError(
                        f"Path still exists after deletion: {path}"
                    )
                except RuntimeError:
                    raise
                except Exception:
                    print(f"[CHECKPOINT RESET] Successfully deleted: {path}")

            except Exception as e:
                log.log_warning_step(
                    "reset_checkpoints",
                    f"Error deleting checkpoint path: {path}; "
                    f"Error: {type(e).__name__}: {e}",
                )
                raise
            

def is_watermark_empty(date_of_data: str = None):
    wtm = get_watermark_table()
    watermark_df = _spark().table(wtm).filter(col("status") == "success")

    if date_of_data:
        watermark_df = (
            watermark_df.filter(col("date_of_data") == date_of_data)   
        )

    return watermark_df.isEmpty()


# =========================================================
# DATA DIR
# =========================================================
def get_data_dir_pattern(
    layer: Literal["raw", "bronze", "silver", "gold"],
    dataset_type: str,
    channel: str = None,
    date_of_data: str = None,
    run_id: str = None
) -> str | Tuple[str, str] | Tuple[str, str, str, str]:
    """
    RAW - VALID AND INVALID DATA -> Tuple[str, str]
    BRONZE - VALID AND INVALID DATA -> Tuple[str, str]
    SILVER - DATA VAULT 2.0 - HUB, LINKS, SATELITE, INVALID DATA -> Tuple[str, str, str, str]
    GOLD - OBT - VALID AND INVALID DATA -> Tuple[str, str]
    """
    base_root = ac.get_config().data_base_path.replace("LAYER", layer)
    errors_root = ac.get_config().error_path.replace("LAYER", layer)

    if layer == "bronze":
        table_name = layer + ".yt." + dataset_type

        return table_name
    
    elif layer == "silver":
        slv_schema = "silver.yt."
        hub = slv_schema + f"hub_{dataset_type}"
        sat = slv_schema + f"sat_{dataset_type}"
        link = slv_schema + f"link_{dataset_type}"
        inv = _join(errors_root, dataset_type, channel)

        _dbutils().fs.mkdirs(inv)
        return hub, sat, link, inv

    base_dir = _join(base_root, dataset_type, date_of_data, run_id)
    errors_dir = _join(errors_root, dataset_type, date_of_data, run_id)

    return base_dir, errors_dir


# =========================================================
# WATERMARK COMPUTE
# =========================================================
def compute_watermark(data: list, column: str) -> str | None:
    try:
        values = [
            row.get(column) if isinstance(row, dict) else row
            for row in data if row
        ]
        values = [v for v in values if v]
        return max(values) if values else None
    except Exception:
        return None


# =========================================================
# GET LAST WATERMARK
# =========================================================
def get_last_success_watermark(
    watermark_table_name: str,
    dataset_type: str,
    mode: Literal["full", "incremental", "reload", "reload_date"] = None,
    date_of_data: str = None,
    layer: Literal["raw", "bronze", "silver", "gold"] = None,
    channel: str = None
) -> str | None:

    dataset_type = f"youtube_{dataset_type}"

    try:
        watermark_df = _spark().table(watermark_table_name)

        condition = (col("status") == "success") & (col("dataset_type") == dataset_type)
        if layer:
            condition = condition & (col("layer") == layer)
        if channel:
            condition = condition & (col("channel") == channel)

        success_df = watermark_df.filter(condition)

        if success_df.isEmpty():
            return None

        if mode in ["reload", "full"] and date_of_data:
            success_df = success_df.filter(
                (col("date_of_data") == date_of_data) & (col("mode") == mode)
            )

        rows = (
            success_df
            .filter(col("watermark_value").isNotNull())
            .orderBy(
                col("date_of_data").desc_nulls_last(),
                col("load_timestamp").desc_nulls_last(),
            )
            .select("watermark_value")
            .limit(1)
            .collect()
        )
        return rows[0]["watermark_value"] if rows else None

    except AnalysisException as e:
        error_message = str(e).lower()

        if (
            "table_or_view_not_found" in error_message
            or "table or view not found" in error_message
        ):
            return None

        raise


# =========================================================
# PENDING RUNS
# =========================================================
def get_pending_runs(
    dataset_type: str,
    channel: str,
    mode: Literal["full", "incremental", "reload", "reload_date"],
    source_layer: Literal["raw", "bronze"],
    target_layer: Literal["bronze", "silver", "gold"],
    watermark_table: str,
    date_of_data: Optional[str] = None
) -> List[dict]:

    dataset_key = f"youtube_{dataset_type}"

    if mode == "reload_date" and not date_of_data:
        raise ValueError("date_of_data is required for reload_date mode")

    if mode != "reload_date":
        date_of_data = None

    source_df = _spark().table(watermark_table).filter(
        (col("dataset_type") == dataset_key) &
        (col("channel") == channel) &
        (col("layer") == source_layer) &
        (col("status") == "success")
    )

    if source_df.isEmpty():
        return []

    columns = ["run_id", "date_of_data", "data_dir", "load_timestamp"]

    if mode in ["full", "reload"]:
        result_df = source_df.select(*columns)

    elif mode == "incremental":
        target_df = _spark().table(watermark_table).filter(
            (col("dataset_type") == dataset_key) &
            (col("channel") == channel) &
            (col("layer") == target_layer) &
            (col("status") == "success")
        ).select("run_id").distinct()

        target_run_ids = [row.run_id for row in target_df.collect()]

        if not target_run_ids:
            result_df = source_df.select(*columns)
        else:
            result_df = source_df.filter(~col("run_id").isin(target_run_ids)).select(*columns)

    elif mode == "reload_date":
        result_df = source_df.filter(col("date_of_data") == date_of_data).select(*columns)

    else:
        raise ValueError(f"Unsupported mode: {mode}")

    return result_df.orderBy("load_timestamp").toPandas().to_dict("records")


# =========================================================
# SAVE WATERMARK
# =========================================================
def _ensure_watermark_namespace() -> None:
    table_name = get_watermark_table()
    name_parts = table_name.split(".")

    if len(name_parts) == 3:
        catalog, schema, _ = name_parts
        _spark().sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    elif len(name_parts) == 2:
        schema, _ = name_parts
        _spark().sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def ensure_watermark_table() -> None:
    """Create the watermark table once, before channels run in parallel (avoids a create race on first run)."""
    with _WATERMARK_TABLE_LOCK:
        _ensure_watermark_namespace()

        table_name = get_watermark_table()
        empty_df = _spark().createDataFrame([], WATERMARK_SCHEMA)

        if _spark().catalog.tableExists(table_name):
            return

        empty_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)


def save_watermark(
    layer: Literal["raw", "bronze", "silver", "gold"],
    channel: str,
    dataset_type: str,
    watermark_column: str,
    watermark_value: str,
    run_id: str,
    date_of_data: str = None,
    mode: Literal["full", "incremental", "reload", "reload_date"] = "incremental",
    status: Literal["failed", "success"] = "success",
    exception: Exception = None
):
    data = {
        "dataset_type": f"youtube_{dataset_type}",
        "channel": channel,
        "run_id": run_id,
        "layer": layer,
        "date_of_data": date_of_data,
        "mode": mode,
        "status": status,
        "watermark_column": watermark_column,
        "watermark_value": watermark_value,
        "load_timestamp": datetime.now().isoformat()
    }

    if layer == "silver":
        hub, sat, link, inv = get_data_dir_pattern(layer, dataset_type, channel, date_of_data, run_id)
        data.update({"hub_dir": hub, "satelite_dir": sat, "link_dir": link, "invalid_dir": inv})
    else:
        base_dir, error_dir = get_data_dir_pattern(layer, dataset_type, channel, date_of_data, run_id)
        data["data_dir"] = base_dir if status == "success" else error_dir
        if status == "success":
            # a successful load with no rows (e.g. no new comments) must still have a readable data_dir
            _dbutils().fs.mkdirs(base_dir)

    if exception:
        data.update({
            "error": str(exception),
            "traceback": "".join(traceback.format_exception(exception))
        })

    row = tuple(None if data.get(f.name) is None else str(data[f.name]) for f in WATERMARK_SCHEMA.fields)
    df = _spark().createDataFrame([row], schema=WATERMARK_SCHEMA)

    with _WATERMARK_TABLE_LOCK:
        ensure_watermark_table()

        if mode == "full":
            query = f"""
                DELETE FROM {get_watermark_table()} 
                WHERE layer = '{layer}' AND channel = '{channel}' AND dataset_type = 'youtube_{dataset_type}'
            """
            _spark().sql(query)

        # always append: the table holds history for every channel/dataset
        df.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .saveAsTable(get_watermark_table())

