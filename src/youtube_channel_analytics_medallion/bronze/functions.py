from datetime import datetime, timezone
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.types import DataType, StructType
from pyspark.sql.functions import (
    col, 
    concat_ws,
    format_string,
    lit, 
    lower,
    regexp_extract,
    to_date,
    trim,
    when
)
import argparse
import json


class LayerError(Exception):
    pass


# =====================================================
# HELPFUL FUNCTIONS
# =====================================================

def parse_channels(value: str | list[str]) -> list[str]:
    """Accepts a list, a JSON array string, or a comma-separated string."""

    if isinstance(value, list):
        channels = value

    elif isinstance(value, str):
        value = value.strip()

        if not value:
            raise argparse.ArgumentTypeError(
                "channels must not be empty"
            )

        if value.startswith("["):
            try:
                channels = json.loads(value)
            except json.JSONDecodeError as exc:
                raise argparse.ArgumentTypeError(
                    f"Invalid JSON channels: {value!r}"
                ) from exc

            if not isinstance(channels, list):
                raise argparse.ArgumentTypeError(
                    "channels must be a JSON array"
                )
        else:
            channels = value.split(",")

    else:
        raise argparse.ArgumentTypeError(
            f"channels must be str or list, got {type(value).__name__}"
        )

    channels = [
        str(channel).strip()
        for channel in channels
        if str(channel).strip()
    ]

    if not channels:
        raise argparse.ArgumentTypeError(
            "channels must not be empty"
        )

    return list(dict.fromkeys(channels))


def assert_schema(df: DataFrame, schema: str | StructType) -> None:
    expected: dict[str, DataType] = {f.name: f.dataType for f in schema.fields}
    actual: dict[str, DataType] = {f.name: f.dataType for f in df.schema.fields}

    missing = expected.keys() - actual.keys()
    if missing:
        raise ValueError(f"Schema mismatch: missing columns {sorted(missing)}")

    mismatches = [
        f"{name}: expected {expected[name]}, got {actual[name]}"
        for name in expected
        if name in actual and actual[name] != expected[name]
    ]
    if mismatches:
        raise ValueError(f"Schema type mismatches: {mismatches}")


def ensure_table_exists(spark: SparkSession, table_name: str):
    try:
        return spark.catalog.tableExists(table_name)
    except:
        return False


def get_date_of_data_column(mode: str, date_of_data: str | None = None) -> Column:
    if mode != "reload" and date_of_data:
        expr = lit(date_of_data)
    else:
        expr = col("etl_metadata.date_of_data") 
        
    return to_date(expr, "yyyy-MM-dd").alias("date_of_data")


def parse_date(value: str) -> str:
    if not value:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"date_of_data must be YYYY-MM-DD, got: {value!r}") from exc
    return value


def str_to_bool(value: str) -> bool:
    if isinstance(value, bool):
        return value
    if value.strip().lower() in ("true", "1", "yes"):
        return True
    if value.strip().lower() in ("false", "0", "no", ""):
        return False
    raise argparse.ArgumentTypeError(f"Expected true/false, got: {value!r}")


def parse_bool_column(input_col: str | Column, alias: str | None = None) -> Column:
    c = input_col if isinstance(input_col, Column) else col(input_col)
    c = lower(trim(c.cast("string")))
    
    result = (
        when(c.isin("true", "1", "yes"), lit(True))
        .when(c.isin("false", "0", "no"), lit(False))
        .otherwise(lit(False))
    )

    return result.alias(alias) if alias else result


def parse_iso8601_duration(input_col: str | Column, alias: str | None = None) -> Column:
    c = input_col if isinstance(input_col, Column) else col(input_col)

    def extract_unit(pattern: str) -> Column:
        value = regexp_extract(c, pattern, 1)
        return when(value == "", lit(0)).otherwise(value.cast("int"))

    days = extract_unit(r"P(\d+)D")
    hours = extract_unit(r"T.*?(\d+)H")
    minutes = extract_unit(r"T.*?(\d+)M")
    seconds = extract_unit(r"T.*?(\d+(?:\.\d+)?)S")

    result = when(
        c.isNotNull(),
        format_string("%02d:%02d:%02d:%02d", days, hours, minutes, seconds),
    )

    return result.alias(alias) if alias else result


def parse_thumbnails_dims(thumnail: str, alias: str | None = None) -> Column:
    width = f"snippet.thumbnails.{thumnail}.width"
    height = f"snippet.thumbnails.{thumnail}.height"
    alias = f"{thumnail}_dims" if not alias else alias

    return concat_ws("x", col(width), col(height)).alias(alias)