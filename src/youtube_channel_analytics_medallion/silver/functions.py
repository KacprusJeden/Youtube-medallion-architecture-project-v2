from pyspark.sql.functions import (
    col,
    lit,
    min,
    expr,
    to_date
)
from typing import Tuple
from pyspark.sql import Column, SparkSession, DataFrame
from pyspark.sql.types import StructType, DataType
from delta.tables import DeltaTable
from youtube_channel_analytics_medallion.metadata import watermark as wtm
from youtube_channel_analytics_medallion.bronze.schemas import (
    COMMENT_SCHEMA, 
    VIDEO_SCHEMA, 
    CHANNEL_SCHEMA
)
from youtube_channel_analytics_medallion.silver.schemas import (
    HUB_CHANNEL_SCHEMA,
    SAT_CHANNEL_SCHEMA,
    HUB_VIDEO_SCHEMA,
    SAT_VIDEO_SCHEMA,
    LINK_VIDEO_SCHEMA,
    HUB_COMMENT_SCHEMA,
    SAT_COMMENT_SCHEMA, 
    LINK_COMMENT_SCHEMA
)

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


def get_current_bronze(spark: SparkSession, dataset_type: str, date_of_data: Column | None) -> DataFrame:
    bronze_path = wtm.get_data_dir_pattern(layer="bronze", dataset_type=dataset_type, date_of_data=date_of_data)

    try:
        bronze_df = DeltaTable.forName(spark, bronze_path).toDF()
    except:
        match dataset_type:
            case "comments":
                schema = COMMENT_SCHEMA
            case "videos":
                schema = VIDEO_SCHEMA
            case "channels":
                schema = CHANNEL_SCHEMA
            case _:
                raise ValueError(f"Unknown dataset_type: {dataset_type}")
            
        bronze_df = spark.createDataFrame([], schema=schema)

    if date_of_data is not None:
        bronze_df = bronze_df.filter(col("date_of_data") == date_of_data)

    return bronze_df


def get_current_silver(
    spark: SparkSession,
    dataset_type: str
) -> Tuple[DataFrame, DataFrame, DataFrame | None]:

    hubs, sats, links, _ = wtm.get_data_dir_pattern(
        layer="silver",
        dataset_type=dataset_type
    )

    match dataset_type:
        case "comments":
            schemas = (
                HUB_COMMENT_SCHEMA,
                SAT_COMMENT_SCHEMA,
                LINK_COMMENT_SCHEMA
            )
        case "videos":
            schemas = (
                HUB_VIDEO_SCHEMA,
                SAT_VIDEO_SCHEMA,
                LINK_VIDEO_SCHEMA
            )
        case "channels":
            schemas = (
                HUB_CHANNEL_SCHEMA,
                SAT_CHANNEL_SCHEMA,
                None
            )
        case _:
            raise ValueError(f"Unknown dataset_type: {dataset_type}")

    tables = (hubs, sats, links)

    result = []
    for table, schema in zip(tables, schemas):
        if schema is None:  
            result.append(None)
        elif table is not None and ensure_table_exists(spark, table):
            result.append(spark.table(table))
        else:
            result.append(spark.createDataFrame([], schema=schema))

    return tuple(result)


def get_hash_values(*cols: Column) -> Column:
    from pyspark.sql.functions import sha2, concat_ws
    return sha2(concat_ws("||", *cols), 256) if len(cols) > 1 else sha2(cols[0], 256)


def get_scd2_closes(dataset_type: str, silver_table: DeltaTable, date_of_data: Column) -> DataFrame:
    match dataset_type:
        case "comments":
            cols = ["hk_comment", "hk_video", "hk_channel"]
        case "videos":
            cols = ["hk_video", "hk_channel"]
        case "channels":
            cols = ["hk_channel"]
        case _:
            raise ValueError(f"Unknown dataset_type: {dataset_type}")

    try:
        closes_df = (
            silver_table.toDF()
            .filter(col("valid_from") > date_of_data)
            .groupBy(*cols)
            .agg(min("valid_from").alias("next_val"))
            .cache()
        )
    except Exception as e:
        raise e

    return closes_df


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
