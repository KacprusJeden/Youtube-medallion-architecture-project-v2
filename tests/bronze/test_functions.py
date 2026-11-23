import argparse
import datetime as dt
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from youtube_channel_analytics_medallion.bronze.functions import (
    assert_schema,
    ensure_table_exists,
    get_date_of_data_column,
    parse_bool_column,
    parse_channels,
    parse_date,
    parse_iso8601_duration,
    parse_thumbnails_dims,
    str_to_bool,
)


# =====================================================
# Pure Python helpers
# =====================================================

class TestParseChannels:
    def test_list(self):
        assert parse_channels(["a", "b"]) == ["a", "b"]

    def test_comma_separated(self):
        assert parse_channels("a, b ,c") == ["a", "b", "c"]

    def test_single_value(self):
        assert parse_channels("abc") == ["abc"]

    def test_json_array(self):
        assert parse_channels('["a", "b"]') == ["a", "b"]

    def test_json_array_with_surrounding_whitespace(self):
        assert parse_channels('  ["a","b"]  ') == ["a", "b"]

    def test_removes_duplicates_preserving_order(self):
        assert parse_channels("b,a,b,a,c") == ["b", "a", "c"]

    def test_drops_blank_items(self):
        assert parse_channels(["a", "", "  ", "b"]) == ["a", "b"]
        assert parse_channels("a,,b,") == ["a", "b"]

    def test_non_string_items_are_stringified(self):
        assert parse_channels([1, 2]) == ["1", "2"]

    @pytest.mark.parametrize("value", ["", "   ", [], ["", " "], ",,,", "[]"])
    def test_empty_raises(self, value):
        with pytest.raises(argparse.ArgumentTypeError, match="must not be empty"):
            parse_channels(value)

    def test_invalid_json_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="Invalid JSON"):
            parse_channels("[a, b")

    @pytest.mark.parametrize("value", [123, None, {"a": 1}])
    def test_invalid_type_raises(self, value):
        with pytest.raises(argparse.ArgumentTypeError, match="must be str or list"):
            parse_channels(value)


class TestParseDate:
    def test_valid_date_is_returned(self):
        assert parse_date("2024-01-31") == "2024-01-31"

    @pytest.mark.parametrize("value", [None, ""])
    def test_empty_returns_today_utc(self, value):
        before = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        result = parse_date(value)
        after = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        assert result in (before, after)

    @pytest.mark.parametrize("value", ["31-01-2024", "2024/01/31", "2024-13-01", "2024-02-30", "abc"])
    def test_invalid_raises(self, value):
        with pytest.raises(argparse.ArgumentTypeError, match="YYYY-MM-DD"):
            parse_date(value)


class TestStrToBool:
    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "yes", " YES "])
    def test_truthy(self, value):
        assert str_to_bool(value) is True

    @pytest.mark.parametrize("value", ["false", "False", "0", "no", "NO", "", "  "])
    def test_falsy(self, value):
        assert str_to_bool(value) is False

    @pytest.mark.parametrize("value", [True, False])
    def test_bool_passthrough(self, value):
        assert str_to_bool(value) is value

    @pytest.mark.parametrize("value", ["maybe", "2", "tak"])
    def test_invalid_raises(self, value):
        with pytest.raises(argparse.ArgumentTypeError, match="Expected true/false"):
            str_to_bool(value)


class TestEnsureTableExists:
    def test_returns_catalog_result(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        assert ensure_table_exists(spark, "c.s.t") is True
        spark.catalog.tableExists.assert_called_once_with("c.s.t")

    def test_returns_false_when_table_missing(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        assert ensure_table_exists(spark, "c.s.t") is False

    def test_returns_false_on_exception(self):
        spark = MagicMock()
        spark.catalog.tableExists.side_effect = Exception("boom")
        assert ensure_table_exists(spark, "c.s.t") is False


# =====================================================
# Spark helpers (require the `spark` fixture)
# =====================================================

class TestAssertSchema:
    schema = StructType([
        StructField("id", StringType()),
        StructField("views", IntegerType()),
    ])

    def test_matching_schema_passes(self, spark):
        df = spark.createDataFrame([("a", 1)], self.schema)
        assert_schema(df, self.schema)

    def test_extra_columns_are_allowed(self, spark):
        wider = StructType(self.schema.fields + [StructField("extra", StringType())])
        df = spark.createDataFrame([("a", 1, "x")], wider)
        assert_schema(df, self.schema)

    def test_missing_column_raises(self, spark):
        df = spark.createDataFrame([("a",)], StructType([StructField("id", StringType())]))
        with pytest.raises(ValueError, match=r"missing columns \['views'\]"):
            assert_schema(df, self.schema)

    def test_type_mismatch_raises(self, spark):
        df = spark.createDataFrame(
            [("a", "1")],
            StructType([StructField("id", StringType()), StructField("views", StringType())]),
        )
        with pytest.raises(ValueError, match="type mismatches"):
            assert_schema(df, self.schema)


class TestGetDateOfDataColumn:
    def _df(self, spark):
        return spark.createDataFrame(
            [("2020-05-06",)], "date_of_data string"
        ).selectExpr("named_struct('date_of_data', date_of_data) as etl_metadata")

    def test_uses_given_date_when_not_reload(self, spark):
        row = self._df(spark).select(get_date_of_data_column("incremental", "2024-01-31")).first()
        assert row["date_of_data"] == dt.date(2024, 1, 31)

    def test_reload_uses_etl_metadata(self, spark):
        row = self._df(spark).select(get_date_of_data_column("reload", "2024-01-31")).first()
        assert row["date_of_data"] == dt.date(2020, 5, 6)

    def test_falls_back_to_etl_metadata_without_date(self, spark):
        row = self._df(spark).select(get_date_of_data_column("incremental", None)).first()
        assert row["date_of_data"] == dt.date(2020, 5, 6)


class TestParseBoolColumn:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("true", True), ("TRUE", True), (" Yes ", True), ("1", True),
            ("false", False), ("0", False), ("no", False),
            ("garbage", False), (None, False),
        ],
    )
    def test_values(self, spark, raw, expected):
        df = spark.createDataFrame([(raw,)], "v string")
        assert df.select(parse_bool_column("v")).first()[0] is expected

    def test_accepts_column_and_alias(self, spark):
        from pyspark.sql.functions import col

        df = spark.createDataFrame([("true",)], "v string")
        out = df.select(parse_bool_column(col("v"), alias="flag"))
        assert out.columns == ["flag"]
        assert out.first()["flag"] is True

    def test_accepts_non_string_input(self, spark):
        df = spark.createDataFrame([(1,), (0,)], "v int")
        assert [r[0] for r in df.select(parse_bool_column("v")).collect()] == [True, False]


class TestParseIso8601Duration:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("PT1H2M3S", "00:01:02:03"),
            ("PT45S", "00:00:00:45"),
            ("PT10M", "00:00:10:00"),
            ("PT2H", "00:02:00:00"),
            ("PT1H30S", "00:01:00:30"),
            ("P1DT2H3M4S", "01:02:03:04"),
            ("P2D", "02:00:00:00"),
            ("PT0S", "00:00:00:00"),
        ],
    )
    def test_values(self, spark, raw, expected):
        df = spark.createDataFrame([(raw,)], "d string")
        assert df.select(parse_iso8601_duration("d")).first()[0] == expected

    def test_null_returns_null(self, spark):
        df = spark.createDataFrame([(None,)], "d string")
        assert df.select(parse_iso8601_duration("d")).first()[0] is None

    def test_alias(self, spark):
        df = spark.createDataFrame([("PT1S",)], "d string")
        assert df.select(parse_iso8601_duration("d", alias="duration")).columns == ["duration"]


class TestParseThumbnailsDims:
    ddl = (
        "snippet struct<thumbnails: struct<"
        "default: struct<width: int, height: int>, "
        "high: struct<width: int, height: int>>>"
    )

    def test_default_alias(self, spark):
        df = spark.createDataFrame([((((120, 90), (480, 360)),),)], self.ddl)
        out = df.select(parse_thumbnails_dims("default"))
        assert out.columns == ["default_dims"]
        assert out.first()[0] == "120x90"

    def test_custom_alias(self, spark):
        df = spark.createDataFrame([((((120, 90), (480, 360)),),)], self.ddl)
        out = df.select(parse_thumbnails_dims("high", alias="hq"))
        assert out.columns == ["hq"]
        assert out.first()[0] == "480x360"

    def test_null_dimensions_give_empty_string(self, spark):
        df = spark.createDataFrame([((((None, None), (None, None)),),)], self.ddl)
        assert df.select(parse_thumbnails_dims("default")).first()[0] == ""
