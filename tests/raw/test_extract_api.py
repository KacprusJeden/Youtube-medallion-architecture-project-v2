from __future__ import annotations
import json
import threading
from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest

# ---------------------------------------------------------------------------
# Bootstrap: `extract_api` imports `watermark` and `log`, and `log` calls
# `app_config.get_config()` at import time.  We patch the config first so the
# import does not fail in a local/pytest environment.
# ---------------------------------------------------------------------------
from youtube_channel_analytics_medallion.config import app_config


def _fake_get_config() -> app_config.AppConfig:
    return app_config.AppConfig(
        data_base_path="dbfs:/mnt/test/raw",
        watermark_table="test_catalog.test_schema.watermark",
        error_path="dbfs:/mnt/test/errors",
        checkpoint_path="dbfs:/mnt/test/checkpoints",
        log_path="dbfs:/mnt/test/logs",
        run_id="test-run",
        task_key="test-task",
    )


app_config.get_config.cache_clear()
with patch.object(app_config, "get_config", _fake_get_config):
    from youtube_channel_analytics_medallion.raw.extract_api import (
        ChannelError,
        CommentError,
        YoutubeData,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def patch_config():
    """Keep `app_config.get_config` patched for the whole test function."""
    with patch.object(app_config, "get_config", _fake_get_config):
        yield


@pytest.fixture(autouse=True)
def mock_dbutils():
    """Mock dbutils secrets/filesystem calls used by extract_api and helpers.

    The mock records every file written via `dbutils.fs.put` in `_stored_files`
    so assertions can inspect the produced JSONL content.
    """
    dbutils_mock = MagicMock()
    dbutils_mock.secrets.get.return_value = "fake-api-key"

    stored_files: dict[str, str] = {}

    def _put(path: str, content: str, overwrite: bool = True) -> None:
        stored_files[path] = content

    dbutils_mock.fs.put.side_effect = _put
    dbutils_mock.fs.mkdirs.return_value = None
    dbutils_mock.fs.cp.return_value = None
    dbutils_mock.fs.ls.return_value = []
    dbutils_mock.fs.rm.return_value = None
    dbutils_mock._stored_files = stored_files

    with (
        patch("youtube_channel_analytics_medallion.raw.extract_api.dbutils", dbutils_mock),
        patch("youtube_channel_analytics_medallion.metadata.log.dbutils", dbutils_mock),
    ):
        yield dbutils_mock


@pytest.fixture(autouse=True)
def mock_log():
    """Mock the logging module imported by extract_api."""
    with patch("youtube_channel_analytics_medallion.raw.extract_api.log") as log_mock:
        yield log_mock


@pytest.fixture(autouse=True)
def mock_wtm():
    """Mock the watermark module imported by extract_api.

    `get_data_dir_pattern` returns deterministic paths derived from the call
    arguments so tests can assert on the written path.
    """
    with patch("youtube_channel_analytics_medallion.raw.extract_api.wtm") as wtm_mock:

        def _dir_pattern(layer, dataset_type, channel, date_of_data, run_id):
            return (
                f"dbfs:/mnt/test/raw/{dataset_type}/{date_of_data}/{run_id}/",
                f"dbfs:/mnt/test/errors/{dataset_type}/{date_of_data}/{run_id}/",
            )

        wtm_mock.get_data_dir_pattern.side_effect = _dir_pattern
        wtm_mock.get_watermark_table.return_value = "test_catalog.test_schema.watermark"
        wtm_mock.get_last_success_watermark.return_value = None
        wtm_mock.save_watermark.return_value = None
        wtm_mock.ensure_watermark_table.return_value = None
        yield wtm_mock


@pytest.fixture
def mock_build():
    """Mock googleapiclient.discovery.build and expose the returned youtube mock."""
    with patch("youtube_channel_analytics_medallion.raw.extract_api.build") as build_mock:
        youtube_mock = MagicMock()
        build_mock.return_value = youtube_mock
        yield build_mock, youtube_mock


@pytest.fixture
def run_params() -> dict:
    """Typical run parameters used across extraction tests."""
    return {
        "run_id": "run-2024-001",
        "date_of_data": "2024-01-15",
        "mode": "incremental",
    }


@pytest.fixture
def channel_item() -> dict:
    """A single YouTube channel resource as returned by channels().list()."""
    return {
        "id": "UC123456789",
        "snippet": {"title": "Test Channel", "description": "A channel for tests"},
        "statistics": {"subscriberCount": "100", "viewCount": "1000"},
        "contentDetails": {"relatedPlaylists": {"uploads": "UU123456789"}},
    }


@pytest.fixture
def video_items() -> list[dict]:
    """Two video resources as returned by videos().list()."""
    return [
        {
            "id": "video-1",
            "snippet": {"title": "First video"},
            "statistics": {"viewCount": "10"},
            "contentDetails": {"duration": "PT1M"},
        },
        {
            "id": "video-2",
            "snippet": {"title": "Second video"},
            "statistics": {"viewCount": "20"},
            "contentDetails": {"duration": "PT2M"},
        },
    ]


@pytest.fixture
def make_top_level_comment():
    """Factory fixture that builds top-level comment snippets."""

    def _make(comment_id: str, published_at: str, updated_at: str | None = None) -> dict:
        updated = updated_at or published_at
        return {
            "id": comment_id,
            "snippet": {
                "topLevelComment": {
                    "snippet": {
                        "authorDisplayName": f"user-{comment_id}",
                        "textDisplay": f"text-{comment_id}",
                        "likeCount": 1,
                        "publishedAt": published_at,
                        "updatedAt": updated,
                    }
                },
                "isPublic": True,
                "canReply": True,
                "totalReplyCount": 0,
            },
        }

    return _make


@pytest.fixture
def yt_instance(mock_dbutils):
    """A ready-to-use YoutubeData instance with a mocked channel handle."""
    return YoutubeData("@test-channel")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _rows_written(dbutils_mock: MagicMock) -> tuple[str, list[dict]]:
    """Return (path, parsed_rows) for the single file written by `save`."""
    assert dbutils_mock.fs.put.call_count == 1
    args = dbutils_mock.fs.put.call_args[0]
    path, content = args[0], args[1]
    rows = [json.loads(line) for line in content.splitlines()]
    return path, rows


# ---------------------------------------------------------------------------
# Construction & low-level behaviour
# ---------------------------------------------------------------------------
def test_init_reads_api_key_from_secrets(mock_dbutils):
    YoutubeData("@channel")
    mock_dbutils.secrets.get.assert_called_once_with(
        scope="kv-kacprusjeden-swec-001", key="YT-API-KEY"
    )


def test_youtube_property_builds_client(mock_build):
    build_mock, youtube_mock = mock_build
    yt = YoutubeData("@channel")
    assert yt.youtube is youtube_mock
    build_mock.assert_called_once_with("youtube", "v3", developerKey="fake-api-key")


def test_youtube_property_is_thread_local(mock_build):
    """Each thread must receive its own google-api-client instance."""
    build_mock, _ = mock_build
    build_mock.side_effect = [MagicMock(), MagicMock(), MagicMock()]

    yt = YoutubeData("@channel")
    clients: list[MagicMock] = []

    def fetch():
        clients.append(yt.youtube)

    threads = [threading.Thread(target=fetch) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(clients) == 3
    assert len({id(c) for c in clients}) == 3
    assert build_mock.call_count == 3


# ---------------------------------------------------------------------------
# _list_channel
# ---------------------------------------------------------------------------
def test_list_channel_by_handle(mock_build, channel_item):
    _, youtube_mock = mock_build
    youtube_mock.channels.return_value.list.return_value.execute.return_value = {
        "items": [channel_item]
    }

    yt = YoutubeData("@test")
    response = yt._list_channel("@test", "snippet,statistics")

    assert response["items"][0]["id"] == channel_item["id"]
    youtube_mock.channels.return_value.list.assert_called_once_with(
        part="snippet,statistics", forHandle="@test"
    )


def test_list_channel_by_id(mock_build, channel_item):
    _, youtube_mock = mock_build
    youtube_mock.channels.return_value.list.return_value.execute.return_value = {
        "items": [channel_item]
    }

    yt = YoutubeData("UC123")
    response = yt._list_channel("UC123", "contentDetails")

    assert response["items"][0]["id"] == channel_item["id"]
    youtube_mock.channels.return_value.list.assert_called_once_with(
        part="contentDetails", id="UC123"
    )


def test_list_channel_falls_back_to_search(mock_build, channel_item):
    _, youtube_mock = mock_build
    youtube_mock.channels.return_value.list.return_value.execute.side_effect = [
        {"items": []},
        {"items": [channel_item]},
    ]
    youtube_mock.search.return_value.list.return_value.execute.return_value = {
        "items": [{"id": {"channelId": channel_item["id"]}}]
    }

    yt = YoutubeData("some-name")
    response = yt._list_channel("some-name", "snippet")

    assert response["items"][0]["id"] == channel_item["id"]
    youtube_mock.search.return_value.list.assert_called_once_with(
        q="some-name", type="channel", part="id"
    )


def test_list_channel_not_found_raises(mock_build):
    _, youtube_mock = mock_build
    youtube_mock.channels.return_value.list.return_value.execute.return_value = {"items": []}
    youtube_mock.search.return_value.list.return_value.execute.return_value = {"items": []}

    yt = YoutubeData("@missing")
    with pytest.raises(ChannelError, match="Channel @missing not found"):
        yt._list_channel("@missing", "snippet")


# ---------------------------------------------------------------------------
# extract_channel_info
# ---------------------------------------------------------------------------
def test_extract_channel_info_success(
    mock_dbutils, mock_build, mock_wtm, mock_log, channel_item, run_params
):
    _, youtube_mock = mock_build
    youtube_mock.channels.return_value.list.return_value.execute.return_value = {
        "items": [channel_item]
    }

    yt = YoutubeData("@test")
    yt.extract_channel_info(channel="@test", **run_params)

    path, rows = _rows_written(mock_dbutils)
    assert path.startswith("dbfs:/mnt/test/raw/channels/")
    assert rows[0]["id"] == channel_item["id"]
    assert rows[0]["etl_metadata"]["channel_handle"] == "@test"
    assert rows[0]["etl_metadata"]["run_id"] == run_params["run_id"]

    mock_wtm.save_watermark.assert_called_once_with(
        "raw",
        "@test",
        "channels",
        "run_id",
        run_params["run_id"],
        run_params["run_id"],
        run_params["date_of_data"],
        run_params["mode"],
        "success",
    )
    mock_log.log_end_step.assert_called_with(
        "extract_channel_info [@test]", "success"
    )


def test_extract_channel_info_failure(
    mock_dbutils, mock_build, mock_wtm, mock_log, run_params
):
    _, youtube_mock = mock_build
    youtube_mock.channels.return_value.list.return_value.execute.side_effect = RuntimeError(
        "api failure"
    )

    yt = YoutubeData("@test")
    with pytest.raises(RuntimeError, match="api failure"):
        yt.extract_channel_info(channel="@test", **run_params)

    path, rows = _rows_written(mock_dbutils)
    assert path.startswith("dbfs:/mnt/test/errors/channels/")
    assert rows[0]["error_type"] == "RuntimeError"
    assert rows[0]["step"] == "extract_channel_info"

    mock_wtm.save_watermark.assert_called_once()
    args = mock_wtm.save_watermark.call_args[0]
    assert args[-2] == "failed"
    mock_log.log_end_step.assert_called_with(
        "extract_channel_info [@test]", "failed", mock_log.log_end_step.call_args[0][2]
    )


# ---------------------------------------------------------------------------
# get_videos_ids
# ---------------------------------------------------------------------------
def test_get_videos_ids(mock_build, channel_item):
    _, youtube_mock = mock_build
    youtube_mock.channels.return_value.list.return_value.execute.return_value = {
        "items": [channel_item]
    }
    youtube_mock.playlistItems.return_value.list.return_value.execute.side_effect = [
        {
            "items": [
                {"contentDetails": {"videoId": "v1"}},
                {"contentDetails": {"videoId": "v2"}},
            ],
            "nextPageToken": "TOKEN",
        },
        {"items": [{"contentDetails": {"videoId": "v3"}}]},
    ]

    yt = YoutubeData("@test")
    ids = yt.get_videos_ids("@test")

    assert ids == ["v1", "v2", "v3"]
    assert youtube_mock.playlistItems.return_value.list.call_count == 2


# ---------------------------------------------------------------------------
# extract_videos_info
# ---------------------------------------------------------------------------
def test_extract_videos_info_success(
    mock_dbutils, mock_build, mock_wtm, mock_log, video_items, run_params
):
    _, youtube_mock = mock_build
    youtube_mock.videos.return_value.list.return_value.execute.return_value = {
        "items": video_items
    }

    yt = YoutubeData("@test")
    yt.extract_videos_info(
        video_ids=["video-1", "video-2"], channel="@test", **run_params
    )

    path, rows = _rows_written(mock_dbutils)
    assert path.startswith("dbfs:/mnt/test/raw/videos/")
    assert {r["id"] for r in rows} == {"video-1", "video-2"}
    for row in rows:
        assert "etl_metadata" in row

    mock_wtm.save_watermark.assert_called_once_with(
        "raw",
        "@test",
        "videos",
        "run_id",
        run_params["run_id"],
        run_params["run_id"],
        run_params["date_of_data"],
        run_params["mode"],
        "success",
    )


def test_extract_videos_info_failure(
    mock_dbutils, mock_build, mock_wtm, mock_log, run_params
):
    _, youtube_mock = mock_build
    youtube_mock.videos.return_value.list.return_value.execute.side_effect = RuntimeError(
        "videos unavailable"
    )

    yt = YoutubeData("@test")
    with pytest.raises(RuntimeError, match="videos unavailable"):
        yt.extract_videos_info(video_ids=["v1"], channel="@test", **run_params)

    mock_dbutils.fs.put.assert_called_once()
    mock_wtm.save_watermark.assert_called_once()
    assert mock_wtm.save_watermark.call_args[0][-2] == "failed"


# ---------------------------------------------------------------------------
# get_comments_for_video
# ---------------------------------------------------------------------------
def test_get_comments_for_video_full_mode(mock_build, make_top_level_comment, run_params):
    _, youtube_mock = mock_build
    youtube_mock.commentThreads.return_value.list.return_value.execute.side_effect = [
        {
            "items": [
                make_top_level_comment("c1", "2024-01-15T10:00:00Z", "2024-01-15T11:00:00Z"),
            ],
            "nextPageToken": "TOKEN",
        },
        {"items": []},
    ]

    yt = YoutubeData("@test")
    comments = yt.get_comments_for_video(
        video_id="v1",
        channel="@test",
        run_id=run_params["run_id"],
        last_watermark=None,
        mode="full",
        date_of_data=run_params["date_of_data"],
    )

    assert len(comments) == 1
    assert comments[0]["comment_id"] == "c1"
    assert comments[0]["watermark_date"] == "2024-01-15T11:00:00Z"
    assert comments[0]["etl_metadata"]["source"] == "youtube_api"


def test_get_comments_for_video_incremental_stops_at_watermark(
    mock_build, make_top_level_comment, run_params
):
    _, youtube_mock = mock_build
    youtube_mock.commentThreads.return_value.list.return_value.execute.side_effect = [
        {
            "items": [
                make_top_level_comment("c_new", "2024-01-15T12:00:00Z"),
                make_top_level_comment("c_old", "2024-01-15T08:00:00Z"),
            ],
            "nextPageToken": "TOKEN",
        },
        {
            "items": [make_top_level_comment("c_ignored", "2024-01-15T07:00:00Z")],
        },
    ]

    yt = YoutubeData("@test")
    comments = yt.get_comments_for_video(
        video_id="v1",
        channel="@test",
        run_id=run_params["run_id"],
        last_watermark="2024-01-15T09:00:00Z",
        mode="incremental",
        date_of_data=run_params["date_of_data"],
    )

    assert [c["comment_id"] for c in comments] == ["c_new"]


def test_get_comments_for_video_http_403_is_ignored(mock_build, run_params):
    from googleapiclient.errors import HttpError

    _, youtube_mock = mock_build
    response = MagicMock()
    response.status = 403
    youtube_mock.commentThreads.return_value.list.return_value.execute.side_effect = HttpError(
        response, b"comments disabled"
    )

    yt = YoutubeData("@test")
    comments = yt.get_comments_for_video(
        video_id="v1",
        channel="@test",
        run_id=run_params["run_id"],
        last_watermark=None,
        mode="full",
        date_of_data=run_params["date_of_data"],
    )

    assert comments == []


# ---------------------------------------------------------------------------
# extract_comments_info
# ---------------------------------------------------------------------------
def test_extract_comments_info_success(
    mock_dbutils, mock_build, mock_wtm, mock_log, make_top_level_comment, run_params
):
    _, youtube_mock = mock_build
    youtube_mock.commentThreads.return_value.list.return_value.execute.side_effect = [
        {
            "items": [
                make_top_level_comment("c1", "2024-01-15T10:00:00Z"),
                make_top_level_comment("c2", "2024-01-15T11:00:00Z"),
            ],
            "nextPageToken": "TOKEN",
        },
        {"items": []},
    ]

    yt = YoutubeData("@test")
    yt.extract_comments_info(
        video_ids=["v1", "v2"], channel="@test", **run_params
    )

    # All comments should be written in one batch (well below batch_size).
    assert mock_dbutils.fs.put.call_count == 1
    path, rows = _rows_written(mock_dbutils)
    assert path.startswith("dbfs:/mnt/test/raw/comments/")
    assert len(rows) == 2

    mock_wtm.save_watermark.assert_called_with(
        "raw",
        "@test",
        "comments",
        "watermark_date",
        "2024-01-15T11:00:00Z",
        run_params["run_id"],
        run_params["date_of_data"],
        run_params["mode"],
        "success",
    )
    mock_log.log_end_step.assert_called_with(
        "extract_comments_info [@test]", "success"
    )


def test_extract_comments_info_with_failures(
    mock_dbutils, mock_build, mock_wtm, mock_log, run_params
):
    # get_comments_for_video swallows exceptions internally, so to exercise the
    # failure path in extract_comments_info we make the method itself raise.
    with patch.object(
        YoutubeData, "get_comments_for_video", side_effect=RuntimeError("thread error")
    ):
        yt = YoutubeData("@test")
        with pytest.raises(CommentError, match="Comments failed for 1 videos"):
            yt.extract_comments_info(video_ids=["v1"], channel="@test", **run_params)

    # Error file for the failed video.
    assert mock_dbutils.fs.put.call_count == 1
    path, rows = _rows_written(mock_dbutils)
    assert path.startswith("dbfs:/mnt/test/errors/comments/")
    assert rows[0] == "v1"

    # Watermark must NOT advance.
    mock_wtm.save_watermark.assert_called_once()
    args = mock_wtm.save_watermark.call_args[0]
    assert args[-2] == "failed"
    assert args[4] is None  # watermark_value


# ---------------------------------------------------------------------------
# save
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dataset_type", ["channels", "videos", "comments"])
def test_save_success_writes_jsonl(
    mock_dbutils, mock_wtm, mock_log, dataset_type
):
    yt = YoutubeData("@test")
    yt.save(
        step_name="test_step",
        channel="@test",
        dataset_type=dataset_type,
        data=[{"id": "1", "value": "a"}],
        mode="full",
        status="success",
        date_of_data="2024-01-15",
        run_id="run-1",
    )

    path, content, overwrite = mock_dbutils.fs.put.call_args[0]
    assert path.startswith(f"dbfs:/mnt/test/raw/{dataset_type}/")
    assert json.loads(content)["id"] == "1"
    mock_log.log_info_step.assert_called_once()


def test_save_failed_writes_error_record(mock_dbutils, mock_wtm, mock_log):
    yt = YoutubeData("@test")
    error = ValueError("something went wrong")
    yt.save(
        step_name="test_step",
        channel="@test",
        dataset_type="channels",
        data=[{"id": "1"}],
        mode="full",
        status="failed",
        date_of_data="2024-01-15",
        run_id="run-1",
        error=error,
    )

    path, rows = _rows_written(mock_dbutils)
    assert path.startswith("dbfs:/mnt/test/errors/channels/")
    assert rows[0]["error_type"] == "ValueError"
    assert rows[0]["step"] == "test_step"
    assert rows[1]["id"] == "1"
    mock_log.log_warning_step.assert_called_once()


def test_save_failed_without_data_still_writes_error(mock_dbutils, mock_wtm, mock_log):
    yt = YoutubeData("@test")
    error = RuntimeError("empty failure")
    yt.save(
        step_name="test_step",
        channel="@test",
        dataset_type="videos",
        data=[],
        mode="full",
        status="failed",
        date_of_data="2024-01-15",
        run_id="run-1",
        error=error,
    )

    _, rows = _rows_written(mock_dbutils)
    assert len(rows) == 1
    assert rows[0]["error_type"] == "RuntimeError"


def test_save_skips_empty_success_data(mock_dbutils, mock_wtm, mock_log):
    yt = YoutubeData("@test")
    yt.save(
        step_name="test_step",
        channel="@test",
        dataset_type="comments",
        data=[],
        mode="full",
        status="success",
        date_of_data="2024-01-15",
        run_id="run-1",
    )

    mock_dbutils.fs.put.assert_not_called()
    mock_log.log_info_step.assert_not_called()
    mock_log.log_warning_step.assert_not_called()


# ---------------------------------------------------------------------------
# get_etl_metadata
# ---------------------------------------------------------------------------
def test_get_etl_metadata_format():
    yt = YoutubeData("@test")
    meta = yt.get_etl_metadata("@test", "run-1", "2024-01-15")

    assert meta["source"] == "youtube_api"
    assert meta["channel_handle"] == "@test"
    assert meta["run_id"] == "run-1"
    assert meta["date_of_data"] == "2024-01-15"
    datetime.strptime(meta["load_date"], "%Y-%m-%dT%H:%M:%S.%f")
