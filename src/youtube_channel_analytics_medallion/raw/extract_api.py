from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
from typing import List, Literal
from youtube_channel_analytics_medallion.metadata import watermark as wtm
from youtube_channel_analytics_medallion.metadata import log
from concurrent.futures import ThreadPoolExecutor, as_completed
from databricks.sdk.runtime import dbutils
import inspect
import threading
import traceback
import json
import uuid

class ChannelError(Exception): pass
class VideoError(Exception): pass
class CommentError(Exception): pass

class YoutubeData:
    LAYER = "raw"

    def __init__(self, *channels: str):
        self.api_key = dbutils.secrets.get(scope="kv-kacprusjeden-swec-001", key="YT-API-KEY")
        self.channels = channels
        self._thread_local = threading.local()

    @property
    def youtube(self):
        # googleapiclient clients are not thread-safe -> one client per thread
        if not hasattr(self._thread_local, "client"):
            self._thread_local.client = build("youtube", "v3", developerKey=self.api_key)

        return self._thread_local.client


    # =========================================
    # ETL METHODS & SAVING
    # =========================================
    def get_etl_metadata(self, channel: str, run_id: str, date_of_data: str) -> dict:
        return {
            "load_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "source": "youtube_api",
            "channel_handle": channel,
            "run_id": run_id,
            "date_of_data": date_of_data
        }

    def save(self, step_name: str, channel: str, dataset_type: str, data: List[dict], mode: str, status: Literal["success", "failed"], date_of_data: str, run_id: str, error: BaseException = None):
        """Write rows to the valid dir (success) or the error dir (failed).

        A failed save with `error` always writes an error record, even when there is no data,
        so every `failed` watermark row points to a file that explains the failure.
        """
        rows = list(data or [])
        if status == "failed" and error is not None:
            rows.insert(0, {
                "error": str(error),
                "error_type": type(error).__name__,
                "step": step_name,
                "channel": channel,
                "dataset_type": dataset_type,
                "run_id": run_id,
                "traceback": "".join(traceback.format_exception(error)),
            })

        if not rows:
            return

        base_dir, error_dir = wtm.get_data_dir_pattern(
            layer=self.LAYER,
            dataset_type=dataset_type,
            channel=channel,
            date_of_data=date_of_data,
            run_id=run_id
        )

        path = base_dir if status == "success" else error_dir
        content = "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in rows)

        channel_file_name = f"{channel}_{uuid.uuid4().hex}.jsonl"

        file_path = f"{path}{channel_file_name}"

        dbutils.fs.put(file_path, content, True)

        communicate = f"[{channel}] {dataset_type} {status}: {len(rows)} rows -> {file_path}"
        if status == "failed":
            log.log_warning_step(step_name=step_name, communicate=communicate)
        else:
            log.log_info_step(step_name=step_name, communicate=communicate)


    def _fail(self, step_name: str, channel: str, dataset_type: str, mode: str, date_of_data: str, run_id: str, error: Exception):
        """Record a failed step (error file + failed watermark + log). Never raises, so the original error is kept."""
        log.log_end_step(f"{step_name} [{channel}]", "failed", error)
        try:
            self.save(step_name, channel, dataset_type, [], mode, "failed", date_of_data, run_id, error)
        except Exception as save_err:
            log.log_error_step(step_name, f"[{channel}] could not write error file", save_err)
        try:
            wtm.save_watermark(self.LAYER, channel, dataset_type, "run_id", None, run_id, date_of_data, mode, "failed", error)
        except Exception as wtm_err:
            log.log_error_step(step_name, f"[{channel}] could not write failed watermark", wtm_err)


    def _list_channel(self, channel: str, part: str) -> dict:
        """Resolve a channel handle (@name) or channel id to a channels().list response."""
        if channel.startswith("@"):
            response = self.youtube.channels().list(part=part, forHandle=channel).execute()
        else:
            response = self.youtube.channels().list(part=part, id=channel).execute()

        if not response.get("items"):
            res = self.youtube.search().list(q=channel, type="channel", part="id").execute()
            if res.get("items"):
                c_id = res["items"][0]["id"]["channelId"]
                response = self.youtube.channels().list(part=part, id=c_id).execute()

        if not response.get("items"):
            raise ChannelError(f"Channel {channel} not found.")
        return response

    def extract_channel_info(self, run_id: str, channel: str, date_of_data: str, mode: str):
        step_name = inspect.currentframe().f_code.co_name # return current function name
        
        log.log_start_step(f"{step_name} [{channel}]")
        try:
            response = self._list_channel(channel, "snippet,statistics,contentDetails")
            items = response["items"]

            for item in items:
                item["etl_metadata"] = self.get_etl_metadata(channel, run_id, date_of_data)

            self.save(step_name, channel, "channels", items, mode, "success", date_of_data, run_id)
        except Exception as e:
            self._fail(step_name, channel, "channels", mode, date_of_data, run_id, e)
            raise

        # outside the try: a watermark failure must not re-save the data as "failed"
        wtm.save_watermark(self.LAYER, channel, "channels", "run_id", run_id, run_id, date_of_data, mode, "success")
        log.log_end_step(f"{step_name} [{channel}]", "success")

    def get_videos_ids(self, channel: str) -> List[str]:
        response = self._list_channel(channel, "contentDetails")
        uploads_playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        video_ids = []
        next_page_token = None

        while True:
            res = self.youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            video_ids.extend([item["contentDetails"]["videoId"] for item in res.get("items", [])])
            next_page_token = res.get("nextPageToken")
            if not next_page_token:
                break
        return video_ids

    def extract_videos_info(self, video_ids: List[str], run_id: str, channel: str, date_of_data: str, mode: str):
        step_name = inspect.currentframe().f_code.co_name
        
        log.log_start_step(f"{step_name} [{channel}]")
        all_videos = []
        batch_size = 100000

        try:
            for i in range(0, len(video_ids), 50):
                batch_ids = video_ids[i:i+50]
                response = self.youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(batch_ids)
                ).execute()
                all_videos.extend(response.get("items", []))

            for video in all_videos:
                video["etl_metadata"] = self.get_etl_metadata(channel, run_id, date_of_data)

            for i in range(0, len(all_videos), batch_size):
                self.save(step_name, channel, "videos", all_videos[i: i + batch_size], mode, "success", date_of_data, run_id)


        except Exception as e:
            self._fail(step_name, channel, "videos", mode, date_of_data, run_id, e)
            raise

        wtm.save_watermark(self.LAYER, channel, "videos", "run_id", run_id, run_id, date_of_data, mode, "success")
        log.log_end_step(f"{step_name} [{channel}]", "success")

    def get_comments_for_video(self, video_id: str, channel: str, run_id: str, last_watermark: str, mode: str, date_of_data: str) -> List[dict]:
        comments = []
        next_page_token = None
        try:
            while True:
                response = self.youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token,
                    order="time"
                ).execute()

                stop_pagination = False
                for item in response.get("items", []):
                    top_comment = item["snippet"]["topLevelComment"]["snippet"]
                    updated_at = top_comment.get("updatedAt") or top_comment.get("publishedAt")

                    if mode == "incremental" and last_watermark and updated_at <= last_watermark:
                        stop_pagination = True
                        break

                    comments.append({
                        "comment_id": item["id"],
                        "video_id": video_id,
                        "channel_id": channel,
                        "comment_author": top_comment.get("authorDisplayName"),
                        "comment_text": top_comment.get("textDisplay"),
                        "comment_like_count": top_comment.get("likeCount"),
                        "comment_publish_date": top_comment.get("publishedAt"),
                        "comment_update_date": top_comment.get("updatedAt"),
                        "comment_is_public": item["snippet"].get("isPublic"),
                        "comment_can_reply": item["snippet"].get("canReply"),
                        "comment_reply_count": item["snippet"].get("totalReplyCount"),
                        "watermark_date": updated_at,
                        "etl_metadata": self.get_etl_metadata(channel, run_id, date_of_data)
                    })

                next_page_token = response.get("nextPageToken")
                if not next_page_token or stop_pagination:
                    break
        except HttpError as e:
            if e.resp.status != 403:
                print(f"HTTP Error for video {video_id}: {e}")
        except Exception as e:
            print(f"General Error for video {video_id}: {e}")
        return comments

    def extract_comments_info(self, video_ids: List[str], run_id: str, channel: str, date_of_data: str, mode: str):
        step_name = inspect.currentframe().f_code.co_name
        log.log_start_step(f"{step_name} [{channel}]")

        last_watermark = None

        if mode == "incremental":
            try:
                last_watermark = wtm.get_last_success_watermark(
                    wtm.get_watermark_table(), "comments", mode, layer=self.LAYER, channel=channel
                )
            except Exception as e:
                log.log_warning_step(step_name, f"[{channel}] could not load last watermark for comments: {e}")

        all_comments = []
        all_dates = []
        failures = []
        batch_size = 50000

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.get_comments_for_video, vid, channel, run_id, last_watermark, mode, date_of_data): vid
                for vid in video_ids
            }

            for future in as_completed(futures):
                vid = futures[future]
                try:
                    result = future.result()
                except Exception:
                    failures.append(vid)
                    continue

                all_comments.extend(result)
                all_dates.extend([c["watermark_date"] for c in result if c.get("watermark_date")])

                if len(all_comments) >= batch_size:
                    self.save(step_name, channel, "comments", all_comments, mode, "success", date_of_data, run_id)
                    all_comments = []

        if all_comments:
            self.save(step_name, channel, "comments", all_comments, mode, "success", date_of_data, run_id)

        if failures:
            # do not advance the watermark when some videos failed
            err = CommentError(f"Comments failed for {len(failures)} videos: {failures[:10]}")
            try:
                self.save(
                    step_name,
                    channel,
                    "comments",
                    failures,
                    mode,
                    "failed",
                    date_of_data,
                    run_id,
                )
            except Exception as save_err:
                log.log_error_step(step_name, f"[{channel}] could not write error file", save_err)
            log.log_end_step(f"{step_name} [{channel}]", "failed", err)
            wtm.save_watermark(self.LAYER, channel, "comments", "watermark_date", None, run_id, date_of_data, mode, "failed", err)
            raise err

        new_watermark = max(all_dates) if all_dates else last_watermark
        wtm.save_watermark(self.LAYER, channel, "comments", "watermark_date", new_watermark, run_id, date_of_data, mode, "success")
        log.log_end_step(f"{step_name} [{channel}]", "success")
