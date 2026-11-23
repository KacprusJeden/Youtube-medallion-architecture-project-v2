from typing import Literal
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone


def process_channel(run_id: str, channel: str, date_of_data: str, mode: Literal["full", "reload", "reload_date", "incremental"] = "incremental"):
    from youtube_channel_analytics_medallion.raw.extract_api import YoutubeData

    yt = YoutubeData(channel)

    yt.extract_channel_info(run_id, channel, date_of_data, mode)

    video_ids = yt.get_videos_ids(channel)

    yt.extract_videos_info(video_ids, run_id, channel, date_of_data, mode)

    yt.extract_comments_info(video_ids, run_id, channel, date_of_data, mode)


def _str_to_bool(value: str) -> bool:
    if isinstance(value, bool):
        return value
    if value.strip().lower() in ("true", "1", "yes"):
        return True
    if value.strip().lower() in ("false", "0", "no", ""):
        return False
    raise argparse.ArgumentTypeError(f"Expected true/false, got: {value!r}")



def _parse_date(value: str) -> str:
    if not value:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"date_of_data must be YYYY-MM-DD, got: {value!r}")
    return value


def main():
    from youtube_channel_analytics_medallion.config.app_config import get_config
    from youtube_channel_analytics_medallion.metadata import log
    from youtube_channel_analytics_medallion.metadata import watermark as wtm
    from youtube_channel_analytics_medallion.metadata.mode import get_mode
    from youtube_channel_analytics_medallion.raw import extract

    pipeline_name = "load_youtube_data"
    task_name = "raw_extract"
    channels = ["@nieinformatyk", "@DataWithBaraa"]

    parser = argparse.ArgumentParser()
    parser.add_argument("--date_of_data", type=_parse_date, default="")
    parser.add_argument("--max_workers", type=int, default=0,
                        help="parallel channel workers; 0 = one per channel")
    parser.add_argument("--reset_checkpoint", type=_str_to_bool, default=False)
    parser.add_argument("--mode", choices=["full", "incremental", "reload", "reload_date"], default=None)
    parser.add_argument("--start_layer", choices=["raw", "bronze", "silver", "gold"], default="raw")
    parser.add_argument("--end_layer", choices=["raw", "bronze", "silver", "gold"], default="gold")
    args, _ = parser.parse_known_args()
    run_id = get_config().run_id

    start_layer = args.start_layer

    mode = args.mode
    if mode is None:
        # get_mode reads the watermark table, so it has to exist first
        wtm.ensure_watermark_table()
        mode = get_mode(args.date_of_data)

    log.log_start_process(
        pipeline_name,
        run_id=run_id,
        channels=channels,
        date_of_data=args.date_of_data,
        mode=mode,
        max_workers=args.max_workers,
        reset_checkpoint=args.reset_checkpoint,
        task_key=task_name,
    )

    if start_layer == "raw":
        log.log_start_step(task_name)

        try:
            wtm.ensure_watermark_table()

            workers = args.max_workers if args.max_workers > 0 else len(channels)
            failed = []
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="channel") as pool:
                futures = {
                    pool.submit(extract.process_channel, run_id, ch, args.date_of_data, mode): ch
                    for ch in channels
                }
                for fut in as_completed(futures):
                    ch = futures[fut]
                    try:
                        fut.result()
                        print(f"[OK] channel {ch}")
                        log.log_info_step(task_name, f"channel {ch} OK")
                    except Exception as e:
                        print(f"[ERROR] channel {ch}: {e}")
                        log.log_error_step(task_name, f"channel {ch} FAILED: {e}", e)
                        failed.append(ch)

            if failed:
                raise RuntimeError(f"Extraction failed for channels: {failed}")

            log.log_end_step(task_name, "success")

            if args.end_layer == "raw":
                log.log_end_pipeline(pipeline_name, "success")
                
            log.flush_logs_to_adls(task_name)
        except Exception as error:
            log.log_end_step(task_name, "failed", error)
            log.log_end_pipeline(pipeline_name, "failed", error)
            log.flush_logs_to_adls(task_name)
            raise
