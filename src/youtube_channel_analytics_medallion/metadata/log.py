from youtube_channel_analytics_medallion.config import app_config as ac
from pathlib import Path
from typing import Literal
import logging as lg
import tempfile
import traceback


__LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
__STEP_HANDLERS: dict[str, lg.FileHandler] = {}

lg.basicConfig(level=lg.INFO, format=__LOG_FORMAT, force=True)


__LOCAL_LOG_DIR: Path | None = None


def __local_log_path(step_name: str) -> Path:
    # Private per-process dir (mode 700): a shared /tmp/<run_id> may be owned by another task's user.
    global __LOCAL_LOG_DIR
    if __LOCAL_LOG_DIR is None:
        __LOCAL_LOG_DIR = Path(tempfile.mkdtemp(prefix=f"{ac.get_config().run_id}_"))
    return __LOCAL_LOG_DIR / f"{step_name}.log"


def __adls_log_path(step_name: str) -> str:
    config = ac.get_config()
    return f"{config.log_path.rstrip('/')}/{config.run_id}/{step_name}.log"


def __attach_step_handler(step_name: str) -> None:
    if step_name in __STEP_HANDLERS:
        return
    path = __local_log_path(step_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = lg.FileHandler(path, mode="a", encoding="utf-8")
    handler.setFormatter(lg.Formatter(__LOG_FORMAT))
    lg.getLogger().addHandler(handler)
    __STEP_HANDLERS[step_name] = handler

def log_start_process(pipeline_name: str, **params):
    params_str = ", ".join(f"{key}={value!r}" for key, value in params.items())
    lg.info(f"Pipeline `{pipeline_name}` started with parameters: %s", params_str or "none")


def log_start_step(step_name: str):
    __attach_step_handler(step_name)
    lg.info(f"{step_name} - START")
    

def log_info_step(step_name: str, communicate: str):
    lg.info(f"{step_name} - {communicate}")


def log_warning_step(step_name: str, communicate: str):
    lg.warning(f"{step_name} - {communicate}")


def __format_traceback(e: BaseException | None) -> str:
    if e is None:
        return "no exception provided"
    return "".join(traceback.format_exception(type(e), e, e.__traceback__))


def log_end_step(step_name: str, status: Literal["success", "failed"], e: Exception = None):
    if status == "success":
        lg.info(f"Step `{step_name}` FINISHED !!!")
    else:
        lg.error(f"Step `{step_name}` ENDED WITH ERROR !!!\n%s", __format_traceback(e))


def log_end_pipeline(pipeline_name: str, status: Literal["success", "failed"], e: Exception = None):
    if status == "success":
        lg.info(f"Pipeline `{pipeline_name}` FINISHED !!!")
    else:
        lg.error(f"Pipeline `{pipeline_name}` ENDED WITH ERROR !!!\n%s", __format_traceback(e))


def flush_logs_to_adls(step_name: str) -> None:
    """Close the step's local log file and copy it to `<log_path>/<run_id>/<step_name>.log`."""
    from databricks.sdk.runtime import dbutils

    handler = __STEP_HANDLERS.pop(step_name, None)
    if handler is not None:
        handler.flush()
        lg.getLogger().removeHandler(handler)
        handler.close()

    # dbutils.fs.cp can't read local file: paths on shared clusters, so read in Python and put.
    content = __local_log_path(step_name).read_text(encoding="utf-8")
    dbutils.fs.put(__adls_log_path(step_name), content, True)
