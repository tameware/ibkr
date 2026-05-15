"""Shared config flatten/merge, CLI → dict mapping, and logging setup for IBKR bot scripts."""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import logging.handlers
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

_IB_STATUS_INFO_CODES_DEFAULT = frozenset({2104, 2106, 2158})


def normalize_config_key(name: str) -> str:
    return name.replace("-", "_")


def flatten_config_sections(data: Dict[str, Any]) -> Dict[str, Any]:
    """Expand one level of nested dicts into flat keys.

    Top-level scalars and lists stay as-is. Nested dict values merge their snake_case keys
    into the result; top-level keys (except those starting with ``_``) override section values.
    """
    section_flat: Dict[str, Any] = {}
    top_level: Dict[str, Any] = {}
    for key, value in data.items():
        sk = normalize_config_key(str(key))
        if sk.startswith("_"):
            continue
        if isinstance(value, dict):
            for subkey, subval in value.items():
                nk = normalize_config_key(str(subkey))
                if nk.startswith("_"):
                    continue
                if nk in section_flat:
                    raise ValueError(f"Duplicate config key {nk!r} (from nested sections)")
                section_flat[nk] = subval
        else:
            top_level[sk] = value
    merged = dict(section_flat)
    merged.update(top_level)
    return merged


def load_config_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a JSON object at the top level")
    return flatten_config_sections(data)


def cli_to_config(args: argparse.Namespace) -> Dict[str, Any]:
    """Map argparse ``Namespace`` to a dict of non-``None`` overrides (excludes ``config`` path)."""
    result: Dict[str, Any] = {}
    for key, value in vars(args).items():
        if key == "config" or value is None:
            continue
        result[key] = value
    return result


def merge_config(file_config: Dict[str, Any], cli_config: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(file_config)
    merged.update(cli_config)
    return merged


def require_fields(config: Dict[str, Any], required_fields: list[str]) -> None:
    missing = [field for field in required_fields if field not in config]
    if missing:
        raise ValueError(f"Missing required configuration fields: {', '.join(missing)}")


def default_config_path(script_file: str | Path) -> str:
    """Default JSON config path beside a bot script (``script.py`` → ``script.json``)."""
    return str(Path(script_file).with_suffix(".json"))


def add_config_argument(
    parser: argparse.ArgumentParser,
    script_file: str | Path,
) -> None:
    parser.add_argument(
        "--config",
        default=default_config_path(script_file),
        help="Path to JSON config file (default: script name with .json suffix)",
    )


def add_logging_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--log_dir")
    parser.add_argument("--log_file")
    parser.add_argument("--level")
    parser.add_argument("--max_bytes", type=int)
    parser.add_argument("--backup_count", type=int)
    parser.add_argument(
        "--console",
        action=argparse.BooleanOptionalAction,
    )


def add_session_hours_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market_timezone")
    parser.add_argument("--market_open_hour", type=int)
    parser.add_argument("--market_open_minute", type=int)
    parser.add_argument("--market_close_hour", type=int)


def add_ib_connection_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_client_id: bool = True,
    include_account: bool = False,
) -> None:
    """Common IBKR socket and contract identity CLI overrides."""
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    if include_client_id:
        parser.add_argument("--client_id", type=int)
    if include_account:
        parser.add_argument("--account")
    parser.add_argument("--symbol")
    parser.add_argument("--sec_type")
    parser.add_argument("--currency")
    parser.add_argument("--exchange")
    parser.add_argument("--primary_exchange")


def load_merged_config(
    args: argparse.Namespace,
    *,
    required: Optional[list[str]] = None,
    config_path_key: str = "config",
) -> Dict[str, Any]:
    """Load JSON config from ``args``, apply CLI overrides, optionally validate keys."""
    path = str(getattr(args, config_path_key))
    file_config = load_config_file(path)
    config = merge_config(file_config, cli_to_config(args))
    if required is not None:
        require_fields(config, required)
    return config


def cfg_bool(config: Dict[str, Any], key: str, default: bool) -> bool:
    if key not in config:
        return default
    v = config[key]
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "off", ""):
        return False
    if s in ("1", "true", "yes", "on"):
        return True
    raise ValueError(f"Invalid boolean for {key!r}: {v!r}")


def build_logger(
    config: Dict[str, Any],
    *,
    logger_name: str,
    default_log_file: str,
) -> logging.Logger:
    """Rotating file log + optional console; closes prior handlers on the named logger."""
    log_dir = Path(str(config.get("log_dir", "logs")))
    log_file = str(config.get("log_file", default_log_file))
    level_name = str(config.get("level", "INFO")).upper()
    max_bytes = int(config.get("max_bytes", 5_000_000))
    backup_count = int(config.get("backup_count", 10))
    use_console = cfg_bool(config, "console", True)

    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level_name, logging.INFO))
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()
    logger.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(threadName)s %(message)s"
    )

    fh = logging.handlers.RotatingFileHandler(
        log_dir / log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    fh.setLevel(getattr(logging, level_name, logging.INFO))
    logger.addHandler(fh)

    if use_console:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        ch.setLevel(getattr(logging, level_name, logging.INFO))
        logger.addHandler(ch)

    return logger


def session_wall_clock(
    config: Dict[str, Any],
    now: Optional[datetime.datetime] = None,
) -> Tuple[datetime.datetime, str, int, int, int]:
    """Current time in ``market_timezone`` and configured regular-session window."""
    tz_name = str(config["market_timezone"])
    tz = ZoneInfo(tz_name)
    if now is None:
        now = datetime.datetime.now(tz=tz)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)
    moh = int(config["market_open_hour"])
    mom = int(config["market_open_minute"])
    mch = int(config["market_close_hour"])
    return now, tz_name, moh, mom, mch


def regular_session_open(
    config: Dict[str, Any],
    now: Optional[datetime.datetime] = None,
) -> bool:
    """True when ``now`` is inside ``[market_open, market_close)`` in ``market_timezone``."""
    now, _, moh, mom, mch = session_wall_clock(config, now=now)
    return (
        (now.hour > moh or (now.hour == moh and now.minute >= mom))
        and now.hour < mch
    )


def log_session_transition(
    logger: logging.Logger,
    config: Dict[str, Any],
    *,
    prev_in_hours: Optional[bool],
    in_hours: bool,
) -> None:
    """Log once at startup outside the window or on the first transition to closed."""
    if prev_in_hours is None and not in_hours:
        now, tz_name, moh, mom, mch = session_wall_clock(config)
        logger.info(
            "Configured regular session is closed at startup (now %s; "
            "window %02d:%02d–%02d:00 %s). Quoting paused.",
            now.strftime("%Y-%m-%d %H:%M:%S %Z"),
            moh,
            mom,
            mch,
            tz_name,
        )
    elif prev_in_hours is True and not in_hours:
        now, tz_name, moh, mom, mch = session_wall_clock(config)
        logger.info(
            "Regular session ended (now %s; configured window %02d:%02d–%02d:00 %s). "
            "Quoting paused; DAY orders may be canceled by the broker at the close.",
            now.strftime("%Y-%m-%d %H:%M:%S %Z"),
            moh,
            mom,
            mch,
            tz_name,
        )


def should_suppress_ib_error(
    config: Dict[str, Any],
    error_code: Any,
    error_string: Any,
    *,
    advanced_order_reject: str = "",
) -> bool:
    """True when this IB API error should not be logged."""
    reject_text = str(advanced_order_reject or "")
    if reject_text and "data farm" in reject_text.lower():
        return True

    codes = config.get("ignored_error_codes") or []
    if str(error_code) in {str(x) for x in codes}:
        return True

    error_text = str(error_string or "")
    for text in config.get("ignore_error_substrings") or []:
        if text and (text in error_text or text in reject_text):
            return True
    return False


def ib_error_is_status_info(
    error_code: Any,
    *,
    status_info_codes: Optional[frozenset[int]] = None,
) -> bool:
    """True for routine IB connection / market-data status codes (log at INFO)."""
    codes = (
        status_info_codes
        if status_info_codes is not None
        else _IB_STATUS_INFO_CODES_DEFAULT
    )
    try:
        return int(error_code) in codes
    except (TypeError, ValueError):
        return str(error_code) in {str(c) for c in codes}


def format_ib_error_message(
    req_id: Any,
    error_time: Any,
    error_code: Any,
    error_string: Any,
    advanced_order_reject: str = "",
) -> str:
    return (
        f"Error reqId={req_id} errorTime={error_time} errorCode={error_code} "
        f"errorString={error_string} advancedOrderRejectJson={advanced_order_reject}"
    )


def log_ib_error(
    logger: logging.Logger,
    config: Dict[str, Any],
    *,
    req_id: Any,
    error_time: Any,
    error_code: Any,
    error_string: Any,
    advanced_order_reject: str = "",
    status_info_codes: Optional[frozenset[int]] = None,
) -> bool:
    """Log IB API error unless suppressed. Returns True if a line was logged."""
    if should_suppress_ib_error(
        config, error_code, error_string, advanced_order_reject=advanced_order_reject
    ):
        return False

    text = str(error_string or "")
    if ib_error_is_status_info(error_code, status_info_codes=status_info_codes):
        msg = f"reqId={req_id} errorTime={error_time} code={error_code} msg={text}"
        if advanced_order_reject:
            msg += f" reject={advanced_order_reject}"
        logger.info("IB status: %s", msg)
        return True

    logger.warning(
        "Error reqId=%s errorTime=%s errorCode=%s errorString=%s advancedOrderRejectJson=%s",
        req_id,
        error_time,
        error_code,
        text,
        advanced_order_reject,
    )
    return True


def safe_cancel_order(client: Any, order_id: int) -> None:
    """Cancel an order via the IB API.

    No-op when disconnected or ``serverVersion()`` is unset (avoids ibapi
    ``useProtoBuf()`` failures mid-teardown). Tries ``OrderCancel`` (new ibapi),
    then one-arg ``cancelOrder``, then legacy two-arg ``cancelOrder(id, "")``.
    """
    if not client.isConnected() or client.serverVersion() is None:
        return

    try:
        from ibapi.order_cancel import OrderCancel
    except ImportError:
        OrderCancel = None  # type: ignore[misc, assignment]

    if OrderCancel is not None:
        try:
            client.cancelOrder(order_id, OrderCancel())
            return
        except TypeError:
            pass

    try:
        client.cancelOrder(order_id)
        return
    except TypeError:
        client.cancelOrder(order_id, "")


def wait_for_ib_ready(
    is_ready: Callable[[], bool],
    *,
    is_connected: Optional[Callable[[], bool]] = None,
    timeout_seconds: float = 30.0,
    poll_seconds: float = 0.1,
) -> bool:
    """Poll until ``is_ready()`` is true or ``timeout_seconds`` elapse."""
    deadline = time.monotonic() + timeout_seconds
    saw_connected = False
    while time.monotonic() < deadline:
        if is_ready():
            return True
        if is_connected is not None:
            if is_connected():
                saw_connected = True
            elif saw_connected:
                return False
        time.sleep(poll_seconds)
    return False
