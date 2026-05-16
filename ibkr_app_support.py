"""Shared config flatten/merge, CLI → dict mapping, and logging setup for IBKR bot scripts."""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import logging.handlers
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union
from zoneinfo import ZoneInfo

DaemonThreadSpec = Union[Callable[[], None], Tuple[str, Callable[[], None]]]

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


def ib_client_id_from_config(config: Dict[str, Any], *, default: int = 1) -> int:
    """Socket API ``client_id`` used when connecting; attributes orders/fills to this session."""
    try:
        return int(config.get("client_id", default))
    except (TypeError, ValueError):
        return default


def open_order_belongs_to_client(order: Any, our_client_id: int) -> bool:
    """True if IB ``Order`` in ``openOrder`` belongs to this API client."""
    if order is None:
        return False
    if not hasattr(order, "clientId"):
        return True
    try:
        return int(order.clientId) == int(our_client_id)
    except (TypeError, ValueError):
        return False


def execution_belongs_to_client(execution: Any, our_client_id: int) -> bool:
    """True if ``execDetails`` execution originated from ``our_client_id``."""
    if execution is None:
        return False
    if not hasattr(execution, "clientId"):
        return True
    try:
        return int(execution.clientId) == int(our_client_id)
    except (TypeError, ValueError):
        return False


def order_status_clients_match(wrapper_client_id: Any, our_client_id: int) -> bool:
    """``orderStatus`` callbacks pass ``clientId`` of the originating client."""
    try:
        return int(wrapper_client_id) == int(our_client_id)
    except (TypeError, ValueError):
        return False


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


def make_stock_contract(
    config: Dict[str, Any],
    *,
    symbol: Optional[str] = None,
    sec_type: Optional[str] = None,
    currency: Optional[str] = None,
    exchange: Optional[str] = None,
    primary_exchange: Optional[str] = None,
) -> Any:
    """Build an IB ``Contract`` for a US-style stock from config (and optional overrides)."""
    from ibapi.contract import Contract

    c = Contract()
    c.symbol = str(symbol if symbol is not None else config["symbol"])
    c.secType = str(sec_type if sec_type is not None else config.get("sec_type", "STK"))
    c.currency = str(currency if currency is not None else config.get("currency", "USD"))
    c.exchange = str(exchange if exchange is not None else config.get("exchange", "SMART"))
    c.primaryExchange = str(
        primary_exchange
        if primary_exchange is not None
        else config.get("primary_exchange", "NYSE")
    )
    return c


class NbboThrottle:
    """Rate-limit NBBO-driven work: skip when interval has not elapsed and the key is unchanged."""

    def __init__(
        self,
        interval_seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.interval_seconds = float(interval_seconds)
        self._clock = clock
        self._last_run_ts = 0.0
        self._last_key: Any = None

    @property
    def last_key(self) -> Any:
        return self._last_key

    @property
    def last_run_ts(self) -> float:
        return self._last_run_ts

    def should_run(
        self,
        nbbo_key: Any,
        *,
        force: bool = False,
        bypass_if: Optional[Callable[[], bool]] = None,
    ) -> bool:
        if force:
            return True
        if bypass_if is not None and bypass_if():
            return True
        now = self._clock()
        if (
            self.interval_seconds > 0
            and (now - self._last_run_ts) < self.interval_seconds
            and nbbo_key == self._last_key
        ):
            return False
        return True

    def mark_ran(self, nbbo_key: Any) -> None:
        self._last_run_ts = self._clock()
        self._last_key = nbbo_key

    def reset(self) -> None:
        self._last_run_ts = 0.0
        self._last_key = None


class NbboCoalescer:
    """Debounce bid/ask tick bursts and cap how often flushes may run.

  * ``interval_seconds`` — trailing quiet period; each tick resets the timer.
  * ``max_interval_seconds`` — minimum time between flushes (wall clock since the
    last flush). During a continuous tick stream, a flush is forced at least this
    often even if the quiet period never elapses. After a quiet-period flush, a
    new flush waits until ``max_interval_seconds`` has passed since the last one.
    """

    def __init__(
        self,
        interval_seconds: float,
        on_flush: Callable[[], None],
        *,
        max_interval_seconds: float = 0.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.interval_seconds = max(0.0, float(interval_seconds))
        self.max_interval_seconds = max(0.0, float(max_interval_seconds))
        self._on_flush = on_flush
        self._clock = clock
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._last_flush_ts: Optional[float] = None
        self._anchor_ts: Optional[float] = None

    def _max_deadline_elapsed(self, now: float) -> bool:
        if self.max_interval_seconds <= 0 or self._anchor_ts is None:
            return False
        return (now - self._anchor_ts) >= self.max_interval_seconds

    def _arm_quiet_timer_locked(self, delay: float) -> None:
        if self._timer is not None:
            self._timer.cancel()
        self._timer = threading.Timer(delay, self._fire)
        self._timer.daemon = True
        self._timer.start()

    def _do_flush(self) -> None:
        now = self._clock()
        self._last_flush_ts = now
        self._anchor_ts = now
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
        self._on_flush()

    def schedule(self) -> None:
        """Note a new tick; flush after quiet period and/or max-interval deadline."""
        if self.interval_seconds <= 0 and self.max_interval_seconds <= 0:
            self._do_flush()
            return

        now = self._clock()
        flush_now = False
        with self._lock:
            if self._anchor_ts is None:
                self._anchor_ts = now
            if self._max_deadline_elapsed(now):
                flush_now = True
                if self._timer is not None:
                    self._timer.cancel()
                    self._timer = None
            elif self.interval_seconds > 0:
                self._arm_quiet_timer_locked(self.interval_seconds)

        if flush_now:
            self._do_flush()

    def _fire(self) -> None:
        """Quiet-period timer: flush unless capped by ``max_interval_seconds``."""
        now = self._clock()
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            if (
                self.max_interval_seconds > 0
                and self._last_flush_ts is not None
                and (now - self._last_flush_ts) < self.max_interval_seconds
            ):
                delay = self.max_interval_seconds - (now - self._last_flush_ts)
                self._arm_quiet_timer_locked(delay)
                return
        self._do_flush()

    def cancel(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self._last_flush_ts = None
            self._anchor_ts = None


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

    log_startup_timezones(config, logger=logger)
    return logger


def log_startup_timezones(
    config: Dict[str, Any],
    *,
    logger: Optional[logging.Logger] = None,
    now: Optional[datetime.datetime] = None,
) -> None:
    """Log local wall clock and, when configured, ``market_timezone`` wall clock at startup."""
    if now is None:
        local_now = datetime.datetime.now().astimezone()
    else:
        local_now = (
            now.astimezone()
            if now.tzinfo is not None
            else now.replace(tzinfo=datetime.timezone.utc).astimezone()
        )

    local_tz = (
        getattr(local_now.tzinfo, "key", None)
        or local_now.tzname()
        or str(local_now.tzinfo)
    )

    def emit(fmt: str, *args: object) -> None:
        if logger is not None:
            logger.info(fmt, *args)
        else:
            print(fmt % args, flush=True)

    emit(
        "Startup local timezone=%s, now=%s",
        local_tz,
        local_now.strftime("%Y-%m-%d %H:%M:%S %Z"),
    )

    market_tz = config.get("market_timezone")
    if not market_tz:
        return

    tz_name = str(market_tz)
    tz = ZoneInfo(tz_name)
    if now is None:
        market_now = datetime.datetime.now(tz=tz)
    elif now.tzinfo is None:
        market_now = now.replace(tzinfo=tz)
    else:
        market_now = now.astimezone(tz)

    emit(
        "Startup market_timezone=%s, now=%s",
        tz_name,
        market_now.strftime("%Y-%m-%d %H:%M:%S %Z"),
    )


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


def disconnect_cleanly(
    client: Any,
    *,
    logger: Optional[logging.Logger] = None,
    market_data_req_ids: Optional[Sequence[int]] = None,
    settle_seconds: float = 1.0,
    done_message: str = "Disconnected cleanly.",
) -> None:
    """Cancel market data (if connected), pause briefly, then disconnect."""
    if market_data_req_ids and client.isConnected() and client.serverVersion() is not None:
        for req_id in market_data_req_ids:
            try:
                client.cancelMktData(req_id)
            except Exception as e:
                if logger is not None:
                    logger.warning("cancelMktData(%s) failed: %s", req_id, e)

    if settle_seconds > 0:
        time.sleep(settle_seconds)

    try:
        client.disconnect()
    except Exception as e:
        if logger is not None:
            logger.warning("Disconnect failed: %s", e)

    if logger is not None:
        logger.info(done_message)


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


def idle_until_shutdown(app: Any, *, poll_seconds: float = 1.0) -> None:
    """Block until ``app.shutdown_flag`` is set or the socket disconnects."""
    while not getattr(app, "shutdown_flag", False):
        if hasattr(app, "isConnected") and not app.isConnected():
            break
        time.sleep(poll_seconds)


def run_bot(
    app: Any,
    config: Dict[str, Any],
    *,
    is_ready: Callable[[], bool],
    main_loop: Callable[[], None],
    extra_daemon_threads: Optional[Sequence[DaemonThreadSpec]] = None,
    connect_timeout_seconds: Optional[float] = None,
    ready_label: str = "IBKR API ready",
) -> int:
    """Connect, start the API thread, wait for readiness, run ``main_loop``, then stop.

    Returns 0 on normal completion, 1 if the ready wait times out.
    """
    host = str(config["host"])
    port = int(config["port"])
    client_id = int(config.get("client_id", getattr(app, "client_id", 1)))
    timeout = float(
        connect_timeout_seconds
        if connect_timeout_seconds is not None
        else config.get("connect_timeout_seconds", 30.0)
    )
    logger = getattr(app, "logger", None)

    if logger is not None:
        logger.info(
            "Connecting to IBKR host=%s port=%s client_id=%s",
            host,
            port,
            client_id,
        )

    app.connect(host, port, client_id)

    threading.Thread(target=app.run, name="IBAPI", daemon=True).start()

    if extra_daemon_threads:
        for spec in extra_daemon_threads:
            if isinstance(spec, tuple):
                name, target = spec
            else:
                name, target = "IBBotExtra", spec
            threading.Thread(target=target, name=name, daemon=True).start()

    if not wait_for_ib_ready(
        is_ready,
        is_connected=app.isConnected,
        timeout_seconds=timeout,
    ):
        if logger is not None:
            logger.error(
                "Timed out after %.0fs waiting for %s (connected=%s). "
                "Check TWS/IB Gateway, API settings, host/port, and client_id=%s.",
                timeout,
                ready_label,
                app.isConnected(),
                client_id,
            )
        try:
            app.disconnect()
        except Exception:
            pass
        return 1

    def handle_sig(*_args: object) -> None:
        if logger is not None:
            logger.info("Stopping...")
        app.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    try:
        main_loop()
    except KeyboardInterrupt:
        handle_sig()
    finally:
        app.stop()

    return 0
