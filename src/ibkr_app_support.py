"""Shared config flatten/merge, CLI → dict mapping, and logging setup for IBKR bot scripts."""

from __future__ import annotations

import argparse
import datetime
from dataclasses import dataclass
import json
import logging
import logging.handlers
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Literal, Optional, Sequence, Tuple, Union
from zoneinfo import ZoneInfo

DaemonThreadSpec = Union[Callable[[], None], Tuple[str, Callable[[], None]]]

_IB_STATUS_INFO_CODES_DEFAULT = frozenset({2104, 2106, 2158})


def normalize_config_key(name: str) -> str:
    return name.replace("-", "_")


def is_ignored_config_key(key: str) -> bool:
    """True for comment/metadata keys omitted from the runtime config dict."""
    nk = normalize_config_key(str(key))
    return nk == "//" or nk.startswith("_")


def _config_json_object_pairs_hook(
    pairs: list[tuple[str, Any]],
) -> Dict[str, Any]:
    """Build a JSON object; allow repeated ``//`` comment keys (stdlib json cannot)."""
    out: Dict[str, Any] = {}
    seen: set[str] = set()
    for key, value in pairs:
        if is_ignored_config_key(key):
            continue
        nk = normalize_config_key(str(key))
        if nk in seen:
            raise ValueError(f"Duplicate config key {nk!r} in JSON object")
        seen.add(nk)
        out[key] = value
    return out


CONFIG_DIR_NAME = "config"
CONFIG_BASE_FILENAME = "base.json"


def config_base_path(for_config_path: str | Path) -> Path:
    """``config/base.json`` adjacent to the bot config file's directory."""
    return Path(for_config_path).resolve().parent / CONFIG_BASE_FILENAME


def _read_config_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f, object_pairs_hook=_config_json_object_pairs_hook)
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a JSON object at the top level")
    return data


def deep_merge_config_sections(
    base: Dict[str, Any],
    override: Dict[str, Any],
) -> Dict[str, Any]:
    """Recursively merge nested config sections; ``override`` wins on conflicts."""
    merged: Dict[str, Any] = dict(base)
    for key, override_val in override.items():
        if is_ignored_config_key(str(key)):
            continue
        base_val = merged.get(key)
        if isinstance(base_val, dict) and isinstance(override_val, dict):
            merged[key] = deep_merge_config_sections(base_val, override_val)
        else:
            merged[key] = override_val
    return merged


def flatten_config_sections(data: Dict[str, Any]) -> Dict[str, Any]:
    """Expand one level of nested dicts into flat keys.

    Top-level scalars and lists stay as-is. Nested dict values merge their snake_case keys
    into the result; top-level keys (except those starting with ``_``) override section values.
    Keys named ``//`` are comments and are omitted from the flattened config.
    """
    section_flat: Dict[str, Any] = {}
    top_level: Dict[str, Any] = {}
    for key, value in data.items():
        if is_ignored_config_key(str(key)):
            continue
        if isinstance(value, dict):
            for subkey, subval in value.items():
                if is_ignored_config_key(str(subkey)):
                    continue
                nk = normalize_config_key(str(subkey))
                if nk in section_flat:
                    raise ValueError(f"Duplicate config key {nk!r} (from nested sections)")
                section_flat[nk] = subval
        else:
            top_level[normalize_config_key(str(key))] = value
    merged = dict(section_flat)
    merged.update(top_level)
    return merged


def load_config_file(path: str, *, merge_base: bool = True) -> Dict[str, Any]:
    """Load a JSON config file, optionally merged with ``config/base.json`` first."""
    config_path = Path(path)
    data = _read_config_json(config_path)

    if merge_base and config_path.name != CONFIG_BASE_FILENAME:
        base_path = config_base_path(config_path)
        if base_path.is_file():
            base_data = _read_config_json(base_path)
            data = deep_merge_config_sections(base_data, data)

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


LEDGERS_DIR_NAME = "ledgers"


def ledgers_dir_from_config(config: Dict[str, Any]) -> Path:
    return Path(str(config.get("ledgers_dir", LEDGERS_DIR_NAME)))


def _sanitize_ledger_token(value: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(value))


def ledger_path_for_strategy(
    strategy: str,
    config: Dict[str, Any],
    *,
    client_id: Optional[int] = None,
    ledgers_dir: Optional[Path] = None,
) -> Path:
    """Path to this strategy's JSON ledger file under ``ledgers/`` (or ``ledgers_dir``)."""
    symbol = _sanitize_ledger_token(str(config.get("symbol", "UNKNOWN")))
    cid = int(
        client_id if client_id is not None else ib_client_id_from_config(config)
    )
    strategy_token = _sanitize_ledger_token(strategy)
    base = ledgers_dir if ledgers_dir is not None else ledgers_dir_from_config(config)
    return base / f"{strategy_token}_{symbol}_{cid}.json"


class PositionLedger:
    """Per-strategy position ledger persisted for parallel bot runs (keyed by client_id)."""

    def __init__(self, path: Path, data: Dict[str, Any]) -> None:
        self._path = path
        self._data = data

    @classmethod
    def open(
        cls,
        strategy: str,
        config: Dict[str, Any],
        *,
        client_id: Optional[int] = None,
        account: Optional[str] = None,
    ) -> PositionLedger:
        path = ledger_path_for_strategy(strategy, config, client_id=client_id)
        cid = int(
            client_id if client_id is not None else ib_client_id_from_config(config)
        )
        acct = str(
            account
            if account is not None
            else (config.get("account") or "")
        ).strip()
        if path.is_file():
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError(f"Ledger file must be a JSON object: {path}")
        else:
            data = {
                "strategy": strategy,
                "symbol": str(config.get("symbol", "")),
                "sec_type": str(config.get("sec_type", "STK")),
                "client_id": cid,
                "account": acct,
                "qty": 0,
                "avg_cost": 0.0,
                "updated_at": None,
                "last_exec_id": None,
                "ib_snapshot_qty": None,
                "ib_snapshot_avg_cost": None,
                "ib_snapshot_account": None,
                "ib_snapshot_at": None,
            }
        ledger = cls(path, data)
        ledger._data["strategy"] = strategy
        ledger._data["symbol"] = str(config.get("symbol", ledger._data.get("symbol", "")))
        ledger._data["sec_type"] = str(
            config.get("sec_type", ledger._data.get("sec_type", "STK"))
        )
        ledger._data["client_id"] = cid
        if acct:
            ledger._data["account"] = acct
        return ledger

    @property
    def path(self) -> Path:
        return self._path

    @property
    def qty(self) -> int:
        return int(self._data.get("qty", 0))

    @property
    def avg_cost(self) -> float:
        return float(self._data.get("avg_cost", 0.0))

    @property
    def ib_snapshot_qty(self) -> Optional[int]:
        raw = self._data.get("ib_snapshot_qty")
        return None if raw is None else int(raw)

    def apply_fill(
        self,
        side: str,
        shares: int,
        price: float,
        *,
        exec_id: Optional[str] = None,
    ) -> bool:
        """Update ledger from a fill. Returns False if ``exec_id`` was already applied."""
        if exec_id and exec_id == self._data.get("last_exec_id"):
            return False
        shares = int(shares)
        if shares <= 0:
            return False
        price = float(price)
        side_key = _normalize_fill_side(side)
        if side_key is None:
            return False

        qty = self.qty
        avg = self.avg_cost
        if side_key == "BUY":
            if qty >= 0:
                total_cost = qty * avg + shares * price
                qty += shares
                avg = total_cost / qty if qty > 0 else 0.0
            else:
                qty += shares
                if qty > 0:
                    avg = price
                elif qty == 0:
                    avg = 0.0
        else:
            shares = min(shares, max(0, qty))
            if shares <= 0:
                return False
            qty -= shares
            if qty <= 0:
                avg = 0.0 if qty == 0 else price

        self._data["qty"] = int(qty)
        self._data["avg_cost"] = float(avg)
        if exec_id:
            self._data["last_exec_id"] = str(exec_id)
        return True

    def record_ib_snapshot(
        self,
        account: str,
        qty: int,
        avg_cost: float,
    ) -> None:
        """Store the latest account-level IB position snapshot (does not change ledger qty)."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._data["ib_snapshot_account"] = str(account)
        self._data["ib_snapshot_qty"] = int(qty)
        self._data["ib_snapshot_avg_cost"] = float(avg_cost)
        self._data["ib_snapshot_at"] = now

    def save(self) -> None:
        self._data["updated_at"] = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        payload = json.dumps(self._data, indent=2, sort_keys=True) + "\n"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
        tmp.replace(self._path)

    def max_sell_shares(self) -> int:
        """Shares this strategy may sell without exceeding ledger or account long size."""
        return max_sell_shares(self)


def max_sell_shares(ledger: PositionLedger) -> int:
    """Cap sell size so the account cannot go short on this symbol.

    Uses ``min(ledger qty, IB position snapshot)`` when a snapshot is available.
    """
    own = max(0, int(ledger.qty))
    ib = ledger.ib_snapshot_qty
    if ib is None:
        return own
    return min(own, max(0, int(ib)))


def clamp_sell_quantity(ledger: PositionLedger, desired_qty: int) -> int:
    """Return ``desired_qty`` capped to :func:`max_sell_shares`."""
    return min(max(0, int(desired_qty)), max_sell_shares(ledger))


def seed_ledger_position(
    ledger: PositionLedger,
    target: Any,
    qty: int,
    *,
    qty_attr: str = "position_size",
    avg_attr: Optional[str] = "avg_cost",
    avg_cost: float = 0.0,
    account: str = "TEST",
    clamp_qty_nonneg: bool = False,
) -> None:
    """Align ledger, IB snapshot, and in-memory position for tests or manual resets."""
    q = max(0, int(qty)) if clamp_qty_nonneg else int(qty)
    ac = float(avg_cost) if q > 0 else 0.0
    ledger._data["qty"] = q
    ledger._data["avg_cost"] = ac
    ledger.record_ib_snapshot(account, q, ac)
    setattr(target, qty_attr, q)
    if avg_attr is not None and hasattr(target, avg_attr):
        setattr(target, avg_attr, ac)


def price_digits_from_config(config: Dict[str, Any]) -> int:
    return int(config.get("price_round_digits", 2))


def price_tick_from_config(config: Dict[str, Any]) -> float:
    return 10.0 ** (-price_digits_from_config(config))


def mid_delta_for_config(config: Dict[str, Any], *, default: float = 0.02) -> float:
    """Minimum half-spread vs mid for self-trade guards (``mid_delta`` or legacy deltas)."""
    tick = price_tick_from_config(config)
    if "mid_delta" in config:
        return max(float(config["mid_delta"]), tick)
    buy_d = float(config.get("buy_delta", default))
    sell_d = float(config.get("sell_delta", default))
    return max(abs(buy_d), abs(sell_d), tick)


def nbbo_mid_rounded(bid: float, ask: float, config: Dict[str, Any]) -> float:
    digits = price_digits_from_config(config)
    return round((float(bid) + float(ask)) / 2.0, digits)


def self_trade_limits_from_nbbo(
    bid: float,
    ask: float,
    config: Dict[str, Any],
) -> Tuple[float, float]:
    """Return ``(buy_cap, sell_floor)`` at ``mid ∓ mid_delta`` for valid NBBO."""
    digits = price_digits_from_config(config)
    delta = mid_delta_for_config(config)
    mid = nbbo_mid_rounded(bid, ask, config)
    buy_cap = round(mid - delta, digits)
    sell_floor = round(mid + delta, digits)
    return buy_cap, sell_floor


def self_trade_limits_from_mid(
    mid: float,
    config: Dict[str, Any],
) -> Tuple[float, float]:
    """Return ``(buy_cap, sell_floor)`` when only a mid proxy is available."""
    digits = price_digits_from_config(config)
    delta = mid_delta_for_config(config)
    m = round(float(mid), digits)
    buy_cap = round(m - delta, digits)
    sell_floor = round(m + delta, digits)
    return buy_cap, sell_floor


def has_valid_nbbo(
    bid: Optional[float],
    ask: Optional[float],
) -> bool:
    if bid is None or ask is None:
        return False
    try:
        b = float(bid)
        a = float(ask)
    except (TypeError, ValueError):
        return False
    return b > 0 and a > b


def is_valid_quote_pair(
    buy_px: Optional[float],
    sell_px: Optional[float],
) -> bool:
    """True when both prices exist and buy is strictly below sell."""
    if buy_px is None or sell_px is None:
        return False
    try:
        return float(buy_px) < float(sell_px)
    except (TypeError, ValueError):
        return False


def nbbo_coalesce_intervals_from_config(
    config: Dict[str, Any],
    *,
    quiet_default: float = 0.35,
    max_default: float = 1.0,
) -> Tuple[float, float]:
    """Return ``(quiet_seconds, max_interval_seconds)`` for :class:`NbboCoalescer`."""
    quiet = float(
        config.get(
            "nbbo_coalesce_seconds",
            config.get("resync_debounce_seconds", quiet_default),
        )
    )
    max_interval = float(config.get("nbbo_coalesce_max_seconds", max_default))
    return quiet, max_interval


@dataclass(frozen=True)
class WorkingOrderReconcilePlan:
    """How to reconcile a working order when ``remaining`` drifts from target."""

    kind: Literal["noop", "cancel", "amend"]
    remaining_now: int = 0
    filled_now: int = 0
    new_total_qty: int = 0


def parse_order_remaining(remaining_raw: Any) -> Optional[int]:
    if remaining_raw is None:
        return None
    try:
        return int(float(remaining_raw))
    except (TypeError, ValueError):
        return None


def plan_working_order_reconcile(
    remaining_raw: Any,
    *,
    current_total_qty: int,
    desired_remaining: int,
    min_order_size: int,
) -> WorkingOrderReconcilePlan:
    """Plan cancel/amend when IB ``remaining`` no longer matches desired working size."""
    remaining_now = parse_order_remaining(remaining_raw)
    if remaining_now is None:
        return WorkingOrderReconcilePlan("noop")
    if remaining_now == desired_remaining:
        return WorkingOrderReconcilePlan("noop")

    filled_now = max(0, int(current_total_qty) - remaining_now)
    if desired_remaining < min_order_size:
        return WorkingOrderReconcilePlan(
            "cancel",
            remaining_now=remaining_now,
            filled_now=filled_now,
        )

    return WorkingOrderReconcilePlan(
        "amend",
        remaining_now=remaining_now,
        filled_now=filled_now,
        new_total_qty=filled_now + desired_remaining,
    )


def clamp_buy_to_avoid_self_trade(
    buy_px: float,
    config: Dict[str, Any],
    *,
    bid: Optional[float] = None,
    ask: Optional[float] = None,
    mid: Optional[float] = None,
) -> float:
    """Never bid above ``mid - mid_delta`` (avoids crossing own sell)."""
    if has_valid_nbbo(bid, ask):
        buy_cap, _ = self_trade_limits_from_nbbo(float(bid), float(ask), config)
    elif mid is not None:
        buy_cap, _ = self_trade_limits_from_mid(float(mid), config)
    else:
        return float(buy_px)
    return min(float(buy_px), buy_cap)


def clamp_sell_to_avoid_self_trade(
    sell_px: float,
    config: Dict[str, Any],
    *,
    bid: Optional[float] = None,
    ask: Optional[float] = None,
    mid: Optional[float] = None,
) -> float:
    """Never offer below ``mid + mid_delta`` (avoids crossing own buy)."""
    if has_valid_nbbo(bid, ask):
        _, sell_floor = self_trade_limits_from_nbbo(float(bid), float(ask), config)
    elif mid is not None:
        _, sell_floor = self_trade_limits_from_mid(float(mid), config)
    else:
        return float(sell_px)
    return max(float(sell_px), sell_floor)


def clamp_quote_prices_to_avoid_self_trade(
    buy_px: float,
    sell_px: float,
    config: Dict[str, Any],
    *,
    bid: Optional[float] = None,
    ask: Optional[float] = None,
    mid: Optional[float] = None,
) -> Tuple[float, float]:
    """Apply buy cap and sell floor together."""
    buy = clamp_buy_to_avoid_self_trade(
        buy_px, config, bid=bid, ask=ask, mid=mid
    )
    sell = clamp_sell_to_avoid_self_trade(
        sell_px, config, bid=bid, ask=ask, mid=mid
    )
    return buy, sell


def _normalize_fill_side(side: str) -> Optional[str]:
    raw = str(side or "").upper()
    if raw in ("BOT", "BUY"):
        return "BUY"
    if raw in ("SLD", "SELL"):
        return "SELL"
    return None


def execution_fill_tuple(execution: Any) -> Optional[Tuple[str, int, float]]:
    """Parse an IB ``execution`` into ``(side, shares, price)``."""
    side = _normalize_fill_side(getattr(execution, "side", ""))
    if side is None:
        return None
    try:
        shares = int(float(execution.shares))
        price = float(execution.price)
    except (TypeError, ValueError):
        return None
    if shares <= 0:
        return None
    return side, shares, price


def sync_attrs_from_ledger(
    ledger: PositionLedger,
    target: Any,
    *,
    qty_attr: str = "position_size",
    avg_attr: Optional[str] = "avg_cost",
    clamp_qty_nonneg: bool = False,
) -> None:
    qty = ledger.qty
    if clamp_qty_nonneg:
        qty = max(0, qty)
    setattr(target, qty_attr, qty)
    if avg_attr is not None and hasattr(target, avg_attr):
        setattr(target, avg_attr, ledger.avg_cost if qty > 0 else 0.0)


def handle_ledger_execution(
    ledger: PositionLedger,
    execution: Any,
    our_client_id: int,
    *,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Apply a client-owned fill to ``ledger`` and persist. Returns True if applied."""
    if not execution_belongs_to_client(execution, our_client_id):
        return False
    fill = execution_fill_tuple(execution)
    if fill is None:
        return False
    side, shares, price = fill
    exec_id = getattr(execution, "execId", None)
    if not ledger.apply_fill(side, shares, price, exec_id=exec_id):
        return False
    ledger.save()
    if logger is not None:
        logger.info(
            "Ledger %s qty=%s avg_cost=%.4f (%s %s @ %.4f execId=%s)",
            ledger.path.name,
            ledger.qty,
            ledger.avg_cost,
            side,
            shares,
            price,
            exec_id,
        )
    return True


def handle_ledger_ib_position(
    ledger: PositionLedger,
    account: str,
    qty: int,
    avg_cost: float,
    *,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Record IB ``position`` snapshot; log if it differs from ledger qty."""
    ledger.record_ib_snapshot(account, qty, avg_cost)
    if logger is not None and ledger.qty != int(qty):
        logger.warning(
            "Ledger %s qty=%s differs from IB position snapshot qty=%s "
            "(account=%s avgCost=%.4f); using ledger qty for trading",
            ledger.path.name,
            ledger.qty,
            int(qty),
            account,
            float(avg_cost),
        )
    ledger.save()


def default_config_path(script_file: str | Path) -> str:
    """Default JSON config path under ``config/`` (``peg_primary.py`` → ``config/peg_primary.json``)."""
    name = Path(script_file).with_suffix(".json").name
    return str(Path("config") / name)


def add_config_argument(
    parser: argparse.ArgumentParser,
    script_file: str | Path,
) -> None:
    parser.add_argument(
        "--config",
        default=default_config_path(script_file),
        help="Path to JSON config file (default: config/<script>.json)",
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
