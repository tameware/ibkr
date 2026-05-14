"""Shared config flatten/merge, CLI → dict mapping, and logging setup for IBKR bot scripts."""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Any, Dict


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
