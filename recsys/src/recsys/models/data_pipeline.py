"""Streaming utilities to slice the T-ECD dataset for next-action modeling.

This keeps downloads small by filtering domains/actions on the fly and
emitting per-user sequences for model training.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional, Set
import math

from datasets import load_dataset


@dataclass
class TECDStreamConfig:
    """Config for pulling a small streaming subset."""

    repo_id: str = "t-tech/T-ECD"
    split: str = "train"
    data_files: Optional[List[str]] = None  # hf://datasets/... patterns for parquet
    domain_value: Optional[str] = None  # attach domain if not present in rows
    domains: Optional[Set[str]] = None
    exclude_actions: Set[str] = field(default_factory=lambda: {"VIEW", "view"})
    max_days: int = 100
    date_key: str = "date"
    timestamp_key: str = "timestamp"
    domain_key: str = "domain"
    action_key: str = "action_type"
    user_key: str = "user_id"
    product_key: str = "product_id"
    keep_fields: Optional[Set[str]] = None
    auth_token: Optional[str] = None


@dataclass
class SequenceConfig:
    """Config for turning rows into supervised next-action examples."""

    max_history: int = 20
    include_product: bool = True
    drop_until_history: bool = True  # skip yielding until we have history


def stream_filtered_rows(cfg: TECDStreamConfig) -> Iterator[Dict[str, Any]]:
    """Stream rows from HF while filtering by domain/action and stopping after N days."""

    if cfg.data_files:
        ds_dict = load_dataset(
            "parquet",
            data_files=cfg.data_files,
            streaming=True,
            token=cfg.auth_token,
        )
        ds = ds_dict["train"]
    else:
        ds = load_dataset(cfg.repo_id, split=cfg.split, streaming=True, token=cfg.auth_token)

    seen_days: Set[str] = set()

    for row in ds:
        if cfg.domain_value and not row.get(cfg.domain_key):
            row[cfg.domain_key] = cfg.domain_value

        domain = row.get(cfg.domain_key)
        if cfg.domains and domain not in cfg.domains:
            continue

        action = row.get(cfg.action_key)
        if action in cfg.exclude_actions:
            continue

        day = _extract_day(row, cfg)
        if day:
            seen_days.add(day)
            if len(seen_days) > cfg.max_days:
                break

        if cfg.keep_fields:
            row = {k: row.get(k) for k in cfg.keep_fields}

        yield row


def build_sequences(
    rows: Iterable[Dict[str, Any]],
    cfg: TECDStreamConfig,
    seq_cfg: SequenceConfig,
) -> Iterator[Dict[str, Any]]:
    """Convert a row stream into per-user sequences with seasonal features."""

    history: Dict[str, Deque] = defaultdict(lambda: deque(maxlen=seq_cfg.max_history))

    for row in rows:
        user = row.get(cfg.user_key)
        action = row.get(cfg.action_key)
        if user is None or action is None:
            continue

        product = row.get(cfg.product_key) if seq_cfg.include_product else None
        ts = _parse_timestamp(row.get(cfg.timestamp_key))
        seasonal = _seasonal_features(ts)

        user_hist = history[user]

        if seq_cfg.drop_until_history and not user_hist:
            user_hist.append((action, product, seasonal))
            continue

        past_actions = [item[0] for item in user_hist]
        past_products = [item[1] for item in user_hist] if seq_cfg.include_product else None
        past_seasonal = [item[2] for item in user_hist]

        yield {
            "user_id": user,
            "history_actions": list(past_actions),
            "history_products": list(past_products) if past_products is not None else None,
            "history_seasonal": list(past_seasonal),
            "target_action": action,
            "target_product": product,
            "timestamp": ts,
        }

        user_hist.append((action, product, seasonal))


def _extract_day(row: Dict[str, Any], cfg: TECDStreamConfig) -> Optional[str]:
    """Pull a YYYY-MM-DD string from date/timestamp fields."""

    if cfg.date_key in row and row[cfg.date_key]:
        return str(row[cfg.date_key])[:10]

    ts = row.get(cfg.timestamp_key)
    if isinstance(ts, timedelta):
        return str((datetime(1970, 1, 1) + ts).date())
    if isinstance(ts, str) and len(ts) >= 10:
        return ts[:10]
    return None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, timedelta):
        return datetime(1970, 1, 1) + value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _seasonal_features(ts: Optional[datetime]) -> List[float]:
    """Return [dow_sin, dow_cos, month_sin, month_cos] for a timestamp."""

    if ts is None:
        return [0.0, 0.0, 0.0, 0.0]

    dow = ts.weekday()
    month = ts.month
    dow_sin = math.sin(2 * math.pi * dow / 7)
    dow_cos = math.cos(2 * math.pi * dow / 7)
    month_sin = math.sin(2 * math.pi * month / 12)
    month_cos = math.cos(2 * math.pi * month / 12)
    return [dow_sin, dow_cos, month_sin, month_cos]
