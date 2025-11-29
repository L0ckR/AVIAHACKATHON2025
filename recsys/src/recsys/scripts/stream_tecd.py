"""CLI для потоковой выборки T-ECD и сохранения локально.

Пример:
python3 -m recsys.scripts.stream_tecd --domains retail_marketplace payments --max-days 1 --limit 20 --output data/tecd_demo.parquet
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from recsys.models import TECDStreamConfig, stream_filtered_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream-filter T-ECD and save locally.")
    parser.add_argument(
        "--domains",
        nargs="+",
        default=None,
        help="Домены для фильтрации (через пробел). Если не указаны — все.",
    )
    parser.add_argument(
        "--exclude-actions",
        nargs="+",
        default=["VIEW", "view"],
        help="Действия для исключения (по умолчанию VIEW/view).",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=100,
        help="Остановиться после N уникальных дней.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Макс. строк для выборки (защита от огромных файлов).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/tecd_subset.parquet"),
        help="Куда писать parquet.",
    )
    parser.add_argument(
        "--data-files",
        nargs="+",
        default=None,
        help="Список HF-путей/паттернов (например, hf://datasets/t-tech/T-ECD/dataset/small/marketplace/events/*.pq). "
        "Если задано — repo_id/split игнорируются и грузим parquet напрямую.",
    )
    parser.add_argument(
        "--domain-value",
        type=str,
        default=None,
        help="Проставить это значение в поле domain, если его нет в строках.",
    )
    parser.add_argument(
        "--auth-token",
        type=str,
        default=None,
        help="HF токен, если понадобится (публичный датасет обычно не требует).",
    )
    parser.add_argument(
        "--repo-id",
        type=str,
        default="t-tech/T-ECD",
        help="ID датасета (по умолчанию t-tech/T-ECD).",
    )
    return parser.parse_args()


def main(args: Optional[argparse.Namespace] = None) -> None:
    args = args or parse_args()
    rows: List[dict] = []

    cfg = TECDStreamConfig(
        repo_id=args.repo_id,
        data_files=args.data_files,
        domain_value=args.domain_value,
        domains=set(args.domains) if args.domains else None,
        exclude_actions=set(args.exclude_actions),
        max_days=args.max_days,
        auth_token=args.auth_token,
    )

    for i, row in enumerate(stream_filtered_rows(cfg)):
        rows.append(row)
        if args.limit and i + 1 >= args.limit:
            break

    if not rows:
        raise SystemExit("Не собрали ни одной строки — проверь фильтры/сеть.")

    table = pa.Table.from_pylist(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, args.output)
    print(f"Сохранили {len(rows)} строк в {args.output}")


if __name__ == "__main__":
    main()
