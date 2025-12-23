#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_watching_records_for_sql.py

从 watching_records.csv 生成可以直接 \copy INTO watching_record 的 CSV。

源文件（../data/etl/watching_records.csv）格式：
    id,movie_id,user_id,status,created_at,star

目标文件（../data/etl/watching_records_for_sql.csv）格式：
    user_id,movie_id,star,status,created_at

处理逻辑：
  - 去掉多余的 id 列；
  - 校验 movie_id / user_id / status / created_at；
  - 规范化 star 为 TRUE/FALSE（PostgreSQL boolean 可直接识别）；
  - 对于重复的 (user_id, movie_id)，只保留一条记录：
        1) 按 status 优先级 watched > watching > wishlist；
        2) 同 status 时保留 created_at 较晚的一条；
        3) 再相同则优先 star = TRUE 的那条。
"""

from __future__ import annotations

import csv
import os
from typing import Dict, Tuple


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

SRC_CSV = os.path.join(ETL_DIR, "watching_records.csv")
OUT_CSV = os.path.join(ETL_DIR, "watching_records_for_sql.csv")


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


# status 优先级：值越大优先级越高
STATUS_PRIORITY = {
    "wishlist": 1,
    "watching": 2,
    "watched": 3,
}


def normalize_status(s: str) -> str:
    """把 status 统一成小写，并确认在允许集合内。非法则返回空串。"""
    if not s:
        return ""
    s = s.strip().lower()
    return s if s in STATUS_PRIORITY else ""


def normalize_star(s: str) -> str:
    """
    规范化 star 字段为 'TRUE' 或 'FALSE'（PostgreSQL boolean 可直接识别）。
    """
    if s is None:
        return "FALSE"
    s = s.strip().upper()
    if s in ("TRUE", "T", "1", "YES", "Y"):
        return "TRUE"
    return "FALSE"


def build_watching_records_for_sql() -> None:
    print(f"[watch] 读取源文件: {SRC_CSV}")
    print(f"[watch] 输出文件: {OUT_CSV}")

    if not os.path.exists(SRC_CSV):
        raise FileNotFoundError(f"找不到 watching_records.csv: {SRC_CSV}")

    ensure_dir_for_file(OUT_CSV)

    total = 0
    skipped_basic = 0
    skipped_created_at = 0
    duplicate_keys = 0

    # key: (user_id, movie_id) -> row dict
    dedup_map: Dict[Tuple[int, int], Dict[str, str]] = {}

    with open(SRC_CSV, "r", encoding="utf-8", newline="") as f_in:
        reader = csv.DictReader(f_in)

        for row in reader:
            total += 1

            movie_id_raw = row.get("movie_id")
            user_id_raw = row.get("user_id")
            status_raw = row.get("status")
            created_at_raw = (row.get("created_at") or "").strip()
            star_raw = row.get("star")

            # 基本字段校验（movie_id / user_id / status）
            if not movie_id_raw or not user_id_raw:
                skipped_basic += 1
                continue

            try:
                movie_id = int(str(movie_id_raw).strip())
                user_id = int(str(user_id_raw).strip())
            except (TypeError, ValueError):
                skipped_basic += 1
                continue

            status = normalize_status(status_raw)
            if not status:
                skipped_basic += 1
                continue

            if not created_at_raw:
                skipped_created_at += 1
                continue

            star = normalize_star(star_raw)

            key = (user_id, movie_id)
            new_row = {
                "user_id": str(user_id),
                "movie_id": str(movie_id),
                "star": star,
                "status": status,
                "created_at": created_at_raw,
            }

            if key not in dedup_map:
                dedup_map[key] = new_row
            else:
                # 已存在一条，按规则决定是否用新记录替换
                duplicate_keys += 1
                old_row = dedup_map[key]

                old_status = old_row["status"]
                old_created = old_row["created_at"]
                old_star = old_row["star"]

                old_pri = STATUS_PRIORITY.get(old_status, 0)
                new_pri = STATUS_PRIORITY.get(status, 0)

                replace = False

                # 1) status 优先级更高
                if new_pri > old_pri:
                    replace = True
                elif new_pri == old_pri:
                    # 2) 同 status 时，created_at 更晚
                    if created_at_raw > old_created:
                        replace = True
                    elif created_at_raw == old_created:
                        # 3) 再相同，star = TRUE 优先
                        if star == "TRUE" and old_star != "TRUE":
                            replace = True

                if replace:
                    dedup_map[key] = new_row

    # 写出 CSV
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(
            f_out,
            fieldnames=["user_id", "movie_id", "star", "status", "created_at"],
        )
        writer.writeheader()
        for row in dedup_map.values():
            writer.writerow(row)

    written = len(dedup_map)

    print(f"[watch] 总记录数: {total}")
    print(f"[watch] 去重后写入行数: {written}")
    print(f"[watch] 跳过行数（id / status / 解析失败）: {skipped_basic}")
    print(f"[watch] 跳过行数（created_at 为空）: {skipped_created_at}")
    print(f"[watch] 检测到重复 (user_id, movie_id) 键对: {duplicate_keys}")
    print("===> watching_records_for_sql.csv 构建完成")


def main() -> None:
    build_watching_records_for_sql()


if __name__ == "__main__":
    main()
