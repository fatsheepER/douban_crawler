#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_movie_ratings_for_sql.py

从 movie_ratings.csv 生成可以直接 \copy INTO movie_rating 的 CSV。

源文件（../data/etl/movie_ratings.csv）格式：
    id,movie_id,user_id,rating,created_at,review

目标文件（../data/etl/movie_ratings_for_sql.csv）格式：
    user_id,movie_id,rating,created_at,review

处理逻辑：
  - 去掉多余的 id 列；
  - 保证 rating 在 [0, 10]；
  - created_at 为空的记录丢弃；
  - review 去掉换行，压成一行，并截断到 200 字符；
  - 对于重复的 (user_id, movie_id)，只保留一条记录：
        默认保留 created_at 较晚的一条。
"""

from __future__ import annotations

import csv
import os
from typing import Dict, Tuple


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

SRC_CSV = os.path.join(ETL_DIR, "movie_ratings.csv")
OUT_CSV = os.path.join(ETL_DIR, "movie_ratings_for_sql.csv")


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def clean_review(text: str, max_len: int = 200) -> str:
    """
    去掉换行和多余空白，并限制长度 <= max_len。
    """
    if text is None:
        text = ""
    # 去掉换行，压成一行
    text = text.replace("\r", " ").replace("\n", " ")
    # 把连续空白压成一个空格
    text = " ".join(text.split())
    # 截断到 max_len
    if len(text) > max_len:
        text = text[:max_len]
    return text


def build_movie_ratings_for_sql() -> None:
    print(f"[ratings] 读取源文件: {SRC_CSV}")
    print(f"[ratings] 输出文件: {OUT_CSV}")

    if not os.path.exists(SRC_CSV):
        raise FileNotFoundError(f"找不到 movie_ratings.csv: {SRC_CSV}")

    ensure_dir_for_file(OUT_CSV)

    total = 0
    skipped_rating_or_id = 0
    skipped_created_at = 0
    duplicate_keys = 0

    # key: (user_id, movie_id)  ->  row dict
    dedup_map: Dict[Tuple[int, int], Dict[str, str]] = {}

    with open(SRC_CSV, "r", encoding="utf-8", newline="") as f_in:
        reader = csv.DictReader(f_in)

        for row in reader:
            total += 1

            movie_id_raw = row.get("movie_id")
            user_id_raw = row.get("user_id")
            rating_raw = row.get("rating")
            created_at_raw = (row.get("created_at") or "").strip()
            review_raw = row.get("review")

            # 基本字段校验
            if not movie_id_raw or not user_id_raw or rating_raw is None:
                skipped_rating_or_id += 1
                continue

            try:
                movie_id = int(str(movie_id_raw).strip())
                user_id = int(str(user_id_raw).strip())
                rating = int(str(rating_raw).strip())
            except (TypeError, ValueError):
                skipped_rating_or_id += 1
                continue

            if rating < 0 or rating > 10:
                skipped_rating_or_id += 1
                continue

            # created_at 不能为空，否则 COPY 到 not null 会报错
            if not created_at_raw:
                skipped_created_at += 1
                continue

            review_clean = clean_review(review_raw, max_len=200)

            key = (user_id, movie_id)
            new_row = {
                "user_id": str(user_id),
                "movie_id": str(movie_id),
                "rating": str(rating),
                "created_at": created_at_raw,
                "review": review_clean,
            }

            if key not in dedup_map:
                dedup_map[key] = new_row
            else:
                # 已经存在一条，按 created_at 选择“较晚”的那条
                duplicate_keys += 1
                old_row = dedup_map[key]
                old_ts = old_row.get("created_at", "")
                new_ts = created_at_raw

                # 时间格式为 "YYYY-MM-DD HH:MM:SS"，字符串比较就够用
                if new_ts > old_ts:
                    dedup_map[key] = new_row

    # 写出 CSV
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(
            f_out,
            fieldnames=["user_id", "movie_id", "rating", "created_at", "review"],
        )
        writer.writeheader()
        for row in dedup_map.values():
            writer.writerow(row)

    written = len(dedup_map)

    print(f"[ratings] 总记录数: {total}")
    print(f"[ratings] 去重后写入行数: {written}")
    print(f"[ratings] 跳过行数（rating 或 id 异常）: {skipped_rating_or_id}")
    print(f"[ratings] 跳过行数（created_at 为空）: {skipped_created_at}")
    print(f"[ratings] 检测到重复 (user_id, movie_id) 键对: {duplicate_keys}")
    print("===> movie_ratings_for_sql.csv 构建完成")


def main() -> None:
    build_movie_ratings_for_sql()


if __name__ == "__main__":
    main()
