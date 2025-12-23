#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
06_build_movie_bridges.py

根据 movie_* 源 CSV + basic_dicts 字典 + movies.csv，
生成可以直接 \copy INTO 三张桥表的 CSV：

输出：
  - movie_genre_for_sql.csv   : movie_id, genre_id
  - movie_region_for_sql.csv  : movie_id, region_id
  - movie_language_for_sql.csv: movie_id, language_id
"""

from __future__ import annotations

import csv
import os
from typing import Dict, Set, Tuple


# ====== 路径配置（etl 脚本在 ./etl 下） ======

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")
BASIC_DICTS_DIR = os.path.join(ETL_DIR, "basic_dicts")

MOVIES_CSV = os.path.join(ETL_DIR, "movies.csv")

MOVIE_GENRES_SRC = os.path.join(ETL_DIR, "movie_genres.csv")
MOVIE_REGIONS_SRC = os.path.join(ETL_DIR, "movie_regions.csv")
MOVIE_LANGUAGES_SRC = os.path.join(ETL_DIR, "movie_languages.csv")

DICT_GENRE_CSV = os.path.join(BASIC_DICTS_DIR, "dict_genre.csv")
DICT_REGION_CSV = os.path.join(BASIC_DICTS_DIR, "dict_region.csv")
DICT_LANGUAGE_CSV = os.path.join(BASIC_DICTS_DIR, "dict_language.csv")

OUT_MOVIE_GENRE = os.path.join(ETL_DIR, "movie_genre_for_sql.csv")
OUT_MOVIE_REGION = os.path.join(ETL_DIR, "movie_region_for_sql.csv")
OUT_MOVIE_LANGUAGE = os.path.join(ETL_DIR, "movie_language_for_sql.csv")


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


# ====== 通用加载函数 ======

def load_movie_id_map(path: str) -> Dict[str, int]:
    """
    从 movies.csv 加载 movie_douban_id -> movie_id 映射。
    """
    mapping: Dict[str, int] = {}
    print(f"[movie] 加载电影映射: {path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            douban_raw = row.get("movie_douban_id")
            id_raw = row.get("id")
            if not douban_raw or not id_raw:
                continue
            douban = str(douban_raw).strip()
            try:
                mid = int(str(id_raw).strip())
            except ValueError:
                continue
            if douban:
                mapping[douban] = mid

    print(f"[movie] 共加载 {len(mapping)} 条 douban_id -> movie_id 映射")
    return mapping


def load_name_id_dict(path: str, id_field: str, name_field: str, label: str) -> Dict[str, int]:
    """
    从 basic_dicts/*.csv 加载 name -> id 映射。
    例如：
      - dict_genre.csv    : id_field='genre_id',    name_field='name'
      - dict_region.csv   : id_field='region_id',   name_field='name'
      - dict_language.csv : id_field='language_id', name_field='name'
    """
    mapping: Dict[str, int] = {}
    print(f"[{label}] 加载字典映射: {path}")

    if not os.path.exists(path):
        print(f"[{label}] 警告：字典文件不存在: {path}")
        return mapping

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name_raw = row.get(name_field)
            id_raw = row.get(id_field)
            if not name_raw or not id_raw:
                continue
            name = str(name_raw).strip()
            try:
                vid = int(str(id_raw).strip())
            except ValueError:
                continue
            if name:
                mapping[name] = vid

    print(f"[{label}] 共加载 {len(mapping)} 条 name -> id 映射")
    return mapping


# ====== 通用桥表构建函数 ======

def build_bridge_csv(
        source_path: str,
        out_path: str,
        movie_map: Dict[str, int],
        dict_map: Dict[str, int],
        name_col: str,
        label: str,
) -> None:
    """
    通用桥表构建：
      source_path: 源 CSV（movie_*s.csv）
      out_path   : 输出 CSV（*_for_sql.csv）
      movie_map  : movie_douban_id -> movie_id
      dict_map   : name -> id（genre_id / region_id / language_id）
      name_col   : 源 CSV 中对应字段名，如 'genre_name' / 'region_name' / 'language_name'
      label      : 日志标签，如 'genre' / 'region' / 'language'

    输出格式统一为： movie_id, <dict_id>
    其中 <dict_id> 是 dict_map 的值。
    """
    ensure_dir_for_file(out_path)

    total_src = 0
    total_out = 0
    skipped_no_movie = 0
    skipped_no_name = 0
    dedup_count = 0

    seen_pairs: Set[Tuple[int, int]] = set()

    print(f"[{label}] 读取源文件: {source_path}")
    if not os.path.exists(source_path):
        print(f"[{label}] 源文件不存在，跳过: {source_path}")
        return

    with open(source_path, "r", encoding="utf-8", newline="") as f_in, \
            open(out_path, "w", encoding="utf-8", newline="") as f_out:

        reader = csv.DictReader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(["movie_id", f"{label}_id"])

        for row in reader:
            total_src += 1

            douban_raw = row.get("movie_douban_id")
            name_raw = row.get(name_col)

            if not douban_raw or not name_raw:
                continue

            douban_id = str(douban_raw).strip()
            name = str(name_raw).strip()

            if not douban_id:
                continue
            if not name:
                continue

            movie_id = movie_map.get(douban_id)
            if movie_id is None:
                skipped_no_movie += 1
                continue

            dict_id = dict_map.get(name)
            if dict_id is None:
                skipped_no_name += 1
                # 可以在需要时打印详细调试：
                # print(f"[{label}] 未找到映射: {name!r}")
                continue

            pair = (movie_id, dict_id)
            if pair in seen_pairs:
                dedup_count += 1
                continue
            seen_pairs.add(pair)

            writer.writerow([movie_id, dict_id])
            total_out += 1

    print(f"[{label}] 源记录数: {total_src}")
    print(f"[{label}] 输出记录数: {total_out}")
    print(f"[{label}] 去重掉的重复记录: {dedup_count}")
    if skipped_no_movie:
        print(f"[{label}] 跳过 {skipped_no_movie} 条（找不到 movie 映射）")
    if skipped_no_name:
        print(f"[{label}] 跳过 {skipped_no_name} 条（找不到 {label} 映射）")
    print(f"[{label}] 已生成: {out_path}")
    print("-" * 60)


def main() -> None:
    print("===> 构建 movie_genre / movie_region / movie_language 三张桥表 CSV")

    # 1. movie 映射
    movie_map = load_movie_id_map(MOVIES_CSV)

    # 2. 三个字典映射
    genre_map = load_name_id_dict(DICT_GENRE_CSV, "genre_id", "name", "genre")
    region_map = load_name_id_dict(DICT_REGION_CSV, "region_id", "name", "region")
    language_map = load_name_id_dict(DICT_LANGUAGE_CSV, "lang_id", "name", "language")

    # 3. 逐个构建桥表 CSV
    build_bridge_csv(
        source_path=MOVIE_GENRES_SRC,
        out_path=OUT_MOVIE_GENRE,
        movie_map=movie_map,
        dict_map=genre_map,
        name_col="genre_name",
        label="genre",
    )

    build_bridge_csv(
        source_path=MOVIE_REGIONS_SRC,
        out_path=OUT_MOVIE_REGION,
        movie_map=movie_map,
        dict_map=region_map,
        name_col="region_name",
        label="region",
    )

    build_bridge_csv(
        source_path=MOVIE_LANGUAGES_SRC,
        out_path=OUT_MOVIE_LANGUAGE,
        movie_map=movie_map,
        dict_map=language_map,
        name_col="language_name",
        label="language",
    )

    print("===> 三张桥表 CSV 构建完成")


if __name__ == "__main__":
    main()
