#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_movie_bridges_and_awards.py

从爬虫生成的 JSONL 中构建以下几个“桥表 / 事实表”的 CSV（staging 版本）：

1. 电影-类型 Movie_Genre（staging）
   输入：../data/raw/{worker_id}/movies_basic.jsonl
   输出：../data/etl/movie_genres.csv
   字段：
       - movie_douban_id
       - genre_name

2. 电影-地区 Movie_Region（staging）
   输入：../data/raw/{worker_id}/movies_details.jsonl
   输出：../data/etl/movie_regions.csv
   字段：
       - movie_douban_id
       - region_name

3. 电影-语言 Movie_Language（staging）
   输入：../data/raw/{worker_id}/movies_details.jsonl
   输出：../data/etl/movie_languages.csv
   字段：
       - movie_douban_id
       - language_name

4. 奖项记录 Award_Record（staging）
   输入：../data/raw/{worker_id}/movie_awards.jsonl
   输出：../data/etl/award_records.csv
   字段：
       - festival_name
       - festival_year
       - award_name
       - award_type
       - is_winner
       - movie_douban_id
       - person_douban_id
       - person_name
       - extra_desc

注意：
- 本脚本只负责“把 JSONL 里拆好的多值字段摊平 + 去重”，
  不直接生成最终使用 INT 外键的表结构。
- 在数据库侧可以先建若干 staging 表，
  再通过 JOIN 到 Movie / Genre / Region / Language / Festival / Award，
  插入最终的 Movie_Genre / Movie_Region / Movie_Language / Award_Record。
"""

from __future__ import annotations

import csv
import json
import os
from typing import Dict, Iterable, List, Set, Tuple


# ====== 路径配置（以 ./etl 为当前目录） ======

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RAW_ROOT_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
ETL_OUT_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

MOVIES_BASIC_FILENAME = "movies_basic.jsonl"
MOVIES_DETAILS_FILENAME = "movies_details.jsonl"
MOVIE_AWARDS_FILENAME = "movie_awards.jsonl"

MOVIE_GENRE_CSV = os.path.join(ETL_OUT_DIR, "movie_genres.csv")
MOVIE_REGION_CSV = os.path.join(ETL_OUT_DIR, "movie_regions.csv")
MOVIE_LANGUAGE_CSV = os.path.join(ETL_OUT_DIR, "movie_languages.csv")
AWARD_RECORDS_CSV = os.path.join(ETL_OUT_DIR, "award_records.csv")


# ====== 小工具 ======

def ensure_dir(path: str) -> None:
    """确保目录存在"""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def iter_worker_subdirs(root_dir: str) -> Iterable[str]:
    """
    遍历 ../data/raw 下所有 worker 子目录，返回绝对路径。
    默认按目录名排序，便于结果稳定。
    """
    if not os.path.exists(root_dir):
        raise FileNotFoundError(f"RAW_ROOT_DIR 不存在: {root_dir}")

    for name in sorted(os.listdir(root_dir)):
        subdir = os.path.join(root_dir, name)
        if os.path.isdir(subdir):
            yield subdir


def iter_jsonl(path: str) -> Iterable[Dict]:
    """逐行读取 jsonl 文件，yield 每一行的 dict"""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[warn] JSON decode 失败，文件={path}，错误={e}")
                continue
            yield obj


# ====== 1. Movie-Genre / Movie-Region / Movie-Language 构建 ======

def build_movie_genre_region_language() -> Tuple[
    List[Tuple[str, str]],
    List[Tuple[str, str]],
    List[Tuple[str, str]],
]:
    """
    从 movies_basic.jsonl & movies_details.jsonl 中构建 3 个桥表的 (movie_douban_id, xxx_name) 记录。

    返回值：
        (movie_genres, movie_regions, movie_languages)
        每个都是 list[(movie_douban_id, name)]
    """
    movie_genres_set: Set[Tuple[str, str]] = set()
    movie_regions_set: Set[Tuple[str, str]] = set()
    movie_languages_set: Set[Tuple[str, str]] = set()

    # --- 1.1 从 movies_basic.jsonl 里抓类型 ---
    for worker_dir in iter_worker_subdirs(RAW_ROOT_DIR):
        basic_path = os.path.join(worker_dir, MOVIES_BASIC_FILENAME)
        if not os.path.exists(basic_path):
            continue

        print(f"[movie-genre] 处理 {basic_path}")
        for obj in iter_jsonl(basic_path):
            mid = str(obj.get("movie_douban_id") or "").strip()
            if not mid:
                continue

            genres = obj.get("genres") or []
            if not isinstance(genres, list):
                continue

            for g in genres:
                g_name = str(g).strip()
                if not g_name:
                    continue
                movie_genres_set.add((mid, g_name))

    # --- 1.2 从 movies_details.jsonl 里抓地区/语言 ---
    for worker_dir in iter_worker_subdirs(RAW_ROOT_DIR):
        details_path = os.path.join(worker_dir, MOVIES_DETAILS_FILENAME)
        if not os.path.exists(details_path):
            continue

        print(f"[movie-region/lang] 处理 {details_path}")
        for obj in iter_jsonl(details_path):
            mid = str(obj.get("movie_douban_id") or "").strip()
            if not mid:
                continue

            regions = obj.get("regions") or []
            if isinstance(regions, list):
                for r in regions:
                    r_name = str(r).strip()
                    if not r_name:
                        continue
                    movie_regions_set.add((mid, r_name))

            languages = obj.get("languages") or []
            if isinstance(languages, list):
                for lang in languages:
                    lang_name = str(lang).strip()
                    if not lang_name:
                        continue
                    movie_languages_set.add((mid, lang_name))

    # 转成 list 返回，顺便 sort 一下便于 diff
    movie_genres = sorted(movie_genres_set)
    movie_regions = sorted(movie_regions_set)
    movie_languages = sorted(movie_languages_set)

    print(f"[movie-genre] 总记录数: {len(movie_genres)}")
    print(f"[movie-region] 总记录数: {len(movie_regions)}")
    print(f"[movie-language] 总记录数: {len(movie_languages)}")

    return movie_genres, movie_regions, movie_languages


# ====== 2. Award_Record 构建（staging） ======

def build_award_records() -> List[Dict[str, str]]:
    """
    从 movie_awards.jsonl 中构建 Award_Record 的 staging 数据（不直接映射到 award_id / movie_id / person_id）。

    返回 list[dict]，字段：
        - festival_name
        - festival_year
        - award_name
        - award_type
        - is_winner
        - movie_douban_id
        - person_douban_id
        - person_name
        - extra_desc
    """
    records: List[Dict[str, str]] = []

    for worker_dir in iter_worker_subdirs(RAW_ROOT_DIR):
        awards_path = os.path.join(worker_dir, MOVIE_AWARDS_FILENAME)
        if not os.path.exists(awards_path):
            continue

        print(f"[award-record] 处理 {awards_path}")
        for obj in iter_jsonl(awards_path):
            movie_douban_id = str(obj.get("movie_douban_id") or "").strip()
            fest_name = str(obj.get("festival_name") or "").strip()
            fest_year = obj.get("festival_year")
            award_name = str(obj.get("award_name") or "").strip()
            award_type = str(obj.get("award_type") or "").strip()  # "Movie" / "Person"
            is_winner = bool(obj.get("is_winner"))  # True / False

            person_douban_id = str(obj.get("person_douban_id") or "").strip()
            person_name = str(obj.get("person_name") or "").strip()
            extra_desc = obj.get("extra_desc")
            if extra_desc is not None:
                extra_desc = str(extra_desc).strip()
                if not extra_desc:
                    extra_desc = None

            # 基本字段缺失直接丢弃
            if not movie_douban_id or not fest_name or fest_year is None or not award_name:
                continue

            # 规范化年份为 int
            try:
                fest_year_int = int(fest_year)
            except Exception:
                # 年份异常就略过
                continue

            # 记录一条 staging Award_Record
            rec = {
                "festival_name": fest_name,
                "festival_year": fest_year_int,
                "award_name": award_name,
                "award_type": award_type,
                "is_winner": "1" if is_winner else "0",
                "movie_douban_id": movie_douban_id,
                "person_douban_id": person_douban_id or "",
                "person_name": person_name or "",
                "extra_desc": extra_desc or "",
            }
            records.append(rec)

    print(f"[award-record] 总记录数: {len(records)}")
    return records


# ====== 3. 写出 CSV ======

def write_movie_bridges_csv(
        movie_genres: List[Tuple[str, str]],
        movie_regions: List[Tuple[str, str]],
        movie_languages: List[Tuple[str, str]],
) -> None:
    ensure_dir(ETL_OUT_DIR)

    # Movie_Genre
    with open(MOVIE_GENRE_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_douban_id", "genre_name"])
        for mid, gname in movie_genres:
            writer.writerow([mid, gname])
    print(f"[write] Movie-Genre CSV 写出完成: {MOVIE_GENRE_CSV}")

    # Movie_Region
    with open(MOVIE_REGION_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_douban_id", "region_name"])
        for mid, rname in movie_regions:
            writer.writerow([mid, rname])
    print(f"[write] Movie-Region CSV 写出完成: {MOVIE_REGION_CSV}")

    # Movie_Language
    with open(MOVIE_LANGUAGE_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_douban_id", "language_name"])
        for mid, lname in movie_languages:
            writer.writerow([mid, lname])
    print(f"[write] Movie-Language CSV 写出完成: {MOVIE_LANGUAGE_CSV}")


def write_award_records_csv(records: List[Dict[str, str]]) -> None:
    ensure_dir(ETL_OUT_DIR)

    fieldnames = [
        "festival_name",
        "festival_year",
        "award_name",
        "award_type",
        "is_winner",
        "movie_douban_id",
        "person_douban_id",
        "person_name",
        "extra_desc",
    ]

    with open(AWARD_RECORDS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)

    print(f"[write] Award_Record CSV 写出完成: {AWARD_RECORDS_CSV}")


# ====== main ======

def main() -> None:
    print("=== 构建 Movie-Genre / Movie-Region / Movie-Language 桥表（staging） ===")
    movie_genres, movie_regions, movie_languages = build_movie_genre_region_language()
    write_movie_bridges_csv(movie_genres, movie_regions, movie_languages)

    print("\n=== 构建 Award_Record 事实表（staging） ===")
    award_records = build_award_records()
    write_award_records_csv(award_records)

    print("\n所有桥表 / 奖项记录 ETL 完成。")


if __name__ == "__main__":
    main()
