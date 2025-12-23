#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_award_record_for_sql.py

从 staging 版 award_records.csv 构建可以直接 \copy INTO award_record 的 CSV。

输入：
  - ../data/etl/award_records.csv
      字段：
        festival_name,festival_year,award_name,award_type,is_winner,
        movie_douban_id,person_douban_id,person_name,extra_desc

  - ../data/etl/movies.csv
      用于 movie_douban_id -> movie.id 映射

  - ../data/etl/persons.csv
      用于 person_douban_id -> person.id 映射

  - ../data/etl/basic_dicts/dict_festival.csv
      festival_id, name, year

  - ../data/etl/basic_dicts/dict_award.csv
      award_id, fest_id, name, type

输出：
  - ../data/etl/award_record_for_sql.csv
      字段：
        award_id,movie_id,person_id,is_winner,description

然后可在 psql 中执行：
  \copy award_record (award_id, movie_id, person_id, is_winner, description)
    from '.../data/etl/award_record_for_sql.csv'
    with (format csv, header true, delimiter ',', encoding 'UTF8');
"""

from __future__ import annotations

import csv
import os
from typing import Dict, Tuple, Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")
DICT_DIR = os.path.join(ETL_DIR, "basic_dicts")

AWARD_RECORDS_CSV = os.path.join(ETL_DIR, "award_records.csv")
MOVIES_CSV = os.path.join(ETL_DIR, "movies.csv")
PERSONS_CSV = os.path.join(ETL_DIR, "persons.csv")
DICT_FESTIVAL_CSV = os.path.join(DICT_DIR, "dict_festival.csv")
DICT_AWARD_CSV = os.path.join(DICT_DIR, "dict_award.csv")

OUT_CSV = os.path.join(ETL_DIR, "award_record_for_sql.csv")


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


# ========== 1. 基础映射加载 ==========

def load_movie_id_map() -> Dict[str, int]:
    """
    movies.csv: id,movie_douban_id,title,...
    返回: {movie_douban_id: movie_id}
    """
    path = MOVIES_CSV
    mapping: Dict[str, int] = {}

    print(f"[movie] 读取 {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            did = str(row.get("movie_douban_id") or "").strip()
            mid_raw = row.get("id")
            if not did or mid_raw is None:
                continue
            try:
                mid = int(str(mid_raw).strip())
            except ValueError:
                continue
            mapping[did] = mid

    print(f"[movie] 映射条数: {len(mapping)}")
    return mapping


def load_person_id_map() -> Dict[str, int]:
    """
    persons.csv: id,person_douban_id,name,...
    返回: {person_douban_id: person_id}
    """
    path = PERSONS_CSV
    mapping: Dict[str, int] = {}

    print(f"[person] 读取 {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            did = str(row.get("person_douban_id") or "").strip()
            pid_raw = row.get("id")
            if not did or pid_raw is None:
                continue
            try:
                pid = int(str(pid_raw).strip())
            except ValueError:
                continue
            mapping[did] = pid

    print(f"[person] 映射条数: {len(mapping)}")
    return mapping


def load_festival_map() -> Dict[Tuple[str, int], int]:
    """
    dict_festival.csv: festival_id,name,year
    返回: {(name, year): fest_id}
    """
    path = DICT_FESTIVAL_CSV
    mapping: Dict[Tuple[str, int], int] = {}

    print(f"[festival] 读取 {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("name") or "").strip()
            year_raw = row.get("year")
            fid_raw = row.get("festival_id")
            if not name or year_raw is None or fid_raw is None:
                continue

            try:
                year = int(str(year_raw).strip())
                fid = int(str(fid_raw).strip())
            except ValueError:
                continue

            mapping[(name, year)] = fid

    print(f"[festival] 映射条数: {len(mapping)}")
    return mapping


def load_award_map() -> Dict[Tuple[int, str, str], int]:
    """
    dict_award.csv: award_id,fest_id,name,type
    返回: {(fest_id, name, type): award_id}
    """
    path = DICT_AWARD_CSV
    mapping: Dict[Tuple[int, str, str], int] = {}

    print(f"[award] 读取 {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fest_raw = row.get("festival_id")
            name = (row.get("name") or "").strip()
            award_type = (row.get("award_type") or "").strip()
            aid_raw = row.get("award_id")
            if fest_raw is None or not name or not award_type or aid_raw is None:
                continue

            try:
                fest_id = int(str(fest_raw).strip())
                aid = int(str(aid_raw).strip())
            except ValueError:
                continue

            key = (fest_id, name, award_type)
            mapping[key] = aid

    print(f"[award] 映射条数: {len(mapping)}")
    return mapping


# ========== 2. 主转换逻辑 ==========

def normalize_bool_from_int_str(val: str) -> str:
    """
    把 '1' / '0' / 'true' / 'false' 等转换成 'TRUE' / 'FALSE' 字符串
    供 \copy 导入 boolean 列使用。
    """
    v = (val or "").strip().lower()
    if v in ("1", "t", "true", "yes", "y"):
        return "TRUE"
    return "FALSE"


def clean_description(text: Optional[str]) -> str:
    """
    清洗 description，去掉换行、两端空白，长度截断到 50。
    空则返回空字符串（导入时 -> NULL）。
    """
    if not text:
        return ""
    s = str(text).replace("\r", " ").replace("\n", " ")
    s = " ".join(s.split())  # 压缩多空格
    return s[:50]


def build_award_record_for_sql() -> None:
    movie_map = load_movie_id_map()
    person_map = load_person_id_map()
    fest_map = load_festival_map()
    award_map = load_award_map()

    src_path = AWARD_RECORDS_CSV
    out_path = OUT_CSV

    print(f"[award_record] 读取源文件: {src_path}")
    print(f"[award_record] 输出文件: {out_path}")

    ensure_dir_for_file(out_path)

    total = 0
    written = 0
    skipped_missing_movie = 0
    skipped_missing_person = 0
    skipped_missing_festival = 0
    skipped_missing_award = 0

    with open(src_path, "r", encoding="utf-8", newline="") as f_in, \
            open(out_path, "w", encoding="utf-8", newline="") as f_out:

        reader = csv.DictReader(f_in)
        fieldnames_out = ["award_id", "movie_id", "person_id", "is_winner", "description"]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames_out)
        writer.writeheader()

        for row in reader:
            total += 1

            fest_name = (row.get("festival_name") or "").strip()
            fest_year_raw = row.get("festival_year")
            award_name = (row.get("award_name") or "").strip()
            award_type = (row.get("award_type") or "").strip().lower()
            is_winner_raw = row.get("is_winner") or ""
            movie_did = (row.get("movie_douban_id") or "").strip()
            person_did = (row.get("person_douban_id") or "").strip()
            extra_desc = row.get("extra_desc") or ""

            # 1) 基本字段检查
            if not fest_name or fest_year_raw is None or not award_name or not movie_did:
                # 这些在 staging 阶段理论上已经过滤过，这里再兜底
                continue

            try:
                fest_year = int(str(fest_year_raw).strip())
            except ValueError:
                continue

            # 2) festival -> fest_id
            fest_key = (fest_name, fest_year)
            fest_id = fest_map.get(fest_key)
            if fest_id is None:
                skipped_missing_festival += 1
                continue

            # 3) award -> award_id
            award_key = (fest_id, award_name, award_type)
            award_id = award_map.get(award_key)
            if award_id is None:
                skipped_missing_award += 1
                continue

            # 4) movie_douban_id -> movie_id
            movie_id = movie_map.get(movie_did)
            if movie_id is None:
                skipped_missing_movie += 1
                continue

            # 5) person_douban_id -> person_id（允许为空）
            person_id: Optional[int]
            if person_did:
                person_id = person_map.get(person_did)
                if person_id is None:
                    skipped_missing_person += 1
                    # 对于 Person 类型奖项，没有 person_id 的记录其实没法用，直接跳过
                    # 也可以视情况改成 person_id=NULL，这里选择严格一点：
                    continue
            else:
                person_id = None

            is_winner = normalize_bool_from_int_str(is_winner_raw)
            description = clean_description(extra_desc)

            writer.writerow({
                "award_id": award_id,
                "movie_id": movie_id,
                "person_id": "" if person_id is None else person_id,
                "is_winner": is_winner,
                "description": description,
            })
            written += 1

    print(f"[award_record] 总记录数: {total}")
    print(f"[award_record] 成功写入: {written}")
    if skipped_missing_movie:
        print(f"[award_record] 跳过 {skipped_missing_movie} 条（缺 movie 映射）")
    if skipped_missing_person:
        print(f"[award_record] 跳过 {skipped_missing_person} 条（缺 person 映射）")
    if skipped_missing_festival:
        print(f"[award_record] 跳过 {skipped_missing_festival} 条（缺 festival 映射）")
    if skipped_missing_award:
        print(f"[award_record] 跳过 {skipped_missing_award} 条（缺 award 映射）")


def main() -> None:
    build_award_record_for_sql()
    print("===> award_record_for_sql.csv 构建完成")


if __name__ == "__main__":
    main()
