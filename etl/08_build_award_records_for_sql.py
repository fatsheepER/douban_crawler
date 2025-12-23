#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_award_records_for_sql.py

把 ETL 层的 award_records.csv 转成可以直接 \copy INTO award_record 的 CSV：

输入（都在 ../data/etl/）：
  - movies.csv            （movie_douban_id -> movie.id）
  - persons.csv           （person_douban_id -> person.id）
  - festivals.csv         （id, name, year）
  - awards.csv            （id, fest_id, name, type）
  - award_records.csv     （festival_name,festival_year,award_name,award_type,is_winner,
                            movie_douban_id,person_douban_id,person_name,extra_desc）

输出：
  - award_record_for_sql.csv （award_id,movie_id,person_id,is_winner,description）
"""

from __future__ import annotations

import csv
import os
from typing import Dict, Tuple

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

MOVIES_CSV = os.path.join(ETL_DIR, "movies.csv")
PERSONS_CSV = os.path.join(ETL_DIR, "persons.csv")
FESTIVALS_CSV = os.path.join(ETL_DIR, "basic_dicts/dict_festival.csv")
AWARDS_CSV = os.path.join(ETL_DIR, "basic_dicts/dict_award.csv")
AWARD_RECORDS_SRC = os.path.join(ETL_DIR, "award_records.csv")

AWARD_RECORDS_OUT = os.path.join(ETL_DIR, "award_record_for_sql.csv")


def load_movie_map(path: str) -> Dict[str, int]:
    """
    movies.csv: id,movie_douban_id,...
    -> {movie_douban_id: id}
    """
    m: Dict[str, int] = {}
    print(f"[movie] 读取 {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            did = (row.get("movie_douban_id") or "").strip()
            mid_raw = row.get("id")
            if not did or not mid_raw:
                continue
            try:
                mid = int(mid_raw)
            except ValueError:
                continue
            m[did] = mid
    print(f"[movie] 映射条数: {len(m)}")
    return m


def load_person_map(path: str) -> Dict[str, int]:
    """
    persons.csv: id,person_douban_id,...
    -> {person_douban_id: id}
    """
    m: Dict[str, int] = {}
    print(f"[person] 读取 {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            did = (row.get("person_douban_id") or "").strip()
            pid_raw = row.get("id")
            if not did or not pid_raw:
                continue
            try:
                pid = int(pid_raw)
            except ValueError:
                continue
            m[did] = pid
    print(f"[person] 映射条数: {len(m)}")
    return m


def load_festival_map(path: str) -> Dict[Tuple[str, int], int]:
    """
    festivals.csv: id,name,year
    -> {(name, year): id}
    """
    m: Dict[Tuple[str, int], int] = {}
    print(f"[festival] 读取 {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("name") or "").strip()
            year_raw = row.get("year")
            id_raw = row.get("id")
            if not name or not year_raw or not id_raw:
                continue
            try:
                year = int(year_raw)
                fid = int(id_raw)
            except ValueError:
                continue
            m[(name, year)] = fid
    print(f"[festival] 映射条数: {len(m)}")
    return m


def load_award_map(path: str) -> Dict[Tuple[int, str, str], int]:
    """
    awards.csv: id,fest_id,name,type
    -> {(fest_id, name, type): id}
    """
    m: Dict[Tuple[int, str, str], int] = {}
    print(f"[award] 读取 {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            id_raw = row.get("id")
            fest_raw = row.get("fest_id")
            name = (row.get("name") or "").strip()
            award_type = (row.get("type") or "").strip()
            if not id_raw or not fest_raw or not name or not award_type:
                continue
            try:
                aid = int(id_raw)
                fid = int(fest_raw)
            except ValueError:
                continue
            key = (fid, name, award_type)
            m[key] = aid
    print(f"[award] 映射条数: {len(m)}")
    return m


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def build_award_records() -> None:
    movie_map = load_movie_map(MOVIES_CSV)
    person_map = load_person_map(PERSONS_CSV)
    festival_map = load_festival_map(FESTIVALS_CSV)
    award_map = load_award_map(AWARDS_CSV)

    print(f"[award_record] 源文件: {AWARD_RECORDS_SRC}")
    print(f"[award_record] 输出文件: {AWARD_RECORDS_OUT}")

    ensure_dir_for_file(AWARD_RECORDS_OUT)

    total = 0
    written = 0
    skipped_no_movie = 0
    skipped_no_festival = 0
    skipped_no_award = 0
    skipped_no_person = 0

    with open(AWARD_RECORDS_SRC, "r", encoding="utf-8", newline="") as fin, \
            open(AWARD_RECORDS_OUT, "w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin)
        writer = csv.writer(fout)
        # award_record 表里我们不写 id，让 serial 自增：
        writer.writerow(["award_id", "movie_id", "person_id", "is_winner", "description"])

        for row in reader:
            total += 1

            fest_name = (row.get("festival_name") or "").strip()
            fest_year_raw = (row.get("festival_year") or "").strip()
            award_name = (row.get("award_name") or "").strip()
            award_type = (row.get("award_type") or "").strip()
            is_winner_raw = (row.get("is_winner") or "").strip()
            movie_did = (row.get("movie_douban_id") or "").strip()
            person_did = (row.get("person_douban_id") or "").strip()
            extra_desc = (row.get("extra_desc") or "").strip()

            # 1) movie_id 映射
            if not movie_did or movie_did not in movie_map:
                skipped_no_movie += 1
                continue
            movie_id = movie_map[movie_did]

            # 2) festival_id 映射
            try:
                fest_year = int(fest_year_raw)
            except ValueError:
                skipped_no_festival += 1
                continue

            fest_key = (fest_name, fest_year)
            if fest_key not in festival_map:
                skipped_no_festival += 1
                continue
            fest_id = festival_map[fest_key]

            # 3) award_id 映射
            award_key = (fest_id, award_name, award_type)
            if award_key not in award_map:
                skipped_no_award += 1
                continue
            award_id = award_map[award_key]

            # 4) person_id 映射（可以为 NULL）
            person_id_str = ""
            if person_did:
                if person_did not in person_map:
                    skipped_no_person += 1
                    continue
                person_id = person_map[person_did]
                person_id_str = str(person_id)

            # 5) is_winner 转成 TRUE/FALSE
            is_winner = "TRUE" if is_winner_raw in ("1", "true", "TRUE", "t", "T") else "FALSE"

            # 6) description：用 extra_desc，去掉换行并截断到 50 字符
            desc = extra_desc.replace("\r", " ").replace("\n", " ").strip()
            if len(desc) > 50:
                desc = desc[:50]

            writer.writerow([award_id, movie_id, person_id_str, is_winner, desc])
            written += 1

    print(f"[award_record] 总记录数: {total}")
    print(f"[award_record] 成功写入: {written}")
    if skipped_no_movie:
        print(f"[award_record] 跳过 {skipped_no_movie} 条（缺 movie 映射）")
    if skipped_no_festival:
        print(f"[award_record] 跳过 {skipped_no_festival} 条（festival 映射失败）")
    if skipped_no_award:
        print(f"[award_record] 跳过 {skipped_no_award} 条（award 映射失败）")
    if skipped_no_person:
        print(f"[award_record] 跳过 {skipped_no_person} 条（person 映射失败）")


if __name__ == "__main__":
    build_award_records()
