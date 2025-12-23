#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
03_build_movies_and_persons.py

从爬虫结果构建：

- movies.csv  : 电影主表（去重后的基础信息 + 简要剧情简介）
- persons.csv : 人物主表（种子人物 + 所有在 cast/crew 出现过的人物）

输入（raw）：
  ../data/raw/{worker_id}/movies_basic.jsonl
  ../data/raw/{worker_id}/movies_summary.jsonl
  ../data/raw/{worker_id}/person_details_fixed.jsonl
  ../data/raw/{worker_id}/movie_cast.jsonl
  ../data/raw/{worker_id}/movie_crew.jsonl

输入（seeds）：
  ../data/seeds/persons_seed.jsonl   （含 person_douban_id + name 等评分信息）

输出（etl）：
  ../data/etl/movies.csv
  ../data/etl/persons.csv

注意：
  - summary 中的换行会被压成空格，避免 CSV 换行问题。
  - persons.csv 的 name 优先顺序：
      1) persons_seed.jsonl 中的 name（中英混合）
      2) movie_cast / movie_crew 中的 name
      3) person_details_fixed.jsonl 中的 name_cn（退路）
  - 本脚本会为 Movie 和 Person 分配自增整数 id 列，
    后续脚本通过 (douban_id -> id) 映射来写各种桥表 / 事实表。
"""

from __future__ import annotations

import csv
import json
import os
from typing import Dict, Iterable

# ========= 路径配置 =========

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_ROOT_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
SEED_DIR = os.path.join(BASE_DIR, "..", "data", "seeds")
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

MOVIES_CSV = os.path.join(ETL_DIR, "movies.csv")
PERSONS_CSV = os.path.join(ETL_DIR, "persons.csv")

PERSON_SEED_PATH = os.path.join(SEED_DIR, "persons_seed.jsonl")


# ========= 小工具 =========

def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def iter_worker_jsonl(file_name: str) -> Iterable[dict]:
    """
    遍历 ../data/raw 下所有 worker 目录，依次读取指定的 jsonl 文件。
    """
    if not os.path.exists(RAW_ROOT_DIR):
        return

    for entry in os.listdir(RAW_ROOT_DIR):
        worker_dir = os.path.join(RAW_ROOT_DIR, entry)
        if not os.path.isdir(worker_dir):
            continue

        path = os.path.join(worker_dir, file_name)
        if not os.path.exists(path):
            continue

        print(f"[load] 读取 {path}")
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"[warn] JSON decode 失败: {path} {e}")
                    continue
                if isinstance(obj, dict):
                    yield obj


# ========= 构建 movies.csv =========

def build_movies() -> None:
    """
    从 movies_basic.jsonl + movies_summary.jsonl 构建 movies.csv。

    这里不做类型/语言/地区桥表，那些已经在其他 ETL 里处理。
    """
    # 汇总基础信息：movie_douban_id -> record
    basic_by_mid: Dict[str, dict] = {}
    for rec in iter_worker_jsonl("movies_basic.jsonl"):
        mid = str(rec.get("movie_douban_id") or "").strip()
        if not mid:
            continue
        # 若有重复，以第一条为准即可
        if mid not in basic_by_mid:
            basic_by_mid[mid] = rec

    # 剧情简介（可能缺失）
    summary_by_mid: Dict[str, str] = {}
    for rec in iter_worker_jsonl("movies_summary.jsonl"):
        mid = str(rec.get("movie_douban_id") or "").strip()
        if not mid:
            continue
        summary = str(rec.get("summary") or "").strip()
        if not summary:
            continue
        # 压平换行，避免 CSV 中断行
        summary_clean = " ".join(summary.split())
        summary_by_mid[mid] = summary_clean

    print(f"[movies] 基本信息条数: {len(basic_by_mid)}")
    print(f"[movies] 有剧情简介的条数: {len(summary_by_mid)}")

    # 为电影分配内部整数 id（1,2,3,...)。
    # 为了稳定可复现，这里按照 Douban ID 数字升序来排。
    all_mids_sorted = sorted(basic_by_mid.keys(), key=lambda x: int(x))

    ensure_dir_for_file(MOVIES_CSV)
    with open(MOVIES_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        # 注意增加了 id 列，后续脚本会用它作为内部主键
        writer.writerow([
            "id",                # 内部自增 id（这里预先分配好）
            "movie_douban_id",   # Douban ID（字符串）
            "title",
            "image_url",
            "release_date",
            "runtime_minutes",
            "summary",
        ])

        for idx, mid in enumerate(all_mids_sorted, start=1):
            b = basic_by_mid[mid]
            title = b.get("title") or ""
            image_url = b.get("image_url") or ""
            release_date = b.get("release_date") or ""
            runtime = b.get("runtime_minutes") or ""
            summary = summary_by_mid.get(mid, "")

            writer.writerow([
                idx,        # id
                mid,        # movie_douban_id
                title,
                image_url,
                release_date,
                runtime,
                summary,
            ])

    print(f"[movies] 已写出 {MOVIES_CSV}")


# ========= 构建 persons.csv =========

def load_seed_person_names() -> Dict[str, str]:
    """
    从 persons_seed.jsonl 中读取 {person_douban_id: name}。

    name 是我们之前在种子脚本里保留的「中英混合」名字，
    优先用于最终 persons.csv。
    """
    mapping: Dict[str, str] = {}
    if not os.path.exists(PERSON_SEED_PATH):
        print(f"[persons_seed] 文件不存在: {PERSON_SEED_PATH}")
        return mapping

    with open(PERSON_SEED_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[seed] JSON decode 失败，跳过一行: {e}")
                continue

            pid = str(obj.get("person_douban_id") or "").strip()
            name = str(obj.get("name") or "").strip()
            if not pid or not name:
                continue

            # 若出现重复 id，保留第一条即可
            if pid not in mapping:
                mapping[pid] = name

    print(f"[persons_seed] 读取到 {len(mapping)} 条带 name 的人物种子")
    return mapping


def load_person_details_fixed() -> Dict[str, dict]:
    """
    从各 worker 的 person_details_fixed.jsonl 中读取人物详细信息：

    字段示例：
      - person_douban_id
      - name_cn
      - avatar_url
      - sex
      - birth_date
      - death_date
      - birth_place_raw
      - birth_region
      - imdb_id
    """
    details_by_pid: Dict[str, dict] = {}

    for rec in iter_worker_jsonl("person_details_fixed.jsonl"):
        pid = str(rec.get("person_douban_id") or "").strip()
        if not pid:
            continue
        # 若有重复（多 worker），取第一条为主
        if pid not in details_by_pid:
            details_by_pid[pid] = rec

    print(f"[person_details_fixed] 读取到 {len(details_by_pid)} 条人物详情")
    return details_by_pid


def load_person_names_from_credits() -> Dict[str, str]:
    """
    从 movie_cast.jsonl + movie_crew.jsonl 中收集人物名称（中英混合），
    防止那些没进种子的人完全没名字。
    """
    name_by_pid: Dict[str, str] = {}

    # cast
    for rec in iter_worker_jsonl("movie_cast.jsonl"):
        pid = str(rec.get("person_douban_id") or "").strip()
        if not pid:
            continue
        if pid in name_by_pid:
            continue
        name = str(rec.get("name") or "").strip()
        if not name:
            continue
        name_by_pid[pid] = name

    # crew
    for rec in iter_worker_jsonl("movie_crew.jsonl"):
        pid = str(rec.get("person_douban_id") or "").strip()
        if not pid:
            continue
        if pid in name_by_pid:
            continue
        name = str(rec.get("name") or "").strip()
        if not name:
            continue
        name_by_pid[pid] = name

    print(f"[credits] 从 cast/crew 中补充到 {len(name_by_pid)} 条人物名称")
    return name_by_pid


def build_persons() -> None:
    """
    综合 persons_seed + person_details_fixed + cast/crew 中的人物，
    构建 persons.csv，并为每个人物分配整数 id。
    """
    seed_names = load_seed_person_names()
    details_by_pid = load_person_details_fixed()
    credit_names = load_person_names_from_credits()

    # 最终要进入 persons.csv 的人物集合：
    #   - 在 person_details_fixed 中出现的（即我们调过 API 的种子人物）
    #   - 在 cast/crew 中出现过的所有人物
    all_pids = set(details_by_pid.keys()) | set(credit_names.keys())
    print(f"[persons] 汇总人物 ID 总数: {len(all_pids)}")

    # 为了保证 id 稳定，按 Douban ID 数字升序
    all_pids_sorted = sorted(all_pids, key=lambda x: int(x))

    ensure_dir_for_file(PERSONS_CSV)
    with open(PERSONS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        # 注意：增加了 id 列，其余列保持原有设计，方便后续使用
        writer.writerow([
            "id",                # 内部自增 id
            "person_douban_id",  # Douban ID
            "name",
            "avatar_url",
            "sex",
            "birth_date",
            "death_date",
            "birth_place_raw",
            "birth_region",
            "imdb_id",
        ])

        wrote = 0
        skipped_no_name = 0

        for idx, pid in enumerate(all_pids_sorted, start=1):
            info = details_by_pid.get(pid, {})

            # name 优先级：
            # 1) persons_seed (中英混合)
            # 2) movie_cast / movie_crew
            # 3) person_details_fixed.name_cn （退路）
            name = (
                    seed_names.get(pid)
                    or credit_names.get(pid)
                    or str(info.get("name_cn") or "").strip()
            )

            if not name:
                # 这里基本只会剩下那种“只在获奖页面出现、我们也没爬详情且在 cast/crew 中也没有名字”的人
                skipped_no_name += 1
                continue

            avatar_url = str(info.get("avatar_url") or "").strip()
            sex = str(info.get("sex") or "").strip()
            birth_date = str(info.get("birth_date") or "").strip()
            death_date = str(info.get("death_date") or "").strip()
            birth_place_raw = str(info.get("birth_place_raw") or "").strip()
            birth_region = str(info.get("birth_region") or "").strip()
            imdb_id = str(info.get("imdb_id") or "").strip()

            writer.writerow([
                idx,          # id
                pid,          # person_douban_id
                name,
                avatar_url,
                sex,
                birth_date,
                death_date,
                birth_place_raw,
                birth_region,
                imdb_id,
            ])
            wrote += 1

    print(f"[persons] 共写入 {wrote} 条人物记录")
    if skipped_no_name:
        print(f"[persons] 跳过 {skipped_no_name} 条（完全拿不到名字）")
    print(f"[persons] 已写出 {PERSONS_CSV}")


# ========= main =========

def main():
    print("===> 构建 movies.csv / persons.csv（含内部整数 id 与 Douban ID 映射）")
    build_movies()
    build_persons()
    print("===> 03_build_movies_and_persons.py 完成")


if __name__ == "__main__":
    main()
