#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_movies_and_persons.py

从爬虫输出的 JSONL 中构建核心维度表的 staging CSV：

输入（按 worker 汇总）：
    ../data/raw/{worker_id}/movies_basic.jsonl
    ../data/raw/{worker_id}/movies_summary.jsonl
    ../data/raw/{worker_id}/person_details_fixed.jsonl  （优先）
    ../data/raw/{worker_id}/person_details.jsonl        （仅在 fixed 不存在时兜底）
    ../data/seeds/persons_seed.jsonl                    （用于人物显示名称）

输出（统一在 ../data/etl/）：
    - movies.csv
        movie_douban_id, title, image_url, release_date,
        runtime_minutes, summary   （summary 已移除内部换行）

    - movie_id_map.csv
        movie_id, movie_douban_id

    - persons.csv
        person_douban_id, name, avatar_url, sex,
        birth_date, death_date, birth_place_raw, birth_region, imdb_id

    - person_id_map.csv
        person_id, person_douban_id
"""

from __future__ import annotations

import csv
import json
import os
from typing import Dict, Iterable, Optional

# ====== 路径配置（以 ./etl 为当前目录） ======

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RAW_ROOT_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
SEED_DIR = os.path.join(BASE_DIR, "..", "data", "seeds")
ETL_OUT_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

MOVIES_BASIC_FILENAME = "movies_basic.jsonl"
MOVIES_SUMMARY_FILENAME = "movies_summary.jsonl"
PERSON_DETAILS_FIXED_FILENAME = "person_details_fixed.jsonl"
PERSON_DETAILS_OLD_FILENAME = "person_details.jsonl"

PERSON_SEED_FILENAME = "persons_seed.jsonl"

MOVIES_CSV = os.path.join(ETL_OUT_DIR, "movies.csv")
MOVIE_ID_MAP_CSV = os.path.join(ETL_OUT_DIR, "movie_id_map.csv")
PERSONS_CSV = os.path.join(ETL_OUT_DIR, "persons.csv")
PERSON_ID_MAP_CSV = os.path.join(ETL_OUT_DIR, "person_id_map.csv")


# ====== 小工具 ======

def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def iter_worker_subdirs(root_dir: str) -> Iterable[str]:
    """
    遍历 ../data/raw 下所有 worker 子目录，返回绝对路径。
    """
    if not os.path.exists(root_dir):
        raise FileNotFoundError(f"RAW_ROOT_DIR 不存在: {root_dir}")

    for name in sorted(os.listdir(root_dir)):
        subdir = os.path.join(root_dir, name)
        if os.path.isdir(subdir):
            yield subdir


def iter_jsonl(path: str):
    """逐行读取 jsonl 文件，yield dict。"""
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


def normalize_str(s: Optional[str]) -> Optional[str]:
    """
    基础清洗：转成字符串 + strip 两端空白/引号。
    注意：不改内部内容，不做中英文拆分。
    """
    if s is None:
        return None
    s = str(s)
    s = s.strip()
    if not s:
        return None
    return s.strip(' "\u3000“”')


def sanitize_text_for_csv(s: Optional[str]) -> Optional[str]:
    """
    用于要写入 CSV 的长文本字段（如 summary）：
    - 去掉两端空白；
    - 把内部的换行/回车/制表符替换为空格，避免 CSV 被拆行。
    """
    s_norm = normalize_str(s)
    if s_norm is None:
        return None
    s_norm = s_norm.replace("\r\n", "\n").replace("\r", "\n")
    s_norm = s_norm.replace("\n", " ").replace("\t", " ")
    # 再简单收一下多余空格
    return " ".join(s_norm.split())


# ====== 0. 从 persons_seed.jsonl 读取「展示用姓名」 ======

def load_person_seed_names() -> Dict[str, str]:
    """
    从 ../data/seeds/persons_seed.jsonl 中读取种子人物的展示名称：
        seed_names[person_douban_id] = name

    这里的 name 通常是「中文名 + 英文名」的格式，例如：
        "娜塔莉·波特曼 Natalie Portman"
    """
    seed_names: Dict[str, str] = {}
    seed_path = os.path.join(SEED_DIR, PERSON_SEED_FILENAME)

    if not os.path.exists(seed_path):
        print(f"[seed_names] 未找到种子人物文件: {seed_path}，将只用 details 里的名字")
        return seed_names

    print(f">>> 从种子文件加载人物名称: {seed_path}")
    for obj in iter_jsonl(seed_path):
        pid = normalize_str(obj.get("person_douban_id"))
        if not pid:
            continue
        name = normalize_str(obj.get("name"))
        if not name:
            continue
        # 种子里的 name 通常已经是你想要的「中+英」格式，
        # 我们只做 strip，不改内部。
        seed_names[pid] = name

    print(f"[seed_names] 从种子文件读取到 {len(seed_names)} 条带 name 的人物")
    return seed_names


# ====== 1. 聚合 Movie 信息 ======

def collect_movies() -> Dict[str, Dict[str, Optional[str]]]:
    """
    汇总所有 worker 的 movies_basic + movies_summary，构造：
        movies[movie_douban_id] = {
            "movie_douban_id": ...,
            "title": ...,
            "image_url": ...,
            "release_date": ...,
            "runtime_minutes": ...,
            "summary": ...,
        }
    """
    movies: Dict[str, Dict[str, Optional[str]]] = {}

    # --- 1.1 先扫 movies_basic.jsonl ---
    print(">>> 收集 movies_basic.jsonl ...")
    for worker_dir in iter_worker_subdirs(RAW_ROOT_DIR):
        basic_path = os.path.join(worker_dir, MOVIES_BASIC_FILENAME)
        if not os.path.exists(basic_path):
            continue

        print(f"  - 处理 {basic_path}")
        for obj in iter_jsonl(basic_path):
            mid = normalize_str(obj.get("movie_douban_id"))
            if not mid:
                continue

            title = normalize_str(obj.get("title"))           # 保持中英混合原样
            image_url = normalize_str(obj.get("image_url"))
            release_date = normalize_str(obj.get("release_date"))

            runtime_raw = obj.get("runtime_minutes")
            if runtime_raw is None or runtime_raw == "":
                runtime_minutes: Optional[str] = None
            else:
                runtime_minutes = str(runtime_raw)

            if mid not in movies:
                movies[mid] = {
                    "movie_douban_id": mid,
                    "title": title,
                    "image_url": image_url,
                    "release_date": release_date,
                    "runtime_minutes": runtime_minutes,
                    "summary": None,
                }
            else:
                rec = movies[mid]
                if title and not rec.get("title"):
                    rec["title"] = title
                if image_url and not rec.get("image_url"):
                    rec["image_url"] = image_url
                if release_date and not rec.get("release_date"):
                    rec["release_date"] = release_date
                if runtime_minutes and not rec.get("runtime_minutes"):
                    rec["runtime_minutes"] = runtime_minutes

    # --- 1.2 再扫 movies_summary.jsonl，补充 summary 字段 ---
    print(">>> 收集 movies_summary.jsonl ...")
    for worker_dir in iter_worker_subdirs(RAW_ROOT_DIR):
        summary_path = os.path.join(worker_dir, MOVIES_SUMMARY_FILENAME)
        if not os.path.exists(summary_path):
            continue

        print(f"  - 处理 {summary_path}")
        for obj in iter_jsonl(summary_path):
            mid = normalize_str(obj.get("movie_douban_id"))
            if not mid:
                continue

            summary = sanitize_text_for_csv(obj.get("summary"))
            if not summary:
                continue

            if mid not in movies:
                movies[mid] = {
                    "movie_douban_id": mid,
                    "title": None,
                    "image_url": None,
                    "release_date": None,
                    "runtime_minutes": None,
                    "summary": summary,
                }
            else:
                rec = movies[mid]
                if not rec.get("summary"):
                    rec["summary"] = summary

    print(f">>> Movie 总数量: {len(movies)}")
    return movies


# ====== 2. 聚合 Person 信息（优先：persons_seed.jsonl 的 name） ======

def collect_persons(seed_names: Dict[str, str]) -> Dict[str, Dict[str, Optional[str]]]:
    """
    汇总所有 worker 的 person_details_fixed.jsonl（如不存在则退回 person_details.jsonl），构造：
        persons[person_douban_id] = {
            "person_douban_id": ...,
            "name": ...,
            ...
        }

    name 字段优先级：
        1) seed_names[pid]           （来自 persons_seed.jsonl，多数是「中+英」）
        2) details 中的 name
        3) details 中的 name_cn

    这样娜塔莉·波特曼就会变成：
        "娜塔莉·波特曼 Natalie Portman"
    而不是只有中文。
    """
    persons: Dict[str, Dict[str, Optional[str]]] = {}

    print(">>> 收集 person_details_fixed / person_details.jsonl ...")
    for worker_dir in iter_worker_subdirs(RAW_ROOT_DIR):
        fixed_path = os.path.join(worker_dir, PERSON_DETAILS_FIXED_FILENAME)
        old_path = os.path.join(worker_dir, PERSON_DETAILS_OLD_FILENAME)

        if os.path.exists(fixed_path):
            details_path = fixed_path
        elif os.path.exists(old_path):
            details_path = old_path
        else:
            continue

        print(f"  - 处理 {details_path}")
        for obj in iter_jsonl(details_path):
            pid = normalize_str(obj.get("person_douban_id"))
            if not pid:
                continue

            # 1. 优先用种子里的 name（通常是中+英）
            seed_name = normalize_str(seed_names.get(pid)) if pid in seed_names else None

            # 2. 再退回 details 里的 name / name_cn
            raw_detail_name = obj.get("name")
            if raw_detail_name is None:
                raw_detail_name = obj.get("name_cn")
            detail_name = normalize_str(raw_detail_name)

            # 3. 实际使用的 name
            name = seed_name or detail_name

            avatar_url = normalize_str(obj.get("avatar_url"))
            sex = normalize_str(obj.get("sex"))
            birth_date = normalize_str(obj.get("birth_date"))
            death_date = normalize_str(obj.get("death_date"))
            birth_place_raw = normalize_str(obj.get("birth_place_raw"))
            birth_region = normalize_str(obj.get("birth_region"))
            imdb_id = normalize_str(obj.get("imdb_id"))

            if pid not in persons:
                persons[pid] = {
                    "person_douban_id": pid,
                    "name": name,
                    "avatar_url": avatar_url,
                    "sex": sex,
                    "birth_date": birth_date,
                    "death_date": death_date,
                    "birth_place_raw": birth_place_raw,
                    "birth_region": birth_region,
                    "imdb_id": imdb_id,
                }
            else:
                rec = persons[pid]
                # name 只在之前为空时才被填充，避免晚来的、信息更少的记录覆盖更好的名字
                if name and not rec.get("name"):
                    rec["name"] = name
                if avatar_url and not rec.get("avatar_url"):
                    rec["avatar_url"] = avatar_url
                if sex and not rec.get("sex"):
                    rec["sex"] = sex
                if birth_date and not rec.get("birth_date"):
                    rec["birth_date"] = birth_date
                if death_date and not rec.get("death_date"):
                    rec["death_date"] = death_date
                if birth_place_raw and not rec.get("birth_place_raw"):
                    rec["birth_place_raw"] = birth_place_raw
                if birth_region and not rec.get("birth_region"):
                    rec["birth_region"] = birth_region
                if imdb_id and not rec.get("imdb_id"):
                    rec["imdb_id"] = imdb_id

    print(f">>> Person 总数量: {len(persons)}")
    return persons


# ====== 3. 写 CSV：movies / movie_id_map / persons / person_id_map ======

def write_movies_and_id_map(movies: Dict[str, Dict[str, Optional[str]]]) -> None:
    ensure_dir(ETL_OUT_DIR)

    mids_sorted = sorted(movies.keys())

    # movies.csv
    with open(MOVIES_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "movie_douban_id",
            "title",
            "image_url",
            "release_date",
            "runtime_minutes",
            "summary",
        ])
        for mid in mids_sorted:
            rec = movies[mid]
            writer.writerow([
                rec.get("movie_douban_id") or "",
                rec.get("title") or "",
                rec.get("image_url") or "",
                rec.get("release_date") or "",
                rec.get("runtime_minutes") or "",
                rec.get("summary") or "",
                ])
    print(f"[write] movies.csv 写出完成: {MOVIES_CSV}")

    # movie_id_map.csv
    with open(MOVIE_ID_MAP_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_id", "movie_douban_id"])
        for idx, mid in enumerate(mids_sorted, start=1):
            writer.writerow([idx, mid])
    print(f"[write] movie_id_map.csv 写出完成: {MOVIE_ID_MAP_CSV}")


def write_persons_and_id_map(persons: Dict[str, Dict[str, Optional[str]]]) -> None:
    ensure_dir(ETL_OUT_DIR)

    pids_sorted = sorted(persons.keys())

    # persons.csv
    with open(PERSONS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "person_douban_id",
            "name",
            "avatar_url",
            "sex",
            "birth_date",
            "death_date",
            "birth_place_raw",
            "birth_region",
            "imdb_id",
        ])
        for pid in pids_sorted:
            rec = persons[pid]
            writer.writerow([
                rec.get("person_douban_id") or "",
                rec.get("name") or "",
                rec.get("avatar_url") or "",
                rec.get("sex") or "",
                rec.get("birth_date") or "",
                rec.get("death_date") or "",
                rec.get("birth_place_raw") or "",
                rec.get("birth_region") or "",
                rec.get("imdb_id") or "",
                ])
    print(f"[write] persons.csv 写出完成: {PERSONS_CSV}")

    # person_id_map.csv
    with open(PERSON_ID_MAP_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["person_id", "person_douban_id"])
        for idx, pid in enumerate(pids_sorted, start=1):
            writer.writerow([idx, pid])
    print(f"[write] person_id_map.csv 写出完成: {PERSON_ID_MAP_CSV}")


# ====== main ======

def main() -> None:
    print("=== 构建 Movie / Person 核心维度 staging ===")

    movies = collect_movies()
    seed_names = load_person_seed_names()
    persons = collect_persons(seed_names)

    write_movies_and_id_map(movies)
    write_persons_and_id_map(persons)

    print("\n全部完成。接下来在数据库里：")
    print("  1) COPY 导入 movie_id_map / person_id_map / movies / persons")
    print("  2) 用 movie_id_map / person_id_map 去 JOIN 各个桥表 / 事实表 staging")


if __name__ == "__main__":
    main()
