#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
05_build_users_and_comments.py

从爬虫结果 movie_ratings.jsonl / movie_watch_records.jsonl 构建：

- 用户维度表 users.csv
- 评分事实表 movie_ratings.csv
- 观影记录事实表 watching_records.csv

输入：
  - ../data/etl/basic_dicts/movies.csv
  - ../data/raw/{worker_id}/movie_ratings.jsonl
  - ../data/raw/{worker_id}/movie_watch_records.jsonl

输出：
  - ../data/etl/basic_dicts/users.csv
  - ../data/etl/basic_dicts/movie_ratings.csv
  - ../data/etl/basic_dicts/watching_records.csv
"""

from __future__ import annotations

import csv
import json
import os
import re
from typing import Dict, Iterable, List, Optional, Tuple


# ====== 路径配置（etl 脚本在 ./etl 下） ======

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_ROOT_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

MOVIES_CSV = os.path.join(ETL_DIR, "movies.csv")

USERS_CSV = os.path.join(ETL_DIR, "users.csv")
MOVIE_RATINGS_CSV = os.path.join(ETL_DIR, "movie_ratings.csv")
WATCHING_RECORDS_CSV = os.path.join(ETL_DIR, "watching_records.csv")


# ====== 小工具 ======

def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def iter_worker_jsonl(file_name: str) -> Iterable[Dict]:
    """
    遍历 ../data/raw 下所有 worker 目录，依次读取给定文件名的 jsonl。
    file_name: "movie_ratings.jsonl" 或 "movie_watch_records.jsonl"
    """
    if not os.path.exists(RAW_ROOT_DIR):
        return

    for entry in sorted(os.listdir(RAW_ROOT_DIR)):
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


def _detect_columns(kind: str, fieldnames: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    自动检测：
      - douban 列名
      - 内部 id 列名

    kind: "movie"
    返回 (douban_col, id_col)，某个找不到则为 None。
    """
    fieldnames_set = set(fieldnames)

    if kind == "movie":
        douban_candidates = ["douban_id", "movie_douban_id", "movie_id"]
        id_candidates = ["id", "movie_id"]
    else:
        raise ValueError(f"未知 kind: {kind}")

    douban_col = next((c for c in douban_candidates if c in fieldnames_set), None)
    id_col = next((c for c in id_candidates if c in fieldnames_set), None)

    return douban_col, id_col


def load_movie_id_map(path: str) -> Dict[str, int]:
    """
    从 movies.csv 中加载 Douban ID -> 内部整数 ID 的映射。
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"缺少 movies 映射 CSV：{path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            print("[movie] CSV 无表头")
            return {}

        douban_col, id_col = _detect_columns("movie", fieldnames)
        print(f"[movie] 检测到列: douban_col={douban_col!r}, id_col={id_col!r}")

        if douban_col is None:
            print("[movie] 未找到豆瓣 ID 列，返回空映射")
            return {}

        mapping: Dict[str, int] = {}
        for row in reader:
            douban_raw = row.get(douban_col)
            if douban_raw is None:
                continue
            douban = str(douban_raw).strip()
            if not douban:
                continue

            internal_raw = row.get(id_col) if id_col is not None else douban
            try:
                internal_id = int(str(internal_raw).strip())
            except (TypeError, ValueError):
                continue

            mapping[douban] = internal_id

    return mapping


# 文本清洗：去掉换行并压缩空白，防止 summary / review 在 CSV 里断行
_TEXT_WS_RE = re.compile(r"\s+")


def normalize_text(value: Optional[str], max_len: Optional[int] = None) -> str:
    """
    用于 username_raw / review 这类字段：
      - 把所有换行变成空格；
      - 压缩连续空白；
      - 去掉首尾空白；
      - 可选截断长度。
    """
    if value is None:
        return ""

    s = str(value)
    # 统一换行
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # 把多行合并成一行
    lines = [ln.strip() for ln in s.split("\n") if ln.strip()]
    s = " ".join(lines)
    # 压缩内部空白
    s = _TEXT_WS_RE.sub(" ", s).strip()

    if max_len is not None and len(s) > max_len:
        s = s[:max_len]

    return s


# ====== 第一步：收集用户，生成 users.csv ======

def collect_users() -> Dict[str, Dict[str, str]]:
    """
    从 movie_ratings.jsonl / movie_watch_records.jsonl 里收集所有 user_hash。
    返回:
      user_hash -> {"name": name, "email": email}
    """
    users: Dict[str, Dict[str, str]] = {}

    for file_name in ("movie_ratings.jsonl", "movie_watch_records.jsonl"):
        for rec in iter_worker_jsonl(file_name):
            user_hash = str(rec.get("user_hash") or "").strip()
            if not user_hash:
                continue

            username_raw = normalize_text(rec.get("username_raw") or "", max_len=50)

            if user_hash not in users:
                # 构造一个安全的占位邮箱
                email = f"{user_hash}@douban.example"
                users[user_hash] = {
                    "name": username_raw,
                    "email": email,
                }

    return users


def build_users_csv(users: Dict[str, Dict[str, str]]) -> Dict[str, int]:
    """
    写出 users.csv，并返回 user_hash -> user_id 映射。
    """
    ensure_dir_for_file(USERS_CSV)

    user_id_map: Dict[str, int] = {}
    next_id = 1

    with open(USERS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "user_hash", "name", "email"])

        for user_hash in sorted(users.keys()):
            info = users[user_hash]
            name = normalize_text(info.get("name") or "", max_len=50)
            email = normalize_text(info.get("email") or "", max_len=100)

            user_id = next_id
            next_id += 1
            user_id_map[user_hash] = user_id

            writer.writerow([user_id, user_hash, name, email])

    print(f"[users] 共写入 {len(user_id_map)} 个用户到 {USERS_CSV}")
    return user_id_map


# ====== 第二步：生成 movie_ratings.csv ======

def build_movie_ratings(
        movie_id_map: Dict[str, int],
        user_id_map: Dict[str, int],
) -> None:
    """
    生成评分事实表 movie_ratings.csv：
      id, movie_id, user_id, rating, created_at, review
    """
    ensure_dir_for_file(MOVIE_RATINGS_CSV)

    total = 0
    skipped_no_movie = 0
    skipped_no_user = 0

    with open(MOVIE_RATINGS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "movie_id", "user_id", "rating", "created_at", "review"])

        next_id = 1

        for rec in iter_worker_jsonl("movie_ratings.jsonl"):
            movie_did = str(rec.get("movie_douban_id") or "").strip()
            user_hash = str(rec.get("user_hash") or "").strip()

            if not movie_did or movie_did not in movie_id_map:
                skipped_no_movie += 1
                continue
            if not user_hash or user_hash not in user_id_map:
                skipped_no_user += 1
                continue

            movie_id = movie_id_map[movie_did]
            user_id = user_id_map[user_hash]

            rating_val = rec.get("rating")
            try:
                rating = int(rating_val)
            except (TypeError, ValueError):
                # rating 不合法就跳过
                continue

            created_at = (rec.get("created_at") or "").strip()
            review_raw = rec.get("review") or ""
            review = normalize_text(review_raw, max_len=1000)

            writer.writerow([
                next_id,
                movie_id,
                user_id,
                rating,
                created_at,
                review,
            ])
            next_id += 1
            total += 1

    print(f"[ratings] 共写入 {total} 条 Movie_Rating 记录 -> {MOVIE_RATINGS_CSV}")
    if skipped_no_movie:
        print(f"[ratings] 跳过 {skipped_no_movie} 条（缺 movie 映射）")
    if skipped_no_user:
        print(f"[ratings] 跳过 {skipped_no_user} 条（缺 user 映射）")


# ====== 第三步：生成 watching_records.csv ======

def build_watching_records(
        movie_id_map: Dict[str, int],
        user_id_map: Dict[str, int],
) -> None:
    """
    生成观影记录事实表 watching_records.csv：
      id, movie_id, user_id, status, created_at, star
    """
    ensure_dir_for_file(WATCHING_RECORDS_CSV)

    total = 0
    skipped_no_movie = 0
    skipped_no_user = 0

    with open(WATCHING_RECORDS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "movie_id", "user_id", "status", "created_at", "star"])

        next_id = 1

        for rec in iter_worker_jsonl("movie_watch_records.jsonl"):
            movie_did = str(rec.get("movie_douban_id") or "").strip()
            user_hash = str(rec.get("user_hash") or "").strip()

            if not movie_did or movie_did not in movie_id_map:
                skipped_no_movie += 1
                continue
            if not user_hash or user_hash not in user_id_map:
                skipped_no_user += 1
                continue

            movie_id = movie_id_map[movie_did]
            user_id = user_id_map[user_hash]

            status = (rec.get("status") or "").strip() or "unknown"
            created_at = (rec.get("created_at") or "").strip()
            star_flag = bool(rec.get("star"))
            star_str = "TRUE" if star_flag else "FALSE"

            writer.writerow([
                next_id,
                movie_id,
                user_id,
                status,
                created_at,
                star_str,
            ])
            next_id += 1
            total += 1

    print(f"[watch] 共写入 {total} 条 Watching_Record 记录 -> {WATCHING_RECORDS_CSV}")
    if skipped_no_movie:
        print(f"[watch] 跳过 {skipped_no_movie} 条（缺 movie 映射）")
    if skipped_no_user:
        print(f"[watch] 跳过 {skipped_no_user} 条（缺 user 映射）")


# ====== 主流程 ======

def main():
    print("===> 构建 users / movie_ratings / watching_records")

    # 1. 加载电影映射
    movie_id_map = load_movie_id_map(MOVIES_CSV)
    print(f"[movies] 映射条数: {len(movie_id_map)}")

    # 2. 收集所有用户并写出 users.csv
    users = collect_users()
    print(f"[users] 收集到 {len(users)} 个唯一 user_hash")
    user_id_map = build_users_csv(users)

    # 3. 生成评分表
    build_movie_ratings(movie_id_map, user_id_map)

    # 4. 生成观影记录表
    build_watching_records(movie_id_map, user_id_map)

    print("===> 05_build_users_and_comments.py 完成")


if __name__ == "__main__":
    main()
