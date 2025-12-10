#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
04_build_credits.py

从爬虫结果 movie_cast.jsonl / movie_crew.jsonl 构建：

- 职位表 Position（使用 crew 中完整的 role 字段去重）
- 参演人员信息表 Cast_Credit
- 幕后职员信息表 Crew_Credit

输入：
  - ../data/etl/movies.csv
  - ../data/etl/persons.csv
  - ../data/etl/positions.csv    (可选，若不存在则自动创建)
  - ../data/raw/{worker_id}/movie_cast.jsonl
  - ../data/raw/{worker_id}/movie_crew.jsonl

输出：
  - ../data/etl/positions.csv
  - ../data/etl/cast_credit.csv
  - ../data/etl/crew_credit.csv
"""

from __future__ import annotations

import csv
import json
import os
import re
from typing import Dict, Iterable, List, Tuple, Optional


# ====== 路径配置（etl 脚本在 ./etl 下） ======

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_ROOT_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
ETL_DIR = os.path.join(BASE_DIR, "..", "data", "etl")

MOVIES_CSV = os.path.join(ETL_DIR, "movies.csv")
PERSONS_CSV = os.path.join(ETL_DIR, "persons.csv")
POSITIONS_CSV = os.path.join(ETL_DIR, "positions.csv")

CREW_CREDIT_CSV = os.path.join(ETL_DIR, "crew_credit.csv")
CAST_CREDIT_CSV = os.path.join(ETL_DIR, "cast_credit.csv")


# ====== 小工具 ======

def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _detect_columns(kind: str, fieldnames: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    自动检测：
      - douban 列名
      - 内部 id 列名

    kind: "movie" 或 "person"
    返回 (douban_col, id_col)，某个找不到则为 None。
    """
    fieldnames_set = set(fieldnames)

    if kind == "movie":
        douban_candidates = ["douban_id", "movie_douban_id", "movie_id"]
        id_candidates = ["id", "movie_id"]
    else:
        douban_candidates = ["douban_id", "person_douban_id", "person_id"]
        id_candidates = ["id", "person_id"]

    douban_col = next((c for c in douban_candidates if c in fieldnames_set), None)
    id_col = next((c for c in id_candidates if c in fieldnames_set), None)

    return douban_col, id_col


def load_id_map_from_csv(path: str, kind: str) -> Dict[str, int]:
    """
    从 CSV 中加载 Douban ID -> 内部整数 ID 的映射。

    - kind="movie":  movie_douban_id -> movie_id
    - kind="person": person_douban_id -> person_id

    若没有独立的 id 列，则直接使用 douban_id 作为内部 ID
    （即 movie_id = int(movie_douban_id)）。
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"缺少映射 CSV：{path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            print(f"[{kind}] CSV 无表头：{path}")
            return {}

        douban_col, id_col = _detect_columns(kind, fieldnames)
        print(f"[{kind}] 检测到列: douban_col={douban_col!r}, id_col={id_col!r}")

        if douban_col is None:
            print(f"[{kind}] 未找到豆瓣 ID 列（{path}），返回空映射")
            return {}

        mapping: Dict[str, int] = {}
        for row in reader:
            douban_raw = row.get(douban_col)
            if douban_raw is None:
                continue
            douban = str(douban_raw).strip()
            if not douban:
                continue

            # 如果没有独立的 id 列，就用 douban 自己当 id
            internal_raw = row.get(id_col) if id_col is not None else douban
            try:
                internal_id = int(str(internal_raw).strip())
            except (TypeError, ValueError):
                # 实在不行就跳过这一行
                continue

            mapping[douban] = internal_id

    return mapping


def iter_worker_jsonl(file_name: str) -> Iterable[Dict]:
    """
    遍历 ../data/raw 下所有 worker 目录，依次读取给定文件名的 jsonl。
    file_name: "movie_cast.jsonl" 或 "movie_crew.jsonl"
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


# ====== 职位表 Position 维护 ======

def load_positions(path: str) -> Tuple[Dict[str, int], List[Dict[str, str]], int]:
    """
    加载已有的职位表，返回：
      - name -> id 映射
      - 原始行列表（用于可能需要重写时保留）
      - 当前最大 id（便于继续自增）
    若文件不存在，则返回空结构。
    """
    positions_by_name: Dict[str, int] = {}
    rows: List[Dict[str, str]] = []
    max_id = 0

    if not os.path.exists(path):
        return positions_by_name, rows, max_id

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("name") or "").strip()
            if not name:
                continue
            try:
                pid = int(row.get("id"))
            except (TypeError, ValueError):
                continue
            positions_by_name[name] = pid
            rows.append({"id": str(pid), "name": name})
            max_id = max(max_id, pid)

    return positions_by_name, rows, max_id


def save_positions(path: str, rows: List[Dict[str, str]]) -> None:
    ensure_dir_for_file(path)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def get_or_create_position_id(
        positions_by_name: Dict[str, int],
        rows: List[Dict[str, str]],
        max_id_ref: List[int],
        name: str,
) -> int:
    """
    从职位 name 拿到 id；不存在则分配新 id 并记录到 rows 里。
    max_id_ref: 单元素 list，用来“引用传递”当前 max_id。
    """
    name = " ".join((name or "").split()) or "Unknown"

    if name in positions_by_name:
        return positions_by_name[name]

    max_id_ref[0] += 1
    new_id = max_id_ref[0]
    positions_by_name[name] = new_id
    rows.append({"id": str(new_id), "name": name})
    print(f"[position] 新增职位: id={new_id}, name={name}")
    return new_id


# ====== Cast / Crew 解析 ======

_PAREN_RE = re.compile(r"[（(](.*?)[)）]")


def extract_role_name(role_field: str) -> str:
    """
    从爬虫中的 role 字段中提取“角色名”，写入 Cast_Credit.role_name。

    典型输入：
      - "配音 Voice (配 碇真嗣)"
      - "演员 Actor (饰 Walter White)"
      - "演员 Actor"
    处理规则：
      1. 优先取括号里的内容；
      2. 括号里如果以 "配 " 或 "饰 " 开头，则去掉前缀；
      3. 如果完全解析不到，就 fallback 为原始 role_field。
    """
    if not role_field:
        return "角色"

    role_field = role_field.strip()

    m = _PAREN_RE.search(role_field)
    if m:
        inner = m.group(1).strip()
        if inner.startswith("配 "):
            inner = inner[2:].strip()
        elif inner.startswith("饰 "):
            inner = inner[2:].strip()

        inner = inner.strip()
        if inner:
            return inner[:50]

    if "饰" in role_field:
        idx = role_field.find("饰")
        candidate = role_field[idx + 1:].strip(" ：:，,")
        if candidate:
            return candidate[:50]

    return role_field[:50]


def is_principal_by_order(order_val: Optional[int], threshold: int = 3) -> bool:
    """
    简单启发式：order <= threshold 视为主创 / 主演。
    """
    if order_val is None:
        return False
    try:
        o = int(order_val)
    except (TypeError, ValueError):
        return False
    return o > 0 and o <= threshold


# ====== 核心 ETL ======

def build_crew_credit(
        movie_id_map: Dict[str, int],
        person_id_map: Dict[str, int],
        positions_by_name: Dict[str, int],
        position_rows: List[Dict[str, str]],
        max_position_id_ref: List[int],
        out_path: str,
) -> None:
    """
    生成幕后职员信息表 Crew_Credit：
      movie_id, person_id, position_id, is_principal

    这里的 Position.name 使用 **完整的 role 字段**（如 "导演 Director"、"摄影 Cinematography"），
    若 role 为空，则退化为 department；再没有就用 "Unknown"。
    """
    ensure_dir_for_file(out_path)

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_id", "person_id", "position_id", "is_principal"])

        total = 0
        skipped_no_movie = 0
        skipped_no_person = 0

        for rec in iter_worker_jsonl("movie_crew.jsonl"):
            movie_did = str(rec.get("movie_douban_id") or "").strip()
            person_did = str(rec.get("person_douban_id") or "").strip()
            role_field = (rec.get("role") or "").strip()
            department = (rec.get("department") or "").strip()
            order_val = rec.get("order")

            if not movie_did or movie_did not in movie_id_map:
                skipped_no_movie += 1
                continue
            if not person_did or person_did not in person_id_map:
                skipped_no_person += 1
                continue

            movie_id = movie_id_map[movie_did]
            person_id = person_id_map[person_did]

            # 职位名：优先 role，退化到 department，最后 Unknown
            pos_raw = role_field or department or "Unknown"
            pos_name = " ".join(pos_raw.split())  # 去掉内部换行/多空格，保留中英文原样

            position_id = get_or_create_position_id(
                positions_by_name,
                position_rows,
                max_position_id_ref,
                pos_name,
            )

            is_principal = is_principal_by_order(order_val)

            writer.writerow([
                movie_id,
                person_id,
                position_id,
                "TRUE" if is_principal else "FALSE",
            ])
            total += 1

    print(f"[crew] 共写入 {total} 条 Crew_Credit 记录")
    if skipped_no_movie:
        print(f"[crew] 跳过 {skipped_no_movie} 条（缺 movie 映射）")
    if skipped_no_person:
        print(f"[crew] 跳过 {skipped_no_person} 条（缺 person 映射）")


def build_cast_credit(
        movie_id_map: Dict[str, int],
        person_id_map: Dict[str, int],
        out_path: str,
) -> None:
    """
    生成参演人员信息表 Cast_Credit：
      movie_id, person_id, role_name, is_principal

    这里仍然使用 extract_role_name，把「配 碇真嗣」「饰 Walter White」这种
    抽成纯角色名，方便展示和查询。
    """
    ensure_dir_for_file(out_path)

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_id", "person_id", "role_name", "is_principal"])

        total = 0
        skipped_no_movie = 0
        skipped_no_person = 0

        for rec in iter_worker_jsonl("movie_cast.jsonl"):
            movie_did = str(rec.get("movie_douban_id") or "").strip()
            person_did = str(rec.get("person_douban_id") or "").strip()
            role_field = (rec.get("role") or "").strip()
            order_val = rec.get("order")

            if not movie_did or movie_did not in movie_id_map:
                skipped_no_movie += 1
                continue
            if not person_did or person_did not in person_id_map:
                skipped_no_person += 1
                continue

            movie_id = movie_id_map[movie_did]
            person_id = person_id_map[person_did]

            role_name = extract_role_name(role_field)
            role_name = " ".join(role_name.split())  # 去掉内部换行 / 多余空白

            is_principal = is_principal_by_order(order_val)

            writer.writerow([
                movie_id,
                person_id,
                role_name,
                "TRUE" if is_principal else "FALSE",
            ])
            total += 1

    print(f"[cast] 共写入 {total} 条 Cast_Credit 记录")
    if skipped_no_movie:
        print(f"[cast] 跳过 {skipped_no_movie} 条（缺 movie 映射）")
    if skipped_no_person:
        print(f"[cast] 跳过 {skipped_no_person} 条（缺 person 映射）")


def main():
    print("===> 构建 Cast_Credit / Crew_Credit 以及 Position 字典")

    # 1. 读取电影 / 人员映射（列名自动探测）
    movie_id_map = load_id_map_from_csv(MOVIES_CSV, kind="movie")
    person_id_map = load_id_map_from_csv(PERSONS_CSV, kind="person")
    print(f"[movies] 映射条数: {len(movie_id_map)}")
    print(f"[persons] 映射条数: {len(person_id_map)}")

    # 2. 职位表：加载已有数据，稍后可能扩展
    positions_by_name, position_rows, max_id = load_positions(POSITIONS_CSV)
    max_id_ref = [max_id]
    print(f"[positions] 已有职位 {len(positions_by_name)} 个，当前 max_id={max_id}")

    # 3. 生成 Crew_Credit（会动态新增 Position）
    build_crew_credit(
        movie_id_map,
        person_id_map,
        positions_by_name,
        position_rows,
        max_id_ref,
        CREW_CREDIT_CSV,
    )

    # 4. 生成 Cast_Credit
    build_cast_credit(
        movie_id_map,
        person_id_map,
        CAST_CREDIT_CSV,
    )

    # 5. 回写 / 新建 Position 表
    save_positions(POSITIONS_CSV, position_rows)
    print(f"[positions] 最终职位数 {len(position_rows)}，已写回 {POSITIONS_CSV}")

    print("===> build_credits.py 完成")


if __name__ == "__main__":
    main()
