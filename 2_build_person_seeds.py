"""
2_build_person_seeds.py

根据已经抓取好的电影演职员信息（movie_cast / movie_crew）
以及获奖信息（movie_awards），构建“种子人物列表”。

输入（按 1_crawl_movies.py 的约定）：
    data/raw/{worker_id}/movie_cast.jsonl
    data/raw/{worker_id}/movie_crew.jsonl
    data/raw/{worker_id}/movie_awards.jsonl

输出：
    data/seeds/persons_seed.jsonl

每一行格式示例：
{
  "person_douban_id": "27256810",
  "name": "黄政民 Hwang Jung Min",
  "total_movies": 5,
  "total_cast_movies": 5,
  "total_crew_movies": 0,
  "best_cast_order": 1,
  "is_actor": true,
  "is_director": false,
  "is_writer": false,
  "award_wins": 1,
  "award_noms": 2,
  "actor_score": 17,
  "crew_score": 0,
  "award_score": 5,
  "total_score": 22,
  "seed_reasons": ["core_actor", "awarded_person"]
}

选种规则（概要）：
  - 核心演员（core_actor）：
        参演电影数 >= 3
     或 参演电影数 >= 2 且 最佳演员排序 best_cast_order <= 3
  - 核心导演（core_director）：
        至少 2 部电影担任导演
  - 核心编剧（core_writer）：
        至少 3 部电影担任编剧
  - 奖项人物（awarded_person）：
        至少 1 次获奖 或 至少 3 次提名
  - 高频常客（frequent_person）：
        未被以上规则选中，且参与电影数 >= 2，
        按 total_score 排序选出前 max_frequent 名（默认 300）
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List, Optional, Set

from crawler_config import RAW_ROOT_DIR, SEED_DIR


# ========== 小工具 ==========

def ensure_dir_for_file(path: str) -> None:
    """确保某个文件的上级目录存在"""
    dirname = os.path.dirname(path)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)


def iter_jsonl(path: str):
    """逐行读取 jsonl 文件，yield 解析后的 dict。文件不存在则直接返回空。"""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            yield obj


# ========== 第一步：收集全局人物统计 ==========

def collect_person_stats() -> Dict[str, Dict[str, Any]]:
    """
    扫描 RAW_ROOT_DIR 下所有 worker 子目录，汇总每个人物的统计信息。
    返回：
      person_stats[person_id] = {
        person_douban_id: str,
        name: Optional[str],

        cast_movies: Set[movie_id],
        crew_movies: Set[movie_id],
        cast_orders: Dict[movie_id, best_order],   # 每部电影中该演员的最小 order
        best_cast_order: Optional[int],

        departments: Set[str],
        roles: Set[str],

        director_movies: Set[movie_id],
        writer_movies: Set[movie_id],

        award_wins: int,
        award_noms: int,
      }
    """
    person_stats: Dict[str, Dict[str, Any]] = {}

    if not os.path.exists(RAW_ROOT_DIR):
        raise FileNotFoundError(f"RAW_ROOT_DIR not found: {RAW_ROOT_DIR}")

    for entry in sorted(os.listdir(RAW_ROOT_DIR)):
        worker_dir = os.path.join(RAW_ROOT_DIR, entry)
        if not os.path.isdir(worker_dir):
            continue

        cast_path = os.path.join(worker_dir, "movie_cast.jsonl")
        crew_path = os.path.join(worker_dir, "movie_crew.jsonl")
        awards_path = os.path.join(worker_dir, "movie_awards.jsonl")

        # ----- cast：演员信息 -----
        for rec in iter_jsonl(cast_path):
            pid = str(rec.get("person_douban_id") or "").strip()
            mid = str(rec.get("movie_douban_id") or "").strip()
            if not pid or not mid:
                continue

            name = (rec.get("name") or "").strip() or None
            department = (rec.get("department") or "").strip()
            role = (rec.get("role") or "").strip()
            order_raw = rec.get("order")

            order: Optional[int] = None
            if isinstance(order_raw, int):
                order = order_raw
            else:
                try:
                    order = int(order_raw)
                except Exception:
                    order = None

            stats = person_stats.get(pid)
            if stats is None:
                stats = {
                    "person_douban_id": pid,
                    "name": name,
                    "cast_movies": set(),        # type: Set[str]
                    "crew_movies": set(),        # type: Set[str]
                    "cast_orders": {},           # type: Dict[str, int]
                    "best_cast_order": None,     # type: Optional[int]
                    "departments": set(),        # type: Set[str]
                    "roles": set(),              # type: Set[str]
                    "director_movies": set(),    # type: Set[str]
                    "writer_movies": set(),      # type: Set[str]
                    "award_wins": 0,
                    "award_noms": 0,
                }
                person_stats[pid] = stats

            if name and not stats["name"]:
                stats["name"] = name

            stats["cast_movies"].add(mid)
            if department:
                stats["departments"].add(department)
            if role:
                stats["roles"].add(role)

            if order is not None:
                # 记录该电影下的最小 order
                prev = stats["cast_orders"].get(mid)
                if prev is None or order < prev:
                    stats["cast_orders"][mid] = order
                # 更新全局 best_cast_order
                best = stats["best_cast_order"]
                if best is None or order < best:
                    stats["best_cast_order"] = order

        # ----- crew：幕后信息 -----
        for rec in iter_jsonl(crew_path):
            pid = str(rec.get("person_douban_id") or "").strip()
            mid = str(rec.get("movie_douban_id") or "").strip()
            if not pid or not mid:
                continue

            name = (rec.get("name") or "").strip() or None
            department = (rec.get("department") or "").strip()
            role = (rec.get("role") or "").strip()

            stats = person_stats.get(pid)
            if stats is None:
                stats = {
                    "person_douban_id": pid,
                    "name": name,
                    "cast_movies": set(),        # type: Set[str]
                    "crew_movies": set(),        # type: Set[str]
                    "cast_orders": {},           # type: Dict[str, int]
                    "best_cast_order": None,     # type: Optional[int]
                    "departments": set(),        # type: Set[str]
                    "roles": set(),              # type: Set[str]
                    "director_movies": set(),    # type: Set[str]
                    "writer_movies": set(),      # type: Set[str]
                    "award_wins": 0,
                    "award_noms": 0,
                }
                person_stats[pid] = stats

            if name and not stats["name"]:
                stats["name"] = name

            stats["crew_movies"].add(mid)
            if department:
                stats["departments"].add(department)
            if role:
                stats["roles"].add(role)

            # 检测导演 / 编剧角色
            dept_lower = department.lower()
            role_lower = role.lower()
            if "导演" in department or "director" in dept_lower or "director" in role_lower:
                stats["director_movies"].add(mid)
            if "编剧" in department or "writer" in dept_lower or "writer" in role_lower:
                stats["writer_movies"].add(mid)

        # ----- awards：获奖 / 提名信息 -----
        for rec in iter_jsonl(awards_path):
            if rec.get("award_type") != "Person":
                continue
            pid = str(rec.get("person_douban_id") or "").strip()
            if not pid:
                continue
            is_winner = bool(rec.get("is_winner"))

            stats = person_stats.get(pid)
            if stats is None:
                stats = {
                    "person_douban_id": pid,
                    "name": None,
                    "cast_movies": set(),        # type: Set[str]
                    "crew_movies": set(),        # type: Set[str]
                    "cast_orders": {},           # type: Dict[str, int]
                    "best_cast_order": None,     # type: Optional[int]
                    "departments": set(),        # type: Set[str]
                    "roles": set(),              # type: Set[str]
                    "director_movies": set(),    # type: Set[str]
                    "writer_movies": set(),      # type: Set[str]
                    "award_wins": 0,
                    "award_noms": 0,
                }
                person_stats[pid] = stats

            if is_winner:
                stats["award_wins"] += 1
            else:
                stats["award_noms"] += 1

    return person_stats


# ========== 第二步：根据规则构建种子列表 ==========

def build_person_seeds(
        max_frequent: int = 300,
        min_total_movies_for_frequent: int = 2,
        output_path: Optional[str] = None,
) -> str:
    """
    根据全局统计结果，应用选种规则，生成 persons_seed.jsonl。

    返回最终写入的文件路径。
    """
    if output_path is None:
        output_dir = SEED_DIR
        ensure_dir_for_file(os.path.join(output_dir, "dummy.txt"))
        output_path = os.path.join(SEED_DIR, "persons_seed.jsonl")
    ensure_dir_for_file(output_path)

    person_stats = collect_person_stats()

    frequent_candidates: List[Dict[str, Any]] = []
    all_records: List[Dict[str, Any]] = []

    for pid, stats in person_stats.items():
        cast_movies: Set[str] = stats["cast_movies"]
        crew_movies: Set[str] = stats["crew_movies"]
        cast_orders: Dict[str, int] = stats["cast_orders"]
        departments: Set[str] = stats["departments"]
        roles: Set[str] = stats["roles"]
        director_movies: Set[str] = stats["director_movies"]
        writer_movies: Set[str] = stats["writer_movies"]

        cast_count = len(cast_movies)
        crew_count = len(crew_movies)
        total_movies = len(cast_movies | crew_movies)
        best_cast_order = stats["best_cast_order"]
        award_wins = stats["award_wins"]
        award_noms = stats["award_noms"]

        # 有一些出现在 awards 中但不在任何电影演职人员表中的人物
        # 舍弃这些数据
        if total_movies == 0:
            continue

        # ---- 角色标签 ----
        is_actor = cast_count > 0

        def _contains_any(text: str, keywords: List[str]) -> bool:
            t = text.lower()
            return any(kw in t for kw in keywords)

        is_director = any(
            ("导演" in d) or _contains_any(d, ["director"])
            for d in departments
        ) or any(_contains_any(r, ["director"]) for r in roles)

        is_writer = any(
            ("编剧" in d) or _contains_any(d, ["writer"])
            for d in departments
        ) or any(_contains_any(r, ["writer"]) for r in roles)

        # ---- 评分 ----
        # 演员分：按每部电影的 best order 加权
        actor_score = 0
        for order in cast_orders.values():
            if order is None:
                actor_score += 1
            elif order <= 3:
                actor_score += 3
            elif order <= 8:
                actor_score += 2
            else:
                actor_score += 1

        director_count = len(director_movies)
        writer_count = len(writer_movies)
        other_crew_count = len(crew_movies - director_movies - writer_movies)
        crew_score = director_count * 3 + writer_count * 2 + other_crew_count * 1

        award_score = award_wins * 3 + award_noms * 1
        total_score = actor_score + crew_score + award_score

        # ---- 选种理由 ----
        seed_reasons: List[str] = []

        # 核心演员
        if cast_count >= 3 or (cast_count >= 2 and best_cast_order is not None and best_cast_order <= 3):
            seed_reasons.append("core_actor")

        # 核心导演 / 编剧
        if is_director and director_count >= 2:
            seed_reasons.append("core_director")
        if is_writer and writer_count >= 3:
            seed_reasons.append("core_writer")

        # 奖项人物
        if award_wins >= 1 or award_noms >= 3:
            seed_reasons.append("awarded_person")

        record = {
            "person_douban_id": pid,
            "name": stats["name"],
            "total_movies": total_movies,
            "total_cast_movies": cast_count,
            "total_crew_movies": crew_count,
            "best_cast_order": best_cast_order,
            "is_actor": is_actor,
            "is_director": is_director,
            "is_writer": is_writer,
            "award_wins": award_wins,
            "award_noms": award_noms,
            "actor_score": actor_score,
            "crew_score": crew_score,
            "award_score": award_score,
            "total_score": total_score,
            "seed_reasons": seed_reasons,
        }

        all_records.append(record)

        # 还没被 core/award 规则选中，但在“高频候选池”里的，后面按分数截断
        if not seed_reasons:
            if total_movies >= min_total_movies_for_frequent and total_score > 0:
                frequent_candidates.append(record)

    # ---- frequent_person 选拔 ----
    frequent_candidates.sort(key=lambda r: r["total_score"], reverse=True)
    selected_frequent = frequent_candidates[:max_frequent]
    for r in selected_frequent:
        r["seed_reasons"].append("frequent_person")

    # ---- 汇总最终 seeds ----
    seeds = [r for r in all_records if r["seed_reasons"]]

    # 写出 jsonl
    with open(output_path, "w", encoding="utf-8") as f:
        for r in seeds:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return output_path


# ========== CLI ==========

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="根据电影演职员与奖项信息，构建种子人物列表 persons_seed.jsonl"
    )
    parser.add_argument(
        "--max-frequent",
        type=int,
        default=300,
        help="最多额外选取多少名 frequent_person（默认 300）",
    )
    parser.add_argument(
        "--min-total-movies-for-frequent",
        type=int,
        default=2,
        help="入选 frequent_person 至少参与多少部种子电影（默认 2）",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="输出文件路径，默认写到 data/seeds/persons_seed.jsonl",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_path = args.output if args.output else None
    path = build_person_seeds(
        max_frequent=args.max_frequent,
        min_total_movies_for_frequent=args.min_total_movies_for_frequent,
        output_path=output_path,
    )
    print(f"种子人物列表已写入: {path}")


if __name__ == "__main__":
    main()
