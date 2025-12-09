"""
build_basic_dicts.py

从爬虫输出的 JSONL（../data/raw/{worker_id}/xxx.jsonl）中抽取基础字典项，
构建以下 5 张“基础字典表”的 CSV 文件，供后续使用 COPY 导入数据库：

输出目录（如果不存在会自动创建）：
    ../data/etl/basic_dicts/

输出文件：
    - dict_genre.csv      ：电影类型字典
        genre_id, name

    - dict_language.csv   ：语言字典
        lang_id, name

    - dict_region.csv     ：地区字典（来自 Movie.regions + Person.birth_region）
        region_id, name

    - dict_festival.csv   ：电影节字典
        festival_id, name, year, url

    - dict_award.csv      ：奖项字典（依赖 festival_id）
        award_id, festival_id, name, award_type

数据来源（按 worker 汇总）：
    ../data/raw/{worker_id}/movies_basic.jsonl
    ../data/raw/{worker_id}/movies_details.jsonl
    ../data/raw/{worker_id}/movie_awards.jsonl
    ../data/raw/{worker_id}/person_details.jsonl

使用方式（在仓库根目录）：
    python -m etl.build_basic_dicts
或
    cd etl && python build_basic_dicts.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, Tuple, Set, Optional


# ========== 路径配置（相对 etl 目录） ==========

CURRENT_DIR = Path(__file__).resolve().parent          # .../etl
PROJECT_ROOT = CURRENT_DIR.parent                      # 项目根目录
DATA_DIR = PROJECT_ROOT / "data"
RAW_ROOT_DIR = DATA_DIR / "raw"                        # ../data/raw
ETL_OUT_DIR = DATA_DIR / "etl" / "basic_dicts"         # ../data/etl/basic_dicts


# ========== 小工具函数 ==========

def iter_worker_files(filename: str):
    """
    遍历 ../data/raw 下所有 worker 目录，依次 yield 某个文件存在的路径。

    例如 iter_worker_files("movies_basic.jsonl")
    """
    if not RAW_ROOT_DIR.exists():
        raise FileNotFoundError(f"RAW_ROOT_DIR 不存在: {RAW_ROOT_DIR}")

    for child in sorted(RAW_ROOT_DIR.iterdir()):
        if not child.is_dir():
            continue
        path = child / filename
        if path.exists():
            yield path


def load_jsonl_lines(path: Path):
    """读取一个 jsonl 文件，逐行 yield 解析后的对象（dict）。"""
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[warn] JSON decode 失败，文件={path.name}，跳过一行: {e}")
                continue
            if isinstance(obj, dict):
                yield obj


def normalize_str(s: Optional[str]) -> Optional[str]:
    """简单字符串清洗：strip + 去掉首尾奇怪引号 / 空白。"""
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    return s.strip(' "\u3000“”')


# ========== 聚合阶段：从 raw 中收集所有候选值 ==========

def collect_basic_sets():
    """
    扫描所有 worker 的原始 jsonl，汇总出若干 set/dict 结构，返回一个总 dict：
        {
          "genres": set[str],
          "languages": set[str],
          "regions": set[str],
          "festivals": dict[(name, year) -> {"name":..., "year":..., "url":...}],
          "awards": set[(festival_name, festival_year, award_name, award_type)],
        }
    """
    genres: Set[str] = set()
    languages: Set[str] = set()
    regions: Set[str] = set()

    festivals: Dict[Tuple[str, Optional[int]], Dict[str, Optional[str]]] = {}
    awards: Set[Tuple[str, Optional[int], str, Optional[str]]] = set()

    # ---- 1. 从 movies_basic.jsonl 抽取 genres ----
    print(">>> 收集电影类型（genres）...")
    for path in iter_worker_files("movies_basic.jsonl"):
        print(f"  - 扫描 {path}")
        for obj in load_jsonl_lines(path):
            raw_genres = obj.get("genres") or []
            if not isinstance(raw_genres, list):
                continue
            for g in raw_genres:
                name = normalize_str(g)
                if not name:
                    continue
                genres.add(name)

    # ---- 2. 从 movies_details.jsonl 抽取 regions, languages ----
    print(">>> 收集电影地区（regions）和语言（languages）...")
    for path in iter_worker_files("movies_details.jsonl"):
        print(f"  - 扫描 {path}")
        for obj in load_jsonl_lines(path):
            raw_regions = obj.get("regions") or []
            if isinstance(raw_regions, list):
                for r in raw_regions:
                    name = normalize_str(r)
                    if name:
                        regions.add(name)

            raw_langs = obj.get("languages") or []
            if isinstance(raw_langs, list):
                for lang in raw_langs:
                    name = normalize_str(lang)
                    if name:
                        languages.add(name)

    # ---- 3. 从 person_details.jsonl 抽取 birth_region，补充到 regions ----
    print(">>> 收集人物出生地区（birth_region -> regions）...")
    for path in iter_worker_files("person_details_fixed.jsonl"):
        print(f"  - 扫描 {path}")
        for obj in load_jsonl_lines(path):
            br = normalize_str(obj.get("birth_region"))
            if br:
                regions.add(br)

    # ---- 4. 从 movie_awards.jsonl 抽取 festival + award ----
    print(">>> 收集电影节（festivals）和奖项（awards）...")
    for path in iter_worker_files("movie_awards.jsonl"):
        print(f"  - 扫描 {path}")
        for obj in load_jsonl_lines(path):
            fest_name = normalize_str(obj.get("festival_name"))
            fest_year = obj.get("festival_year")
            fest_url = normalize_str(obj.get("festival_url"))

            if fest_year is not None:
                try:
                    fest_year = int(fest_year)
                except (TypeError, ValueError):
                    fest_year = None

            if fest_name:
                key = (fest_name, fest_year)
                if key not in festivals:
                    festivals[key] = {
                        "name": fest_name,
                        "year": fest_year,
                        "url": fest_url,
                    }
                else:
                    # 如果之前没有 url，这次有 url，就补上
                    if not festivals[key].get("url") and fest_url:
                        festivals[key]["url"] = fest_url

            award_name = normalize_str(obj.get("award_name"))
            award_type_raw = normalize_str(obj.get("award_type"))
            if award_name:
                # 标准化 award_type 为 'movie' / 'person'（如果识别不出就保留原样小写）
                award_type: Optional[str]
                if award_type_raw:
                    lowered = award_type_raw.lower()
                    if "person" in lowered or "个人" in lowered or "演员" in lowered or "导演" in lowered or "编剧" in lowered:
                        award_type = "person"
                    elif "movie" in lowered or "影片" in lowered or "电影" in lowered:
                        award_type = "movie"
                    else:
                        award_type = lowered
                else:
                    award_type = None

                awards.add((fest_name or "", fest_year, award_name, award_type))

    print("\n>>> 聚合结果：")
    print(f"  - 类型（genres）数量           : {len(genres)}")
    print(f"  - 语言（languages）数量        : {len(languages)}")
    print(f"  - 地区（regions）数量          : {len(regions)}")
    print(f"  - 电影节（festivals）数量      : {len(festivals)}")
    print(f"  - 奖项（awards）组合数量       : {len(awards)}")

    return {
        "genres": genres,
        "languages": languages,
        "regions": regions,
        "festivals": festivals,
        "awards": awards,
    }


# ========== 写 CSV 阶段：根据聚合结果生成字典表 ==========

def ensure_out_dir():
    ETL_OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n>>> 输出目录: {ETL_OUT_DIR}")


def write_genre_dict(genres: Set[str]):
    path = ETL_OUT_DIR / "dict_genre.csv"
    print(f"  - 写出类型字典: {path}")
    rows = sorted(genres)  # 按名称排序，保证稳定
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["genre_id", "name"])
        for idx, name in enumerate(rows, start=1):
            writer.writerow([idx, name])


def write_language_dict(languages: Set[str]):
    path = ETL_OUT_DIR / "dict_language.csv"
    print(f"  - 写出语言字典: {path}")
    rows = sorted(languages)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["lang_id", "name"])
        for idx, name in enumerate(rows, start=1):
            writer.writerow([idx, name])


def write_region_dict(regions: Set[str]):
    path = ETL_OUT_DIR / "dict_region.csv"
    print(f"  - 写出地区字典: {path}")
    rows = sorted(regions)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["region_id", "name"])
        for idx, name in enumerate(rows, start=1):
            writer.writerow([idx, name])


def write_festival_dict(festivals: Dict[Tuple[str, Optional[int]], Dict[str, Optional[str]]]):
    """
    返回 festival_key -> festival_id 的映射，给奖项字典使用。
    """
    path = ETL_OUT_DIR / "dict_festival.csv"
    print(f"  - 写出电影节字典: {path}")

    # 排序规则：先按 name，再按 year（None 在最后）
    keys_sorted = sorted(
        festivals.keys(),
        key=lambda k: (k[0], k[1] if k[1] is not None else 999999),
    )

    festival_id_map: Dict[Tuple[str, Optional[int]], int] = {}

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["festival_id", "name", "year", "url"])
        for idx, key in enumerate(keys_sorted, start=1):
            fest = festivals[key]
            name = fest.get("name") or ""
            year = fest.get("year")
            url = fest.get("url") or ""
            festival_id_map[key] = idx
            writer.writerow([idx, name, year if year is not None else "", url])

    return festival_id_map


def write_award_dict(
        awards: Set[Tuple[str, Optional[int], str, Optional[str]]],
        festival_id_map: Dict[Tuple[str, Optional[int]], int],
):
    path = ETL_OUT_DIR / "dict_award.csv"
    print(f"  - 写出奖项字典: {path}")

    # 过滤掉 festival 未识别到的奖项（极少数数据异常情况）
    cleaned = []
    for fest_name, fest_year, award_name, award_type in awards:
        key = (fest_name or "", fest_year)
        fest_id = festival_id_map.get(key)
        if not fest_id:
            # 打个 log，但不至于炸掉
            print(f"[warn] 找不到对应电影节，跳过奖项: festival={key}, award={award_name}")
            continue
        cleaned.append((fest_name or "", fest_year, award_name, award_type, fest_id))

    # 排序规则：电影节名称、年份、奖项名称
    cleaned_sorted = sorted(
        cleaned,
        key=lambda x: (x[0], x[1] if x[1] is not None else 999999, x[2]),
    )

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["award_id", "festival_id", "name", "award_type"])
        for idx, (_fest_name, _fest_year, award_name, award_type, fest_id) in enumerate(cleaned_sorted, start=1):
            writer.writerow([
                idx,
                fest_id,
                award_name,
                award_type or "",
                ])


# ========== 主流程 ==========

def main():
    print("=== 构建基础字典表：genre / language / region / festival / award ===")

    data = collect_basic_sets()
    ensure_out_dir()

    # 1) 简单字典：genre, language, region
    write_genre_dict(data["genres"])
    write_language_dict(data["languages"])
    write_region_dict(data["regions"])

    # 2) festival 字典（需要返回 ID 映射给 award 用）
    festival_id_map = write_festival_dict(data["festivals"])

    # 3) award 字典
    write_award_dict(data["awards"], festival_id_map)

    print("\n=== 基础字典表构建完成 ===")
    print(f"输出目录: {ETL_OUT_DIR}")


if __name__ == "__main__":
    main()
