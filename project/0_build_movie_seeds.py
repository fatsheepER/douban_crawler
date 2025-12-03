"""
0_build_movie_seeds.py

根据豆瓣 /j/chart/top_list 接口，抓取若干类型榜单的前 N 部电影，
生成全局“种子电影列表”，输出为 JSON Lines 文件，供后续爬虫使用。

依赖：
- chart_top_list.fetch_top_movies_for_type

输出：
- data/seeds/movies_seed.jsonl

每一行形如：
{
  "movie_douban_id": "1292052",
  "title": "肖申克的救赎",
  "sources": [
    {
      "type_id": 11,
      "type_label": "剧情片",
      "rank_in_type": 1
    }
  ]
}
"""
from __future__ import annotations

import json
import os
import random
import time
from typing import Dict, List, Any

from movie_info.chart_top_list import fetch_top_movies_for_type


"""配置 CONFIGURATION"""

# 想抓哪些 type_id 就写在这里
# 这些 label 纯粹是给人看的，不影响抓取逻辑
# 我列出了网页上有排行榜的全部分类
# 并根据 **个人喜好** 进行筛选
TYPE_CONFIGS: List[Dict[str, Any]] = [
    {"type_id": 11, "label": "剧情片"},
    {"type_id": 24, "label": "喜剧片"},
    {"type_id":  5, "label": "动作片"},
    {"type_id": 13, "label": "爱情片"},
    {"type_id": 17, "label": "科幻片"},
    {"type_id": 25, "label": "动画片"},
    {"type_id": 10, "label": "悬疑片"},
    {"type_id": 19, "label": "惊悚片"},
    {"type_id": 20, "label": "恐怖片"},
    {"type_id":  1, "label": "纪录片"},
    {"type_id": 23, "label":   "短片"},
    {"type_id":  6, "label": "情色片"},
    {"type_id": 14, "label": "音乐片"},    # 100:90 只有 94 条
    # {"type_id":  7, "label": "歌舞片"},    # 100:90 只有 72 条
    {"type_id": 28, "label": "家庭片"},
    {"type_id": 10, "label": "悬疑片"},
    # {"type_id":  8, "label": "儿童片"},    # 100:90 只有 37 条
    {"type_id":  2, "label": "传记片"},
    {"type_id":  4, "label": "历史片"},
    {"type_id": 22, "label": "战争片"},
    {"type_id":  3, "label": "犯罪片"},
    {"type_id": 27, "label": "西部片"},    # 100:90 只有 33 条
    {"type_id": 16, "label": "奇幻片"},
    {"type_id": 15, "label": "冒险片"},
    # {"type_id": 12, "label": "灾难片"},    # 100:90 只有 22 条
    {"type_id": 29, "label": "武侠片"},    # 100:90 只有 49 条
    {"type_id": 30, "label": "古装片"},    # 100:90 只有 94 条
    # {"type_id": 18, "label": "运动片"},    # 100:90 只有 62 条
    # {"type_id": 31, "label": "黑色电影"},  # 100:90 只有 17 条
]

# 每个类型抓多少条
TOTAL_LIMIT_PER_TYPE: int = 100

# 输出文件路径
OUTPUT_PATH: str = "data/seeds/movies_seed.jsonl"

# 简单的“人畜无害”节流：每种类型之间随机 sleep 一小会儿
SLEEP_BETWEEN_TYPES_MIN = 1.5
SLEEP_BETWEEN_TYPES_MAX = 3.0


"""核心逻辑 CORE LOGIC"""

def ensure_dir_for_file(path: str) -> None:
    """确保输出文件所在的目录存在"""
    dirname = os.path.dirname(path)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)


def build_movie_seeds() -> Dict[str, Dict[str, Any]]:
    """
    抓取多个 type 的榜单，并合并为一个去重后的种子字典。

    返回值：
        { movie_douban_id: {...seed_record...}, ... }
    """
    seeds: Dict[str, Dict[str, Any]] = {}

    for cfg in TYPE_CONFIGS:
        type_id = cfg["type_id"]
        label = cfg.get("label") or f"type_{type_id}"

        print(f"===> 开始抓取 type_id={type_id} ({label}) 的前 {TOTAL_LIMIT_PER_TYPE} 部电影")

        # 利用你已经写好的工具函数
        movies = fetch_top_movies_for_type(
            type_id=type_id,
            total_limit=TOTAL_LIMIT_PER_TYPE,
            interval_id="100:90",  # 这个参数你之前默认也是这么用的
        )

        print(f"[type={type_id}] 实际抓到 {len(movies)} 条")

        # 按榜单顺序记录 rank
        for idx, m in enumerate(movies, start=1):
            mid = str(m.get("movie_douban_id") or "").strip()
            title = (m.get("title") or "").strip()

            if not mid:
                continue

            if mid not in seeds:
                seeds[mid] = {
                    "movie_douban_id": mid,
                    "title": title,
                    "sources": [],
                }

            seeds[mid]["sources"].append(
                {
                    "type_id": type_id,
                    "type_label": label,
                    "rank_in_type": idx,
                }
            )

        # 类型之间稍微歇一会，温柔一点
        sleep_sec = random.uniform(SLEEP_BETWEEN_TYPES_MIN, SLEEP_BETWEEN_TYPES_MAX)
        print(f"[type={type_id}] 完成，本轮休息 {sleep_sec:.2f} 秒\n")
        time.sleep(sleep_sec)

    return seeds


def write_seeds_to_jsonl(seeds: Dict[str, Dict[str, Any]], output_path: str) -> None:
    """把 seeds 写到 JSON Lines 文件中"""
    ensure_dir_for_file(output_path)

    # 为了稳定可读，按 movie_douban_id 排个序
    all_ids = sorted(seeds.keys())

    count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for mid in all_ids:
            record = seeds[mid]
            # 保险起见：保证 title 不为空字符串（允许 None）
            title = record.get("title") or None
            record["title"] = title

            line = json.dumps(record, ensure_ascii=False)
            f.write(line + "\n")
            count += 1

    print(f"已写入种子电影 {count} 条 -> {output_path}")


"""主程序 MAIN"""

def main():
    print("==== 0_build_movie_seeds: 开始构建种子电影列表 ====\n")
    seeds = build_movie_seeds()
    print(f"总计去重后的种子电影数量：{len(seeds)}\n")

    write_seeds_to_jsonl(seeds, OUTPUT_PATH)
    print("==== 任务完成 ====")


if __name__ == "__main__":
    main()
