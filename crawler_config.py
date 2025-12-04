"""
crawler_config.py

集中放各个爬虫脚本共用的路径和节流参数。
"""

from __future__ import annotations

import os

# ===== 路径相关 =====

BASE_DATA_DIR = "data"

SEED_DIR = os.path.join(BASE_DATA_DIR, "seeds")
# RAW_DIR = os.path.join(BASE_DATA_DIR, "raw")
RAW_ROOT_DIR = os.path.join(BASE_DATA_DIR, "raw")

# 种子电影列表（由 0_build_movie_seeds.py 生成）
MOVIE_SEED_PATH = os.path.join(SEED_DIR, "movies_seed.jsonl")

# 电影详情爬虫输出
# MOVIE_BASIC_PATH   = os.path.join(RAW_DIR, "movies_basic.jsonl")   # ld+json 基本信息
# MOVIE_DETAILS_PATH = os.path.join(RAW_DIR, "movies_details.jsonl") # 地区 / 语言
# MOVIE_SUMMARY_PATH = os.path.join(RAW_DIR, "movies_summary.jsonl") # 剧情简介
# MOVIE_CAST_PATH    = os.path.join(RAW_DIR, "movie_cast.jsonl")     # 演员
# MOVIE_CREW_PATH    = os.path.join(RAW_DIR, "movie_crew.jsonl")     # 幕后
# MOVIE_AWARDS_PATH  = os.path.join(RAW_DIR, "movie_awards.jsonl")   # 奖项记录

# ===== 抓取节奏相关 =====

# 普通请求之间的随机间隔
REQUEST_MIN_INTERVAL = 1.5
REQUEST_MAX_INTERVAL = 3.0

# 每多少次请求后长休息一次
LONG_BREAK_EVERY = 40
LONG_BREAK_MIN   = 15
LONG_BREAK_MAX   = 30

# 重试相关
MAX_RETRY          = 3
RETRY_BACKOFF_MIN  = 10
RETRY_BACKOFF_MAX  = 30
