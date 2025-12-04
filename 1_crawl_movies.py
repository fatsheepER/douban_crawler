"""
1_crawl_movies.py

从 movies_seed.jsonl 中读取种子电影 ID，
对每部电影抓取：

- ld+json 基本信息（名称、封面、上映日期、片长、类型等）
- 主页面 #info 区块中的制片国家/地区、语言
- 剧情简介（#link-report-intra）
- /celebrities 演职员信息（cast / crew）
- /awards 奖项记录

输出为若干 JSON Lines 文件：

- data/raw/{worker_id}/movies_basic.jsonl
- data/raw/{worker_id}/movies_details.jsonl
- data/raw/{worker_id}/movies_summary.jsonl
- data/raw/{worker_id}/movie_cast.jsonl
- data/raw/{worker_id}/movie_crew.jsonl
- data/raw/{worker_id}/movie_awards.jsonl

支持多 worker 切分任务：
- 通过 --worker-id 和 --num-workers 指定当前 worker 要处理的子集
- 默认 num_workers = 3
- 默认只爬前 10 部（max_movies=10），用于本地测试
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from typing import Any, Dict, List, Optional

from utils import fetch_html

# 按你的实际模块路径调整下面这些 import
from movie_info.ld_json import parse_movie_basic_from_ld_json
from movie_info.summary import parse_summary
from movie_info.details import parse_details
from person.celebrities import parse_celebrities
from award.movie_awards import parse_awards

from crawler_config import (
    MOVIE_SEED_PATH,
    RAW_ROOT_DIR,
    REQUEST_MIN_INTERVAL,
    REQUEST_MAX_INTERVAL,
    LONG_BREAK_EVERY,
    LONG_BREAK_MIN,
    LONG_BREAK_MAX,
    MAX_RETRY,
    RETRY_BACKOFF_MIN,
    RETRY_BACKOFF_MAX,
)

# ====== URL 模板 ======

SUBJECT_URL_TMPL = "https://movie.douban.com/subject/{movie_id}/"
CELEBRITIES_URL_TMPL = "https://movie.douban.com/subject/{movie_id}/celebrities"
AWARDS_URL_TMPL = "https://movie.douban.com/subject/{movie_id}/awards/"


# ====== 小工具 ======

_request_counter = 0  # 全局请求计数器


def ensure_dir_for_file(path: str) -> None:
    """确保某个文件的上级目录存在"""
    dirname = os.path.dirname(path)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)


def polite_sleep() -> None:
    """在每次请求后小睡一下，偶尔长休息"""
    global _request_counter
    _request_counter += 1

    # 普通间隔
    interval = random.uniform(REQUEST_MIN_INTERVAL, REQUEST_MAX_INTERVAL)
    time.sleep(interval)

    # 长休息
    if _request_counter % LONG_BREAK_EVERY == 0:
        long_sleep = random.uniform(LONG_BREAK_MIN, LONG_BREAK_MAX)
        print(f"[throttle] 已发送 {_request_counter} 次请求，长休眠 {long_sleep:.2f} 秒")
        time.sleep(long_sleep)


def fetch_page_with_retry(url: str) -> Optional[str]:
    """
    带重试的抓取函数：
    - 调用 utils.fetch_html
    - 出现异常时等待一段时间重试
    - 最多重试 MAX_RETRY 次
    """
    for attempt in range(1, MAX_RETRY + 1):
        try:
            html = fetch_html(url)
            polite_sleep()
            return html
        except Exception as e:
            print(f"[fetch] 请求失败 (attempt={attempt}/{MAX_RETRY}): {url}")
            print(f"        错误: {e!r}")
            if attempt >= MAX_RETRY:
                print("[fetch] 放弃该 URL")
                break

            backoff = random.uniform(RETRY_BACKOFF_MIN, RETRY_BACKOFF_MAX)
            print(f"[fetch] {backoff:.2f}s 后重试...")
            time.sleep(backoff)

    return None


def load_seed_movie_ids_for_worker(
        worker_id: int,
        num_workers: int,
        max_movies: Optional[int] = None,
) -> List[str]:
    """
    从 movies_seed.jsonl 中读取属于当前 worker 的 movie_douban_id 列表。

    规则：
    - 按行号从 0 开始计数
    - 行号 % num_workers == worker_id 的行归当前 worker
    - max_movies 限制当前 worker 最多处理多少部（None 表示不限）
    """
    if not os.path.exists(MOVIE_SEED_PATH):
        raise FileNotFoundError(f"种子文件不存在: {MOVIE_SEED_PATH}")

    if num_workers <= 0:
        num_workers = 1
    if worker_id < 0 or worker_id >= num_workers:
        print(f"[warn] worker_id={worker_id} 不在 [0, {num_workers})，自动改为 0")
        worker_id = 0

    ids: List[str] = []
    line_idx = -1

    with open(MOVIE_SEED_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            line_idx += 1
            # 不属于当前 worker 的行直接跳过
            if line_idx % num_workers != worker_id:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[seed] JSON decode 失败，跳过一行: {e}")
                continue

            mid = str(obj.get("movie_douban_id") or "").strip()
            if not mid:
                continue

            ids.append(mid)
            if max_movies is not None and len(ids) >= max_movies:
                break

    return ids


# ====== 单部电影抓取逻辑 ======

def crawl_single_movie(
        movie_id: str,
        f_basic,
        f_details,
        f_summary,
        f_cast,
        f_crew,
        f_awards,
) -> None:
    """
    抓取单部电影的所有信息，并写入对应 jsonl 文件。
    文件句柄在外面统一打开，便于流式写入。
    """
    print(f"\n==== 开始抓取电影 {movie_id} ====")

    # --- 1. 主页面 HTML ---
    subject_url = SUBJECT_URL_TMPL.format(movie_id=movie_id)
    subject_html = fetch_page_with_retry(subject_url)
    if not subject_html:
        print(f"[movie={movie_id}] 获取 subject 页面失败，跳过该电影")
        return

    # --- 2. 基本信息（ld+json） ---
    basic = parse_movie_basic_from_ld_json(subject_html) or {}
    # 统一字段名：movie_douban_id
    movie_douban_id = basic.get("douban_id") or movie_id

    basic_record = {
        "movie_douban_id": movie_douban_id,
        "title": basic.get("name"),
        "image_url": basic.get("image_url"),
        "release_date": basic.get("release_date"),
        "genres": basic.get("genres") or [],
        "runtime_minutes": basic.get("runtime_minutes"),
    }
    f_basic.write(json.dumps(basic_record, ensure_ascii=False) + "\n")

    # --- 3. 地区 / 语言 ---
    details = parse_details(subject_html) or {}
    details_record = {
        "movie_douban_id": movie_douban_id,
        "regions": details.get("regions") or [],
        "languages": details.get("languages") or [],
    }
    f_details.write(json.dumps(details_record, ensure_ascii=False) + "\n")

    # --- 4. 剧情简介 ---
    summary_text = parse_summary(subject_html)
    if summary_text:
        summary_record = {
            "movie_douban_id": movie_douban_id,
            "summary": summary_text,
        }
        f_summary.write(json.dumps(summary_record, ensure_ascii=False) + "\n")
    else:
        print(f"[movie={movie_id}] 没有抓到剧情简介")

    # --- 5. 演职员信息 ---
    cele_url = CELEBRITIES_URL_TMPL.format(movie_id=movie_id)
    cele_html = fetch_page_with_retry(cele_url)
    if cele_html:
        cele_data = parse_celebrities(cele_html, movie_douban_id=movie_douban_id)
        for rec in cele_data.get("cast", []):
            # 确保 movie_douban_id 填好
            rec.setdefault("movie_douban_id", movie_douban_id)
            f_cast.write(json.dumps(rec, ensure_ascii=False) + "\n")

        for rec in cele_data.get("crew", []):
            rec.setdefault("movie_douban_id", movie_douban_id)
            f_crew.write(json.dumps(rec, ensure_ascii=False) + "\n")
    else:
        print(f"[movie={movie_id}] 获取演职员页面失败")

    # --- 6. 奖项信息 ---
    awards_url = AWARDS_URL_TMPL.format(movie_id=movie_id)
    awards_html = fetch_page_with_retry(awards_url)
    if awards_html:
        awards_data = parse_awards(awards_html, movie_douban_id=movie_douban_id)
        for rec in awards_data.get("awards", []):
            rec.setdefault("movie_douban_id", movie_douban_id)
            f_awards.write(json.dumps(rec, ensure_ascii=False) + "\n")
    else:
        print(f"[movie={movie_id}] 获取 awards 页面失败")

    print(f"==== 电影 {movie_id} 抓取完成 ====")


# ====== 参数解析 & 主流程 ======

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批量抓取豆瓣电影详情（支持多 worker 切分任务）"
    )
    parser.add_argument(
        "--worker-id",
        type=int,
        default=0,
        help="当前 worker 的编号，从 0 开始（默认 0）",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=3,
        help="总 worker 数量，用于切分种子电影（默认 3）",
    )
    parser.add_argument(
        "--max-movies",
        type=int,
        default=10,
        help="当前 worker 最多处理多少部电影，默认 10（测试用）。设为 0 或负数表示不限。",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    worker_id = args.worker_id
    num_workers = args.num_workers
    max_movies = args.max_movies if args.max_movies and args.max_movies > 0 else None

    print(
        f"===> 启动 1_crawl_movies.py | worker_id={worker_id}, "
        f"num_workers={num_workers}, max_movies={max_movies or '不限'}"
    )
    print(f"种子文件: {MOVIE_SEED_PATH}")

    # 根据 worker 切分种子
    movie_ids = load_seed_movie_ids_for_worker(
        worker_id=worker_id,
        num_workers=num_workers,
        max_movies=max_movies,
    )
    print(f"当前 worker 将抓取 {len(movie_ids)} 部电影。")

    # 统一输出目录：data/raw/{worker_id}/
    worker_dir = os.path.join(RAW_ROOT_DIR, str(worker_id))

    basic_path = os.path.join(worker_dir, "movies_basic.jsonl")
    details_path = os.path.join(worker_dir, "movies_details.jsonl")
    summary_path = os.path.join(worker_dir, "movies_summary.jsonl")
    cast_path = os.path.join(worker_dir, "movie_cast.jsonl")
    crew_path = os.path.join(worker_dir, "movie_crew.jsonl")
    awards_path = os.path.join(worker_dir, "movie_awards.jsonl")

    # 确保输出目录存在
    for path in [basic_path, details_path, summary_path, cast_path, crew_path, awards_path]:
        ensure_dir_for_file(path)

    # 以覆盖模式写出（测试阶段）。以后跑正式全量，可以改成 "a" 追加。
    with open(basic_path, "w", encoding="utf-8") as f_basic, \
            open(details_path, "w", encoding="utf-8") as f_details, \
            open(summary_path, "w", encoding="utf-8") as f_summary, \
            open(cast_path, "w", encoding="utf-8") as f_cast, \
            open(crew_path, "w", encoding="utf-8") as f_crew, \
            open(awards_path, "w", encoding="utf-8") as f_awards:

        for idx, mid in enumerate(movie_ids, start=1):
            print(f"\n===== [{idx}/{len(movie_ids)}] movie_id={mid} =====")
            crawl_single_movie(
                movie_id=mid,
                f_basic=f_basic,
                f_details=f_details,
                f_summary=f_summary,
                f_cast=f_cast,
                f_crew=f_crew,
                f_awards=f_awards,
            )

    print("\n当前 worker 抓取完毕。输出文件：")
    print(f"- 基本信息: {basic_path}")
    print(f"- 地区语言: {details_path}")
    print(f"- 剧情简介: {summary_path}")
    print(f"- 演员 cast: {cast_path}")
    print(f"- 幕后 crew: {crew_path}")
    print(f"- 奖项记录: {awards_path}")


if __name__ == "__main__":
    main()
