"""
4_crawl_movie_comments.py

根据 movies_seed.jsonl 中的种子电影列表，抓取每部电影的短评/想看信息。

使用 comments/movie_comments.py 中的 parse_comments_page 解析 HTML，自己做翻页与限速控制。

默认抓取：
  - 短评（status=P）：5 页 -> 最多 100 条
  - 想看（status=F）：2 页 -> 最多 40 条

输入：
  data/seeds/movies_seed.jsonl

输出（按 worker 划分）：
  data/raw/{worker_id}/movie_ratings.jsonl
  data/raw/{worker_id}/movie_watch_records.jsonl

其中：
  - movie_ratings.jsonl  对应 Movie_Rating 表的行（只来自 P 页看过且有评分的记录）
  - movie_watch_records.jsonl 对应 Watching_Record 表的行（P 页 watched + F 页 wishlist）
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from typing import Any, Dict, List, Optional
import requests

from bs4 import BeautifulSoup  # 主要是给类型提示用，不强依赖

from utils import fetch_html
from comments.movie_comments import parse_comments_page

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


# ====== 小工具 & 限速 ======

_request_counter = 0


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

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 404:
                print(f"[fetch] 404 Not Found，直接跳过：{url}")
                # 不重试，不 sleep，直接放弃这个 URL
                return None

            print(
                f"[fetch] HTTP error status={status} "
                f"(attempt={attempt}/{MAX_RETRY}): {url}"
            )

        # 其他网络错误（超时、连接错误等）还是按原逻辑重试
        except Exception as e:
            print(f"[fetch] 请求失败 (attempt={attempt}/{MAX_RETRY}): {url}")
            print(f"        错误: {e!r}")

        if attempt >= MAX_RETRY:
            print("[fetch] 达到最大重试次数，放弃该 URL")
            break

        backoff = random.uniform(RETRY_BACKOFF_MIN, RETRY_BACKOFF_MAX)
        print(f"[fetch] {backoff:.2f}s 后重试...")
        time.sleep(backoff)

    return None


# ====== 从种子中切分电影列表 ======

def load_seed_movie_ids_for_worker(
        worker_id: int,
        num_workers: int,
        max_movies: Optional[int] = None,
) -> List[str]:
    """
    从 movies_seed.jsonl 中读取属于当前 worker 的 movie_douban_id 列表。

    规则：
      - 按行号从 0 开始计数；
      - 行号 % num_workers == worker_id 的行归当前 worker；
      - max_movies 限制当前 worker 最多处理多少部（None 表示不限）。
    """
    if not os.path.exists(MOVIE_SEED_PATH):
        raise FileNotFoundError(f"种子文件不存在: {MOVIE_SEED_PATH}")

    if num_workers <= 0:
        num_workers = 1
    if worker_id < 0 or worker_id >= num_workers:
        print(f"[warn] worker_id={worker_id} 不在 [0, {num_workers})，自动改为 0")
        worker_id = 0

    ids: List[str] = []
    seen: set[str] = set()
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
            if mid in seen:
                continue
            seen.add(mid)

            ids.append(mid)
            if max_movies is not None and len(ids) >= max_movies:
                break

    return ids


# ====== 评论抓取核心逻辑 ======

def _build_comments_url(movie_id: str, page_idx: int, status_flag: str) -> str:
    """
    构造短评/想看页面 URL。

    - status_flag="P": /comments?start=...&status=P&sort=new_score
    - status_flag="F": /comments?start=...&status=F
    """
    start = page_idx * 20
    if status_flag.upper() == "P":
        return (
            f"https://movie.douban.com/subject/{movie_id}/comments"
            f"?start={start}&limit=20&status=P&sort=new_score"
        )
    else:
        return (
            f"https://movie.douban.com/subject/{movie_id}/comments"
            f"?start={start}&limit=20&status=F"
        )


def fetch_movie_comments_with_retry(
        movie_id: str,
        num_pages: int,
        status_flag: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    连续抓取某电影的若干页短评/想看页面，带重试与限速。

    返回：
      {
        "ratings": [...],
        "watch_records": [...],
      }
    """
    all_ratings: List[Dict[str, Any]] = []
    all_watch_records: List[Dict[str, Any]] = []

    for page_idx in range(num_pages):
        url = _build_comments_url(movie_id, page_idx, status_flag)
        html = fetch_page_with_retry(url)
        if not html:
            print(f"[movie={movie_id}] status={status_flag} 第 {page_idx} 页抓取失败，跳过")
            continue

        page_data = parse_comments_page(html, movie_id, status_flag=status_flag)
        all_ratings.extend(page_data.get("ratings", []))
        all_watch_records.extend(page_data.get("watch_records", []))

    return {
        "ratings": all_ratings,
        "watch_records": all_watch_records,
    }


def crawl_single_movie_comments(
        movie_id: str,
        f_ratings,
        f_watch_records,
        num_pages_p: int,
        num_pages_f: int,
) -> None:
    """
    抓取单部电影的短评/想看，写入 jsonl 文件。
    """
    print(f"\n==== 开始抓取电影 {movie_id} 的短评与想看 ====")

    # 短评（看过）页：status=P
    if num_pages_p > 0:
        data_p = fetch_movie_comments_with_retry(
            movie_id, num_pages=num_pages_p, status_flag="P"
        )
        ratings_p = data_p.get("ratings", [])
        watch_p = data_p.get("watch_records", [])
    else:
        ratings_p, watch_p = [], []

    # 想看页：status=F
    if num_pages_f > 0:
        data_f = fetch_movie_comments_with_retry(
            movie_id, num_pages=num_pages_f, status_flag="F"
        )
        ratings_f = data_f.get("ratings", [])
        watch_f = data_f.get("watch_records", [])
    else:
        ratings_f, watch_f = [], []

    # 按类型分别写出
    for rec in ratings_p + ratings_f:
        f_ratings.write(json.dumps(rec, ensure_ascii=False) + "\n")

    for rec in watch_p + watch_f:
        f_watch_records.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(
        f"[movie={movie_id}] ratings={len(ratings_p) + len(ratings_f)}, "
        f"watch_records={len(watch_p) + len(watch_f)}"
    )
    print(f"==== 电影 {movie_id} 短评与想看抓取完成 ====")


# ====== 参数解析 & 主流程 ======

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="根据 movies_seed.jsonl 批量抓取豆瓣电影短评/想看（支持多 worker 切分任务）"
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
    parser.add_argument(
        "--pages-p",
        type=int,
        default=5,
        help="每部电影抓取多少页短评（status=P），默认 5 页 -> 最多 100 条。",
    )
    parser.add_argument(
        "--pages-f",
        type=int,
        default=2,
        help="每部电影抓取多少页想看（status=F），默认 2 页 -> 最多 40 条。",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    worker_id = args.worker_id
    num_workers = args.num_workers
    max_movies = args.max_movies if args.max_movies and args.max_movies > 0 else None
    pages_p = max(0, args.pages_p)
    pages_f = max(0, args.pages_f)

    print(
        f"===> 启动 4_crawl_movie_comments.py | worker_id={worker_id}, "
        f"num_workers={num_workers}, max_movies={max_movies or '不限'}, "
        f"pages_p={pages_p}, pages_f={pages_f}"
    )
    print(f"种子电影文件: {MOVIE_SEED_PATH}")

    movie_ids = load_seed_movie_ids_for_worker(
        worker_id=worker_id,
        num_workers=num_workers,
        max_movies=max_movies,
    )
    print(f"当前 worker 将抓取 {len(movie_ids)} 部电影的短评/想看。")

    # 输出路径：data/raw/{worker_id}/movie_ratings.jsonl & movie_watch_records.jsonl
    worker_dir = os.path.join(RAW_ROOT_DIR, str(worker_id))
    ratings_path = os.path.join(worker_dir, "movie_ratings.jsonl")
    watch_path = os.path.join(worker_dir, "movie_watch_records.jsonl")

    ensure_dir_for_file(ratings_path)
    ensure_dir_for_file(watch_path)

    # 以覆盖模式写出（测试阶段）。以后跑正式全量，可以考虑改成 "a" 追加。
    with open(ratings_path, "w", encoding="utf-8") as f_ratings, \
            open(watch_path, "w", encoding="utf-8") as f_watch:

        for idx, mid in enumerate(movie_ids, start=1):
            print(f"\n===== [{idx}/{len(movie_ids)}] movie_id={mid} =====")
            crawl_single_movie_comments(
                movie_id=mid,
                f_ratings=f_ratings,
                f_watch_records=f_watch,
                num_pages_p=pages_p,
                num_pages_f=pages_f,
            )

    print("\n当前 worker 抓取完毕。输出文件：")
    print(f"- 评分记录: {ratings_path}")
    print(f"- 观影记录: {watch_path}")


if __name__ == "__main__":
    main()
