"""
3_crawl_persons.py

根据 persons_seed.jsonl 中的种子人物列表，调用
豆瓣移动端人物 API，抓取人物基础信息。

输入：
    data/seeds/persons_seed.jsonl

输出（按 worker 划分）：
    data/raw/{worker_id}/person_details.jsonl

每行格式示例（来自 person/details_api.parse_person_from_api_json）：
{
  "person_douban_id": "27242075",
  "name_cn": "雷·利奥塔 Ray Liotta",
  "avatar_url": "https://img1.doubanio.com/...",
  "sex": "男",
  "birth_date": "1954年12月18日",
  "death_date": "2022年5月26日",
  "birth_place_raw": "美国,新泽西州,纽瓦克",
  "birth_region": "美国",
  "imdb_id": "nm0000501"
}

脚本特性：
  - 支持多 worker 切分任务：
        --worker-id
        --num-workers   （默认 3）
        --max-persons   （测试阶段仅抓前若干个）
  - 每次请求后随机 sleep，偶尔长休眠，避免被 ban。
  - 失败时重试 MAX_RETRY 次。
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from typing import Any, Dict, List, Optional

from crawler_config import (
    RAW_ROOT_DIR,
    SEED_DIR,
    REQUEST_MIN_INTERVAL,
    REQUEST_MAX_INTERVAL,
    LONG_BREAK_EVERY,
    LONG_BREAK_MIN,
    LONG_BREAK_MAX,
    MAX_RETRY,
    RETRY_BACKOFF_MIN,
    RETRY_BACKOFF_MAX,
)

from person.details_api import fetch_person_details


# ====== 常量路径 ======

PERSON_SEED_PATH = os.path.join(SEED_DIR, "persons_seed.jsonl")


# ====== 小工具 ======

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


def fetch_person_with_retry(person_id: str) -> Optional[Dict[str, Any]]:
    """
    带重试的人物抓取函数：
    - 调用 person.details_api.fetch_person_details
    - 捕获异常 / None，按退避策略重试
    """
    for attempt in range(1, MAX_RETRY + 1):
        try:
            data = fetch_person_details(person_id)
            polite_sleep()
            if data is None:
                print(f"[person={person_id}] API 返回 None")
            return data
        except Exception as e:
            print(
                f"[fetch_person] 请求失败 (person={person_id}, "
                f"attempt={attempt}/{MAX_RETRY}): {e!r}"
            )
            if attempt >= MAX_RETRY:
                print(f"[fetch_person] 放弃该人物 {person_id}")
                break

            backoff = random.uniform(RETRY_BACKOFF_MIN, RETRY_BACKOFF_MAX)
            print(f"[fetch_person] {backoff:.2f}s 后重试...")
            time.sleep(backoff)

    return None


def load_seed_person_ids_for_worker(
        worker_id: int,
        num_workers: int,
        max_persons: Optional[int] = None,
) -> List[str]:
    """
    从 persons_seed.jsonl 中读取属于当前 worker 的 person_douban_id 列表。

    规则：
      - 按行号从 0 开始计数；
      - 行号 % num_workers == worker_id 的行归当前 worker；
      - max_persons 限制当前 worker 最多处理多少人（None 表示不限）。
    """
    if not os.path.exists(PERSON_SEED_PATH):
        raise FileNotFoundError(f"种子人物文件不存在: {PERSON_SEED_PATH}")

    if num_workers <= 0:
        num_workers = 1
    if worker_id < 0 or worker_id >= num_workers:
        print(f"[warn] worker_id={worker_id} 不在 [0, {num_workers})，自动改为 0")
        worker_id = 0

    ids: List[str] = []
    seen: set[str] = set()
    line_idx = -1

    with open(PERSON_SEED_PATH, "r", encoding="utf-8") as f:
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
                print(f"[seed_person] JSON decode 失败，跳过一行: {e}")
                continue

            pid = str(obj.get("person_douban_id") or "").strip()
            if not pid:
                continue
            if pid in seen:
                continue
            seen.add(pid)

            ids.append(pid)
            if max_persons is not None and len(ids) >= max_persons:
                break

    return ids


# ====== 单个人物抓取逻辑 ======

def crawl_single_person(person_id: str, f_out) -> None:
    """抓取单个人物的信息并写入 jsonl 文件"""
    print(f"\n==== 开始抓取人物 {person_id} ====")
    data = fetch_person_with_retry(person_id)
    if not data:
        print(f"[person={person_id}] 获取人物信息失败")
        return

    # 确保 person_douban_id 字段存在且是字符串
    pid = str(data.get("person_douban_id") or "").strip() or person_id
    data["person_douban_id"] = pid

    f_out.write(json.dumps(data, ensure_ascii=False) + "\n")
    print(f"==== 人物 {person_id} 抓取完成 ====")


# ====== 参数解析 & 主流程 ======

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="根据 persons_seed.jsonl 批量抓取豆瓣人物详情（支持多 worker 切分任务）"
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
        help="总 worker 数量，用于切分种子人物（默认 3）",
    )
    parser.add_argument(
        "--max-persons",
        type=int,
        default=50,
        help="当前 worker 最多处理多少个人物，默认 50（测试用）。设为 0 或负数表示不限。",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    worker_id = args.worker_id
    num_workers = args.num_workers
    max_persons = args.max_persons if args.max_persons and args.max_persons > 0 else None

    print(
        f"===> 启动 3_crawl_persons.py | worker_id={worker_id}, "
        f"num_workers={num_workers}, max_persons={max_persons or '不限'}"
    )
    print(f"人物种子文件: {PERSON_SEED_PATH}")

    person_ids = load_seed_person_ids_for_worker(
        worker_id=worker_id,
        num_workers=num_workers,
        max_persons=max_persons,
    )
    print(f"当前 worker 将抓取 {len(person_ids)} 个人物。")

    # 输出路径：data/raw/{worker_id}/person_details.jsonl
    worker_dir = os.path.join(RAW_ROOT_DIR, str(worker_id))
    out_path = os.path.join(worker_dir, "person_details.jsonl")
    ensure_dir_for_file(out_path)

    # 以覆盖模式写出。以后跑正式全量，可以考虑改成 "a" 追加。
    with open(out_path, "w", encoding="utf-8") as f_out:
        for idx, pid in enumerate(person_ids, start=1):
            print(f"\n===== [{idx}/{len(person_ids)}] person_id={pid} =====")
            crawl_single_person(pid, f_out)

    print("\n当前 worker 抓取完毕。输出文件：")
    print(f"- 人物详情: {out_path}")


if __name__ == "__main__":
    main()
