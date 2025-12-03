"""
chart_top_list.py

    调用豆瓣 /j/chart/top_list 接口，抓取某个分类下的“榜单种子电影”列表。

    示例 URL：
    - /j/chart/top_list?type=11&interval_id=100%3A90&action=&start=1000&limit=20

    用途：
    - 批量获取某个 type 对应的前 N 条电影的 Douban ID
    - 作为后续电影信息/演职人员/短评爬虫的入口
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from utils import fetch_html

BASE_URL = "https://movie.douban.com/j/chart/top_list"

def _build_top_list_url(
        type_id: int,
        start: int = 0,
        limit: int = 20,
        interval_id = "100:90",
) -> str:
    """
    根据参数构造接口的完整 URL

    :param type_id: 榜单类型，例如 11-剧情片
    :param start: 起始偏移，例如 0，20，40
    :param limit: 本页条数，默认 20
    :param interval_id: 区间参数，100:90 够项目用的了
    :return: 接口 URL
    """
    params = {
        "type": type_id,
        "interval_id": interval_id,
        "start": start,
        "limit": limit,
        "action": "",   # 浏览器里就是这么传的
    }
    query = urlencode(params)   # 100:90 -> 100%3A90
    return f"{BASE_URL}?{query}"

def _fetch_top_list_json(
        type_id: int,
        start: int = 0,
        limit: int = 20,
        interval_id: str = "100:90",
) -> List[Dict[str, Any]]:
    """请求一页榜单接口，返回原始 JSON list"""
    url = _build_top_list_url(type_id, start, limit, interval_id)
    html = fetch_html(url)
    if html is None:
        print("获取 HTML 文档失败")
        return []

    try:
        data = json.loads(html)
    except json.JSONDecodeError as e:
        print(f"[top_list] JSON decode failed: {e}")
        return []

    if not isinstance(data, list):
        print(f"[top_list] Unexpected JSON root type: {type(data)!r}")
        return []

    return [x for x in data if isinstance(x, dict)]

def _parse_movie_item(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    从单条电影 dict 中抽取出需要的信息。
    既然目标是提取种子电影列表，ID 和 title 就够了。
    注意后者不是中文+英文的标准格式名称，只用于 sanity check。

    :param raw: 接口返回的一条电影 JSON
    :return: 提取出的电影 Douban ID 和中文名
    """
    movie_id = str(raw.get("id") or "").strip()
    if not movie_id:
        print("获取电影 ID 失败")
        return None

    title = (raw.get("title") or "").strip()

    return {
        "movie_douban_id": movie_id,
        "title": title
    }


"""外部解析函数"""
def fetch_top_movies_for_type(
        type_id: int,
        total_limit: int = 100,
        interval_id: str = "100:90",    # 默认值就挺好
        page_size: int = 20,            # 默认值就挺好
) -> List[Dict[str, str]]:
    """
    抓取 type_id 所属类型的榜单前 total_limit 条电影，分页请求并汇总。

    :param type_id: 榜单类型编号
    :param total_limit: 总共需要多少条
    :param interval_id: 好于同类型 a%-b% 的电影，决定了最多能抓出多少条
    :param page_size: 偏移量，默认 20
    :return:
    """
    results: List[Dict[str, str]] = []

    # 总共抓取 total_limit 条
    start = 0
    while start < total_limit:
        this_limit = min(page_size, total_limit - start)    # 这一页要抓多少条

        raw_list = _fetch_top_list_json(
            type_id, start, limit=this_limit, interval_id=interval_id
        )
        if not raw_list:
            print(f"抓取 start={start} 的 list json 时出错")
            continue

        # 每条提取出 ID 和 title
        for raw in raw_list:
            item = _parse_movie_item(raw)
            if item is None:
                print("从 raw 提取 item 时出错")
                continue
            results.append(item)

        if len(raw_list) < this_limit:
            print("没有抓取到预期的条数")
            break

        start += this_limit

    return results

def fetch_seed_movie_ids(
        type_id: int,
        total_limit: int = 100,
        interval_id: str = "100:90",
) -> List[str]:
    """简化 fetch_top_movies_for_type 的输出，只要 ID，方便丢给其他爬虫模块"""
    movies = fetch_top_movies_for_type(
        type_id=type_id,
        total_limit=total_limit,
        interval_id=interval_id,
    )

    ids: List[str] = []
    seen = set()
    for m in movies:
        mid = m.get("movie_douban_id")
        if not mid or mid in seen:
            continue
        seen.add(mid)
        ids.append(mid)

    return ids


"""示例函数"""
def main():
    type_id = 11
    seeds = fetch_top_movies_for_type(type_id, total_limit=40)

    print(f"[type={type_id}] seed movies count = {len(seeds)}")
    for m in seeds:
        print(f"- {m['movie_douban_id']} | {m['title']}")

    ids = fetch_seed_movie_ids(type_id, total_limit=40, interval_id="100:90")
    print("seed ids:", ids)


if __name__ == '__main__':
    main()