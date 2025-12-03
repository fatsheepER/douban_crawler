"""
person/details_api.py

    调用豆瓣移动端接口
        https://m.douban.com/rexxar/api/v2/elessar/subject/{person_id}
    抓取人物基础信息（精简版）。

    抽取字段：
    - person_douban_id  豆瓣人物 ID
    - name_cn           中文名
    - avatar_url        头像 URL（normal 尺寸，退化到 cover_img）
    - sex               性别
    - birth_date        出生日期
    - death_date        去世日期（如有）
    - birth_place_raw   出生地原始文本
    - birth_region      出生国家/地区（从出生地中解析出的第一段）
    - imdb_id           IMDb 编号
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from movie_info.utils import fetch_html

BASE_API_URL = "https://m.douban.com/rexxar/api/v2/elessar/subject/{person_id}"


"""小工具"""

def _extract_region_from_place(place: str) -> Optional[str]:
    """
    从出生地文本中尝试提取国家/地区部分。例如：
    - "美国,纽约,皇后区" -> "美国"
    - "古巴,哈瓦那"      -> "古巴"
    """
    if not place:
        return None

    normalized = place.replace("，", ",")
    parts = [p.strip() for p in normalized.split(",") if p.strip()]
    if not parts:
        return None
    return parts[0]


def _info_list_to_dict(info_list: List[List[str]]) -> Dict[str, str]:
    """
    extra.info 是一个 [[label, value], ...] 的二维数组，
    把它转换成 {label: value} 的 dict，方便后面按 key 取值。

    例如：
    [
      ["性别", "男"],
      ["出生日期", "1942年11月17日"],
      ["出生地", "美国,纽约,皇后区"],
      ["IMDb编号", "nm0000217"],
      ["去世日期", "2022年5月26日"],
    ]
    -> {
      "性别": "男",
      "出生日期": "1942年11月17日",
      "出生地": "美国,纽约,皇后区",
      "IMDb编号": "nm0000217",
      "去世日期": "2022年5月26日",
      ...
    }
    """
    result: Dict[str, str] = {}
    if not isinstance(info_list, list):
        return result

    for item in info_list:
        if not isinstance(item, list) or len(item) < 2:
            continue
        label = str(item[0]).strip()
        value = str(item[1]).strip()
        if label:
            result[label] = value
    return result


def _safe_get(d: Dict[str, Any], *keys, default=None):
    """
    安全多级取 dict 的小工具，避免一堆 if 嵌套。
    _safe_get(data, "cover", "normal", "url")
    """
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


"""主解析逻辑"""

def parse_person_from_api_json(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    给定从 mobile API 拿到的 dict，抽取人物基础信息（精简字段）。
    """
    # 基本标识
    person_id = str(data.get("id") or "").strip() or None
    name_cn = (data.get("title") or "").strip() or None

    # extra.info -> dict
    extra = data.get("extra") or {}
    info_dict = _info_list_to_dict(extra.get("info") or [])

    sex = info_dict.get("性别")
    birth_date = info_dict.get("出生日期")
    death_date = info_dict.get("去世日期")
    birth_place_raw = info_dict.get("出生地")
    birth_region = _extract_region_from_place(birth_place_raw) if birth_place_raw else None
    imdb_id = info_dict.get("IMDb编号") or info_dict.get("IMDB编号")

    # 头像：优先用 cover.normal.url，没有再退到 cover_img.url
    avatar_url = (
            _safe_get(data, "cover", "normal", "url")
            or _safe_get(data, "cover_img", "url")
            or None
    )

    return {
        "person_douban_id": person_id,
        "name_cn": name_cn,
        "avatar_url": avatar_url,
        "sex": sex,
        "birth_date": birth_date,
        "death_date": death_date,
        "birth_place_raw": birth_place_raw,
        "birth_region": birth_region,
        "imdb_id": imdb_id,
    }


def fetch_person_details(person_id: str) -> Optional[Dict[str, Any]]:
    """
    对外的获取人物信息函数：
    - 调用 mobile API 拿到 json
    - 解析为我们自己的结构化 dict（精简字段）
    """
    url = BASE_API_URL.format(person_id=person_id)
    text = fetch_html(url)   # 这里返回的是 json 字符串
    if not text:
        print(f"[person_api] 获取 API 文本失败: {url}")
        return None

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"[person_api] JSON decode 失败: {e}")
        return None

    if not isinstance(data, dict):
        print(f"[person_api] root json 不是 dict: {type(data)!r}")
        return None

    return parse_person_from_api_json(data)


"""示例 main 函数"""

def main():
    # 示例：雷·利奥塔（R.I.P.）
    person_id = "27242075"

    info = fetch_person_details(person_id)
    if info is None:
        print("未能获取人物信息")
        return

    from pprint import pprint
    pprint(info)


if __name__ == "__main__":
    main()
