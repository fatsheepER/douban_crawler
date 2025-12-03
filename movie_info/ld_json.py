from __future__ import annotations
import json
from bs4 import BeautifulSoup
import re
from typing import Any, Dict, List, Optional
from utils import fetch_html

_LENIENT_DECODER = json.JSONDecoder(strict=False)

movie_url = "https://movie.douban.com/subject/3262044/"

def _parse_ld_json(html: str) -> Optional[Dict[str, Any]]:
    """从 HTML 文本中解析出 ld+json 部分（容忍控制字符）"""
    soup = BeautifulSoup(html, "lxml")

    script = soup.find("script", type="application/ld+json")
    if script is None:
        print("[ld_json] no <script type='application/ld+json'> found")
        return None

    raw = script.string or script.get_text()
    if not raw:
        print("[ld_json] empty ld+json script")
        return None

    raw = raw.strip()

    try:
        data = _LENIENT_DECODER.decode(raw)
        # 如果有多种 @type，这里可以再判断一次：
        # if isinstance(data, dict) and data.get("@type") == "Movie":
        #     return data
        return data
    except json.JSONDecodeError as e:
        print("[ld_json] lenient json decode failed:", e)
        # 调试时可以把上下文打出来
        start = max(e.pos - 60, 0)
        end = min(e.pos + 60, len(raw))
        print("[ld_json] error context:", raw[start:end])
        return None

def _extract_subject_id(url: str) -> Optional[str]:
    """
    从 ldjson 的 url 字段提取出豆瓣 subject id

    :param url: ld+json url
    :return: subject_id
    """
    if not url:
        return None

    # 去掉收尾空格和斜杠
    cleaned = url.strip().strip("/")
    parts = cleaned.split("/")

    # 找到紧跟在 subject 后面的一段
    try:
        idx = parts.index("subject")
        subject_part = parts[idx+1]
    except (ValueError, IndexError):
        # 不符合预期格式，退而取最后一段的所有数字
        digits = "".join(ch for ch in parts[-1] if ch.isdigit())
        return digits or None

    # 确保是纯数字
    if subject_part.isdigit():
        return subject_part

    # 否则退退退
    digits = "".join(ch for ch in parts[-1] if ch.isdigit())
    return digits or None

"""
提取 runtime 的 regex
看不懂
"""
_DURATION_RE = re.compile(
    r'^P'                                 # 开头 P
    r'(?:(?P<days>\d+)D)?'                # 可选天数
    r'(?:T'                               # 时间部分以 T 开头
    r'(?:(?P<hours>\d+)H)?'               # 可选小时
    r'(?:(?P<minutes>\d+)M)?'             # 可选分钟
    r'(?:(?P<seconds>\d+)S)?'             # 可选秒
    r')?$'
)

def _parse_duration_to_minutes(duration: str) -> Optional[int]:
    """
    将 ld+json 中 ISO8601 时长（形如 'PT2H58M'）解析为总分钟数

    :param duration: 时长字段
    :return: 总分钟数
    """
    if not duration:
        return None

    duration = duration.strip().upper()
    m = _DURATION_RE.match(duration)
    if not m:
        return None

    days = int(m.group("days") or 0)
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)

    total_minutes = days * 24 * 60 + hours * 60 + minutes
    # 对于秒 简单的四舍五入即可
    if seconds:
        total_minutes += round(seconds / 60)

    return total_minutes or None

def parse_movie_basic_from_ld_json(html: str) -> Dict[str, Any]:
    """
    首先从 HTML 中提取 ld+json，然后
    从 ldjson(dict) 中抽取电影的基础信息。
    返回一个结构化 dict。

    :param html: HTML 文档
    :return: 包含电影基础信息的 dict
    """
    ld = _parse_ld_json(html)

    if not isinstance(ld, dict):
        return None

    # 1. 电影 Douban ID
    raw_url = ld.get("url") or ""
    douban_id = _extract_subject_id(str(raw_url))

    # 2. 基本字段：名称、封面 URL、上映日期
    name = ld.get("name") or None
    image_url = ld.get("image") or None
    release_date = ld.get("datePublished") or None

    # 3. 类型列表
    genres: List[str] = []
    raw_genres = ld.get("genre")
    if isinstance(raw_genres, list):
        genres = [str(g).strip() for g in raw_genres if str(g).strip()]
    elif isinstance(raw_genres, str):
        # 如果遇到了 “剧情 / 犯罪” 这种形式
        parts = raw_genres.replace("、", "/").split("/")
        genres = [p.strip() for p in parts if p.strip()]
    else:
        genres = []

    # 4. 片长：ISO8601 -> Int 分钟
    raw_duration = ld.get("duration") or None
    runtime_minutes = _parse_duration_to_minutes(raw_duration)

    return {
        "douban_id": douban_id,
        "name": name,
        "image_url": image_url,
        "release_date": release_date,
        "genres": genres,
        "runtime_minutes": runtime_minutes
    }

def main():
    html = fetch_html(movie_url)
    movie_base = parse_movie_basic_from_ld_json(html)

    if movie_base is not None:
        print(movie_base)
    else:
        print("No valid ld json for this movie.")

if __name__ == '__main__':
    main()