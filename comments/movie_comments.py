"""
movie_comments.py

    解析一部电影短评/想看页面，抽取出短评条目和观影记录条目。

    - /subject/{movie_id}/comments?status=P  : 短评（看过）
    - /subject/{movie_id}/comments?status=F  : 想看

    默认一页 20 条记录，通过 start = 0, 20, 40, ... 翻页。
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from utils import fetch_html


"""正则和小工具"""

# allstar40 rating -> 4 星
_RATING_CLASS_RE = re.compile(r"allstar(\d+)")

def _hash_username(username: str) -> str:
    """
    将用户名做一次简单哈希，作为匿名 user_id。
    这里用 sha256，截前 16 位即可，既稳定又匿名。
    """
    if not username:
        return ""
    h = hashlib.sha256(username.encode("utf-8")).hexdigest()
    return h[:16]

def _status_flag_to_logical_status(status_flag: str) -> str:
    """
    URL 中的 status 参数 -> Watching_Record.status 字段语义。

    - P : watched
    - F : wishlist
    未来如果有在看，可以扩展。
    """
    status_flag = (status_flag or "").upper()
    if status_flag == "P":
        return "watched"
    if status_flag == "F":
        return "wishlist"
    # 兜底：不知道是啥就先照抄
    return status_flag.lower() or "unknown"


"""内部解析子函数"""

def _parse_rating_from_span(span) -> Optional[int]:
    """
    从 span.rating 的 class 中解析出整数评分（10 分制）。
    Douban: allstar10/20/30/40/50 -> 1~5 星
    我们： 1~5 星 * 2 -> 2,4,6,8,10（与数据字典的 0~10 int 对齐）
    """
    if span is None:
        return None

    classes = span.get("class", [])
    class_str = " ".join(classes)
    m = _RATING_CLASS_RE.search(class_str)
    if not m:
        return None

    try:
        star_raw = int(m.group(1))  # 10, 20, ..., 50
    except ValueError:
        return None

    if star_raw <= 0:
        return None

    star_5 = star_raw // 10  # 10->1星，50->5星
    if star_5 <= 0:
        return None

    # 映射到 10 分制整数
    return star_5 * 2

def _pick_status_from_info_span(info_span) -> Optional[str]:
    """
    从 <span class="comment-info"> 中提取观影状态文本（“看过” / “想看”）。

    结构类似：
    <span class="comment-info">
        <a>用户名</a>
        <span>看过</span>
        <span class="allstar40 rating"></span>
        <span class="comment-time">...</span>
        ...
    </span>
    """
    if info_span is None:
        return None

    spans = info_span.find_all("span", recursive=False)
    if not spans:
        return None

    text = spans[0].get_text(strip=True)
    return text or None

def _parse_single_comment_item(
        item,
        movie_douban_id: str,
        status_flag: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    解析单个 <div class="comment-item">，返回：

    (rating_record, watch_record)

    - rating_record 对应 Movie_Rating 表的一行（或 None）
    - watch_record  对应 Watching_Record 表的一行（非 None 时）
    """
    comment_div = item.find("div", class_="comment")
    if comment_div is None:
        print("获取 comment-div 失败")
        return None, None

    # info-span：用户名、状态、评分、时间
    info_span = comment_div.find("span", class_="comment-info")
    if info_span is None:
        print("获取 info-span 失败")
        return None, None

    # 用户名
    user_a = info_span.find("a")
    username = user_a.get_text(strip=True) if user_a else ""
    if not username:
        print("获取用户名失败")
        return None, None

    # 保护隐私，存用户名哈希值
    user_hash = _hash_username(username)

    # 观影状态 for sanity check
    status_text = _pick_status_from_info_span(info_span)
    logical_status = _status_flag_to_logical_status(status_flag)

    # 评分
    rating_span = info_span.find("span", class_=re.compile(r"allstar\d+"))
    rating_value = _parse_rating_from_span(rating_span)

    # 短评时间
    # 没有解析，形如“2009-03-31 10:11:04”
    time_span = info_span.find("span", class_="comment-time")
    created_at = time_span.get_text(strip=True) if time_span else None

    # 短评文本（P 页和 F 页都有）
    p_tag = comment_div.find("p")
    review_text= ""
    if p_tag is not None:
        short_span = p_tag.find("span", class_="short")
        if short_span:
            review_text = short_span.get_text(strip=True)
        else:
            review_text = p_tag.get_text(strip=True)    # 我没看到过这种
    else:
        print("获取短评 p-tag 失败")

    watch_record: Dict[str, Any] = {
        "movie_douban_id": movie_douban_id,
        "user_hash": user_hash,
        "username_raw": username,   # for what
        "status": logical_status,
        "star": False,              # default value
        "created_at": created_at,
        "status_raw": status_text,  # sanity check
    }

    rating_record: Optional[Dict[str, Any]] = None

    # 只有“看过”且有评分时才记作有效的评分记录
    if logical_status == "watched" and rating_value is not None:
        rating_record = {
            "movie_douban_id": movie_douban_id,
            "user_hash": user_hash,
            "username_raw": username,
            "rating": rating_value,
            "created_at": created_at,
            "review": review_text or None,
        }

    return rating_record, watch_record


"""外部解析函数"""

def parse_comments_page(
        html: str,
        movie_douban_id: str,
        status_flag: str = "P"
) -> Dict[str, List[Dict[str, Any]]]:
    """
    解析一页短评/想看页面的 HTML 文档，返回该页全部（20 条）记录

    :param html: 页面 HTML 文档
    :param movie_douban_id: 所属的电影 Douban ID
    :param status_flag: P-短评页，F-想看页
    :return: {"ratings": [{...}, ...], "watch_records": [{...}, ...]}
    """
    soup = BeautifulSoup(html, "lxml")

    root = soup.select_one("#comments")
    if root is None:
        print("获取文档 root 失败")
        return {"ratings": [], "watch_records": []}

    ratings: List[Dict[str, Any]] = []
    watch_records: List[Dict[str, Any]] = []

    for item in root.find_all("div", class_="comment-item"):
        rating_rec, watch_rec = _parse_single_comment_item(item,
                                                           movie_douban_id,
                                                           status_flag)
        if watch_rec is not None:
            watch_records.append(watch_rec)
        if rating_rec is not None:
            ratings.append(rating_rec)

    return {
        "ratings": ratings,
        "watch_records": watch_records,
    }

def fetch_movie_comments(
        movie_douban_id: str,
        num_pages: int = 3,
        status_flag: str = "P"
) -> Dict[str, List[Dict[str, Any]]]:
    """
    连续抓取电影的多页全部影评，并汇总
    
    :param movie_douban_id: 
    :param num_pages: 
    :param status_flag: 
    :return: 
    """
    all_ratings: List[Dict[str, Any]] = []
    all_watch_records: List[Dict[str, Any]] = []

    for page_idx in range(num_pages):
        start = page_idx * 20
        if status_flag.upper() == "P":
            url = (
                f"https://movie.douban.com/subject/{movie_douban_id}/comments"
                f"?start={start}&limit=20&status=P&sort=new_score"
            )
        else:
            url = (
                f"https://movie.douban.com/subject/{movie_douban_id}/comments"
                f"?start={start}&limit=20&status=F"
            )

        html = fetch_html(url)
        page_data = parse_comments_page(html, movie_douban_id, status_flag)

        all_ratings.extend(page_data["ratings"])
        all_watch_records.extend(page_data["watch_records"])

    return {
        "ratings": all_ratings,
        "watch_records": all_watch_records,
    }


"""示例 main 函数"""

def main():
    movie_id = "1292268"

    data_p = fetch_movie_comments(movie_id, num_pages=3, status_flag="P")
    print(f"[P] ratings count = {len(data_p['ratings'])}")
    print(f"[P] watch_records count = {len(data_p['watch_records'])}")

    data_f = fetch_movie_comments(movie_id, num_pages=1, status_flag="F")
    print(f"[F] ratings count = {len(data_f['ratings'])}")
    print(f"[F] watch_records count = {len(data_f['watch_records'])}")

    from pprint import pprint
    pprint(data_p)
    # pprint(data_f)


if __name__ == '__main__':
    main()