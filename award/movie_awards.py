"""
movie_awards.py

    解析某部电影的获奖情况页面，得到该电影每条获奖记录
    的相关信息，建立获奖记录与电影节、人物的关联，以及
    是否获奖或只是提名。
"""
import re
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from utils import fetch_html

"""正则和小工具"""

_PERSON_ID_RE = re.compile(r"/personage/(\d+)/")
_YEAR_RE = re.compile(r"\d{4}")
_PAREN_CONTENT_RE = re.compile(r"[（(].*?[)）]")
_NOMINATION_WORD_RE = re.compile(r"提名")

def _extract_person_id_from_href(href: str) -> Optional[str]:
    """从 href 标签里提取人物 Douban ID"""
    if not href:
        return None

    m = _PERSON_ID_RE.search(href)
    return m.group(1) if m else None

def _extract_year(text: str) -> Optional[int]:
    """从类似 ' (1991)' 的文本中提取年份"""
    if not text:
        return None
    m = _YEAR_RE.search(text)
    return int(m.group()) if m else None

def _clean_award_name(raw_title: str) -> str:
    """去掉 '(提名)' 等括号内容，保留干净奖项名称"""
    if not raw_title:
        return ""
    return _PAREN_CONTENT_RE.sub("", raw_title).strip()

def _is_nomination(raw_title: str) -> bool:
    if not raw_title:
        return False
    return _NOMINATION_WORD_RE.search(raw_title) is not None


"""内部解析子函数"""

def _parse_festival_header(block) -> Optional[Dict[str, Any]]:
    """
    从一个 div.awards 块中解析电影节信息：
    名称、URL、年份。
    """
    hd = block.find("div", class_="hd")
    if not hd:
        return None

    h2 = hd.find("h2")
    if not h2:
        return None

    # 名称及链接
    fest_link = h2.find("a")
    festival_url = fest_link.get("href").strip() if fest_link and fest_link.has_attr("href") else None
    festival_name = fest_link.get_text(strip=True) if fest_link else h2.get_text(strip=True)

    # 年份
    year_span = h2.find("span", class_="year")
    festival_year = _extract_year(year_span.get_text()) if year_span else None

    return {
        "festival_name": festival_name,
        "festival_url": festival_url,
        "festival_year": festival_year,
    }

def _parse_award_ul(
        ul,
        festival_info: Dict[str, Any],
        movie_douban_id: str,
) -> Optional[Dict[str, Any]]:
    """
    解析单个 <ul class="award">，返回一条获奖/提名记录。
    没有有效数据时返回 None。
    """
    lis = ul.find_all("li", recursive=False)
    if not lis:
        return None

    # 第 1 个 li：获奖记录名称，例如“最佳影片（提名）”
    raw_title = lis[0].get_text(strip=True)
    if not raw_title:
        return None

    is_nomination = _is_nomination(raw_title)
    is_winner = not is_nomination
    award_name = _clean_award_name(raw_title)

    award_type: Optional[str] = None    # "Person" / "Movie"
    person_id: Optional[str] = None
    person_name: Optional[str] = None
    extra_desc: Optional[str] = None

    # 第 2 个 li：获奖人物/团队/电影本身
    if len(lis) >= 2:
        second_li = lis[1]
        links = second_li.find_all("a")

        # 有 a 标签说明是人物获奖
        # 对于多人获奖情况
        # 只取第一个 a 标签
        if links:
            first = links[0]
            href = first.get("href", "")
            person_id = _extract_person_id_from_href(href)
            person_name = first.get_text(strip=True)
            award_type = "Person"
        # 否则奖项属于电影或者有额外描述
        else:
            txt = second_li.get_text(strip=True)
            extra_desc = txt if txt else None
            award_type = "Movie"
    # 甚至没有第二个 li？
    # 我没有看到这样的情况，但这肯定是电影得奖了
    else:
        award_type = "Movie"

    record: Dict[str, Any] = {
        "movie_douban_id": movie_douban_id,

        # 电影节信息
        "festival_name":    festival_info.get("festival_name"),
        "festival_url":     festival_info.get("festival_url"),
        "festival_year":    festival_info.get("festival_year"),

        # 奖项信息
        "award_name":   award_name,
        "result_raw":   raw_title,
        "is_winner":    is_winner,

        # 颁发对象信息
        "award_type":       award_type,
        "person_douban_id": person_id,
        "person_name":      person_name,
        "extra_desc":       extra_desc,
    }
    return record


"""主解析函数"""

def parse_awards(
        html: str, movie_douban_id: str = ""
) -> Dict[str, List[Dict[str, Any]]]:
    """
    从 /awards 页面抓取该电影的所有获奖记录信息

    :param html: 奖项页面 HTML 文档
    :param movie_douban_id: 电影 Douban ID
    :return: {'awards': [record, ...]}，每个 record 为一条获奖记录
    """
    soup = BeautifulSoup(html, 'lxml')

    article = soup.select_one("#content > div > div.article")
    if article is None:
        return {"awards": []}

    awards: List[Dict[str, Any]] = []

    # 每个 div.awards 对应一个电影节
    for block in article.select("div.awards"):
        festival_info = _parse_festival_header(block)
        if not festival_info:
            print("抓取一条电影节的基础信息时失败")
            continue

        # 每个 ul.award 是该电影节下的一条获奖记录
        for ul in block.find_all("ul", class_="award", recursive=False):
            record = _parse_award_ul(ul, festival_info, movie_douban_id)
            if record is None:
                print("抓取一条获奖记录信息时失败")
                continue
            awards.append(record)

    return {"awards": awards}


"""示例 main 函数"""

def main():
    movie_id = "1292268"
    url = "https://movie.douban.com/subject/1292268/awards/"

    html = fetch_html(url)
    data = parse_awards(html, movie_id)

    if not data["awards"]:
        print("No award fetched")
        return

    from pprint import pprint
    pprint(data)

if __name__ == '__main__':
    main()
