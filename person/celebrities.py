import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup
from movie_info.utils import fetch_html

_PERSON_ID_RE = re.compile(r"/personage/(\d+)/")
_CAST_WORD_RE = re.compile(r"\bcast\b", re.I)

def _extract_person_id_from_href(href: str) -> Optional[str]:
    """从 href 标签里提取人物的 Douban ID"""
    if not href:
        return None

    m = _PERSON_ID_RE.search(href)
    return m.group(1) if m else None

def parse_celebrities(html: str, movie_douban_id: str = "") -> Dict[str, List[Dict[str, Any]]]:
    """
    从影片的 /celebrities 页面抓取该电影所有的演职人员信息。


    :param html: 演职表页面的 HTML 文档
    :param movie_douban_id: 所属电影 Douban ID，方便填电影人物关联表，不一定用得上
    :return: 返回 1 个 2 级 JSON 字典，外层分为 cast 和 crew，内层为抓取到的 celebrity 对象基本信息。包括 Douban ID，名称（中文+英文），部门，职位，顺序。
    """
    soup = BeautifulSoup(html, "lxml")

    root = soup.select_one("#celebrities")
    if root is None:
        return {"cast": [], "crew": []}

    cast: List[Dict[str, Any]] = []
    crew: List[Dict[str, Any]] = []

    # 处理各个部门
    for wrapper in root.select("div.list-wrapper"):
        # 部门名称
        h2 = wrapper.find("h2")
        department = h2.get_text(strip=True) if h2 else ""

        ul = wrapper.find("ul", class_="celebrities-list")
        if not ul:
            continue

        # 处理部门内的各个人员
        # 用顺序做一个 order 标号
        # 说不定会用到呢
        for idx, li in enumerate(ul.select("li.celebrity"), start=1):
            # 从 a 标签获取 Person Douban ID
            a = li.find("a", href=True)
            if not a:
                continue

            href = a["href"]
            perosn_id = _extract_person_id_from_href(href)

            # 姓名 name
            name_a = li.select_one("span.name a") or a
            name = name_a.get_text(strip=True)

            # 角色 role
            # 有些部门（例如制片人）下不显示 role，采用部门名
            role_span = li.select_one("span.role")
            role = role_span.get_text(strip=True) if role_span else department

            record = {
                "movie_douban_id": movie_douban_id,
                "person_douban_id": perosn_id,
                "name": name,
                "department": department,
                "role": role,
                "order": idx,
            }

            # 部门是演员的归到 cast
            # 其他一律属于 crew
            is_cast = (
                department.startswith("演员") or
                _CAST_WORD_RE.search(department) is not None
            )

            if is_cast:
                cast.append(record)
            else:
                crew.append(record)

    return {"cast": cast, "crew": crew}

def main():
    movie_cele_url = "https://movie.douban.com/subject/2373195/celebrities"
    html = fetch_html(movie_cele_url)

    celebrities = parse_celebrities(html, movie_douban_id="2373195")

    if celebrities is None:
        print("No celebrities fetched")
        return

    print(celebrities)

if __name__ == '__main__':
    main()