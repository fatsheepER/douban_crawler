"""
person/details.py

    解析豆瓣人物信息页 /personage/{person_id} 的内容，抽取人物基础信息。

    包括：
    - 姓名（中文+英文）
    - 性别（男/女）
    - 出生日期
    - 去世日期（如有）
    - 出生地（原始文本+国家/地区部分）
    - IMDb 编号
    - 头像 URL

    由于 /personage 有反爬机制，脚本不能正常获取 HTML 文档。
"""
from __future__ import annotations

from typing import Any, Dict, Optional
from bs4 import BeautifulSoup

from utils import fetch_html


"""小工具"""

def _normalize_label(text: str) -> str:
    """
    清洗 li.label 里的文本，去掉空白和中英文冒号。

    例如：
    - "性别: " -> "性别"
    - "出生日期：" -> "出生日期"
    """
    if text is None:
        return ""
    t = text.strip()
    # 去掉尾部的冒号（中文/英文）
    while t.endswith((":", "：")):
        t = t[:-1].rstrip()
    return t

def _extract_region_from_place(place: str) -> Optional[str]:
    """
    从出生地文本中尝试提取国家/地区部分。如果格式不符合预期，则返回 None。

    例如：
    - "美国,新泽西州,纽瓦克" -> "美国"
    - "古巴,哈瓦那"          -> "古巴"
    """
    if not place:
        return None

    # 统一全角 / 半角逗号
    normalized = place.replace("，", ",")
    parts = [p.strip() for p in normalized.split(",") if p.strip()]
    if not parts:
        return None
    return parts[0]


"""外部解析函数"""

def parse_person_details(
        html: str,
        person_douban_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    解析人物信息页 HTML 文档，获取人物详情信息。

    :param html: 人物信息页 HTML 文档
    :param person_douban_id: 人物 Douban ID，加入字典中方便写表
    :return: 人物信息字典
    """
    soup = BeautifulSoup(html, "lxml")

    # 找到人物信息的主块：section.subject-target 第一个 div
    section = soup.select_one("#content > div > div.article > section.subject-target")
    if section is None:
        print("未找到 section.subject-target")
        return {
            "person_douban_id": person_douban_id,
            "name": None,
            "sex": None,
            "birth_date": None,
            "death_date": None,
            "birth_place_raw": None,
            "birth_country": None,
            "imdb_id": None,
            "avatar_url": None,
        }

    # 官方结构一般是 section 下第一个 div 是人物信息块
    block = section.find("div")
    if block is None:
        print("未找到人物信息 block")
        block = section

    # h1: 姓名 name
    h1 = block.find("h1")
    name = h1.get_text(strip=True) if h1 else None

    # 头像 avatar url，没有就算
    avatar_url: Optional[str] = None
    left_div = block.find("div", class_="left")
    if left_div is not None:
        avatar_img = left_div.select_one("div.avatar-container img")
        if avatar_img and avatar_img.has_attr("src"):
            avatar_url = avatar_img["src"].strip() or None

    # right > ul > li 列表
    right_div = block.find("div", class_="right")
    info_ul = right_div.find("ul") if right_div else None

    sex: Optional[str] = None
    birth_date: Optional[str] = None
    death_date: Optional[str] = None
    birth_place_raw: Optional[str] = None
    birth_region: Optional[str] = None
    imdb_id: Optional[str] = None

    if info_ul is not None:
        for li in info_ul.find_all("li", recursive=False):
            label_span = li.find("span", class_="label")
            value_span = li.find("span", class_="value")
            if not label_span or not value_span:
                continue

            label = _normalize_label(label_span.get_text())
            value = value_span.get_text(strip=True)

            if not label:
                continue

            if label.startswith("性别"):
                sex = value or None
            elif label.startswith("出生日期"):
                birth_date = value or None
            elif label.startswith("去世日期"):
                death_date = value or None
            elif label.startswith("出生地"):
                birth_place_raw = value or None
                if birth_place_raw:
                    birth_region = _extract_region_from_place(birth_place_raw)
            elif "imdb" in label.lower():
                imdb_id = value or None

    return {
        "person_douban_id": person_douban_id,
        "name": name,
        "sex": sex,
        "birth_date": birth_date,
        "death_date": death_date,
        "birth_place_raw": birth_place_raw,
        "birth_region": birth_region,
        "imdb_id": imdb_id,
        "avatar_url": avatar_url,
    }


"""示例函数"""
def main():
    person_id = "27260193"
    url = f"https://www.douban.com/personage/{person_id}/"

    html = fetch_html(url)
    if not html:
        print("获取 HTML 文档失败")
        return

    from pprint import pprint
    pprint(html)

    data = parse_person_details(html, person_id)

    pprint(data)


if __name__ == '__main__':
    main()