"""
movie_info/details.py
    处理一些无法从 ld+json 中取得的字段。

    包括：
        - 制片国家/地区
        - 语言
"""

from bs4 import BeautifulSoup
from typing import List, Dict, Any

from utils import fetch_html

REGION_SELECTOR = "#info > span:nth-child(12)"
LANGUAGE_SELECTOR = "#info > span:nth-child(14)"

def  _extract_list_field_from(info_div, label_keyword: str) -> List[str]:
    """从 #info 区块中提取多值属性"""
    # 1. 找到对应的 <span class="pl">制片国家/地区:</span>
    label_span = info_div.find(
        "span",
        class_="pl",
        string=lambda s: s and label_keyword in s
    )
    if not label_span:
        return []

    # 2. 收集它后面到 <br> 之前的所有文本节点
    pieces: List[str] = []
    for sib in label_span.next_siblings:
        # 遇到 <br> 就说明这一行结束了
        if getattr(sib, "name", None) == "br":
            break

        if isinstance(sib, str):
            pieces.append(sib)
        else:
            # 标签节点（一般不会有，但防一手）
            pieces.append(sib.get_text(" ", strip=True))

    raw = "".join(pieces).strip()
    if not raw:
        return []

    # 3. 去掉外层的引号和奇怪空白
    raw = raw.strip(' "\u3000“”')

    # 4. 统一一下分隔符，然后按 "/" 拆分
    # 有些页面可能用 "、" 或多余空格，这里一起处理掉
    raw = raw.replace("、", "/")
    # 也可以视情况再加一些替换，比如全角空格
    parts = [p.strip(' \u3000"“”') for p in raw.split("/")]

    # 5. 过滤掉空字符串
    return [p for p in parts if p]

def parse_details(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    info_div = soup.select_one("#info")

    if info_div is None:
        # 没有 #info，返回一个结构完整但为空的 dict
        return {
            "regions": [],
            "languages": [],
        }

    regions = _extract_list_field_from(info_div, "制片国家/地区")
    languages = _extract_list_field_from(info_div, "语言")

    return {
        "regions": regions,
        "languages": languages,
    }

def main():
    html = fetch_html("https://movie.douban.com/subject/3262044/")
    details = parse_details(html)

    if details is None:
        print("No detail fetched")

    print(details)

if __name__ == '__main__':
    main()
