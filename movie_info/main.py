import json

import requests
from bs4 import BeautifulSoup

from movie_info.utils import fetch_html

movie_url = "https://movie.douban.com/subject/1299799/"

TITLE_SELECTOR = "#content > h1 > span:nth-child(1)"
YEAR_SELECTOR = "#content > h1 > span.year"
RUNTIME_SELECTOR = "#info > span:nth-child(21)"
POSTER_URL_SELECTOR = "#mainpic > a > img"

IMBD_PATH = "//*[@id=\"info\"]/text()[7]"

def parse_text(html: str, selector: str) -> str | None:
    """从 HTML 文件中解析出 selector 内的文本内容"""
    soup = BeautifulSoup(html, "lxml")

    node = soup.select_one(selector)
    if node is None:
        return None

    return node.get_text(strip=True)

def parse_title(html: str) -> str | None:
    """从 HTML 文本中解析出电影标题"""
    return parse_text(html, TITLE_SELECTOR)

def parse_poster_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")

    img = soup.select_one(POSTER_URL_SELECTOR)
    if img is None:
        return None

    return img.get("src")


def parse_json_ld(html):
    pass


def main():
    html = fetch_html(movie_url)
    title = parse_title(html)

    if title is None:
        print("TITLE NOT FOUND")
    else:
        print("FETCH TITLE: ", title)

    img_url = parse_poster_url(html)

    if img_url is not None:
        print("FETCH IMG URL: ", img_url)

    ld_json = parse_json_ld(html)

    if ld_json is not None:
        print(ld_json)

if __name__ == '__main__':
    main()
