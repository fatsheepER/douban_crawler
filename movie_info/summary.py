from typing import Optional
from bs4 import BeautifulSoup
from utils import fetch_html

SUMMARY_SELECTOR = "#link-report-intra > span:nth-child(1)"

def parse_summary(html: str) -> Optional[str]:
    """从 HTML 中解析完整的剧情简介"""
    soup = BeautifulSoup(html, "lxml")

    summary_span = soup.select_one(SUMMARY_SELECTOR)
    if summary_span is None:
        return None

    parts = [s.strip() for s in summary_span.stripped_strings if s.strip()]
    if not parts:
        return None

    summary = "\n".join(parts)
    return summary.strip(' "\u3000“”')  # 最后去掉外部引号

def main():
    html = fetch_html("https://movie.douban.com/subject/1292268/")

    summary = parse_summary(html)
    if summary is None:
        print("No summary fetched.")
        return

    print(summary)

if __name__ == '__main__':
    main()