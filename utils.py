import requests

def fetch_html(url: str) -> str:
    """发起 HTTP 请求， 返回 HTML 文本"""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    }

    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status() # raise exception when receive error code

    # let's make it clear!
    resp.encoding = "utf-8"
    return resp.text