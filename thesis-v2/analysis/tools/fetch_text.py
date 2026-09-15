"""Fetch a URL and print visible text (HTML) or PDF text (via pdfminer if present).

Usage: python fetch_text.py <url> [max_chars] [start_marker]
"""
import html
import re
import sys
import urllib.request


def main():
    url = sys.argv[1]
    max_chars = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
    marker = sys.argv[3] if len(sys.argv) > 3 else None
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    data = urllib.request.urlopen(req, timeout=60).read()
    if data[:4] == b"%PDF":
        try:
            from pdfminer.high_level import extract_text  # type: ignore
            import io
            text = extract_text(io.BytesIO(data))
        except Exception as e:  # noqa: BLE001
            print("PDF; pdfminer unavailable:", e)
            return
    else:
        t = data.decode("utf-8", errors="ignore")
        t = re.sub(r"<script.*?</script>|<style.*?</style>", "", t, flags=re.S)
        t = re.sub(r"<[^>]+>", " ", t)
        text = html.unescape(re.sub(r"\s+", " ", t))
    if marker:
        i = text.find(marker)
        if i >= 0:
            text = text[i:]
    print(text[:max_chars])


if __name__ == "__main__":
    main()
