# src/hamcontestlog/fetch/http.py
from io import BytesIO

import requests


def stream_bytes(url: str) -> BytesIO:
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    buf = BytesIO()
    for chunk in resp.iter_content(chunk_size=8192):
        if chunk:
            buf.write(chunk)
    buf.seek(0)
    return buf

