# import requests
# from bs4 import BeautifulSoup
# import json

# url = "https://alhabibshop.com/ar/affiliate-coupon-516fa621-2473-430c-bcfa-5d10e27b6e7b"

# session = requests.Session()

# #GET page to obtain CSRF token
# resp = session.get(url, headers={
#     "User-Agent": "Mozilla/5.0",
#     "Accept-Language": "ar",
# })

# html = resp.text
# soup = BeautifulSoup(html, "html.parser")

# #try every common CSRF token locations:
# token = None

# #1. meta tag
# meta = soup.find("meta", {"name": "csrf-token"})
# if meta:
#     token = meta.get("content")

# #2. hidden input
# if not token:
#     inp = soup.find("input", {"name": "_token"})
#     if inp:
#         token = inp.get("value")

# #3. Laravel inline script
# if not token:
#     for script in soup.find_all("script"):
#         if "csrfToken" in str(script):
#             # extract token from JS
#             token = str(script).split("csrfToken\":\"")[1].split("\"")[0]
#             break

# print("TOKEN FOUND:", token)

# resp.close()

# #POST to fetch the real data
# payload = {
#     "_token": token,
#     "filter-opt": "all",
# }

# headers = {
#     "User-Agent": "Mozilla/5.0",
#     "Accept-Language": "ar",
#     "Content-Type": "application/json",
#     "X-Requested-With": "XMLHttpRequest",
#     "Accept": "application/json, text/javascript, */*; q=0.01"
# }

# json_data = json.dumps(payload)

# xhr = session.post(url, headers=headers, data=json_data)

# print("Status:", xhr.status_code)
# print(xhr.text)  # this is your real data (HTML or JSON)

import requests
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor

session = requests.Session()

HEADERS_GET = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ar",
}

HEADERS_POST = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ar",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}


def extract_csrf_token(html):
    """Extracts CSRF token from HTML (3 strategies)."""
    soup = BeautifulSoup(html, "html.parser")

    meta = soup.find("meta", {"name": "csrf-token"})
    if meta:
        return meta["content"]

    inp = soup.find("input", {"name": "_token"})
    if inp:
        return inp["value"]

    for script in soup.find_all("script"):
        if "csrfToken" in str(script):
            return str(script).split("csrfToken\":\"")[1].split("\"")[0]

    return None


def fetch_coupon_data(url):
    """Fetch coupon page -> POST for real data -> return parsed content."""
    
    # Step 1: GET page
    resp = session.get(url, headers=HEADERS_GET)
    token = extract_csrf_token(resp.text)
    if not token:
        return {"url": url, "error": "Missing CSRF token"}

    # Step 2: POST to same URL for JSON / HTML response
    payload = {"_token": token, "filter-opt": "all"}
    xhr = session.post(url, headers=HEADERS_POST, data=json.dumps(payload))

    if xhr.status_code != 200:
        return {"url": url, "error": f"POST failed ({xhr.status_code})"}

    return {"url": url, "content": xhr.text}


def process_all(urls, workers=5):
    """Fetch many coupon pages efficiently."""
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for result in executor.map(fetch_coupon_data, urls):
            results.append(result)
    return results


# Example usage
urls = [
    "https://alhabibshop.com/ar/affiliate-coupon-516fa621-2473-430c-bcfa-5d10e27b6e7b"
]

results = process_all(urls, workers=10)

for r in results:
    print(r["url"])
    print("Error:", r.get("error"))
    print("Content length:", len(r.get("content", "")))
    print("=" * 40)
    print(r['content'])
