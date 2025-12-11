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
import time
import random

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


# Extract CSRF token from HTML
def extract_csrf_token(html):
    soup = BeautifulSoup(html, "html.parser")

    meta = soup.find("meta", {"name": "csrf-token"})
    if meta:
        return meta["content"]

    inp = soup.find("input", {"name": "_token"})
    if inp:
        return inp.get("value")

    for script in soup.find_all("script"):
        if "csrfToken" in str(script):
            return str(script).split("csrfToken\":\"")[1].split("\"")[0]

    return None


# Request wrapper with retry for 429
def safe_request(method, url, session, **kwargs):
    for attempt in range(5):
        response = session.request(method, url, **kwargs)

        # If rate-limited
        if response.status_code == 429:
            wait = 1.5 + random.random() * 1.5
            print(f"[429] Rate limited. Retrying in {wait:.2f}s...")
            time.sleep(wait)
            continue

        return response

    return None  # Too many failures


# Main function
def fetch_coupon_data(url):
    session = requests.Session()   # NEW session per URL → safest
    
    # Step 1 — Get HTML to extract CSRF token
    resp = safe_request("GET", url, session, headers=HEADERS_GET)
    if not resp or resp.status_code != 200:
        return {"url": url, "error": "GET failed"}

    token = extract_csrf_token(resp.text)
    if not token:
        return {"url": url, "error": "Token missing"}

    payload = {"_token": token, "filter-opt": "all"}

    # Step 2 — POST to retrieve the real content
    resp2 = safe_request(
        "POST",
        url,
        session,
        headers=HEADERS_POST,
        data=json.dumps(payload)
    )

    if not resp2:
        return {"url": url, "error": "POST failed"}
    if resp2.status_code != 200:
        return {"url": url, "error": f"POST returned {resp2.status_code}"}

    return {
        "url": url,
        "content": resp2.text   # Real data (HTML fragment)
    }


# Process a list of URLs safely, sequentially
def process_urls(urls):
    results = []
    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] Processing: {url}")
        result = fetch_coupon_data(url)
        results.append(result)

        # polite delay (prevents rate limits)
        time.sleep(0.7 + random.random() * 0.4)

    return results


# Example usage
urls = """https://alhabibshop.com/ar/affiliate-coupon-5e6472e7-d34c-4815-9865-12b6f29e40ed
https://alhabibshop.com/ar/affiliate-coupon-80516241-f2cd-4874-994f-dcc91a7ecbed
https://alhabibshop.com/ar/affiliate-coupon-5434c105-5ec0-40fe-8e22-50986fbf9d21
https://alhabibshop.com/ar/affiliate-coupon-1d65d3c4-16fa-493d-aff6-791a0b55a9fa
https://alhabibshop.com/ar/affiliate-coupon-ac928244-2173-4029-a121-c0545763e9ff
https://alhabibshop.com/ar/affiliate-coupon-5ba98561-b5b0-468b-b0b5-2719bb29040e
https://alhabibshop.com/ar/affiliate-coupon-f5976552-5592-4bfd-9a44-4738111ecede
https://alhabibshop.com/ar/affiliate-coupon-fcfa4ca6-17e0-4955-b565-e8e393b29c56
https://alhabibshop.com/ar/affiliate-coupon-516fa621-2473-430c-bcfa-5d10e27b6e7b
https://alhabibshop.com/ar/affiliate-coupon-4abea0b8-fd7f-4945-bca9-f746ec3f498a
https://alhabibshop.com/ar/affiliate-coupon-cdf76216-cb49-49a2-82bd-ae46e4417677
https://alhabibshop.com/ar/affiliate-coupon-68494034-108f-4096-a4a0-333ac1f14eee
https://alhabibshop.com/ar/affiliate-coupon-40ff7b17-7d02-4718-9d35-e0bd64f3f956
https://alhabibshop.com/ar/affiliate-coupon-87989acb-49ae-42d6-9682-1cf5482325db
https://alhabibshop.com/ar/affiliate-coupon-88079dc0-430d-421a-96a3-556ce8da67fd
https://alhabibshop.com/ar/affiliate-coupon-4e374fcd-7715-481f-ac6f-c0bd88f28ea1
https://alhabibshop.com/ar/affiliate-coupon-e9665581-e11f-48f8-8ddd-432ebb4052c2
https://alhabibshop.com/ar/affiliate-coupon-74509cda-0464-43bf-94c4-44adf118c593
https://alhabibshop.com/ar/affiliate-coupon-8061de69-464f-446b-a3f7-84213c57710a
https://alhabibshop.com/ar/affiliate-coupon-59754428-0255-4110-94ff-d516de52c3d9
https://alhabibshop.com/ar/affiliate-coupon-9fc434ca-6bdf-43ba-8966-45fa1f38a486
https://alhabibshop.com/ar/affiliate-coupon-8d1d934e-69fa-4bc1-a85d-8515b5c13441
https://alhabibshop.com/ar/affiliate-coupon-259446db-50b5-457b-894b-72a84cddcd4f
https://alhabibshop.com/ar/affiliate-coupon-4899071e-9911-4667-96c3-466f62b56b59"""

urls = urls.split('\n')

data = process_urls(urls)

for item in data:
    print(item["url"], "->", "OK" if "content" in item else item["error"])
