import requests
from bs4 import BeautifulSoup
import json

url = "https://alhabibshop.com/ar/affiliate-coupon-516fa621-2473-430c-bcfa-5d10e27b6e7b"

session = requests.Session()

#GET page to obtain CSRF token
resp = session.get(url, headers={
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ar",
})

html = resp.text
soup = BeautifulSoup(html, "html.parser")

#try every common CSRF token locations:
token = None

#1. meta tag
meta = soup.find("meta", {"name": "csrf-token"})
if meta:
    token = meta.get("content")

#2. hidden input
if not token:
    inp = soup.find("input", {"name": "_token"})
    if inp:
        token = inp.get("value")

#3. Laravel inline script
if not token:
    for script in soup.find_all("script"):
        if "csrfToken" in str(script):
            # extract token from JS
            token = str(script).split("csrfToken\":\"")[1].split("\"")[0]
            break

print("TOKEN FOUND:", token)

#POST to fetch the real data
payload = {
    "_token": token,
    "filter-opt": "all",
}

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ar",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}

json_data = json.dumps(payload)

xhr = session.post(url, headers=headers, data=json_data)

print("Status:", xhr.status_code)
print(xhr.text)  # this is your real data (HTML or JSON)