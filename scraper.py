import asyncio
import json
import math
import logging
import random
import re
import os
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from curl_cffi.requests import AsyncSession

load_dotenv()

URL = "https://www.ibood.com/fr/s-fr/all-offers"
API_URL = "https://api.ibood.io/search/items/live"
BASE_PRODUCT_URL = "https://www.ibood.com/fr/s-fr/o/{slug}/{product_id}"

TAKE = 23
OUTPUT_FILE = "products.json"
COOKIES_FILE = "cookies.json"

PROXY = os.getenv("PROXY", None)
CONCURRENCY = 100

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def cookies_to_string(cookies):
    return "; ".join([f"{c['name']}={c['value']}" for c in cookies])


async def get_cookies():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--lang=fr-FR", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(URL, wait_until="load")

        await asyncio.sleep(10)

        cookies = await context.cookies()
        cookie_string = cookies_to_string(cookies)

        with open(COOKIES_FILE, "w", encoding="utf-8") as f:
            json.dump({"cookies": cookies, "cookie_string": cookie_string}, f, indent=4)

        await browser.close()
        return cookie_string


async def fetch_api_page(session, skip, semaphore):
    params = {"s": "relevance", "skip": skip, "take": TAKE, "meta": "price"}
    async with semaphore:
        try:
            await asyncio.sleep(random.uniform(0.2, 0.7))
            r = await session.get(API_URL, params=params)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"API error skip={skip}: {e}")
            return e


async def fetch_html(session, url, semaphore):
    async with semaphore:
        try:
            await asyncio.sleep(random.uniform(0.3, 0.9))
            r = await session.get(url)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error(f"HTML error {url}: {e}")
            return None


def extract_gtin(html):
    if not html:
        return ""
    matches = re.findall(r"\b\d{13}\b", html)
    return matches[0] if matches else ""


async def main():
    cookie_string = await get_cookies()

    api_headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Ibex-Language": "fr",
        "Ibex-Shop-Id": "e7613c64-855e-5724-bd3c-2219de9881f2",
        "Ibex-Tenant-Id": "eafb3ef2-e1ba-4f01-b67a-b0447bea74eb",
        "Origin": "https://www.ibood.com",
        "Referer": "https://www.ibood.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
    }

    html_headers = {
        "Accept": "text/html,*/*",
        "Referer": "https://www.ibood.com/fr/s-fr/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Cookie": cookie_string
    }

    semaphore = asyncio.Semaphore(CONCURRENCY)

    extra_params = {
        "http_version": "v2",
        "timeout": 15,
        "allow_redirects": True
    }

    async with AsyncSession(headers=api_headers, proxy=PROXY, impersonate="chrome142", **extra_params) as api_session, \
               AsyncSession(headers=html_headers, proxy=PROXY, impersonate="chrome142", **extra_params) as html_session:

        logger.info("Fetching first API page")
        first = await fetch_api_page(api_session, 0, semaphore)

        data = first["data"]
        total_items = data["totalItems"]
        total_pages = math.ceil(total_items / TAKE)

        logger.info(f"Items: {total_items} Pages: {total_pages}")

        products = []
        seen = set()

        def process_items(items):
            for item in items:
                slug = item.get("slug")
                pid = item.get("classicProductId")
                name = item.get("title")

                price_raw = item.get("referencePrice", {}).get("price")
                try:
                    price = float(price_raw) if price_raw else None
                except Exception as e:
                    logger.error(e)
                    price = None

                if slug and pid:
                    link = BASE_PRODUCT_URL.format(slug=slug, product_id=pid)

                    if link not in seen:
                        seen.add(link)
                        products.append({
                            "product_link": link,
                            "product_name": name,
                            "supplier_price": price,
                            "product_gtin": ""
                        })

        process_items(data["items"])

        tasks = [
            fetch_api_page(api_session, p * TAKE, semaphore)
            for p in range(1, total_pages)
        ]

        results = await asyncio.gather(*tasks)

        for res in results:
            if isinstance(res, Exception):
                continue
            process_items(res["data"]["items"])

        logger.info(f"Collected products: {len(products)}")

        html_tasks = [
            fetch_html(html_session, p["product_link"], semaphore)
            for p in products
        ]

        html_results = await asyncio.gather(*html_tasks)

        for i, html in enumerate(html_results):
            products[i]["product_gtin"] = extract_gtin(html)

        logger.info("GTIN extraction complete")

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(products, f, indent=4, ensure_ascii=False)

        logger.info(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
