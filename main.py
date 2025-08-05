from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import xmltodict
from typing import List
from urllib.parse import urlparse
import asyncio

app = FastAPI()

class SitemapURL(BaseModel):
    url: str

@app.post("/process-sitemaps")
async def process_sitemaps(payload: List[SitemapURL]):
    print("Received payload:", payload)

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SitemapFetcher/1.0; +https://yourdomain.com)"
    }

    sem = asyncio.Semaphore(5)  # Limit concurrency

    async def fetch(url):
        async with sem:
            try:
                async with httpx.AsyncClient(timeout=20.0, headers=headers) as client:
                    print(f"\n🔍 Processing: {url}")
                    response = await client.get(url)
                    response.raise_for_status()
                    return url, response
            except Exception as e:
                print(f"❌ Error fetching {url}:", e)
                return url, None

    tasks = [fetch(entry.url) for entry in payload]
    results = await asyncio.gather(*tasks)

    output = []

    for url, response in results:
        if response is None:
            continue
        try:
            parsed = xmltodict.parse(response.text)

            if "urlset" in parsed:
                urls = parsed["urlset"].get("url", [])
                if not isinstance(urls, list):
                    urls = [urls]
                for entry in urls:
                    loc = entry.get("loc")
                    lastmod = entry.get("lastmod")
                    domain = urlparse(loc).netloc if loc else None
                    output.append({
                        "domain": domain,
                        "url": loc,
                        "lastmod": lastmod
                    })

            elif "sitemapindex" in parsed:
                sitemaps = parsed["sitemapindex"].get("sitemap", [])
                if not isinstance(sitemaps, list):
                    sitemaps = [sitemaps]
                for sitemap in sitemaps:
                    loc = sitemap.get("loc")
                    lastmod = sitemap.get("lastmod")
                    domain = urlparse(loc).netloc if loc else None
                    output.append({
                        "domain": domain,
                        "blogSitemapUrl": loc,
                        "lastmod": lastmod
                    })

            else:
                print(f"⚠️ Unknown XML structure for {url}")

        except Exception as e:
            print(f"❌ Error parsing XML from {url}:", e)
            continue

    print("\n📤 Final Output:", output)
    return output


