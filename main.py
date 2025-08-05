rom fastapi import FastAPI
from pydantic import BaseModel
import httpx
import xmltodict
from typing import List
from urllib.parse import urlparse
import asyncio
from datetime import datetime, timedelta

app = FastAPI()

class SitemapURL(BaseModel):
    url: str

# Helper to compute current week's Monday
def get_current_week():
    today = datetime.utcnow()
    monday = today - timedelta(days=today.weekday())
    return monday.strftime('%Y-%m-%d')

@app.post("/process-sitemaps")
async def process_sitemaps(payload: List[SitemapURL]):
    print("Received payload:", payload)

    async with httpx.AsyncClient(timeout=20.0) as client:
        tasks = [client.get(entry.url) for entry in payload]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    post_counts = {}
    current_week = get_current_week()

    for sitemap_url, response in zip(payload, responses):
        print(f"\n🔍 Processing: {sitemap_url.url}")
        if isinstance(response, Exception):
            print("❌ Error fetching:", response)
            continue

        try:
            parsed = xmltodict.parse(response.text)
            print("✅ Parsed XML root keys:", parsed.keys())

            if "urlset" in parsed:
                urls = parsed["urlset"].get("url", [])
                if not isinstance(urls, list):
                    urls = [urls]

                count = 0
                domain = urlparse(sitemap_url.url).netloc

                for entry in urls:
                    loc = entry.get("loc")
                    if loc:
                        count += 1

                if domain not in post_counts:
                    post_counts[domain] = 0
                post_counts[domain] += count

        except Exception as e:
            print("❌ Error parsing XML:", e)
            continue

    # Prepare output format for n8n
    output = []
    for domain, count in post_counts.items():
        output.append({
            "domain": domain,
            "count": count,
            "week": current_week
        })

    print("\n📤 Final Output:", output)
    return output

