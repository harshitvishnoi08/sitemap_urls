from fastapi import FastAPI
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
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    return monday.strftime('%Y-%m-%d')

from collections import defaultdict

@app.post("/process-sitemaps")
async def process_sitemaps(payload: List[SitemapURL]):
    print("Received payload:", payload)

    async with httpx.AsyncClient(timeout=20.0,verify=False) as client:
        tasks = [client.get(entry.url) for entry in payload]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    post_counts = {}
    domain_failures = defaultdict(list)
    parsed_success_domains = set()
    current_week = get_current_week()

    for sitemap_url, response in zip(payload, responses):
        domain = urlparse(sitemap_url.url).netloc
        print(f"\n🔍 Processing: {sitemap_url.url}")

        if isinstance(response, Exception):
            print("❌ Error fetching:", response)
            domain_failures[domain].append(f"Fetch error: {str(response)}")
            continue

        try:
            if not response.text.strip().startswith("<?xml"):
                print("❌ Skipping non-XML content from:", sitemap_url.url)
                domain_failures[domain].append("Not XML")
                continue

            parsed = xmltodict.parse(response.text)
            print("✅ Parsed XML root keys:", parsed.keys())

            if "urlset" in parsed:
                urls = parsed["urlset"].get("url", [])
                if not isinstance(urls, list):
                    urls = [urls]

                count = sum(1 for entry in urls if entry.get("loc"))
                post_counts[domain] = post_counts.get(domain, 0) + count
                parsed_success_domains.add(domain)

        except Exception as e:
            print("❌ Error parsing XML:", e)
            domain_failures[domain].append(f"Parse error: {str(e)}")
            continue

    output = [
        {"domain": domain, "count": count, "week": current_week}
        for domain, count in post_counts.items()
    ]

    # 💡 Final counts
    all_domains_sent = set(urlparse(entry.url).netloc for entry in payload)
    successful_domains = set(post_counts.keys())
    failed_domains = all_domains_sent - successful_domains

    print("\n📊 Summary Stats:")
    print(f"📥 Domains sent: {len(all_domains_sent)}")
    print(f"✅ Domains succeeded: {len(successful_domains)}")
    print(f"❌ Domains failed: {len(failed_domains)}")
    print(f"🧾 Failed domain details: {dict(domain_failures)}")

    return {
        "output": output,
        "summary": {
            "total_domains_sent": len(all_domains_sent),
            "total_domains_successful": len(successful_domains),
            "total_domains_failed": len(failed_domains),
            "failed_domains": list(failed_domains),
            "failed_details": dict(domain_failures),
        }
    }
