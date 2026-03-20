# ===== main.py =====
# STEP 1: Keyword Search - loop Apify search actor per keyword
# STEP 2: Unique Post - filter dup -> transcript -> check description -> append sheet

import os
from apify_client import ApifyClient
from sheets import get_keywords, get_existing_links, append_unique_posts
from config import (
    SEARCH_ACTOR_ID,
    TRANSCRIPT_ACTOR_ID,
    TIKTOK_LIMIT,
    TIKTOK_REGION,
    TIKTOK_SORT_TYPE,
    TIKTOK_PUBLISH_TIME,
)

APIFY_TOKEN = os.environ["APIFY_TOKEN"]
client = ApifyClient(APIFY_TOKEN)


def search_tiktok(keyword):
    run_input = {
        "keyword": keyword,
        "limit": TIKTOK_LIMIT,
        "isUnlimited": False,
        "region": TIKTOK_REGION,
        "sortType": TIKTOK_SORT_TYPE,
        "publishTime": TIKTOK_PUBLISH_TIME,
    }
    print(f"  [Search] '{keyword}' region={TIKTOK_REGION} publishTime={TIKTOK_PUBLISH_TIME}")
    run = client.actor(SEARCH_ACTOR_ID).call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"           got {len(items)} items")
    return items


def get_transcript(tiktok_url):
    run_input = {
        "tiktokUrl": tiktok_url,
    }
    try:
        run = client.actor(TRANSCRIPT_ACTOR_ID).call(run_input=run_input)
        results = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        if results:
            return str(results[0].get("transcript", "")).strip()
    except Exception as e:
        print(f"  [warn] transcript error: {e}")
    return ""


def is_related(transcript, keyword_description):
    if not transcript or not keyword_description:
        return False
    tl = transcript.lower()
    terms = [t.strip().lower() for t in keyword_description.replace(",", " ").split() if t.strip()]
    return any(t in tl for t in terms)


def main():
    print("=" * 50)
    print("STEP 1: Keyword Search")
    print("=" * 50)

    keywords = get_keywords()
    print(f"found {len(keywords)} keyword(s)\n")

    all_found = {}
    for kw_info in keywords:
        items = search_tiktok(kw_info["keyword"])
        for item in items:
            link = str(item.get("share_url", "")).strip()
            if link and link not in all_found:
                all_found[link] = kw_info

    print(f"\ntotal links found: {len(all_found)}")

    print()
    print("=" * 50)
    print("STEP 2: Unique Post")
    print("=" * 50)

    existing = get_existing_links()
    print(f"existing links in UniquePost: {len(existing)}")

    new_links = {lnk: inf for lnk, inf in all_found.items() if lnk not in existing}
    print(f"new unique links: {len(new_links)}\n")

    if not new_links:
        print("no new links. done!")
        return

    rows = []
    for link, info in new_links.items():
        print(f"  [Transcript] {link}")
        transcript = get_transcript(link)

        if not transcript:
            print("    no transcript -> skip")
            continue

        if is_related(transcript, info["description"]):
            print(f"    related to '{info['description']}' -> add")
            rows.append([
                info["group"],
                info["keyword"],
                link,
                transcript,
            ])
        else:
            print("    not related -> skip")

    print(f"\nappending {len(rows)} rows to UniquePost...")
    append_unique_posts(rows)
    print("done!")


if __name__ == "__main__":
    main()
