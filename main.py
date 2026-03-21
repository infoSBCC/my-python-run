# ===== main.py =====
# STEP 1: Keyword Search - loop Apify search actor per keyword
# STEP 2: Unique Post - filter dup -> transcript (batch) -> Claude label -> append sheet

import os
import datetime
import anthropic
from apify_client import ApifyClient
from sheets import get_keywords, get_existing_links, append_unique_posts
from config import (
    SEARCH_ACTOR_ID,
    TRANSCRIPT_ACTOR_ID,
    TIKTOK_LIMIT,
    TIKTOK_SORT_TYPE,
    TIKTOK_PUBLISH_TIME,
)

APIFY_TOKEN = os.environ["APIFY_TOKEN"]
CLAUDE_API_KEY = os.environ["CLAUDE_API_KEY"]

apify_client = ApifyClient(APIFY_TOKEN)
claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)


def search_tiktok(keyword):
    run_input = {
        "keyword": keyword,
        "limit": TIKTOK_LIMIT,
        "isUnlimited": False,
        "sortType": TIKTOK_SORT_TYPE,
        "publishTime": TIKTOK_PUBLISH_TIME,
    }
    print(f" [Search] '{keyword}' region={TIKTOK_REGION} publishTime={TIKTOK_PUBLISH_TIME}")
    run = apify_client.actor(SEARCH_ACTOR_ID).call(run_input=run_input)
    items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f" got {len(items)} items")
    return items


def get_transcripts_batch(links):
    """
    Run Transcript Actor once for ALL links at the same time (batch).
    Returns dict: { url -> transcript_text }
    """
    if not links:
        return {}

    run_input = {
        "startUrls": [{"url": lnk} for lnk in links],
    }
    print(f" [Transcript Batch] sending {len(links)} links to actor...")
    try:
        run = apify_client.actor(TRANSCRIPT_ACTOR_ID).call(run_input=run_input)
        results = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f" [Transcript Batch] got {len(results)} results")
        transcript_map = {}
        for item in results:
            url = str(item.get("tiktokUrl", item.get("url", ""))).strip()
            transcript = str(item.get("transcript", "")).strip()
            if url:
                transcript_map[url] = transcript
        return transcript_map
    except Exception as e:
        print(f" [warn] batch transcript error: {e}")
        return {}


def label_with_claude(transcript, keyword_description):
    """
    Use Claude API to judge whether the transcript is related to keyword_description.
    Returns 'yes' or 'Non'.
    """
    if not transcript or not keyword_description:
        return "Non"

    prompt = (
        f"You are a content relevance classifier.\n\n"
        f"Keyword Description:\n{keyword_description}\n\n"
        f"TikTok Transcript:\n{transcript}\n\n"
        f"Is this transcript relevant to the keyword description above?\n"
        f"Reply with ONLY one word: 'yes' if relevant, 'Non' if not relevant."
    )

    try:
        message = claude_client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=10,
            messages=[{"role": "user", "content": prompt}],
        )
        answer = message.content[0].text.strip().lower()
        if "yes" in answer:
            return "yes"
        else:
            return "Non"
    except Exception as e:
        print(f" [warn] Claude label error: {e}")
        return "Non"


def main():
    today = datetime.date.today().isoformat()
    print("=" * 50)
    print("STEP 1: Keyword Search")
    print("=" * 50)

    keywords = get_keywords()
    print(f"found {len(keywords)} keyword(s)\n")

    # all_found: { link -> { kw_info, item_metadata } }
    all_found = {}
    for kw_info in keywords:
        items = search_tiktok(kw_info["keyword"])
        for item in items:
            link = str(item.get("share_url", "")).strip()
            if link and link not in all_found:
                all_found[link] = {
                    "kw_info": kw_info,
                    "author_name": str(item.get("authorName", item.get("author", {}).get("nickname", ""))).strip(),
                    "author_unique_id": str(item.get("authorUniqueId", item.get("author", {}).get("uniqueId", ""))).strip(),
                    "description": str(item.get("text", item.get("desc", ""))).strip(),
                    "video_duration": str(item.get("videoDuration", item.get("video", {}).get("duration", ""))).strip(),
                    "music_title": str(item.get("musicTitle", item.get("music", {}).get("title", ""))).strip(),
                }

    print(f"\ntotal links found: {len(all_found)}")
    print()

    print("=" * 50)
    print("STEP 2: Unique Post")
    print("=" * 50)

    existing = get_existing_links()
    print(f"existing links in UniquePost: {len(existing)}")

    new_links = {lnk: meta for lnk, meta in all_found.items() if lnk not in existing}
    print(f"new unique links: {len(new_links)}\n")

    if not new_links:
        print("no new links. done!")
        return

    # --- Batch transcript: run actor once for ALL links ---
    all_link_list = list(new_links.keys())
    transcript_map = get_transcripts_batch(all_link_list)

    print()
    print("=" * 50)
    print("STEP 3: Claude Label + Build Rows")
    print("=" * 50)

    rows = []
    for link, meta in new_links.items():
        transcript = transcript_map.get(link, "").strip()
        kw_info = meta["kw_info"]
        print(f" [Label] {link}")
        if not transcript:
            print("   no transcript -> label Non")
            use_label = "Non"
        else:
            use_label = label_with_claude(transcript, kw_info["description"])
            print(f"   Claude label -> {use_label}")

        # Build row matching UniquePost sheet columns:
        # Date Post | Link | keyword group | AuthorName | AuthorUniqueID | Description | Transcription | VideoDuration | MusicTitle | Use
        rows.append([
            today,
            link,
            kw_info["group"],
            meta["author_name"],
            meta["author_unique_id"],
            meta["description"],
            transcript,
            meta["video_duration"],
            meta["music_title"],
            use_label,
        ])

    print(f"\nappending {len(rows)} rows to UniquePost...")
    append_unique_posts(rows)
    print("done!")


if __name__ == "__main__":
    main()
