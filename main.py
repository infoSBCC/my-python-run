# ===== main.py =====
# STEP 1: Keyword Search - loop Apify search actor per keyword
# STEP 2: Unique Post - filter dup -> Claude label (by description) -> transcript only "yes" -> append sheet

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
        print(f"  [Search] '{keyword}' publishTime={TIKTOK_PUBLISH_TIME}")
        run = apify_client.actor(SEARCH_ACTOR_ID).call(run_input=run_input)
        items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"  got {len(items)} items")
        return items


def get_transcript_single(link):
        run_input = {"startUrls": [{"url": link}]}
        print(f"  [Transcript] fetching: {link}")
        try:
                    run = apify_client.actor(TRANSCRIPT_ACTOR_ID).call(run_input=run_input)
                    results = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
                    for item in results:
                                    url = str(item.get("tiktokUrl", item.get("url", ""))).strip()
                                    transcript = str(item.get("transcript", "")).strip()
                                    if url:
                                                        return transcript
                                                return ""
except Exception as e:
        print(f"  [warn] transcript error: {e}")
        return ""


def label_with_claude(description, keyword_description):
        if not description or not keyword_description:
                    return "Non"
                prompt = (
                            f"You are a content relevance classifier.\n\n"
                            f"Keyword Description:\n{keyword_description}\n\n"
                            f"TikTok Video Description:\n{description}\n\n"
                            f"Is this video description relevant to the keyword description above?\n"
                            f"Reply with ONLY one word: 'yes' if relevant, 'Non' if not relevant."
                )
    try:
                message = claude_client.messages.create(
                                model="claude-sonnet-4-5-20250929",
                                max_tokens=1000,
                                messages=[{"role": "user", "content": prompt}],
                )
                answer = message.content[0].text.strip().lower()
                if "yes" in answer:
                                return "yes"
    else:
            return "Non"
    except Exception as e:
        print(f"  [warn] Claude label error: {e}")
        return "Non"


def main():
        today = datetime.date.today().isoformat()
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
                                                    author = item.get("author", {})
                                                    video = item.get("video", {})
                                                    music = item.get("added_sound_music_info", item.get("music", {}))
                                                    all_found[link] = {
                                                        "kw_info": kw_info,
                                                        "create_time": str(item.get("create_time", "")).strip(),
                                                        "author_name": str(author.get("nickname", "")).strip(),
                                                        "author_unique_id": str(author.get("search_user_desc", author.get("uniqueId", ""))).strip(),
                                                        "author_follower": str(author.get("follower_count", "")).strip(),
                                                        "description": str(item.get("desc", "")).strip(),
                                                        "video_duration": str(video.get("duration", "")).strip(),
                                                        "music_title": str(music.get("title", "")).strip(),
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

    print("=" * 50)
    print("STEP 3: Claude Label (by Description)")
    print("=" * 50)
    yes_links = []
    label_map = {}
    for link, meta in new_links.items():
                description = meta["description"]
        kw_info = meta["kw_info"]
        print(f"  [Label] {link}")
        if not description:
                        print("    no description -> label Non")
                        use_label = "Non"
else:
            use_label = label_with_claude(description, kw_info["description"])
            print(f"    Claude label -> {use_label}")
        label_map[link] = use_label
        if use_label == "yes":
                        yes_links.append(link)

    print(f"\n'yes' links: {len(yes_links)} / {len(new_links)}")
    print()

    print("=" * 50)
    print("STEP 4: Fetch Transcript (yes only, one by one)")
    print("=" * 50)
    transcript_map = {}
    for link in yes_links:
                transcript = get_transcript_single(link)
        transcript_map[link] = transcript
    print(f"transcripts fetched: {len(transcript_map)}")
    print()

    print("=" * 50)
    print("STEP 5: Build Rows & Append")
    print("=" * 50)
    rows = []
    for link, meta in new_links.items():
                use_label = label_map[link]
        transcript = transcript_map.get(link, "")
        kw_info = meta["kw_info"]
        rows.append([
                        meta["create_time"],
                        link,
                        meta["author_name"],
                        meta["author_unique_id"],
                        meta["author_follower"],
                        meta["description"],
                        transcript,
                        meta["video_duration"],
                        meta["music_title"],
                        use_label,
                        kw_info["group"],
        ])

    print(f"appending {len(rows)} rows to UniquePost...")
    append_unique_posts(rows)
    print("done!")


if __name__ == "__main__":
        main()
