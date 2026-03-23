# ===== main.py =====
# STEP 1: Keyword Search - loop Apify search actor per keyword
# STEP 2: Unique Post - filter dup -> Claude label (by description) -> transcript only "yes" -> append sheet

import os
import json
import time
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


def label_batch_with_claude(posts_to_label):
    """
    Label multiple posts in one Claude call.
    posts_to_label: list of {"id": link, "description": str, "keyword_description": str}
    Returns: dict {link: "yes" or "Non"}
    """
    if not posts_to_label:
        return {}

    # Build the batch prompt
    posts_text = ""
    for i, post in enumerate(posts_to_label):
        posts_text += (
            f"---\n"
            f"POST_ID: {post['id']}\n"
            f"KEYWORD_DESCRIPTION: {post['keyword_description']}\n"
            f"VIDEO_DESCRIPTION: {post['description']}\n"
        )
    posts_text += "---\n"

    prompt = (
        "You are a content relevance classifier.\n\n"
        "Below is a list of TikTok posts. For each post, decide if the VIDEO_DESCRIPTION "
        "is relevant to its KEYWORD_DESCRIPTION.\n\n"
        f"{posts_text}\n"
        "Reply ONLY with a valid JSON array. Each element must be:\n"
        '{"id": "<POST_ID>", "label": "yes" or "Non"}\n\n'
        "No explanation. No markdown. Just the JSON array."
    )

    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"    [attempt {attempt}/{MAX_RETRIES}]")
            message = claude_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=5000,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = message.content[0].text.strip()

            # Clean potential markdown fences
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()

            results = json.loads(raw)
            label_map = {}
            for item in results:
                post_id = item.get("id", "")
                label = item.get("label", "Non")
                label_map[post_id] = "yes" if "yes" in label.lower() else "Non"

            # Verify all posts got a label, retry if some are missing
            missing = [p["id"] for p in posts_to_label if p["id"] not in label_map]
            if missing:
                print(f"    [warn] {len(missing)} posts missing from response")
                if attempt < MAX_RETRIES:
                    print(f"    retrying in 5s...")
                    time.sleep(5)
                    continue
                # Last attempt: fill missing as Non
                for pid in missing:
                    label_map[pid] = "Non"

            return label_map

        except Exception as e:
            print(f"    [warn] attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                wait = 5 * attempt  # 5s, 10s
                print(f"    retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    [error] all {MAX_RETRIES} attempts failed, labeling batch as Non")
                return {post["id"]: "Non" for post in posts_to_label}


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
                    "author_unique_id": str(
                        author.get("search_user_desc", author.get("uniqueId", ""))
                    ).strip(),
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
    print("STEP 3: Claude Label (BATCH)")
    print("=" * 50)

    # Build batch input (skip posts with no description)
    posts_to_label = []
    no_desc_links = []
    for link, meta in new_links.items():
        if not meta["description"]:
            no_desc_links.append(link)
        else:
            posts_to_label.append({
                "id": link,
                "description": meta["description"],
                "keyword_description": meta["kw_info"]["description"],
            })

    # Batch call — split into chunks of 50 to stay within token limits
    BATCH_SIZE = 50
    label_map = {}

    # No-description posts default to Non
    for link in no_desc_links:
        label_map[link] = "Non"
        print(f"  [Label] {link} -> Non (no description)")

    for i in range(0, len(posts_to_label), BATCH_SIZE):
        chunk = posts_to_label[i : i + BATCH_SIZE]
        print(f"  [Batch] labeling posts {i+1}-{i+len(chunk)} / {len(posts_to_label)}")
        chunk_labels = label_batch_with_claude(chunk)
        label_map.update(chunk_labels)
        for post in chunk:
            lbl = chunk_labels.get(post["id"], "Non")
            print(f"    {post['id'][:60]}... -> {lbl}")

    yes_links = [link for link, lbl in label_map.items() if lbl == "yes"]
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
        use_label = label_map.get(link, "Non")
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
