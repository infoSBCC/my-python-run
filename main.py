# ===== main.py =====
# STEP 1: Keyword Search  - loop Apify search actor per keyword
# STEP 2: Unique Post     - filter dup -> Claude label -> append UniquePost sheet
# STEP 5: Fetch Stats     - filter yes + after cutoff -> run stats actor -> append AllPost sheet
# STEP 6: Delta Filter    - compare today vs yesterday comments -> pass tier threshold -> next step

import os
import json
import datetime
from google import genai
from google.genai import types
from apify_client import ApifyClient
from sheets import (
    get_keywords,
    get_existing_links,
    append_unique_posts,
    get_yes_links_after_cutoff,
    append_all_posts,
    get_active_links_by_delta,
    append_comments,
    get_type_criteria,
    get_issue_criteria,
    get_instruction,
    get_other_instruction,
    get_unlabeled_comments,
    get_other_issue_comments,
    batch_update_type_and_issue,
    batch_update_issue_only,
    append_issue_criteria,
)
from config import (
    SEARCH_ACTOR_ID,
    STATS_ACTOR_ID,
    TIKTOK_SORT_TYPE,
    COMMENT_ACTOR_ID,
    COMMENT_MAX_ITEMS,
    OTHER_ISSUE_THRESHOLD,
    OTHER_SAMPLE_SIZE,
    CLASSIFY_BATCH_SIZE,
)

APIFY_TOKEN  = os.environ["APIFY_TOKEN"]
GEMINI_API   = os.environ["GEMINI_API"]

apify_client  = ApifyClient(APIFY_TOKEN)
gemini_client = genai.Client(api_key=GEMINI_API)



def _gemini_call(prompt, thinking=False):
    """
    เรียก Gemini API และคืน response text
    thinking=True → ใช้ thinking_level=HIGH (สำหรับ Phase 2)
    """
    cfg = types.GenerateContentConfig(max_output_tokens=8192)
    if thinking:
        cfg = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_level="HIGH"),
            max_output_tokens=8192,
        )
    response = gemini_client.models.generate_content(
        model="gemini-3-flash-preview",
        contents=prompt,
        config=cfg,
    )
    return response.text.strip()


def normalize_link(url):
    """ตัด query string ออก เหลือแค่ส่วนก่อน ? เพื่อป้องกัน link เดิมแต่ param ต่างกัน"""
    return url.split("?")[0].rstrip("/")


def search_tiktok(keyword, limit, time_range):
    run_input = {
        "keyword":     keyword,
        "limit":       limit,
        "isUnlimited": False,
        "sortType":    TIKTOK_SORT_TYPE,
        "publishTime": time_range,
    }
    print(f"  [Search] '{keyword}'  limit={limit}  publishTime={time_range}")
    run   = apify_client.actor(SEARCH_ACTOR_ID).call(run_input=run_input)
    items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"    got {len(items)} items")
    return items


def label_with_claude(posts_to_label):
    """
    Label a chunk of posts in one Claude call.
    posts_to_label: list of {"id": link, "description": str, "keyword_description": str}
    Returns: dict {link: "yes" | "Non" | "fail"}
    """
    if not posts_to_label:
        return {}

    posts_text = ""
    for post in posts_to_label:
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

    try:
        print(f"  [Gemini] labeling {len(posts_to_label)} posts...")
        raw = _gemini_call(prompt)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()

        results   = json.loads(raw)
        label_map = {}
        for item in results:
            post_id = item.get("id", "")
            label   = item.get("label", "Non")
            label_map[post_id] = "yes" if "yes" in label.lower() else "Non"

        # Posts missing from response → fail
        for p in posts_to_label:
            if p["id"] not in label_map:
                label_map[p["id"]] = "fail"

        return label_map

    except Exception as e:
        print(f"  [warn] labeling failed: {e} — marking all as fail")
        return {post["id"]: "fail" for post in posts_to_label}


def fetch_stats(links):
    """
    Run the stats actor for a list of TikTok links.
    Returns: dict {postPage: {"likes": int, "comments": int, "shares": int, "bookmarks": int}}
    """
    run_input = {
        "customMapFunction":    "(object) => { return {...object} }",
        "dateRange":            "DEFAULT",
        "includeSearchKeywords": False,
        "location":             "US",
        "maxItems":             1000,
        "sortType":             "RELEVANCE",
        "startUrls":            links,
    }
    print(f"  [Stats] running actor for {len(links)} links...")
    run     = apify_client.actor(STATS_ACTOR_ID).call(run_input=run_input)
    items   = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"    got {len(items)} results")

    stats_map = {}
    for item in items:
        link = normalize_link(str(item.get("postPage", "")))  # normalize ก่อน เพื่อให้ตรงกับ yes_links
        if link:
            stats_map[link] = {
                "likes":     int(item.get("likes",      0)),
                "comments":  int(item.get("comments",   0)),
                "shares":    int(item.get("shares",     0)),
                "bookmarks": int(item.get("bookmarks",  0)),
            }
    return stats_map



def fetch_comments(links, scrape_date):
    """
    Run xtdata/tiktok-comment-scraper สำหรับ links ที่ผ่าน delta filter
    Returns: list of rows สำหรับ append ไป Comments sheet
    """
    run_input = {
        "maxItems":           COMMENT_MAX_ITEMS,
        "shouldScrapeAll":    False,
        "shouldScrapeReplies": False,
        "urls":               links,
    }
    print(f"  [Comments] running actor for {len(links)} links...")
    run   = apify_client.actor(COMMENT_ACTOR_ID).call(run_input=run_input)
    items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"    got {len(items)} comments")

    # สร้าง map: aweme_id → original link (เพื่อใช้เป็น key join)
    aweme_to_link = {}
    for lnk in links:
        # extract video id จาก url เช่น .../video/7211250685902359850...
        clean = normalize_link(lnk)
        parts = clean.split("/video/")
        if len(parts) == 2:
            vid_id = parts[1].strip()
            aweme_to_link[vid_id] = clean

    rows = []
    for item in items:
        # aweme_id อาจเป็น int หรือ string → แปลงให้เป็น string เสมอก่อน lookup
        aweme_id   = str(item.get("aweme_id", "")).strip()
        video_link = aweme_to_link.get(aweme_id, "")

        # user fields เป็น nested dict: item["user"]["uid"]
        user = item.get("user", {}) or {}

        rows.append([
            video_link,
            str(item.get("cid", "")).strip(),
            str(item.get("text", "")).strip(),
            item.get("create_time", ""),
            int(item.get("digg_count", 0) or 0),
            int(item.get("reply_comment_total", 0) or 0),
            str(user.get("uid", "")).strip(),
            str(user.get("unique_id", "")).strip(),
            str(user.get("nickname", "")).strip(),
            int(user.get("follower_count", 0) or 0),
            str(user.get("region", "")).strip(),
            scrape_date,
        ])
    return rows



def classify_comments_batch(batch, type_criteria, issue_criteria, instruction):
    """
    Phase 1: จัดประเภท TypeLabel + IssueLabels สำหรับ 1 batch (max 100)
    Returns: list of {"cid": str, "type_label": str, "issue_labels": str}
    """
    type_block  = "\n".join(
        f'{t["name"]}: {t["criteria"]}' for t in type_criteria
    ) + "\nOther: ความคิดเห็นที่ไม่ตรงกับประเภทใดข้างต้น"

    issue_block = "\n".join(
        f'{i["name"]}: {i["criteria"]}' for i in issue_criteria
    ) + "\nOther: ประเด็นที่ไม่ตรงกับรายการใดข้างต้น"

    comments_block = ""
    for c in batch:
        comments_block += f"---\nID: {c['cid']}\nTEXT: {c['text']}\n"
    comments_block += "---"

    prompt = (
        f"{instruction}\n\n"
        f"=== ประเภทความคิดเห็น (TypeCriteria) ===\n{type_block}\n\n"
        f"=== ประเด็น (IssueCriteria) ===\n{issue_block}\n\n"
        f"=== ความคิดเห็นที่ต้องจัดประเภท ===\n{comments_block}\n\n"
        "ตอบเป็น JSON array เท่านั้น รูปแบบ:\n"
        '[{"id":"...", "type":"...", "issues":["..."]}]\n'
        "ถ้าไม่มี issue ที่ตรง ให้ issues = [\"Other\"]\n"
        "ห้ามอธิบายเพิ่มเติม ห้ามใส่ markdown"
    )

    try:
        print(f"  [Gemini] classifying {len(batch)} comments...")
        raw = _gemini_call(prompt)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()

        results = json.loads(raw)
        output  = []
        for item in results:
            issues_list = item.get("issues", ["Other"])
            if not issues_list:
                issues_list = ["Other"]
            output.append({
                "cid":          str(item.get("id", "")),
                "type_label":   str(item.get("type", "Other")),
                "issue_labels": "|".join(issues_list),
            })
        return output

    except Exception as e:
        print(f"  [warn] classify batch failed: {e} — marking as Other")
        return [{"cid": c["cid"], "type_label": "Other", "issue_labels": "Other"}
                for c in batch]


def detect_other_issues(other_comments, issue_criteria, other_instruction):
    """
    Phase 2: วิเคราะห์ IssueLabels=Other → หาประเด็นใหม่ไม่เกิน 2 ประเด็น
    other_comments: list of {"row_index", "cid", "text"}
    Returns: (new_issues, mapping)
      new_issues: list of {"name": str, "criteria": str}
      mapping:    dict {cid: "issue_name or Other"}
    """
    import random
    sample = other_comments
    if len(other_comments) > OTHER_SAMPLE_SIZE:
        sample = random.sample(other_comments, OTHER_SAMPLE_SIZE)
        print(f"  sampled {OTHER_SAMPLE_SIZE} from {len(other_comments)} Other comments")

    issue_block = "\n".join(
        f'{i["name"]}: {i["criteria"]}' for i in issue_criteria
    )
    comments_block = ""
    for c in sample:
        comments_block += f"---\nID: {c['cid']}\nTEXT: {c['text']}\n"
    comments_block += "---"

    prompt = (
        f"{other_instruction}\n\n"
        f"=== ประเด็นที่มีอยู่แล้ว ===\n{issue_block}\n\n"
        f"=== ความคิดเห็น IssueLabels=Other ({len(sample)} รายการ) ===\n"
        f"{comments_block}"
    )

    try:
        print(f"  [Gemini+thinking] detecting new issues from {len(sample)} comments...")
        raw = _gemini_call(prompt, thinking=True)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()

        result    = json.loads(raw)
        new_types = result.get("new_issues", [])
        mapping   = {item["id"]: item.get("new_issue", "Other")
                     for item in result.get("mapping", [])}
        return new_types, mapping

    except Exception as e:
        print(f"  [warn] other detection failed: {e}")
        return [], {}


def main():
    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 1: Keyword Search")
    print("=" * 50)

    keywords = get_keywords()
    print(f"found {len(keywords)} keyword(s)\n")

    all_found = {}
    for kw_info in keywords:
        items = search_tiktok(kw_info["keyword"], kw_info["limit"], kw_info["time_range"])
        for item in items:
            link = str(item.get("share_url", "")).strip()
            if link and link not in all_found:
                author = item.get("author", {})
                video  = item.get("video", {})
                music  = item.get("added_sound_music_info", item.get("music", {}))
                all_found[link] = {
                    "kw_info":          kw_info,
                    "create_time":      str(item.get("create_time", "")).strip(),
                    "author_name":      str(author.get("nickname", "")).strip(),
                    "author_unique_id": str(
                        author.get("search_user_desc", author.get("uniqueId", ""))
                    ).strip(),
                    "author_follower":  str(author.get("follower_count", "")).strip(),
                    "description":      str(item.get("desc", "")).strip(),
                    "video_duration":   str(video.get("duration", "")).strip(),
                    "music_title":      str(music.get("title", "")).strip(),
                }

    print(f"\ntotal links found: {len(all_found)}")
    print()

    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 2: Unique Post")
    print("=" * 50)

    existing  = get_existing_links()
    print(f"existing links in UniquePost: {len(existing)}")
    new_links = {lnk: meta for lnk, meta in all_found.items() if lnk not in existing}
    print(f"new unique links: {len(new_links)}\n")

    if not new_links:
        print("no new links. done!")
        return

    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 3: Claude Label (batch 50, no retry)")
    print("=" * 50)

    posts_to_label = []
    label_map      = {}
    for link, meta in new_links.items():
        if not meta["description"]:
            label_map[link] = "Non"
            print(f"  [skip] {link[:60]}... -> Non (no description)")
        else:
            posts_to_label.append({
                "id":                  link,
                "description":         meta["description"],
                "keyword_description": meta["kw_info"]["description"],
            })

    BATCH_SIZE = 50
    for i in range(0, len(posts_to_label), BATCH_SIZE):
        chunk = posts_to_label[i : i + BATCH_SIZE]
        print(f"  [Batch] posts {i+1}-{i+len(chunk)} / {len(posts_to_label)}")
        chunk_labels = label_with_claude(chunk)
        label_map.update(chunk_labels)
        for post in chunk:
            lbl = chunk_labels.get(post["id"], "Non")
            print(f"    {post['id'][:60]}... -> {lbl}")

    yes_count = sum(1 for lbl in label_map.values() if lbl == "yes")
    print(f"\n'yes' posts: {yes_count} / {len(new_links)}")
    print()

    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 4: Append to UniquePost")
    print("=" * 50)

    rows = []
    for link, meta in new_links.items():
        use_label = label_map.get(link, "Non")
        kw_info   = meta["kw_info"]
        rows.append([
            meta["create_time"],
            link,
            meta["author_name"],
            meta["author_unique_id"],
            meta["author_follower"],
            meta["description"],
            "",              # Transcription — ไม่ได้ดึง ปล่อยว่าง
            meta["video_duration"],
            meta["music_title"],
            use_label,
            kw_info["group"],
        ])

    print(f"appending {len(rows)} rows to UniquePost...")
    append_unique_posts(rows)
    print()

    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 5: Fetch Stats → AllPost")
    print("=" * 50)

    # ดึง link ที่ Use="yes" และ PublishDate > cutoff จาก UniquePost ทั้งหมด
    yes_links = get_yes_links_after_cutoff()

    if not yes_links:
        print("no qualifying links for stats. done!")
        return

    # Run stats actor (ส่งทีเดียวทั้งหมด)
    scrape_date = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    stats_map   = fetch_stats(yes_links)

    # Build rows สำหรับ AllPost: [Link, Like, Comment, Share, Save, ScrapeDate]
    all_post_rows = []
    for link in yes_links:
        s = stats_map.get(link, {})
        all_post_rows.append([
            link,
            s.get("likes",     0),
            s.get("comments",  0),
            s.get("shares",    0),
            s.get("bookmarks", 0),
            scrape_date,
        ])

    print(f"appending {len(all_post_rows)} rows to AllPost...")
    append_all_posts(all_post_rows)
    print("done!")


    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 6: Delta Filter → active links")
    print("=" * 50)

    active_links = get_active_links_by_delta()

    if not active_links:
        print("no links passed delta filter. done!")
        return


    print(f"\n{len(active_links)} link(s) passed — proceeding to comment scrape.")
    print()

    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 7: Scrape Comments → Comments sheet")
    print("=" * 50)

    scrape_date   = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    comment_rows  = fetch_comments(active_links, scrape_date)

    print(f"appending {len(comment_rows)} comment rows to Comments sheet...")
    append_comments(comment_rows)
    print("done!")



    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 8: Classify Comments (Phase 1)")
    print("=" * 50)

    type_criteria     = get_type_criteria()
    issue_criteria    = get_issue_criteria()
    instruction       = get_instruction()
    unlabeled         = get_unlabeled_comments()

    # debug: ตรวจสอบว่าโหลดข้อมูลมาได้จริงไหม
    print(f"  types loaded   : {len(type_criteria)}")
    for t in type_criteria:
        print(f"    - {t['name']}: {t['criteria'][:60]}")
    print(f"  issues loaded  : {len(issue_criteria)}")
    for i in issue_criteria:
        print(f"    - {i['name']}: {i['criteria'][:60]}")
    print(f"  instruction len: {len(instruction)} chars")
    if not instruction:
        print("  [WARN] instruction is EMPTY — check Instruction sheet, column InstructionDetail")
    if not type_criteria:
        print("  [WARN] type_criteria is EMPTY — check TypeCriteria sheet, column NameType / CriteriaType")
    if not issue_criteria:
        print("  [WARN] issue_criteria is EMPTY — check IssueCriteria sheet, column NameIssue / CriteriaIssue")

    # build cid → row_index map สำหรับ update กลับ
    cid_to_row = {c["cid"]: c["row_index"] for c in unlabeled}

    label_updates = []
    for i in range(0, len(unlabeled), CLASSIFY_BATCH_SIZE):
        chunk = unlabeled[i : i + CLASSIFY_BATCH_SIZE]
        print(f"  [Batch] comments {i+1}-{i+len(chunk)} / {len(unlabeled)}")
        results = classify_comments_batch(chunk, type_criteria, issue_criteria, instruction)
        for r in results:
            row = cid_to_row.get(r["cid"])
            if row:
                label_updates.append({
                    "row_index":    row,
                    "type_label":   r["type_label"],   # maps to CommentType
                    "issue_labels": r["issue_labels"],  # maps to CommentIssue
                })

    batch_update_type_and_issue(label_updates)
    print(f"  classified {len(label_updates)} comment(s)")
    print()

    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 9: Other Issue Detection (Phase 2)")
    print("=" * 50)

    other_comments = get_other_issue_comments()

    if len(other_comments) <= OTHER_ISSUE_THRESHOLD:
        print(f"  Other ({len(other_comments)}) ≤ {OTHER_ISSUE_THRESHOLD} — skip phase 2. done!")
        return

    print(f"  Other ({len(other_comments)}) > {OTHER_ISSUE_THRESHOLD} — running detection...")
    other_instruction = get_other_instruction()
    new_issues, mapping = detect_other_issues(other_comments, issue_criteria, other_instruction)

    if not new_issues:
        print("  no new issues found. done!")
        return

    print(f"  new issues: {[n['name'] for n in new_issues]}")

    # อัปเดต IssueLabels ของ comment ที่ถูก map ไปประเด็นใหม่
    issue_updates = []
    cid_to_row_other = {c["cid"]: c["row_index"] for c in other_comments}
    for cid, new_issue in mapping.items():
        row = cid_to_row_other.get(cid)
        if row and new_issue != "Other":
            issue_updates.append({
                "row_index":    row,
                "issue_labels": new_issue,
            })

    batch_update_issue_only(issue_updates)
    print(f"  updated {len(issue_updates)} comment(s) with new issues")

    # append ประเด็นใหม่ไปที่ IssueCriteria sheet
    append_issue_criteria(new_issues)
    print("done!")


if __name__ == "__main__":
    main()
