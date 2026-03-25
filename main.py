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
    get_other_issue_comments_by_group,
    get_postid_to_group,
    get_issue_criteria_all,
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



def _gemini_call(prompt, thinking=False, max_retries=3):
    """
    เรียก Gemini API และคืน response text
    thinking=True → ใช้ thinking_level=HIGH (สำหรับ Phase 2)
    retry อัตโนมัติเมื่อเจอ 429 rate limit
    """
    import time
    cfg = types.GenerateContentConfig(max_output_tokens=16384)
    if thinking:
        cfg = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_level="HIGH"),
            max_output_tokens=16384,
        )
    for attempt in range(1, max_retries + 1):
        try:
            response = gemini_client.models.generate_content(
                model="gemini-3-flash-preview",
                contents=prompt,
                config=cfg,
            )
            return response.text.strip()
        except Exception as e:
            err = str(e)
            if "429" in err or "RESOURCE_EXHAUSTED" in err:
                # parse retry delay จาก error message ถ้ามี
                wait = 30
                import re
                m = re.search(r"retry[^0-9]*(\d+)", err, re.IGNORECASE)
                if m:
                    wait = int(m.group(1)) + 5
                if attempt < max_retries:
                    print(f"  [rate limit] 429 — waiting {wait}s before retry {attempt}/{max_retries}...")
                    time.sleep(wait)
                    continue
            raise  # re-raise ถ้าไม่ใช่ rate limit หรือหมด retry


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

    # ใช้ index แทน URL เป็น id ใน prompt เพราะ URL ยาวเกินไป Gemini อาจ return ไม่ตรง
    idx_to_link = {str(i): post["id"] for i, post in enumerate(posts_to_label)}

    posts_text = ""
    for i, post in enumerate(posts_to_label):
        safe_desc = post["description"].replace('"', "'").replace("\n", " ")
        safe_kw   = post["keyword_description"].replace('"', "'").replace("\n", " ")
        posts_text += (
            f"---\n"
            f"IDX: {i}\n"
            f"KEYWORD: {safe_kw}\n"
            f"VIDEO: {safe_desc}\n"
        )
    posts_text += "---\n"

    prompt = (
        "You are a content relevance classifier.\n\n"
        "Below is a list of TikTok posts. For each post, decide if the VIDEO description "
        "is relevant to its KEYWORD.\n\n"
        f"{posts_text}\n"
        "Reply ONLY with a valid JSON array. Each element must be:\n"
        '{"idx": <number>, "label": "yes" or "Non"}\n\n'
        "No explanation. No markdown. Just the JSON array."
    )

    try:
        print(f"  [Gemini] labeling {len(posts_to_label)} posts...")
        raw = _gemini_call(prompt)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()
        # extract JSON array
        start = raw.find("[")
        end   = raw.rfind("]")
        if start != -1 and end != -1:
            raw = raw[start:end+1]

        results   = json.loads(raw)
        label_map = {}
        for item in results:
            idx   = str(item.get("idx", ""))
            label = item.get("label", "Non")
            link  = idx_to_link.get(idx)
            if link:
                label_map[link] = "yes" if "yes" in label.lower() else "Non"

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
        # aweme_id = PostID — ใช้เป็น key join กับ UniquePost / AllPost
        aweme_id = str(item.get("aweme_id", "")).strip()
        post_id  = aweme_id  # aweme_id คือ PostID เดียวกัน

        # user fields เป็น nested dict: item["user"]["uid"]
        user = item.get("user", {}) or {}

        rows.append([
            post_id,
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
    Phase 1: จัดประเภท TypeLabel + IssueLabels
    NameType/NameIssue เป็น English single word → ง่ายต่อการ validate
    """
    type_names  = [t["name"] for t in type_criteria] + ["Other"]
    issue_names = [i["name"] for i in issue_criteria] + ["Other"]
    valid_types  = set(type_names)
    valid_issues = set(issue_names)

    def _clean(text):
        return text.replace('"', "'").replace("\n", " ").replace("\r", " ")

    type_block = "\n".join(
        f'- {t["name"]}: {_clean(t["criteria"][:120])}'
        for t in type_criteria
    ) + "\n- Other: ไม่ตรงกับประเภทใด"

    issue_block = "\n".join(
        f'- {i["name"]}: {_clean(i["criteria"][:120])}'
        for i in issue_criteria
    ) + "\n- Other: ไม่ตรงกับประเด็นใด"

    valid_type_str  = ", ".join(f'"{n}"' for n in type_names)
    valid_issue_str = ", ".join(f'"{n}"' for n in issue_names)

    idx_to_cid = {str(i): c["cid"] for i, c in enumerate(batch)}

    comments_block = ""
    for i, c in enumerate(batch):
        safe_text = (c["text"]
                     .replace("\\", " ").replace('"', "'")
                     .replace("\n", " ").replace("\r", " ").replace("\t", " "))
        comments_block += f"---\nIDX: {i}\nTEXT: {safe_text}\n"
    comments_block += "---"

    prompt = (
        f"{instruction}\n\n"
        f"=== ประเภทความคิดเห็น ===\n{type_block}\n\n"
        f"=== ประเด็น ===\n{issue_block}\n\n"
        f"=== ความคิดเห็น ===\n{comments_block}\n\n"
        f"Valid type values: {valid_type_str}\n"
        f"Valid issue values: {valid_issue_str}\n\n"
        "ตอบเป็น JSON array เท่านั้น รูปแบบ:\n"
        '[{"idx":0, "type":"Criticize", "issues":["Burden","Trust"]}]\n'
        "type ต้องเป็นค่าจาก Valid type values เท่านั้น\n"
        "issues ต้องเป็น list จาก Valid issue values เท่านั้น ถ้าไม่มีให้ใส่ [\"Other\"]\n"
        "ห้ามอธิบาย ห้ามใส่ markdown"
    )

    try:
        print(f"  [Gemini] classifying {len(batch)} comments...")
        raw = _gemini_call(prompt)
        print(f"  [RAW] {raw[:200]}")
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()
        start = raw.find("[")
        end   = raw.rfind("]")
        if start != -1 and end != -1:
            raw = raw[start:end+1]

        results = json.loads(raw)
        output  = []
        responded_idxs = set()
        for item in results:
            idx = str(item.get("idx", ""))
            cid = idx_to_cid.get(idx)
            if not cid:
                continue
            responded_idxs.add(idx)

            # type — รองรับทั้ง string และ int index (fallback)
            raw_type = item.get("type", "Other")
            if isinstance(raw_type, int) or str(raw_type).strip().isdigit():
                try:
                    type_label = type_names[int(raw_type)]
                except (IndexError, ValueError):
                    type_label = "Other"
            else:
                type_label = str(raw_type).strip()
                if type_label not in valid_types:
                    type_label = "Other"

            # issues — รองรับทั้ง string list และ int index list (fallback)
            raw_issues = item.get("issues", ["Other"])
            issue_labels_list = []
            for ii in raw_issues:
                if isinstance(ii, int) or str(ii).strip().isdigit():
                    try:
                        issue_labels_list.append(issue_names[int(ii)])
                    except (IndexError, ValueError):
                        issue_labels_list.append("Other")
                else:
                    s = str(ii).strip()
                    issue_labels_list.append(s if s in valid_issues else "Other")
            if not issue_labels_list:
                issue_labels_list = ["Other"]

            issue_str = "|".join(issue_labels_list)
            print(f"  [LABEL] idx={idx} → {type_label} | {issue_str}")
            output.append({
                "cid":          cid,
                "type_label":   type_label,
                "issue_labels": issue_str,
            })

        # fill missing
        for idx, cid in idx_to_cid.items():
            if idx not in responded_idxs:
                print(f"  [MISSING] idx={idx} → Other")
                output.append({"cid": cid, "type_label": "Other", "issue_labels": "Other"})
        return output

    except Exception as e:
        print(f"  [warn] classify batch failed: {e}")
        print(f"  [RAW on error] {raw[:300] if 'raw' in locals() else 'no raw'}")
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


def generate_issue_criteria_for_group(group, sample_comments, instruction, existing_issue_names):
    """
    ให้ Gemini สร้าง IssueCriteria ชุดใหม่สำหรับ KeywordGroup ที่ยังไม่มี criteria
    sample_comments: list of {"cid", "text"}  (ใช้ไม่เกิน 100 ตัวอย่าง)
    Returns: list of {"name": str, "criteria": str}
    """
    import random
    sample = sample_comments if len(sample_comments) <= 100 else random.sample(sample_comments, 100)

    comments_block = "\n".join(
        f'- {c["text"].replace(chr(10)," ")[:150]}' for c in sample
    )
    existing_str = ", ".join(existing_issue_names) if existing_issue_names else "ยังไม่มี"

    prompt = (
        f"{instruction}\n\n"
        f"KeywordGroup ใหม่: {group}\n"
        f"ประเด็นที่มีอยู่แล้ว (ห้ามซ้ำ): {existing_str}\n\n"
        f"ตัวอย่าง comment ใน group นี้ ({len(sample)} รายการ):\n{comments_block}\n\n"
        "งาน: วิเคราะห์ comment เหล่านี้แล้วสร้างประเด็น (Issue) ที่เหมาะสมสำหรับ keyword group นี้\n"
        "จำนวน: 3-8 ประเด็น ขึ้นกับความหลากหลายของ comment\n"
        "ชื่อประเด็น: ภาษาอังกฤษ single word ไม่มี space (เช่น Transit, Burden, Trust)\n\n"
        "ตอบเป็น JSON array เท่านั้น รูปแบบ:\n"
        '[{"name":"IssueName", "criteria":"อธิบายลักษณะ comment ที่จัดอยู่ในประเด็นนี้ 1-2 ประโยค"}]\n'
        "ห้ามอธิบายเพิ่ม ห้ามใส่ markdown"
    )

    try:
        print(f"  [Gemini] generating IssueCriteria for group '{group}' ({len(sample)} samples)...")
        raw = _gemini_call(prompt)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()
        start = raw.find("[")
        end   = raw.rfind("]")
        if start != -1 and end != -1:
            raw = raw[start:end+1]
        results = json.loads(raw)
        new_criteria = [
            {"name": str(r.get("name","")).strip(),
             "criteria": str(r.get("criteria","")).strip()}
            for r in results
            if str(r.get("name","")).strip()
        ]
        print(f"  generated {len(new_criteria)} issue(s): {[c['name'] for c in new_criteria]}")
        return new_criteria
    except Exception as e:
        print(f"  [warn] generate criteria failed: {e}")
        return []


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
                # extract post_id จาก id field หรือจาก URL เป็น fallback
                raw_id  = str(item.get("id", "")).strip()
                post_id = raw_id if raw_id else link.split("/video/")[-1].split("?")[0]
                all_found[normalize_link(link)] = {
                    "kw_info":          kw_info,
                    "post_id":          post_id,
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
            meta["post_id"],
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

    # ดึง link + post_id ที่ Use="yes" และ PublishDate > cutoff จาก UniquePost ทั้งหมด
    yes_links_data = get_yes_links_after_cutoff()

    if not yes_links_data:
        print("no qualifying links for stats. done!")
        return

    yes_links      = [d["link"]    for d in yes_links_data]
    link_to_postid = {d["link"]: d["post_id"] for d in yes_links_data}

    # Run stats actor (ส่งทีเดียวทั้งหมด)
    scrape_date = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    stats_map   = fetch_stats(yes_links)

    # Build rows สำหรับ AllPost: [Link, PostID, Like, Comment, Share, Save, ScrapeDate]
    all_post_rows = []
    for link in yes_links:
        s = stats_map.get(link, {})
        all_post_rows.append([
            link,
            link_to_postid.get(link, ""),
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

    active_links_data = get_active_links_by_delta()

    if not active_links_data:
        print("no links passed delta filter. done!")
        return

    print(f"\n{len(active_links_data)} link(s) passed — proceeding to comment scrape.")
    print()

    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 7: Scrape Comments → Comments sheet")
    print("=" * 50)

    active_links  = [d["link"] for d in active_links_data]
    scrape_date   = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    comment_rows  = fetch_comments(active_links, scrape_date)

    print(f"appending {len(comment_rows)} comment rows to Comments sheet...")
    append_comments(comment_rows)
    print("done!")



    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 8: Classify Comments (Phase 1)")
    print("=" * 50)

    type_criteria        = get_type_criteria()
    issue_criteria_all   = get_issue_criteria_all()   # dict {group: [criteria]}
    instruction          = get_instruction()
    unlabeled            = get_unlabeled_comments()   # รวม post_id แล้ว
    postid_to_group      = get_postid_to_group()

    print(f"  types loaded   : {len(type_criteria)}")
    print(f"  issue groups   : {list(issue_criteria_all.keys())}")
    print(f"  instruction len: {len(instruction)} chars")

    # จัดกลุ่ม unlabeled comment ตาม KeywordGroup
    groups_map = {}   # {group: [comment]}
    for c in unlabeled:
        group = postid_to_group.get(c["post_id"], "_unknown_") or "_unknown_"
        groups_map.setdefault(group, []).append(c)

    print(f"  comment groups : { {g: len(v) for g, v in groups_map.items()} }")

    # รวม existing issue names สำหรับตรวจซ้ำตอน generate
    all_existing_names = {
        item["name"]
        for items in issue_criteria_all.values()
        for item in items
    }

    # build cid → list of row_indices (รองรับ duplicate CID)
    cid_to_rows = {}
    for c in unlabeled:
        cid_to_rows.setdefault(c["cid"], []).append(c["row_index"])

    label_updates = []

    for group, group_comments in groups_map.items():
        print(f"\n  === Group: {group} ({len(group_comments)} comments) ===")

        # ดึง IssueCriteria ของ group นี้
        issue_criteria = issue_criteria_all.get(group, [])

        # ถ้ายังไม่มี criteria → สร้างใหม่อัตโนมัติ (ข้าม _unknown_)
        if not issue_criteria:
            if group == "_unknown_":
                print(f"  [SKIP] group '_unknown_' — PostID missing, cannot generate criteria")
            else:
                print(f"  [NEW GROUP] '{group}' has no IssueCriteria — generating...")
                issue_criteria = generate_issue_criteria_for_group(
                    group, group_comments, instruction, all_existing_names
                )
                if issue_criteria:
                    append_issue_criteria(issue_criteria, keyword_group=group)
                    issue_criteria_all[group] = issue_criteria
                    all_existing_names.update(c["name"] for c in issue_criteria)
                else:
                    print(f"  [warn] could not generate criteria for '{group}' — using Other for all")

        # Classify ทีละ batch
        for i in range(0, len(group_comments), CLASSIFY_BATCH_SIZE):
            chunk = group_comments[i : i + CLASSIFY_BATCH_SIZE]
            print(f"  [Batch] {i+1}-{i+len(chunk)} / {len(group_comments)}")
            results = classify_comments_batch(chunk, type_criteria, issue_criteria, instruction)
            for r in results:
                for row in cid_to_rows.get(r["cid"], []):
                    label_updates.append({
                        "row_index":    row,
                        "type_label":   r["type_label"],
                        "issue_labels": r["issue_labels"],
                    })

    batch_update_type_and_issue(label_updates)
    print(f"\n  classified {len(label_updates)} comment(s) total")
    print()

    # ------------------------------------------------------------------ #
    print("=" * 50)
    print("STEP 9: Other Issue Detection (Phase 2) — per KeywordGroup")
    print("=" * 50)

    other_instruction  = get_other_instruction()
    # จัดกลุ่ม Other comments ตาม KeywordGroup
    other_by_group = get_other_issue_comments_by_group(postid_to_group)

    total_issue_updates = []

    for group, other_comments in other_by_group.items():
        if len(other_comments) <= OTHER_ISSUE_THRESHOLD:
            print(f"  [{group}] Other={len(other_comments)} ≤ {OTHER_ISSUE_THRESHOLD} — skip")
            continue

        print(f"  [{group}] Other={len(other_comments)} > {OTHER_ISSUE_THRESHOLD} — running detection...")
        group_criteria = issue_criteria_all.get(group, [])
        new_issues, mapping = detect_other_issues(other_comments, group_criteria, other_instruction)

        if not new_issues:
            print(f"  [{group}] no new issues found")
            continue

        print(f"  [{group}] new issues: {[n['name'] for n in new_issues]}")

        cid_to_row_other = {c["cid"]: c["row_index"] for c in other_comments}
        for cid, new_issue in mapping.items():
            row = cid_to_row_other.get(cid)
            if row and new_issue != "Other":
                total_issue_updates.append({
                    "row_index":    row,
                    "issue_labels": new_issue,
                })

        append_issue_criteria(new_issues, keyword_group=group)

    if total_issue_updates:
        batch_update_issue_only(total_issue_updates)
        print(f"  updated {len(total_issue_updates)} comment(s) with new issues")
    print("done!")


if __name__ == "__main__":
    main()
