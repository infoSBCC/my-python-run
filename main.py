# ===== main.py =====
# TikTok Social Listening Pipeline (Refactored)
#
# Flow:
#   STEP 1  Keyword Search      → Apify search per keyword
#   STEP 2  Unique Post Filter   → dedup by normalized link
#   STEP 3  Gemini Relevance     → yes / Non labeling
#   STEP 4  Append UniquePost    → write to sheet
#   STEP 5  Fetch Stats          → Apify stats → AllPost (with dedup)
#   STEP 6  Delta Filter         → today vs yesterday comment delta
#   STEP 7  Scrape Comments      → Apify comments (in-memory)
#   STEP 8  Classify Comments    → Phase 1 Type+Issue in-memory
#   STEP 9  Other Issue Detect   → Phase 2 new issues in-memory
#   STEP 10 Write Comments       → append all classified rows to sheet

import os
import json
import time
import random
import datetime
import logging
import re

from google import genai
from google.genai import types
from apify_client import ApifyClient

from sheets import (
    normalize_link,
    get_keywords,
    get_existing_links,
    append_unique_posts,
    get_yes_links_after_cutoff,
    get_existing_allpost_keys,
    append_all_posts,
    get_active_links_by_delta,
    get_existing_comment_ids,
    append_comments,
    get_type_criteria,
    get_issue_criteria_all,
    get_instruction,
    get_other_instruction,
    get_postid_to_group,
    append_issue_criteria,
)
from config import (
    SEARCH_ACTOR_ID,
    STATS_ACTOR_ID,
    TIKTOK_SORT_TYPE,
    COMMENT_ACTOR_ID,
    COMMENT_MAX_ITEMS,
    CLASSIFY_BATCH_SIZE,
    OTHER_ISSUE_THRESHOLD,
    OTHER_SAMPLE_SIZE,
    GEMINI_MAX_RETRIES,
    GEMINI_BASE_WAIT_SEC,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------
APIFY_TOKEN  = os.environ["APIFY_TOKEN"]
GEMINI_API   = os.environ["GEMINI_API"]

apify_client  = ApifyClient(APIFY_TOKEN)
gemini_client = genai.Client(api_key=GEMINI_API)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Gemini helpers                                                         ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def _gemini_call(prompt, *, thinking=False):
    """
    เรียก Gemini API พร้อม exponential backoff สำหรับ 429
    """
    cfg = types.GenerateContentConfig(max_output_tokens=16384)
    if thinking:
        cfg = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_level="HIGH"),
            max_output_tokens=16384,
        )
    for attempt in range(1, GEMINI_MAX_RETRIES + 1):
        try:
            resp = gemini_client.models.generate_content(
                model="gemini-3-flash-preview",
                contents=prompt,
                config=cfg,
            )
            return resp.text.strip()
        except Exception as e:
            err = str(e)
            if "429" in err or "RESOURCE_EXHAUSTED" in err:
                wait = GEMINI_BASE_WAIT_SEC * (2 ** (attempt - 1))
                m = re.search(r"retry[^0-9]*(\d+)", err, re.IGNORECASE)
                if m:
                    wait = max(wait, int(m.group(1)) + 5)
                if attempt < GEMINI_MAX_RETRIES:
                    log.warning("rate-limit 429 — waiting %ds (attempt %d/%d)",
                                wait, attempt, GEMINI_MAX_RETRIES)
                    time.sleep(wait)
                    continue
            raise


def _parse_json_array(raw):
    """
    ดึง JSON array จาก raw text ที่อาจมี markdown fence / ข้อความปน
    Returns: list (parsed JSON array)
    Raises: ValueError ถ้า parse ไม่ได้
    """
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3].strip()
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1:
        raise ValueError(f"no JSON array found in: {raw[:200]}")
    return json.loads(raw[start:end + 1])


def _parse_json_object(raw):
    """
    ดึง JSON object จาก raw text
    Returns: dict
    Raises: ValueError ถ้า parse ไม่ได้
    """
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3].strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"no JSON object found in: {raw[:200]}")
    return json.loads(raw[start:end + 1])


def _clean_text(text):
    return str(text).replace('"', "'").replace("\n", " ").replace("\r", " ").replace("\t", " ").replace("\\", " ")


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  STEP 1 — Apify TikTok Search                                          ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def search_tiktok(keyword, limit, time_range):
    run_input = {
        "keyword":     keyword,
        "limit":       limit,
        "isUnlimited": False,
        "sortType":    TIKTOK_SORT_TYPE,
        "publishTime": time_range,
    }
    log.info("[Search] '%s'  limit=%d  publishTime=%s", keyword, limit, time_range)
    run   = apify_client.actor(SEARCH_ACTOR_ID).call(run_input=run_input)
    items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
    log.info("  → got %d items", len(items))
    return items


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  STEP 3 — Gemini Relevance Labeling                                    ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def label_with_gemini(posts_to_label):
    """
    Label a chunk of posts in one Gemini call.
    Returns: dict {link: "yes" | "Non" | "fail"}
    """
    if not posts_to_label:
        return {}

    idx_to_link = {str(i): p["id"] for i, p in enumerate(posts_to_label)}

    posts_text = ""
    for i, post in enumerate(posts_to_label):
        posts_text += (
            f"---\nIDX: {i}\n"
            f"KEYWORD: {_clean_text(post['keyword_description'])}\n"
            f"VIDEO: {_clean_text(post['description'])}\n"
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
        log.info("[Gemini] labeling %d posts...", len(posts_to_label))
        raw = _gemini_call(prompt)
        results = _parse_json_array(raw)
        label_map = {}
        for item in results:
            idx = str(item.get("idx", ""))
            label = item.get("label", "Non")
            link = idx_to_link.get(idx)
            if link:
                label_map[link] = "yes" if "yes" in str(label).lower() else "Non"

        for p in posts_to_label:
            if p["id"] not in label_map:
                label_map[p["id"]] = "fail"
        return label_map

    except Exception as e:
        log.warning("labeling failed: %s — marking all as fail", e)
        return {p["id"]: "fail" for p in posts_to_label}


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  STEP 5 — Fetch Stats via Apify                                        ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def fetch_stats(links):
    run_input = {
        "customMapFunction":    "(object) => { return {...object} }",
        "dateRange":            "DEFAULT",
        "includeSearchKeywords": False,
        "location":             "US",
        "maxItems":             1000,
        "sortType":             "RELEVANCE",
        "startUrls":            links,
    }
    log.info("[Stats] running actor for %d links...", len(links))
    run   = apify_client.actor(STATS_ACTOR_ID).call(run_input=run_input)
    items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
    log.info("  → got %d results", len(items))

    stats_map = {}
    for item in items:
        link = normalize_link(str(item.get("postPage", "")))
        if link:
            stats_map[link] = {
                "likes":     int(item.get("likes", 0) or 0),
                "comments":  int(item.get("comments", 0) or 0),
                "shares":    int(item.get("shares", 0) or 0),
                "bookmarks": int(item.get("bookmarks", 0) or 0),
            }
    return stats_map


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  STEP 7 — Scrape Comments via Apify (returns in-memory rows)            ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def fetch_comments(links, scrape_date, link_to_group=None):
    """
    ดึง comment จาก Apify แล้วคืนเป็น list of dict (in-memory)
    ไม่เขียนลง sheet — จะส่งต่อไป classify ก่อน
    """
    run_input = {
        "maxItems":           COMMENT_MAX_ITEMS,
        "shouldScrapeAll":    False,
        "shouldScrapeReplies": False,
        "urls":               links,
    }
    log.info("[Comments] running actor for %d links...", len(links))
    run   = apify_client.actor(COMMENT_ACTOR_ID).call(run_input=run_input)
    items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
    log.info("  → got %d comments", len(items))

    # map aweme_id → original link
    aweme_to_link = {}
    for lnk in links:
        clean = normalize_link(lnk)
        parts = clean.split("/video/")
        if len(parts) == 2:
            aweme_to_link[parts[1].strip()] = clean

    comment_dicts = []
    for item in items:
        aweme_id = str(item.get("aweme_id", "")).strip()
        user = item.get("user", {}) or {}
        video_link = aweme_to_link.get(aweme_id, "")
        kw_group = (link_to_group or {}).get(video_link, "")

        comment_dicts.append({
            "post_id":       aweme_id,
            "cid":           str(item.get("cid", "")).strip(),
            "text":          str(item.get("text", "")).strip(),
            "comment_date":  item.get("create_time", ""),
            "digg_count":    int(item.get("digg_count", 0) or 0),
            "reply_count":   int(item.get("reply_comment_total", 0) or 0),
            "author_uid":    str(user.get("uid", "")).strip(),
            "author_id":     str(user.get("unique_id", "")).strip(),
            "author_name":   str(user.get("nickname", "")).strip(),
            "author_follower": int(user.get("follower_count", 0) or 0),
            "author_region": str(user.get("region", "")).strip(),
            "scrape_date":   scrape_date,
            "keyword_group": kw_group,
            "type_label":    "",
            "issue_labels":  "",
        })
    return comment_dicts


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  STEP 8 — Classify Comments (Phase 1) — in-memory                      ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def classify_comments_batch(batch, type_criteria, issue_criteria, instruction):
    """
    Phase 1: จัดประเภท TypeLabel + IssueLabels
    batch: list of comment dicts (ต้องมี "cid", "text")
    Returns: list of {"cid": str, "type_label": str, "issue_labels": str}
    """
    type_names  = [t["name"] for t in type_criteria] + ["Other"]
    issue_names = [i["name"] for i in issue_criteria] + ["Other"]
    valid_types  = set(type_names)
    valid_issues = set(issue_names)

    type_block = "\n".join(
        f'- {t["name"]}: {_clean_text(t["criteria"][:120])}'
        for t in type_criteria
    ) + "\n- Other: ไม่ตรงกับประเภทใด"

    issue_block = "\n".join(
        f'- {i["name"]}: {_clean_text(i["criteria"][:120])}'
        for i in issue_criteria
    ) + "\n- Other: ไม่ตรงกับประเด็นใด"

    valid_type_str  = ", ".join(f'"{n}"' for n in type_names)
    valid_issue_str = ", ".join(f'"{n}"' for n in issue_names)

    idx_to_cid = {str(i): c["cid"] for i, c in enumerate(batch)}

    comments_block = ""
    for i, c in enumerate(batch):
        comments_block += f"---\nIDX: {i}\nTEXT: {_clean_text(c['text'])}\n"
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
        log.info("[Gemini] classifying %d comments...", len(batch))
        raw = _gemini_call(prompt)
        results = _parse_json_array(raw)
        output = []
        responded_idxs = set()

        for item in results:
            idx = str(item.get("idx", ""))
            cid = idx_to_cid.get(idx)
            if not cid:
                continue
            responded_idxs.add(idx)

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

            raw_issues = item.get("issues", ["Other"])
            issue_list = []
            for ii in raw_issues:
                if isinstance(ii, int) or str(ii).strip().isdigit():
                    try:
                        issue_list.append(issue_names[int(ii)])
                    except (IndexError, ValueError):
                        issue_list.append("Other")
                else:
                    s = str(ii).strip()
                    issue_list.append(s if s in valid_issues else "Other")
            if not issue_list:
                issue_list = ["Other"]

            output.append({
                "cid":          cid,
                "type_label":   type_label,
                "issue_labels": "|".join(issue_list),
            })

        for idx, cid in idx_to_cid.items():
            if idx not in responded_idxs:
                log.debug("missing idx=%s → Other", idx)
                output.append({"cid": cid, "type_label": "Other", "issue_labels": "Other"})
        return output

    except Exception as e:
        log.warning("classify batch failed: %s", e)
        return [{"cid": c["cid"], "type_label": "Other", "issue_labels": "Other"}
                for c in batch]


def generate_issue_criteria_for_group(group, sample_comments, instruction, existing_names):
    """
    ให้ Gemini สร้าง IssueCriteria ชุดใหม่สำหรับ KeywordGroup ที่ยังไม่มี criteria
    Returns: list of {"name": str, "criteria": str}
    """
    sample = sample_comments if len(sample_comments) <= 100 else random.sample(sample_comments, 100)

    comments_block = "\n".join(
        f'- {_clean_text(c["text"])[:150]}' for c in sample
    )
    existing_str = ", ".join(existing_names) if existing_names else "ยังไม่มี"

    prompt = (
        f"คุณเป็นผู้เชี่ยวชาญด้านการวิเคราะห์ความคิดเห็น\n\n"
        f"KeywordGroup: {group}\n"
        f"ประเด็นที่มีอยู่แล้ว (ห้ามซ้ำ): {existing_str}\n\n"
        f"ตัวอย่าง comment ใน group นี้ ({len(sample)} รายการ):\n{comments_block}\n\n"
        "งาน: วิเคราะห์ comment เหล่านี้แล้วสร้างประเด็น (Issue) ที่เหมาะสม\n"
        "จำนวน: 3-8 ประเด็น ขึ้นกับความหลากหลายของ comment\n\n"
        "กฎสำคัญ:\n"
        "- ชื่อประเด็น (name) ต้องเป็นภาษาอังกฤษ single word ไม่มี space/underscore\n"
        "- ตัวอย่างชื่อที่ถูกต้อง: Transit, Burden, Trust, Zoning, Scope\n"
        "- criteria อธิบายเป็นภาษาไทย 1-2 ประโยค\n\n"
        "ตอบเป็น JSON array เท่านั้น รูปแบบ:\n"
        '[{"name":"IssueName", "criteria":"อธิบายลักษณะ comment"}]\n'
        "ห้ามอธิบายเพิ่ม ห้ามใส่ markdown"
    )

    try:
        log.info("[Gemini] generating IssueCriteria for group '%s' (%d samples)...",
                 group, len(sample))
        raw = _gemini_call(prompt)
        results = _parse_json_array(raw)
        new_criteria = [
            {"name": str(r.get("name", "")).strip(),
             "criteria": str(r.get("criteria", "")).strip()}
            for r in results
            if str(r.get("name", "")).strip()
        ]
        log.info("  generated %d issue(s): %s",
                 len(new_criteria), [c["name"] for c in new_criteria])
        return new_criteria
    except Exception as e:
        log.warning("generate criteria failed: %s", e)
        return []


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  STEP 9 — Other Issue Detection (Phase 2) — in-memory                  ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def detect_other_issues(other_comments, issue_criteria, other_instruction):
    """
    Phase 2: วิเคราะห์ IssueLabels=Other → หาประเด็นใหม่ไม่เกิน 2 ประเด็น
    Returns: (new_issues, mapping)
      new_issues: list of {"name": str, "criteria": str}
      mapping:    dict {cid: "issue_name" or "Other"}
    """
    sample = other_comments
    if len(other_comments) > OTHER_SAMPLE_SIZE:
        sample = random.sample(other_comments, OTHER_SAMPLE_SIZE)
        log.info("  sampled %d from %d Other comments", OTHER_SAMPLE_SIZE, len(other_comments))

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
        log.info("[Gemini+thinking] detecting new issues from %d comments...", len(sample))
        raw = _gemini_call(prompt, thinking=True)
        result = _parse_json_object(raw)
        new_types = result.get("new_issues", [])
        # รองรับทั้ง "new_issue" (string) และ "new_issues" (list) จาก Gemini
        mapping = {}
        for item in result.get("mapping", []):
            cid = str(item.get("id", ""))
            new_val = item.get("new_issues", item.get("new_issue", "Other"))
            if isinstance(new_val, list):
                mapping[cid] = "|".join(new_val) if new_val else "Other"
            else:
                mapping[cid] = str(new_val).strip() or "Other"
        return new_types, mapping

    except Exception as e:
        log.warning("other detection failed: %s", e)
        return [], {}


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  MAIN PIPELINE                                                         ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def main():
    run_start = time.time()

    # ── STEP 1: Keyword Search ──────────────────────────────────────────
    log.info("=" * 55)
    log.info("STEP 1: Keyword Search")
    log.info("=" * 55)

    keywords = get_keywords()
    log.info("found %d keyword(s)", len(keywords))

    all_found = {}
    for kw_info in keywords:
        items = search_tiktok(kw_info["keyword"], kw_info["limit"], kw_info["time_range"])
        for item in items:
            link = str(item.get("share_url", "")).strip()
            if not link:
                continue
            norm = normalize_link(link)
            if norm in all_found:
                continue
            author = item.get("author", {})
            video  = item.get("video", {})
            music  = item.get("added_sound_music_info", item.get("music", {}))
            raw_id = str(item.get("id", "")).strip()
            post_id = raw_id if raw_id else link.split("/video/")[-1].split("?")[0]
            all_found[norm] = {
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

    log.info("total unique links found: %d", len(all_found))

    # ── STEP 2: Unique Post Filter ──────────────────────────────────────
    log.info("=" * 55)
    log.info("STEP 2: Unique Post Filter")
    log.info("=" * 55)

    existing = get_existing_links()
    new_links = {lnk: meta for lnk, meta in all_found.items() if lnk not in existing}
    log.info("new unique links: %d", len(new_links))

    if not new_links:
        log.info("no new links — skipping to STEP 5")
    else:
        # ── STEP 3: Gemini Relevance Label ──────────────────────────────
        log.info("=" * 55)
        log.info("STEP 3: Gemini Relevance Label")
        log.info("=" * 55)

        posts_to_label = []
        label_map = {}
        for link, meta in new_links.items():
            if not meta["description"]:
                label_map[link] = "Non"
            else:
                posts_to_label.append({
                    "id":                  link,
                    "description":         meta["description"],
                    "keyword_description": meta["kw_info"]["description"],
                })

        BATCH = 50
        for i in range(0, len(posts_to_label), BATCH):
            chunk = posts_to_label[i:i + BATCH]
            log.info("  [Batch] posts %d-%d / %d", i + 1, i + len(chunk), len(posts_to_label))
            chunk_labels = label_with_gemini(chunk)
            label_map.update(chunk_labels)

        yes_count = sum(1 for v in label_map.values() if v == "yes")
        log.info("'yes' posts: %d / %d", yes_count, len(new_links))

        # ── STEP 4: Append UniquePost ───────────────────────────────────
        log.info("=" * 55)
        log.info("STEP 4: Append to UniquePost")
        log.info("=" * 55)

        rows = []
        for link, meta in new_links.items():
            use_label = label_map.get(link, "Non")
            kw_info = meta["kw_info"]
            rows.append([
                meta["create_time"], link, meta["post_id"],
                meta["author_name"], meta["author_unique_id"],
                meta["author_follower"], meta["description"],
                "",  # Transcription
                meta["video_duration"], meta["music_title"],
                use_label, kw_info["group"],
            ])
        append_unique_posts(rows)

    # ── STEP 5: Fetch Stats → AllPost (with dedup) ──────────────────────
    log.info("=" * 55)
    log.info("STEP 5: Fetch Stats → AllPost")
    log.info("=" * 55)

    yes_links_data = get_yes_links_after_cutoff()
    if not yes_links_data:
        log.info("no qualifying links for stats — done!")
        return

    # Dedup: ข้าม link ที่มี AllPost record วันนี้แล้ว
    existing_ap_keys = get_existing_allpost_keys()
    today_date_str = datetime.date.today().isoformat()
    links_to_fetch = [d for d in yes_links_data
                      if (d["link"], today_date_str) not in existing_ap_keys]
    log.info("links needing stats today: %d (skipped %d already scraped)",
             len(links_to_fetch), len(yes_links_data) - len(links_to_fetch))

    if links_to_fetch:
        fetch_links = [d["link"] for d in links_to_fetch]
        link_to_postid = {d["link"]: d["post_id"] for d in links_to_fetch}
        link_to_kwgroup = {d["link"]: d.get("keyword_group", "") for d in links_to_fetch}

        scrape_date = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        stats_map = fetch_stats(fetch_links)

        all_post_rows = []
        for link in fetch_links:
            s = stats_map.get(link, {})
            all_post_rows.append([
                link,
                link_to_postid.get(link, ""),
                s.get("likes", 0),
                s.get("comments", 0),
                s.get("shares", 0),
                s.get("bookmarks", 0),
                scrape_date,
                link_to_kwgroup.get(link, ""),
            ])
        append_all_posts(all_post_rows)
    else:
        log.info("all links already have stats today — skip fetch")

    # ── STEP 6: Delta Filter ────────────────────────────────────────────
    log.info("=" * 55)
    log.info("STEP 6: Delta Filter → active links")
    log.info("=" * 55)

    active_links_data = get_active_links_by_delta()
    if not active_links_data:
        log.info("no links passed delta filter — done!")
        return
    log.info("%d link(s) passed — proceeding to comment scrape", len(active_links_data))

    # ── STEP 7: Scrape Comments (in-memory) ─────────────────────────────
    log.info("=" * 55)
    log.info("STEP 7: Scrape Comments (in-memory)")
    log.info("=" * 55)

    active_links  = [d["link"] for d in active_links_data]
    link_to_group = {d["link"]: d.get("keyword_group", "") for d in active_links_data}
    scrape_date   = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    all_comments = fetch_comments(active_links, scrape_date, link_to_group)

    # Dedup กับ CommentID ที่มีอยู่แล้วใน sheet (ดึงเฉพาะ column)
    existing_cids = get_existing_comment_ids()
    before_count = len(all_comments)
    all_comments = [c for c in all_comments if c["cid"] not in existing_cids]
    log.info("after dedup: %d comments (removed %d duplicates)",
             len(all_comments), before_count - len(all_comments))

    if not all_comments:
        log.info("no new comments — done!")
        return

    # ── STEP 8: Classify Comments (Phase 1) — in-memory ────────────────
    log.info("=" * 55)
    log.info("STEP 8: Classify Comments (Phase 1) — in-memory")
    log.info("=" * 55)

    type_criteria      = get_type_criteria()
    issue_criteria_all = get_issue_criteria_all()
    instruction        = get_instruction()

    log.info("types loaded   : %d", len(type_criteria))
    log.info("issue groups   : %s", list(issue_criteria_all.keys()))

    # จัดกลุ่ม comment ตาม KeywordGroup
    postid_to_group_fb = get_postid_to_group()

    groups_map = {}
    for c in all_comments:
        group = c.get("keyword_group", "").strip()
        if not group and c.get("post_id"):
            group = postid_to_group_fb.get(c["post_id"], "").strip()
        if not group:
            group = "_unknown_"
        groups_map.setdefault(group, []).append(c)

    log.info("comment groups: %s", {g: len(v) for g, v in groups_map.items()})

    # Track existing issue names สำหรับตรวจซ้ำตอน generate
    all_existing_names = {
        item["name"]
        for items in issue_criteria_all.values()
        for item in items
    }

    # CID → comment dict สำหรับ lookup
    cid_to_comment = {c["cid"]: c for c in all_comments}

    for group, group_comments in groups_map.items():
        log.info("=== Group: %s (%d comments) ===", group, len(group_comments))

        issue_criteria = issue_criteria_all.get(group, [])

        # ถ้ายังไม่มี criteria → สร้างใหม่อัตโนมัติ (ข้าม _unknown_)
        if not issue_criteria:
            if group == "_unknown_":
                log.warning("group '_unknown_' — PostID missing, cannot generate criteria")
            else:
                log.info("[NEW GROUP] '%s' has no IssueCriteria — generating...", group)
                issue_criteria = generate_issue_criteria_for_group(
                    group, group_comments, instruction, all_existing_names
                )
                if issue_criteria:
                    # เขียน criteria ลง IssueCriteria sheet ทันที
                    append_issue_criteria(issue_criteria, keyword_group=group)
                    issue_criteria_all[group] = issue_criteria
                    all_existing_names.update(c["name"] for c in issue_criteria)
                else:
                    log.warning("could not generate criteria for '%s'", group)

        # Classify ทีละ batch → update in-memory comment dicts
        for i in range(0, len(group_comments), CLASSIFY_BATCH_SIZE):
            chunk = group_comments[i:i + CLASSIFY_BATCH_SIZE]
            log.info("  [Batch] %d-%d / %d", i + 1, i + len(chunk), len(group_comments))
            results = classify_comments_batch(chunk, type_criteria, issue_criteria, instruction)
            for r in results:
                if r["cid"] in cid_to_comment:
                    cid_to_comment[r["cid"]]["type_label"] = r["type_label"]
                    cid_to_comment[r["cid"]]["issue_labels"] = r["issue_labels"]

    classified_count = sum(1 for c in all_comments if c["type_label"])
    log.info("Phase 1 classified: %d / %d", classified_count, len(all_comments))

    # ── STEP 9: Other Issue Detection (Phase 2) — in-memory ────────────
    log.info("=" * 55)
    log.info("STEP 9: Other Issue Detection (Phase 2) — in-memory")
    log.info("=" * 55)

    other_instruction = get_other_instruction()

    # จัดกลุ่ม Other comments ตาม KeywordGroup
    other_by_group = {}
    for c in all_comments:
        if c.get("issue_labels") == "Other":
            grp = c.get("keyword_group", "").strip() or "_unknown_"
            other_by_group.setdefault(grp, []).append(c)

    for group, other_comments in other_by_group.items():
        if len(other_comments) <= OTHER_ISSUE_THRESHOLD:
            log.info("[%s] Other=%d ≤ %d — skip Phase 2",
                     group, len(other_comments), OTHER_ISSUE_THRESHOLD)
            continue

        log.info("[%s] Other=%d > %d — running detection...",
                 group, len(other_comments), OTHER_ISSUE_THRESHOLD)
        group_criteria = issue_criteria_all.get(group, [])
        new_issues, mapping = detect_other_issues(other_comments, group_criteria, other_instruction)

        if not new_issues:
            log.info("[%s] no new issues found", group)
            continue

        log.info("[%s] new issues: %s", group, [n["name"] for n in new_issues])

        # Update in-memory comments
        for c in other_comments:
            new_issue = mapping.get(c["cid"])
            if new_issue and new_issue != "Other":
                c["issue_labels"] = new_issue

        # เขียน new criteria ลง sheet
        append_issue_criteria(new_issues, keyword_group=group)
        issue_criteria_all.setdefault(group, []).extend(new_issues)

    # ── STEP 10: Write All Classified Comments to Sheet ─────────────────
    log.info("=" * 55)
    log.info("STEP 10: Write Classified Comments → Comments sheet")
    log.info("=" * 55)

    comment_rows = []
    for c in all_comments:
        comment_rows.append([
            c["post_id"],
            c["cid"],
            c["text"],
            c["comment_date"],
            c["digg_count"],
            c["reply_count"],
            c["author_uid"],
            c["author_id"],
            c["author_name"],
            c["author_follower"],
            c["author_region"],
            c["scrape_date"],
            c["keyword_group"],
            c["type_label"],
            c["issue_labels"],
        ])

    append_comments(comment_rows)

    elapsed = time.time() - run_start
    log.info("=" * 55)
    log.info("PIPELINE COMPLETE — %d comments written — %.1fs elapsed", len(comment_rows), elapsed)
    log.info("=" * 55)


if __name__ == "__main__":
    main()
