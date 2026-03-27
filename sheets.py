# ===== sheets.py =====
# Google Sheets helper — singleton client, cached reads, batch operations

import datetime
import logging
import gspread
import json
import os
from google.oauth2.service_account import Credentials
from config import (
    SCOPES,
    KEYWORD_SHEET_ID,
    KEYWORD_SHEET_NAME,
    KEYWORD_COL,
    KEYWORD_GROUP_COL,
    KEYWORD_DESC_COL,
    KEYWORD_SCRAPE_COL,
    KEYWORD_TIME_COL,
    RESULT_SHEET_ID,
    UNIQUE_POST_SHEET_NAME,
    ALL_POST_SHEET_NAME,
    COMMENTS_SHEET_NAME,
    CRITERIA_SHEET_ID,
    TYPE_CRITERIA_SHEET_NAME,
    ISSUE_CRITERIA_SHEET_NAME,
    INSTRUCTION_SHEET_NAME,
    OTHER_INSTRUCTION_SHEET_NAME,
    PUBLISH_LOOKBACK_DAYS,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton gspread client
# ---------------------------------------------------------------------------
_client = None

def get_client():
    global _client
    if _client is None:
        creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        _client = gspread.authorize(creds)
        log.info("gspread client authorized (singleton)")
    return _client


# ---------------------------------------------------------------------------
# Sheet-level cache (per-run)
# ---------------------------------------------------------------------------
_sheet_cache = {}

def get_sheet(spreadsheet_id, sheet_name, *, force=False):
    key = (spreadsheet_id, sheet_name)
    if force or key not in _sheet_cache:
        client = get_client()
        _sheet_cache[key] = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    return _sheet_cache[key]


def clear_cache():
    _sheet_cache.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize_link(url):
    return str(url).strip().split("?")[0].rstrip("/")


def _publish_cutoff_ts():
    cutoff_date = datetime.date.today() - datetime.timedelta(days=PUBLISH_LOOKBACK_DAYS)
    return int(datetime.datetime.combine(cutoff_date, datetime.time.min).timestamp())


def _parse_scrape_date(raw):
    raw = str(raw).strip()
    if not raw:
        return None
    try:
        return datetime.datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Header definitions
# ---------------------------------------------------------------------------
UNIQUE_POST_HEADERS = [
    "PublishDate", "Link", "PostID", "AuthorName", "AuthorUniqueID",
    "AuthorFollower", "Description", "Transcription", "VideoDuration",
    "MusicTitle", "Use", "KeywordGroup",
]

ALL_POST_HEADERS = [
    "Link", "PostID", "Like", "Comment", "Share", "Save","Views",
    "ScrapeDate", "KeywordGroup",
]

COMMENTS_HEADERS = [
    "PostID", "CommentID", "CommentText", "CommentDate", "DiggCount",
    "ReplyCount", "AuthorUID", "AuthorUniqueID", "AuthorNickname",
    "AuthorFollower", "AuthorRegion", "ScrapeDate", "KeywordGroup",
    "CommentType", "CommentIssue",
]


# ---------------------------------------------------------------------------
# Keyword Sheet
# ---------------------------------------------------------------------------
def get_keywords():
    sheet = get_sheet(KEYWORD_SHEET_ID, KEYWORD_SHEET_NAME)
    records = sheet.get_all_records()
    result = []
    for row in records:
        kw = str(row.get(KEYWORD_COL, "")).strip()
        if not kw:
            continue
        grp = str(row.get(KEYWORD_GROUP_COL, "")).strip()
        desc = str(row.get(KEYWORD_DESC_COL, "")).strip()
        try:
            limit = int(row.get(KEYWORD_SCRAPE_COL, ""))
        except (ValueError, TypeError):
            limit = 100
        time_range = str(row.get(KEYWORD_TIME_COL, "")).strip().upper() or "ALL_TIME"
        result.append({
            "keyword": kw, "group": grp, "description": desc,
            "limit": limit, "time_range": time_range,
        })
    log.info("loaded %d keyword(s)", len(result))
    return result


# ---------------------------------------------------------------------------
# UniquePost Sheet
# ---------------------------------------------------------------------------
def get_existing_links():
    sheet = get_sheet(RESULT_SHEET_ID, UNIQUE_POST_SHEET_NAME)
    records = sheet.get_all_records()
    existing = set()
    for row in records:
        link = normalize_link(row.get("Link", row.get("link", "")))
        if link:
            existing.add(link)
    log.info("existing links in UniquePost: %d", len(existing))
    return existing


def get_yes_links_after_cutoff():
    sheet = get_sheet(RESULT_SHEET_ID, UNIQUE_POST_SHEET_NAME)
    records = sheet.get_all_records()
    cutoff = _publish_cutoff_ts()
    result = []
    for row in records:
        use = str(row.get("Use", "")).strip().lower()
        link = normalize_link(row.get("Link", ""))
        post_id = str(row.get("PostID", "")).strip()
        try:
            publish_ts = int(row.get("PublishDate", 0))
        except (ValueError, TypeError):
            publish_ts = 0
        keyword_group = str(row.get("KeywordGroup", "")).strip()
        if use == "yes" and link and publish_ts > cutoff:
            result.append({"link": link, "post_id": post_id, "keyword_group": keyword_group})
    log.info("yes-links after %d-day cutoff: %d", PUBLISH_LOOKBACK_DAYS, len(result))
    return result


def get_postid_to_group():
    sheet = get_sheet(RESULT_SHEET_ID, UNIQUE_POST_SHEET_NAME)
    records = sheet.get_all_records()
    result = {}
    for row in records:
        post_id = str(row.get("PostID", "")).strip()
        group = str(row.get("KeywordGroup", "")).strip() or "_unknown_"
        if post_id:
            result[post_id] = group
    return result


def append_unique_posts(new_rows):
    sheet = get_sheet(RESULT_SHEET_ID, UNIQUE_POST_SHEET_NAME)
    header = sheet.row_values(1)
    if not header:
        sheet.append_row(UNIQUE_POST_HEADERS)
        log.info("created header in UniquePost")
    if new_rows:
        sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
        log.info("appended %d rows to UniquePost", len(new_rows))


# ---------------------------------------------------------------------------
# AllPost Sheet — with dedup (link + scrape_date[:10])
# ---------------------------------------------------------------------------
def get_existing_allpost_keys():
    sheet = get_sheet(RESULT_SHEET_ID, ALL_POST_SHEET_NAME)
    records = sheet.get_all_records()
    keys = set()
    for row in records:
        link = normalize_link(row.get("Link", ""))
        sd = str(row.get("ScrapeDate", "")).strip()[:10]
        if link and sd:
            keys.add((link, sd))
    return keys


def append_all_posts(new_rows):
    sheet = get_sheet(RESULT_SHEET_ID, ALL_POST_SHEET_NAME)
    header = sheet.row_values(1)
    if not header:
        sheet.append_row(ALL_POST_HEADERS)
        log.info("created header in AllPost")
    if new_rows:
        sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
        log.info("appended %d rows to AllPost", len(new_rows))


# ---------------------------------------------------------------------------
# AllPost delta filter
# ---------------------------------------------------------------------------
def get_active_links_by_delta():
    sheet = get_sheet(RESULT_SHEET_ID, ALL_POST_SHEET_NAME, force=True)
    records = sheet.get_all_records()

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    today_map = {}
    yesterday_map = {}
    postid_map = {}
    group_map = {}

    for row in records:
        link = normalize_link(row.get("Link", ""))
        post_id = str(row.get("PostID", "")).strip()
        scrape_date = _parse_scrape_date(row.get("ScrapeDate", ""))
        try:
            comments = int(row.get("Comment", 0))
        except (ValueError, TypeError):
            comments = 0
        if not link or scrape_date is None:
            continue
        if post_id:
            postid_map[link] = post_id
        grp = str(row.get("KeywordGroup", "")).strip()
        if grp:
            group_map[link] = grp

        if scrape_date == today:
            today_map[link] = comments
        elif scrape_date == yesterday:
            yesterday_map[link] = comments

    # fallback group from UniquePost
    missing_group_links = [l for l in postid_map if l not in group_map]
    if missing_group_links:
        try:
            pid_to_grp = get_postid_to_group()
            for link in missing_group_links:
                pid = postid_map[link]
                if pid in pid_to_grp:
                    group_map[link] = pid_to_grp[pid]
        except Exception as e:
            log.warning("fallback UniquePost lookup failed: %s", e)

    active = []
    for link, ct in today_map.items():
        if link not in yesterday_map:
            log.debug("delta: %s no yesterday → use today as delta", link[:60])
            delta = ct
        else:
            delta = ct - yesterday_map[link]

        if ct >= 10_000:
            passes = delta > 1_000
        elif ct >= 1_000:
            passes = delta > 500
        elif ct >= 100:
            passes = delta > 100
        else:
            passes = delta > 20

        status = "pass" if passes else "skip"
        log.debug("delta: %s comments=%d delta=%d → %s", link[:60], ct, delta, status)
        if passes:
            active.append({
                "link": link,
                "post_id": postid_map.get(link, ""),
                "keyword_group": group_map.get(link, ""),
            })

    log.info("active links after delta filter: %d", len(active))
    return active


# ---------------------------------------------------------------------------
# Comments Sheet — dedup by CommentID (fetch column only)
# ---------------------------------------------------------------------------
def get_existing_comment_ids():
    sheet = get_sheet(RESULT_SHEET_ID, COMMENTS_SHEET_NAME)
    header = sheet.row_values(1)
    if not header:
        return set()
    try:
        cid_col_idx = header.index("CommentID") + 1
    except ValueError:
        return set()
    col_values = sheet.col_values(cid_col_idx)
    return {str(v).strip() for v in col_values[1:] if str(v).strip()}


def append_comments(new_rows):
    sheet = get_sheet(RESULT_SHEET_ID, COMMENTS_SHEET_NAME)
    header = sheet.row_values(1)
    if not header:
        sheet.append_row(COMMENTS_HEADERS)
        log.info("created header in Comments")
    if new_rows:
        sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
        log.info("appended %d rows to Comments", len(new_rows))


# ---------------------------------------------------------------------------
# Criteria & Instruction
# ---------------------------------------------------------------------------
def get_type_criteria():
    sheet = get_sheet(CRITERIA_SHEET_ID, TYPE_CRITERIA_SHEET_NAME)
    return [
        {"name": str(r.get("NameType", "")).strip(),
         "criteria": str(r.get("CriteriaType", "")).strip()}
        for r in sheet.get_all_records()
        if str(r.get("NameType", "")).strip()
    ]


def get_issue_criteria_all():
    sheet = get_sheet(CRITERIA_SHEET_ID, ISSUE_CRITERIA_SHEET_NAME)
    result = {}
    for r in sheet.get_all_records():
        name = str(r.get("NameIssue", "")).strip()
        crit = str(r.get("CriteriaIssue", "")).strip()
        group = str(r.get("KeywordGroup", "")).strip() or "_global_"
        if name:
            result.setdefault(group, []).append({"name": name, "criteria": crit})
    return result


def get_issue_criteria(keyword_group=None):
    all_c = get_issue_criteria_all()
    if keyword_group is None:
        combined = []
        for items in all_c.values():
            combined.extend(items)
        return combined
    return all_c.get(keyword_group, [])


def get_instruction():
    sheet = get_sheet(CRITERIA_SHEET_ID, INSTRUCTION_SHEET_NAME)
    records = sheet.get_all_records()
    if records:
        return str(records[0].get("InstructionDetail", "")).strip()
    return ""


def get_other_instruction():
    sheet = get_sheet(CRITERIA_SHEET_ID, OTHER_INSTRUCTION_SHEET_NAME)
    records = sheet.get_all_records()
    if records:
        return str(records[0].get("OtherInstructionDetail", "")).strip()
    return ""


def append_issue_criteria(new_issues, keyword_group=""):
    sheet = get_sheet(CRITERIA_SHEET_ID, ISSUE_CRITERIA_SHEET_NAME)
    rows = [[item["name"], item["criteria"], keyword_group] for item in new_issues]
    if rows:
        sheet.append_rows(rows, value_input_option="USER_ENTERED")
        log.info("appended %d new issue(s) to IssueCriteria (group=%s)", len(rows), keyword_group)


# ---------------------------------------------------------------------------
# Batch update helpers (for classified comments)
# ---------------------------------------------------------------------------
def batch_update_type_and_issue(updates):
    if not updates:
        return
    sheet = get_sheet(RESULT_SHEET_ID, COMMENTS_SHEET_NAME, force=True)
    header = sheet.row_values(1)
    try:
        type_col = header.index("CommentType") + 1
        issue_col = header.index("CommentIssue") + 1
    except ValueError as e:
        log.error("column not found: %s", e)
        return

    cell_data = []
    for u in updates:
        r = u["row_index"]
        cell_data.append({"range": gspread.utils.rowcol_to_a1(r, type_col),
                          "values": [[u["type_label"]]]})
        cell_data.append({"range": gspread.utils.rowcol_to_a1(r, issue_col),
                          "values": [[u["issue_labels"]]]})

    CHUNK = 400
    for i in range(0, len(cell_data), CHUNK):
        sheet.batch_update(cell_data[i:i+CHUNK], value_input_option="USER_ENTERED")
    log.info("batch_update: %d comment(s) labeled", len(updates))


def batch_update_issue_only(updates):
    if not updates:
        return
    sheet = get_sheet(RESULT_SHEET_ID, COMMENTS_SHEET_NAME, force=True)
    header = sheet.row_values(1)
    try:
        issue_col = header.index("CommentIssue") + 1
    except ValueError as e:
        log.error("column not found: %s", e)
        return

    cell_data = [
        {"range": gspread.utils.rowcol_to_a1(u["row_index"], issue_col),
         "values": [[u["issue_labels"]]]}
        for u in updates
    ]
    CHUNK = 500
    for i in range(0, len(cell_data), CHUNK):
        sheet.batch_update(cell_data[i:i+CHUNK], value_input_option="USER_ENTERED")
    log.info("batch_update issue: %d comment(s) updated", len(updates))
