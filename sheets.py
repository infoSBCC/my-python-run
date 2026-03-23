# ===== sheets.py =====
# Google Sheets helper functions

import datetime
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
    UNIQUE_POST_SHEET_ID,
    UNIQUE_POST_SHEET_NAME,
    ALL_POST_SHEET_ID,
    ALL_POST_SHEET_NAME,
    PUBLISH_DATE_CUTOFF,
    COMMENTS_SHEET_ID,
    COMMENTS_SHEET_NAME,
    CRITERIA_SHEET_ID,
    TYPE_CRITERIA_SHEET_NAME,
    ISSUE_CRITERIA_SHEET_NAME,
    INSTRUCTION_SHEET_NAME,
    OTHER_INSTRUCTION_SHEET_NAME,
)


def _normalize_link(url):
    """ตัด query string ออก เหลือแค่ส่วนก่อน ?"""
    return str(url).strip().split("?")[0].rstrip("/")


# UniquePost sheet header columns (must match Google Sheet exactly)
UNIQUE_POST_HEADERS = [
    "PublishDate",
    "Link",
    "AuthorName",
    "AuthorUniqueID",
    "AuthorFollower",
    "Description",
    "Transcription",
    "VideoDuration",
    "MusicTitle",
    "Use",
    "keyword group",
]

# AllPost sheet header columns (must match Google Sheet exactly)
ALL_POST_HEADERS = [
    "Link",
    "Like",
    "Comment",
    "Share",
    "Save",
    "ScrapeDate",
    "TypeLabel",    # ประเภทความคิดเห็น — ว่างถ้ายังไม่จัด
    "IssueLabels",  # ประเด็น คั่นด้วย | หรือ "Other" — ว่างถ้ายังไม่จัด
]


def get_client():
    creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_sheet(spreadsheet_id, sheet_name):
    client = get_client()
    return client.open_by_key(spreadsheet_id).worksheet(sheet_name)


# --- Keyword Sheet ---
def get_keywords():
    sheet = get_sheet(KEYWORD_SHEET_ID, KEYWORD_SHEET_NAME)
    records = sheet.get_all_records()
    result = []
    for row in records:
        kw        = str(row.get(KEYWORD_COL, "")).strip()
        grp       = str(row.get(KEYWORD_GROUP_COL, "")).strip()
        desc      = str(row.get(KEYWORD_DESC_COL, "")).strip()
        limit_raw = row.get(KEYWORD_SCRAPE_COL, "")
        try:
            limit = int(limit_raw)
        except (ValueError, TypeError):
            limit = 100
        time_range = str(row.get(KEYWORD_TIME_COL, "")).strip().upper()
        if not time_range:
            time_range = "ALL_TIME"
        if kw:
            result.append({
                "keyword":     kw,
                "group":       grp,
                "description": desc,
                "limit":       limit,
                "time_range":  time_range,
            })
    return result


# --- UniquePost Sheet ---
def get_existing_links():
    sheet = get_sheet(UNIQUE_POST_SHEET_ID, UNIQUE_POST_SHEET_NAME)
    records = sheet.get_all_records()
    existing = set()
    for row in records:
        link = _normalize_link(row.get("Link", row.get("link", "")))
        if link:
            existing.add(link)
    return existing


def get_yes_links_after_cutoff():
    """
    ดึง Link จาก UniquePost ที่ Use = "yes" และ PublishDate > PUBLISH_DATE_CUTOFF
    Returns: list of link strings
    """
    sheet = get_sheet(UNIQUE_POST_SHEET_ID, UNIQUE_POST_SHEET_NAME)
    records = sheet.get_all_records()
    result = []
    for row in records:
        use  = str(row.get("Use", "")).strip().lower()
        link = _normalize_link(row.get("Link", ""))
        try:
            publish_ts = int(row.get("PublishDate", 0))
        except (ValueError, TypeError):
            publish_ts = 0
        if use == "yes" and link and publish_ts > PUBLISH_DATE_CUTOFF:
            result.append(link)
    print(f"  found {len(result)} yes-links after cutoff")
    return result


def append_unique_posts(new_rows):
    """
    Append rows to UniquePost sheet.
    Each row: [PublishDate, Link, AuthorName, AuthorUniqueID, AuthorFollower,
               Description, Transcription, VideoDuration, MusicTitle, Use, keyword group]
    """
    sheet = get_sheet(UNIQUE_POST_SHEET_ID, UNIQUE_POST_SHEET_NAME)
    header = sheet.row_values(1)
    if not header:
        sheet.append_row(UNIQUE_POST_HEADERS)
        print("created header in UniquePost")
    if new_rows:
        sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
        print(f"appended {len(new_rows)} rows to UniquePost")
    else:
        print("no new rows to append")


# --- AllPost Sheet ---
def append_all_posts(new_rows):
    """
    Append rows to AllPost sheet.
    Each row: [Link, Like, Comment, Share, Save, ScrapeDate]
    """
    sheet = get_sheet(ALL_POST_SHEET_ID, ALL_POST_SHEET_NAME)
    header = sheet.row_values(1)
    if not header:
        sheet.append_row(ALL_POST_HEADERS)
        print("created header in AllPost")
    if new_rows:
        sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
        print(f"appended {len(new_rows)} rows to AllPost")
    else:
        print("no new rows to append to AllPost")


# --- AllPost delta filter ---
def get_active_links_by_delta():
    """
    อ่าน AllPost ทั้งหมด แล้วเปรียบเทียบ Comment วันนี้ vs เมื่อวาน (by Link)
    คืน list of links ที่ผ่าน tier threshold ดังนี้:
      Tier 1: comments >= 10,000  → delta > 1,000
      Tier 2: 1,000 <= comments < 10,000  → delta > 500
      Tier 3: 100   <= comments < 1,000   → delta > 100
      Tier 4: comments < 100              → delta > 20
    link ที่ไม่มีข้อมูลเมื่อวาน → ข้าม
    """
    sheet   = get_sheet(ALL_POST_SHEET_ID, ALL_POST_SHEET_NAME)
    records = sheet.get_all_records()

    today_str     = datetime.date.today().isoformat()            # "2026-03-23"
    yesterday_str = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

    # แยก records ตามวันที่ scrape
    today_map     = {}   # link → comment count (วันนี้)
    yesterday_map = {}   # link → comment count (เมื่อวาน)

    for row in records:
        link        = str(row.get("Link", "")).strip()
        scrape_raw  = str(row.get("ScrapeDate", "")).strip()   # "2026-03-23 06:54:59 UTC"
        try:
            comments = int(row.get("Comment", 0))
        except (ValueError, TypeError):
            comments = 0

        if not link or not scrape_raw:
            continue

        scrape_date = scrape_raw[:10]   # ตัดเอาแค่ "YYYY-MM-DD"

        if scrape_date == today_str:
            today_map[link] = comments
        elif scrape_date == yesterday_str:
            yesterday_map[link] = comments

    # คำนวณ delta และตรวจ tier
    active_links = []
    for link, comments_today in today_map.items():
        if link not in yesterday_map:
            continue   # ไม่มีข้อมูลเมื่อวาน → ข้ามไป

        delta = comments_today - yesterday_map[link]

        if comments_today >= 10_000:
            passes = delta > 1_000
        elif comments_today >= 1_000:
            passes = delta > 500
        elif comments_today >= 100:
            passes = delta > 100
        else:
            passes = delta > 20

        if passes:
            active_links.append(link)
            print(f"  [delta] {link[:60]}... comments={comments_today}  delta={delta}  -> pass")
        else:
            print(f"  [delta] {link[:60]}... comments={comments_today}  delta={delta}  -> skip")

    print(f"  active links: {len(active_links)}")
    return active_links


# Comments sheet header columns (must match Google Sheet exactly)
COMMENTS_HEADERS = [
    "VideoLink",         # key สำหรับ join กับ AllPost / UniquePost
    "CommentID",
    "CommentText",
    "CommentDate",       # unix timestamp ของ comment
    "DiggCount",         # likes บน comment
    "ReplyCount",
    "AuthorUID",
    "AuthorUniqueID",    # @username
    "AuthorNickname",
    "AuthorFollower",
    "AuthorRegion",
    "ScrapeDate",        # เวลาที่ run actor
]


# --- Comments Sheet ---
def append_comments(new_rows):
    """
    Append rows to Comments sheet.
    Each row: [VideoLink, CommentID, CommentText, CommentDate, DiggCount,
               ReplyCount, AuthorUID, AuthorUniqueID, AuthorNickname,
               AuthorFollower, AuthorRegion, ScrapeDate]
    """
    sheet = get_sheet(COMMENTS_SHEET_ID, COMMENTS_SHEET_NAME)
    header = sheet.row_values(1)
    if not header:
        sheet.append_row(COMMENTS_HEADERS)
        print("created header in Comments")
    if new_rows:
        sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
        print(f"appended {len(new_rows)} rows to Comments")
    else:
        print("no comment rows to append")


# --- Criteria & Instruction ---
def get_type_criteria():
    """คืน list of {"name": str, "criteria": str}"""
    sheet = get_sheet(CRITERIA_SHEET_ID, TYPE_CRITERIA_SHEET_NAME)
    return [
        {"name": str(r.get("NameType", "")).strip(),
         "criteria": str(r.get("CriteriaType", "")).strip()}
        for r in sheet.get_all_records()
        if str(r.get("NameType", "")).strip()
    ]


def get_issue_criteria():
    """คืน list of {"name": str, "criteria": str}"""
    sheet = get_sheet(CRITERIA_SHEET_ID, ISSUE_CRITERIA_SHEET_NAME)
    return [
        {"name": str(r.get("NameIssue", "")).strip(),
         "criteria": str(r.get("CriteriaIssue", "")).strip()}
        for r in sheet.get_all_records()
        if str(r.get("NameIssue", "")).strip()
    ]


def get_instruction():
    """คืน instruction string จาก Instruction sheet (cell A2 row แรกของข้อมูล)"""
    sheet = get_sheet(CRITERIA_SHEET_ID, INSTRUCTION_SHEET_NAME)
    records = sheet.get_all_records()
    if records:
        return str(records[0].get("InstructionDetail", "")).strip()
    return ""


def get_other_instruction():
    """คืน instruction string จาก OtherInstruction sheet"""
    sheet = get_sheet(CRITERIA_SHEET_ID, OTHER_INSTRUCTION_SHEET_NAME)
    records = sheet.get_all_records()
    if records:
        return str(records[0].get("OtherInstructionDetail", "")).strip()
    return ""


def append_issue_criteria(new_issues):
    """
    Append ประเด็นใหม่ไปที่ IssueCriteria sheet
    new_issues: list of {"name": str, "criteria": str}
    """
    sheet = get_sheet(CRITERIA_SHEET_ID, ISSUE_CRITERIA_SHEET_NAME)
    rows = [[item["name"], item["criteria"]] for item in new_issues]
    if rows:
        sheet.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"  appended {len(rows)} new issue(s) to IssueCriteria")


# --- Comments sheet label operations ---
def get_unlabeled_comments():
    """
    คืน list of {"row_index": int, "cid": str, "text": str}
    สำหรับ comment ที่ TypeLabel ว่าง (ยังไม่ถูกจัด)
    row_index คือ row จริงใน sheet (1-based, นับรวม header)
    """
    sheet = get_sheet(COMMENTS_SHEET_ID, COMMENTS_SHEET_NAME)
    records = sheet.get_all_records()
    result = []
    for i, row in enumerate(records):
        type_label = str(row.get("TypeLabel", "")).strip()
        if not type_label:
            result.append({
                "row_index": i + 2,  # +2 เพราะ header อยู่แถวที่ 1
                "cid":  str(row.get("CommentID", "")).strip(),
                "text": str(row.get("CommentText", "")).strip(),
            })
    print(f"  unlabeled comments: {len(result)}")
    return result


def get_other_issue_comments():
    """
    คืน list of {"row_index": int, "cid": str, "text": str}
    สำหรับ comment ที่ IssueLabels = "Other"
    """
    sheet = get_sheet(COMMENTS_SHEET_ID, COMMENTS_SHEET_NAME)
    records = sheet.get_all_records()
    result = []
    for i, row in enumerate(records):
        issue_label = str(row.get("IssueLabels", "")).strip()
        if issue_label == "Other":
            result.append({
                "row_index": i + 2,
                "cid":  str(row.get("CommentID", "")).strip(),
                "text": str(row.get("CommentText", "")).strip(),
            })
    print(f"  IssueLabels=Other comments: {len(result)}")
    return result


def batch_update_type_and_issue(updates):
    """
    อัปเดต TypeLabel และ IssueLabels ใน Comments sheet พร้อมกัน
    updates: list of {"row_index": int, "type_label": str, "issue_labels": str}
    ใช้ batch_update เพื่อลด API call
    """
    if not updates:
        return
    sheet = get_sheet(COMMENTS_SHEET_ID, COMMENTS_SHEET_NAME)

    # หา column index ของ TypeLabel และ IssueLabels
    header = sheet.row_values(1)
    try:
        type_col  = header.index("TypeLabel")  + 1  # 1-based
        issue_col = header.index("IssueLabels") + 1
    except ValueError as e:
        print(f"  [error] column not found: {e}")
        return

    cell_data = []
    for u in updates:
        r = u["row_index"]
        cell_data.append({"range": gspread.utils.rowcol_to_a1(r, type_col),
                          "values": [[u["type_label"]]]})
        cell_data.append({"range": gspread.utils.rowcol_to_a1(r, issue_col),
                          "values": [[u["issue_labels"]]]})

    sheet.batch_update(cell_data, value_input_option="USER_ENTERED")
    print(f"  batch_update: {len(updates)} comment(s) labeled")


def batch_update_issue_only(updates):
    """
    อัปเดตเฉพาะ IssueLabels ใน Comments sheet (ใช้ตอน Phase 2)
    updates: list of {"row_index": int, "issue_labels": str}
    """
    if not updates:
        return
    sheet = get_sheet(COMMENTS_SHEET_ID, COMMENTS_SHEET_NAME)
    header = sheet.row_values(1)
    try:
        issue_col = header.index("IssueLabels") + 1
    except ValueError as e:
        print(f"  [error] column not found: {e}")
        return

    cell_data = [
        {"range": gspread.utils.rowcol_to_a1(u["row_index"], issue_col),
         "values": [[u["issue_labels"]]]}
        for u in updates
    ]
    sheet.batch_update(cell_data, value_input_option="USER_ENTERED")
    print(f"  batch_update issue: {len(updates)} comment(s) updated")
