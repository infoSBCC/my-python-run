# ===== sheets.py =====
# Google Sheets helper functions

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
    UNIQUE_POST_SHEET_ID,
    UNIQUE_POST_SHEET_NAME,
)

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
                    kw    = str(row.get(KEYWORD_COL, "")).strip()
                    grp   = str(row.get(KEYWORD_GROUP_COL, "")).strip()
                    desc  = str(row.get(KEYWORD_DESC_COL, "")).strip()
                    limit_raw = row.get(KEYWORD_SCRAPE_COL, "")
                    try:
                                    limit = int(limit_raw)
except (ValueError, TypeError):
            limit = 100  # fallback ถ้าไม่มีค่าหรือค่าไม่ใช่ตัวเลข
        if kw:
                        result.append({
                                            "keyword":     kw,
                                            "group":       grp,
                                            "description": desc,
                                            "limit":       limit,
                        })
                return result

# --- UniquePost Sheet ---
def get_existing_links():
        sheet = get_sheet(UNIQUE_POST_SHEET_ID, UNIQUE_POST_SHEET_NAME)
        records = sheet.get_all_records()
        existing = set()
        for row in records:
                    link = str(row.get("Link", row.get("link", ""))).strip()
                    if link:
                                    existing.add(link)
                            return existing

def append_unique_posts(new_rows):
        """
            Append rows to UniquePost sheet.
                Each row must be a list in the same order as UNIQUE_POST_HEADERS:
                    [PublishDate, Link, AuthorName, AuthorUniqueID, AuthorFollower,
                         Description, Transcription, VideoDuration, MusicTitle, Use, keyword group]
                             """
        sheet = get_sheet(UNIQUE_POST_SHEET_ID, UNIQUE_POST_SHEET_NAME)
        header = sheet.row_values(1)

    # Write header if sheet is empty
        if not header:
                    sheet.append_row(UNIQUE_POST_HEADERS)
                    print("created header in UniquePost")

        if new_rows:
                    sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
                    print(f"appended {len(new_rows)} rows to UniquePost")
else:
        print("no new rows to append")
