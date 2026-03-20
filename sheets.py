# ===== sheets.py =====
# โค้ดทุกอย่างที่เกี่ยวกับการติดต่อ Google Sheets

import gspread
import json
import os
from datetime import datetime
from google.oauth2.service_account import Credentials
from config import SHEET_ID, RAW_SHEET, SCOPES, SHEET_COLUMNS


def get_client():
    """สร้าง gspread client จาก GitHub Secret"""
    creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_sheet(sheet_name):
    """เปิด worksheet ที่ต้องการ"""
    client = get_client()
    return client.open_by_key(SHEET_ID).worksheet(sheet_name)


def ensure_header(sheet):
    """สร้าง header row ถ้ายังไม่มี"""
    existing = sheet.row_values(1)
    if not existing:
        sheet.append_row(SHEET_COLUMNS)
        print("สร้าง header แล้ว")


def write_tiktok_to_sheet(items):
    """รับ list of dict จาก Apify แล้วเขียนลง sheet raw"""
    sheet = get_sheet(RAW_SHEET)
    ensure_header(sheet)

    rows = []
    for item in items:
        stats = item.get("statistics", {})
        author = item.get("author", {})
        video  = item.get("video", {})
        music  = item.get("music", {})

        # แปลง Unix timestamp เป็นวันที่อ่านได้
        create_ts = item.get("create_time", 0)
        create_dt = datetime.utcfromtimestamp(create_ts).strftime("%Y-%m-%d %H:%M:%S") if create_ts else ""

        row = {
            "aweme_id":        item.get("aweme_id", ""),
            "desc":            item.get("desc", ""),
            "create_time":     create_dt,
            "region":          item.get("region", ""),
            "share_url":       item.get("share_url", ""),
            "author_unique_id": author.get("unique_id", ""),
            "author_nickname": author.get("nickname", ""),
            "digg_count":      stats.get("digg_count", 0),
            "comment_count":   stats.get("comment_count", 0),
            "share_count":     stats.get("share_count", 0),
            "play_count":      stats.get("play_count", 0),
            "video_duration":  video.get("duration", 0),
            "music_title":     music.get("title", ""),
        }
        rows.append([row.get(col, "") for col in SHEET_COLUMNS])

    if rows:
        sheet.append_rows(rows)
        print(f"เขียน {len(rows)} แถวลง sheet '{RAW_SHEET}' สำเร็จ")
    else:
        print("ไม่มีข้อมูลให้เขียน")
