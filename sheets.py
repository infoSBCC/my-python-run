# ===== sheets.py =====
# โค้ดทุกอย่างที่เกี่ยวกับการติดต่อ Google Sheets

import gspread
import json
import os
import pandas as pd
from google.oauth2.service_account import Credentials
from config import SHEET_ID, SHEET_NAME, SCOPES


def get_client():
    """สร้าง gspread client จาก GitHub Secret"""
    creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_sheet(sheet_name=None):
    """เปิด worksheet ที่ต้องการ"""
    client = get_client()
    name = sheet_name or SHEET_NAME
    return client.open_by_key(SHEET_ID).worksheet(name)


def read_sheet(sheet_name=None):
    """ดึงข้อมูลทั้งหมดจาก sheet มาเป็น DataFrame"""
    sheet = get_sheet(sheet_name)
    data = sheet.get_all_records()
    return pd.DataFrame(data)


def append_rows(rows, sheet_name=None):
    """เพิ่มแถวใหม่ต่อท้าย sheet"""
    sheet = get_sheet(sheet_name)
    sheet.append_rows(rows)
    print(f"เพิ่ม {len(rows)} แถวสำเร็จ")
