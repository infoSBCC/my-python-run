# ===== main.py =====
# ไฟล์หลัก: รัน Apify Actor -> เก็บผลลง Google Sheet

import os
from apify_client import ApifyClient
from sheets import write_tiktok_to_sheet
from config import (
    ACTOR_ID,
    TIKTOK_KEYWORD,
    TIKTOK_LIMIT,
    TIKTOK_REGION,
    TIKTOK_SORT_TYPE,
    TIKTOK_PUBLISH_TIME,
)

# --- โหลด Apify API Token จาก GitHub Secret ---
APIFY_TOKEN = os.environ["APIFY_TOKEN"]

# --- ตั้งค่า input สำหรับ Actor ---
run_input = {
    "keyword":     TIKTOK_KEYWORD,
    "limit":       TIKTOK_LIMIT,
    "region":      TIKTOK_REGION,
    "sortType":    TIKTOK_SORT_TYPE,
    "publishTime": TIKTOK_PUBLISH_TIME,
    "isUnlimited": False,
}

print(f"กำลัง run Actor: {ACTOR_ID}")
print(f"ค้นหา: '{TIKTOK_KEYWORD}' | Region: {TIKTOK_REGION} | Limit: {TIKTOK_LIMIT}")
print()

# --- Run Actor และรอผล ---
client = ApifyClient(APIFY_TOKEN)
run = client.actor(ACTOR_ID).call(run_input=run_input)

# --- ดึงผลลัพธ์ ---
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
print(f"ได้ข้อมูล {len(items)} รายการ")

# --- เขียนลง Google Sheet ---
write_tiktok_to_sheet(items)

print()
print("เสร็จสิ้น!")
