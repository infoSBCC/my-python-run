# ===== config.py =====
# แก้ค่าที่นี่ที่เดียว ไม่ต้องแตะไฟล์อื่น

# ---------- Google Sheets ----------
SHEET_ID   = "1jOsRV1Q8BbLM1p1HJAdvJ6liK9VlG3sUX1LalDcxSks"
RAW_SHEET  = "raw"          # sheet ที่เก็บ raw data จาก TikTok
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------- Apify / TikTok Actor ----------
ACTOR_ID   = "novi/advanced-search-tiktok-api"

# --- ปรับค่า search ตรงนี้ ---
TIKTOK_KEYWORD     = "ภาษี รถติด"   # คำค้นหา
TIKTOK_LIMIT       = 50        # จำนวน video สูงสุด
TIKTOK_REGION      = "TH"      # รหัสประเทศ เช่น TH, US, GB
TIKTOK_SORT_TYPE   = 1         # 0=Relevance, 1=Most Liked, 2=Most Recent
TIKTOK_PUBLISH_TIME = "ALL_TIME"   # ALL_TIME, YESTERDAY, WEEK, MONTH, THREE_MONTH, SIX_MONTH

# ---------- คอลัมน์ที่บันทึกลง Google Sheet ----------
# เลือกเฉพาะ field ที่อยากเก็บจาก output ของ Actor
SHEET_COLUMNS = [
    "TIKTOK_KEYWORD"
    "aweme_id",        # Video ID
    "desc",            # Caption + hashtags
    "create_time",     # Unix timestamp
    "region",          # ประเทศที่อัปโหลด
    "share_url",       # URL ของวิดีโอ
    "author_unique_id", # @username
    "author_nickname", # ชื่อแสดง
    "digg_count",      # ยอดไลก์
    "comment_count",   # ยอดคอมเมนต์
    "share_count",     # ยอดแชร์
    "play_count",      # ยอดวิว
    "video_duration",  # ความยาว (ms)
    "music_title",     # ชื่อเพลง
]
