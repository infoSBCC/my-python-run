# ===== config.py =====

# ---------- Google Sheets ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Sheet 1: Keyword Sheet (ดึง keyword มาค้นหา)
KEYWORD_SHEET_ID   = "14t4EbZ42IkmD-21VfVueuAVdUr67_v159FQLvknbtnQ"
KEYWORD_SHEET_NAME = "keyword"               # ชื่อ sheet ที่เก็บ keyword
KEYWORD_COL        = "Keyword Search"        # column keyword ที่ใช้ค้นหา
KEYWORD_GROUP_COL  = "KeywordGroup"         # column ที่ใช้ติด label ใน post
KEYWORD_DESC_COL   = "Keyword Description"   # column คำอธิบาย (ใช้ตรวจสอบ transcript)

# Sheet 2: UniquePost Sheet
UNIQUE_POST_SHEET_ID   = "1eyN3iREZD068lBSgZy1OzrCILxVlSKio_EJHF2AhnF0"
UNIQUE_POST_SHEET_NAME = "UniquePost"        # ชื่อ sheet ที่เก็บ unique post

# ---------- Apify / TikTok Actors ----------
SEARCH_ACTOR_ID     = "novi/advanced-search-tiktok-api"
TRANSCRIPT_ACTOR_ID = "sian.agency/best-tiktok-ai-transcript-extractor"

# ---------- Search Config ----------
TIKTOK_LIMIT      = 100     # จำนวน video สูงสุดต่อ keyword
TIKTOK_SORT_TYPE  = 0      # 0=Relevance, 1=Most Liked, 2=Most Recent
TIKTOK_PUBLISH_TIME = "WEEK"  # ย้อนหลัง 1 สัปดาห์ (~6 วัน) = WEEK
