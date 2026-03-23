# ===== config.py =====

# ---------- Google Sheets ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Sheet 1: Keyword Sheet (ดึง keyword มาค้นหา)
KEYWORD_SHEET_ID   = "14t4EbZ42IkmD-21VfVueuAVdUr67_v159FQLvknbtnQ"
KEYWORD_SHEET_NAME = "keyword"              # ชื่อ sheet ที่เก็บ keyword
KEYWORD_COL        = "Keyword Search"       # column keyword ที่ใช้ค้นหา
KEYWORD_GROUP_COL  = "KeywordGroup"         # column ที่ใช้ติด label ใน post
KEYWORD_DESC_COL   = "Keyword Description"  # column คำอธิบาย (ใช้ตรวจสอบ transcript)
KEYWORD_SCRAPE_COL = "TotalScrape"          # column จำนวน video สูงสุดต่อ keyword
KEYWORD_TIME_COL   = "TimeRange"            # column ช่วงเวลาย้อนหลัง (เช่น ALL_TIME, WEEK)

# Sheet 2: UniquePost Sheet
UNIQUE_POST_SHEET_ID   = "1eyN3iREZD068lBSgZy1OzrCILxVlSKio_EJHF2AhnF0"
UNIQUE_POST_SHEET_NAME = "UniquePost"

# Sheet 3: AllPost Sheet (ผลลัพธ์ stats ของคลิป)
ALL_POST_SHEET_ID   = "1eyN3iREZD068lBSgZy1OzrCILxVlSKio_EJHF2AhnF0"  # ใส่ Sheet ID ที่ถูกต้อง
ALL_POST_SHEET_NAME = "AllPost"

# ---------- Apify / TikTok Actors ----------
SEARCH_ACTOR_ID = "novi/advanced-search-tiktok-api"
STATS_ACTOR_ID  = "apidojo/tiktok-scraper"   # ใส่ actor ID สำหรับดึง stats

# ---------- Search Config ----------
TIKTOK_SORT_TYPE = 0  # 0=Relevance, 1=Most Liked, 2=Most Recent

# ---------- Filter Config ----------
PUBLISH_DATE_CUTOFF = 1735689600  # Jan 1, 2025 00:00:00 UTC (= หลัง 31 Dec 2024)

# Sheet 4: Comments Sheet
COMMENTS_SHEET_ID   = "1eyN3iREZD068lBSgZy1OzrCILxVlSKio_EJHF2AhnF0"  # ใส่ Sheet ID ที่ถูกต้อง
COMMENTS_SHEET_NAME = "Comments"

# ---------- Comment Scraper ----------
COMMENT_ACTOR_ID    = "xtdata/tiktok-comment-scraper"
COMMENT_MAX_ITEMS   = 500

# ---------- Criteria & Instruction Sheets (all in Keyword Sheet Spreadsheet) ----------
CRITERIA_SHEET_ID         = "14t4EbZ42IkmD-21VfVueuAVdUr67_v159FQLvknbtnQ"  # same as KEYWORD_SHEET_ID
TYPE_CRITERIA_SHEET_NAME  = "TypeCriteria"
ISSUE_CRITERIA_SHEET_NAME = "IssueCriteria"
INSTRUCTION_SHEET_NAME    = "Instruction"
OTHER_INSTRUCTION_SHEET_NAME = "OtherInstruction"

# ---------- Classify Config ----------
OTHER_ISSUE_THRESHOLD = 100   # trigger Phase 2 ถ้า IssueLabels="Other" เกินนี้
OTHER_SAMPLE_SIZE     = 500   # ส่ง Claude ไม่เกินนี้ (sample ถ้าเกิน)
CLASSIFY_BATCH_SIZE   = 100   # classify ทีละกี่ comment ต่อ 1 Claude call
