# ===== config.py =====
# Central configuration for TikTok Social Listening Pipeline

# ---------- Google Sheets ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Sheet 1: Keyword Sheet (ดึง keyword มาค้นหา)
KEYWORD_SHEET_ID   = "14t4EbZ42IkmD-21VfVueuAVdUr67_v159FQLvknbtnQ"
KEYWORD_SHEET_NAME = "keyword"
KEYWORD_COL        = "Keyword Search"
KEYWORD_GROUP_COL  = "KeywordGroup"
KEYWORD_DESC_COL   = "Keyword Description"
KEYWORD_SCRAPE_COL = "TotalScrape"
KEYWORD_TIME_COL   = "TimeRange"

# Sheet 2-4: Result Sheets (อยู่ใน spreadsheet เดียวกัน)
RESULT_SHEET_ID        = "1eyN3iREZD068lBSgZy1OzrCILxVlSKio_EJHF2AhnF0"
UNIQUE_POST_SHEET_NAME = "UniquePost"
ALL_POST_SHEET_NAME    = "AllPost"
COMMENTS_SHEET_NAME    = "Comments"

# Sheet 5: Criteria & Instruction (อยู่ใน Keyword Spreadsheet)
CRITERIA_SHEET_ID            = KEYWORD_SHEET_ID
TYPE_CRITERIA_SHEET_NAME     = "TypeCriteria"
ISSUE_CRITERIA_SHEET_NAME    = "IssueCriteria"
INSTRUCTION_SHEET_NAME       = "Instruction"
OTHER_INSTRUCTION_SHEET_NAME = "OtherInstruction"

# ---------- Apify / TikTok Actors ----------
SEARCH_ACTOR_ID  = "novi/advanced-search-tiktok-api"
STATS_ACTOR_ID   = "apidojo/tiktok-scraper"
COMMENT_ACTOR_ID = "xtdata/tiktok-comment-scraper"

# ---------- Search Config ----------
TIKTOK_SORT_TYPE = 0  # 0=Relevance, 1=Most Liked, 2=Most Recent

# ---------- Filter Config ----------
# PublishDate: ดึงเฉพาะ video ที่เผยแพร่ภายใน N วันย้อนหลังจากวันที่ run
PUBLISH_LOOKBACK_DAYS = 7

# ---------- Comment Scraper ----------
COMMENT_MAX_ITEMS = 500

# ---------- Classify Config ----------
CLASSIFY_BATCH_SIZE   = 50    # classify ทีละกี่ comment ต่อ 1 Gemini call
OTHER_ISSUE_THRESHOLD = 100   # trigger Phase 2 ถ้า IssueLabels="Other" เกินนี้
OTHER_SAMPLE_SIZE     = 500   # ส่ง Gemini ไม่เกินนี้ (sample ถ้าเกิน)

# ---------- Retry / Backoff ----------
GEMINI_MAX_RETRIES    = 5
GEMINI_BASE_WAIT_SEC  = 15    # exponential backoff: base * 2^attempt
