# ===== main.py =====
# Flow:
# STEP 1 - Keyword Search: ดึง keyword จาก Google Sheet -> loop รัน Apify search actor
# STEP 2 - Unique Post: กรอง link ซ้ำ -> ดึง transcript -> ตรวจ keyword description -> append sheet

import os
from apify_client import ApifyClient
from sheets import get_keywords, get_existing_links, append_unique_posts
from config import (
    SEARCH_ACTOR_ID, TRANSCRIPT_ACTOR_ID,
    TIKTOK_LIMIT, TIKTOK_REGION, TIKTOK_SORT_TYPE, TIKTOK_PUBLISH_TIME,
)

# --- โหลด Apify API Token จาก GitHub Secret ---
APIFY_TOKEN = os.environ["APIFY_TOKEN"]
client = ApifyClient(APIFY_TOKEN)


# ─────────────────────────────────────────────
# STEP 1 helpers
# ─────────────────────────────────────────────

def search_tiktok(keyword):
        """
            รัน novi/advanced-search-tiktok-api สำหรับ keyword เดียว
                JSON input ตาม doc:
                      keyword      : str   - คำค้นหา
                            limit        : int   - จำนวน result สูงสุด
                                  isUnlimited  : bool  - ดึงทั้งหมด (ช้า) ใช้ False
                                        region       : str   - รหัสประเทศ 2 ตัวอักษร เช่น TH
                                              sortType     : int   - 0=Relevance, 1=Most Liked, 2=Most Recent
                                                    publishTime  : str   - ALL_TIME | YESTERDAY | WEEK | MONTH | THREE_MONTH | SIX_MONTH
                                                        """
        run_input = {
            "keyword": keyword,
            "limit": TIKTOK_LIMIT,
            "isUnlimited": False,
            "region": TIKTOK_REGION,
            "sortType": TIKTOK_SORT_TYPE,
            "publishTime": TIKTOK_PUBLISH_TIME,
        }
        print(f"  [Search] keyword='{keyword}' | region={TIKTOK_REGION} | publishTime={TIKTOK_PUBLISH_TIME}")
        run = client.actor(SEARCH_ACTOR_ID).call(run_input=run_input)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"           ได้ {len(items)} รายการ")
        return items


# ─────────────────────────────────────────────
# STEP 2 helpers
# ─────────────────────────────────────────────

def get_transcript(tiktok_url):
        """
            รัน sian.agency/best-tiktok-ai-transcript-extractor สำหรับ link เดียว
                JSON input ตาม doc:
                      tiktokUrl : str - URL ของวิดีโอ TikTok (รองรับทุก format)
                          Output field ที่ใช้: "transcript" (AI-generated full transcript text)
                              """
        run_input = {
            "tiktokUrl": tiktok_url,
        }
        try:
                    run = client.actor(TRANSCRIPT_ACTOR_ID).call(run_input=run_input)
                    results = list(client.dataset(run["defaultDatasetId"]).iterate_items())
                    if results:
                                    transcript = str(results[0].get("transcript", "")).strip()
                                    return transcript
        except Exception as e:
                    print(f"     [warn] ดึง transcript ไม่สำเร็จ: {e}")
                return ""


def is_related(transcript, keyword_description):
        """
            ตรวจสอบว่า transcript เกี่ยวข้องกับ keyword description หรือไม่
                แยก description ด้วย comma/space แล้ว match อย่างน้อย 1 คำ
                    """
    if not transcript or not keyword_description:
                return False
            transcript_lower = transcript.lower()
    terms = [t.strip().lower() for t in keyword_description.replace(",", " ").split() if t.strip()]
    return any(t in transcript_lower for t in terms)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
        # ══════════════════════════════════════════
        print("=" * 55)
    print("STEP 1: Keyword Search")
    print("=" * 55)

    keywords = get_keywords()
    print(f"พบ {len(keywords)} keyword(s) ใน sheet\n")

    # loop ค้นหาทีละ keyword รวม link ทั้งหมด
    # all_found_links: {link: {"group": ..., "keyword": ..., "description": ...}}
    all_found_links = {}

    for kw_info in keywords:
                items = search_tiktok(kw_info["keyword"])
                for item in items:
                                link = str(item.get("share_url", "")).strip()
                                if link and link not in all_found_links:
                                                    all_found_links[link] = kw_info

                        print(f"\nรวม link ที่ค้นพบทั้งหมด: {len(all_found_links)} รายการ")

    # ══════════════════════════════════════════
    print()
    print("=" * 55)
    print("STEP 2: Unique Post")
    print("=" * 55)

    # ดึง link ที่มีอยู่แล้วใน UniquePost sheet
    existing_links = get_existing_links()
    print(f"Link ที่มีอยู่ใน UniquePost แล้ว: {len(existing_links)} รายการ")

    # กรองเฉพาะ link ใหม่ที่ไม่ซ้ำ
    new_links = {link: info for link, info in all_found_links.items()
                                  if link not in existing_links}
    print(f"Link ใหม่ที่ไม่ซ้ำ: {len(new_links)} รายการ\n")

    if not new_links:
                print("ไม่มี link ใหม่ เสร็จสิ้น!")
        return

    # ดึง transcript ทีละ link แล้วกรองตาม keyword description
    rows_to_append = []

    for link, info in new_links.items():
                print(f"  [Transcript] {link}")
        transcript = get_transcript(link)

        if not transcript:
                        print("               ไม่มี transcript -> ข้าม")
                        continue

        if is_related(transcript, info["description"]):
                        print(f"               ✓ เกี่ยวข้องกับ '{info['description']}' -> เพิ่มลง UniquePost")
                        rows_to_append.append([
                            info["group"],        # keyword_group
                            info["keyword"],      # keyword
                            link,                 # link
                            transcript,           # transcript
                        ])
else:
            print(f"               ✗ ไม่เกี่ยวข้อง -> ข้าม")

    # append ลง sheet UniquePost
    print(f"\nกำลัง append {len(rows_to_append)} แถวลง UniquePost...")
    append_unique_posts(rows_to_append)

    print()
    print("เสร็จสิ้น!")


if __name__ == "__main__":
        main()
