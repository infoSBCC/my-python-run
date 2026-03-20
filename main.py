import pandas as pd

# Google Sheet ID
SHEET_ID = "1jOsRV1Q8BbLM1p1HJAdvJ6liK9VlG3sUX1LalDcxSks"
SHEET_NAME = "Sheet1"

# สร้าง URL สำหรับ export เป็น CSV (ไม่ต้องใช้ API Key)
url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"

# ดึงข้อมูลจาก Google Sheet มาเป็น DataFrame
df = pd.read_csv(url)

print("===== ข้อมูลจาก Google Sheet =====")
print(df)
print()
print("===== ข้อมูลเบื้องต้น =====")
print(f"จำนวนแถว: {len(df)}")
print(f"คอลัมน์: {list(df.columns)}")
print()
print(df.to_string(index=False))
