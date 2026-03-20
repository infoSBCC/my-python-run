import pandas as pd
import gspread
import json
import os
from google.oauth2.service_account import Credentials

# โหลด credentials จาก GitHub Secret
creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
creds_dict = json.loads(creds_json)

scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)

# Google Sheet ID
SHEET_ID = "1jOsRV1Q8BbLM1p1HJAdvJ6liK9VlG3sUX1LalDcxSks"
sheet = client.open_by_key(SHEET_ID).sheet1

# ดึงข้อมูลมาเป็น DataFrame
data = sheet.get_all_records()
df = pd.DataFrame(data)

print("===== ข้อมูลจาก Google Sheet =====")
print(df)
print()
print(f"จำนวนแถว: {len(df)}, คอลัมน์: {list(df.columns)}")
