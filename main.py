# ===== main.py =====
# ไฟล์หลัก สั่งรันและเรียกใช้งาน sheets.py

from sheets import append_rows, read_sheet

# ---- เพิ่มข้อมูลใหม่ ----
new_rows = [
    ["a003", "grape", 7],
    ["a004", "tomato", 5],
]
append_rows(new_rows)

# ---- แสดงข้อมูลทั้งหมด ----
print()
print("===== ข้อมูลทั้งหมดใน sheet test =====")
df = read_sheet()
print(df)
