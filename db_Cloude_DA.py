# -*- coding: utf-8 -*-
"""
db_Cloude_DA.py — Truy vấn dữ liệu sách bằng SQLite
Dữ liệu nguồn: data_source/books_data.csv
Database lưu tại: data_source/books.db
"""

import pandas as pd
import sqlite3
from tabulate import tabulate

# --------------------------------------------------
# PHẦN 1: ĐỌC CSV VÀ NẠP VÀO SQLITE
# --------------------------------------------------

df = pd.read_csv("data_source/books_data.csv")

# Tạo database SQLite (file .db lưu tại chỗ)
conn = sqlite3.connect("data_source/books.db")

# Đẩy DataFrame vào bảng "sach" (ghi đè nếu đã tồn tại)
df.to_sql("sach", conn, if_exists="replace", index=False)
print("Đã nạp dữ liệu vào SQLite:", len(df), "bản ghi\n")


# --------------------------------------------------
# PHẦN 2: HÀM IN BẢNG ĐẸP
# --------------------------------------------------

def chay_truy_van(tieu_de, cau_sql, cat_ten=True):
    """Chạy câu SQL và in kết quả dạng bảng cột-hàng rõ ràng."""
    ket_qua = pd.read_sql_query(cau_sql, conn)

    # Cắt ngắn tên sách nếu quá dài để tránh lệch cột
    if cat_ten and "Ten_Sach" in ket_qua.columns:
        ket_qua["Ten_Sach"] = ket_qua["Ten_Sach"].apply(
            lambda x: (str(x)[:45] + "...") if len(str(x)) > 45 else str(x)
        )

    # In tiêu đề
    do_rong = max(len(tieu_de) + 6, 60)
    print("\n" + "=" * do_rong)
    print(f"  {tieu_de}")
    print("=" * do_rong)

    # In bảng dùng tabulate với viền đẹp
    print(tabulate(
        ket_qua,
        headers=ket_qua.columns,
        tablefmt="fancy_grid",
        showindex=False,
        floatfmt=".2f",
        numalign="center",
        stralign="left",
    ))
    print()


# --------------------------------------------------
# PHẦN 3: CÁC TRUY VẤN
# --------------------------------------------------

# # --- Truy vấn 0: Toàn bộ dữ liệu ---
# chay_truy_van(
#     "Toàn bộ dữ liệu trong bảng sách",
#     "SELECT * FROM sach"
# )

# # ---Truy vấn 1: Hiển thị chỉ cột Ten_Sach và Gia_Tien của tất cả sách---
# chay_truy_van(
#     "Hiển thị chỉ cột Ten_Sach và Gia_Tien của tất cả sách",
#     """
#     SELECT Ten_Sach, Gia_Tien_GBP
#     FROM sach
#     """
# )

# --- Truy vấn 2: Danh sách có Điểm đánh giá bằng 4 ---
chay_truy_van(
    "Sách có đánh giá bằng 4 sao",
    """
    SELECT Ten_Sach, Diem_Danh_Gia
    FROM sach
    WHERE Diem_Danh_Gia = 4
    """
)

# # --- Truy vấn 3: Sắp xếp danh sách theo giá từ cao xuống thấp ---
# chay_truy_van(
#     "Danh sách sách có giá từ cao xuống thấp",
#     """
#     SELECT Ten_Sach, Gia_Tien_GBP, Diem_Danh_Gia
#     FROM sach
#     ORDER BY Gia_Tien_GBP DESC              -- Sắp xếp theo giá từ cao xuống thấp
#     """
# )


# --- Truy vấn 4: Lấy 10 sách đầu danh sách ---
chay_truy_van(
    "Lấy 10 sách đầu danh sách",
    """
    SELECT Ten_Sach, Gia_Tien_GBP, Diem_Danh_Gia
    FROM sach
    LIMIT 10
    """
)
# --- Truy vấn 5: Thống kê theo điểm đánh giá ---
chay_truy_van(
    "Thống kê số lượng & giá trung bình theo số sao",
    """
    SELECT
        Diem_Danh_Gia               AS So_Sao,
        COUNT(*)                    AS So_Luong_Sach,
        ROUND(AVG(Gia_Tien_GBP), 2) AS Gia_TB_GBP,
        ROUND(MIN(Gia_Tien_GBP), 2) AS Gia_Thap_Nhat,
        ROUND(MAX(Gia_Tien_GBP), 2) AS Gia_Cao_Nhat
    FROM sach
    GROUP BY Diem_Danh_Gia
    ORDER BY Diem_Danh_Gia DESC
    """,
    cat_ten=False
)

# --- Truy vấn 5: Top 5 sách đắt nhất ---
chay_truy_van(
    "TOP 5 sách đắt nhất",
    """
    SELECT Ten_Sach, Gia_Tien_GBP, Diem_Danh_Gia
    FROM sach
    ORDER BY Gia_Tien_GBP DESC
    LIMIT 5
    """
)

# --------------------------------------------------
# PHẦN 4: ĐÓNG KẾT NỐI
# --------------------------------------------------
conn.close()
print("Đã đóng kết nối SQLite.")
print("Database lưu tại: data_source/books.db")
