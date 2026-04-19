# -*- coding: utf-8 -*-
import requests                    # thư viện để gửi yêu cầu HTTP đến website
from bs4 import BeautifulSoup      # thư viện đọc và cắt nội dung HTML
import pandas as pd                # thư viện tạo bảng dữ liệu (giống Excel)
import time                        # thư viện để tạo khoảng nghỉ giữa mỗi lần lấy


# --------------------------------------------------
# PHẦN 1: CẤU HÌNH
# --------------------------------------------------

# Địa chỉ trang web muốn lấy dữ liệu
DUONG_DAN_WEB = "https://books.toscrape.com"

# Giả mạo trình duyệt để website không chặn kết nối
# (Nếu không có dòng này, web sẽ từ chối truy cập)
THONG_TIN_TRINH_DUYET = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
    )
}

# Số trang muốn lấy (mỗi trang có 20 cuốn sách)
SO_TRANG_TOI_DA = 3

# Tên file CSV để lưu kết quả
FILE_LUU = "data_source/books_data.csv"


# --------------------------------------------------
# PHẦN 2: HÀM GỬI YÊU CẦU ĐẾN WEBSITE
# --------------------------------------------------

def lay_noi_dung_trang(duong_dan):
    """
    Hàm này gửi yêu cầu đến website và trả về nội dung HTML của trang đó.
    Nếu thất bại thì báo lỗi thay vì làm chương trình bị tắt.
    """
    try:
        # Gửi yêu cầu GET đến website (giống như bạn mở trang trên trình duyệt)
        phan_hoi = requests.get(duong_dan, headers=THONG_TIN_TRINH_DUYET, timeout=10)

        # Kiểm tra xem có lấy thành công không (mã 200 = thành công)
        phan_hoi.raise_for_status()

        print(f"  Lấy thành công: {duong_dan}")
        return phan_hoi.text   # trả về nội dung HTML dạng chữ

    except requests.exceptions.RequestException as loi:
        print(f"  Không thể kết nối: {loi}")
        return None            # trả về rỗng nếu thất bại


# --------------------------------------------------
# PHẦN 3: HÀM ĐỌC VÀ TÁCH THÔNG TIN TỪ HTML
# --------------------------------------------------

# Bảng chuyển đổi điểm đánh giá từ chữ sang số
BANG_CHU_SANG_SO = {
    "One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5
}

def tach_thong_tin_sach(noi_dung_html):
    """
    Hàm này nhận vào nội dung HTML của trang
    và tách ra: Tên sách, Giá, Điểm đánh giá, Tình trạng còn hàng.
    """
    canh_cay = BeautifulSoup(noi_dung_html, "html.parser")

    # Danh sách sẽ chứa tất cả sách lấy được
    danh_sach_sach = []

    # Tìm tất cả thẻ chứa thông tin từng cuốn sách
    tat_ca_the_sach = canh_cay.find_all("article", class_="product_pod")

    for the_sach in tat_ca_the_sach:
        # --- Tên sách ---
        ten_sach = the_sach.find("h3").find("a")["title"]

        # --- Giá tiền ---
        gia_tien = the_sach.find("p", class_="price_color").text.strip()
        # Xóa ký tự pound (£) và chuyển sang số thực
        gia_so = float(gia_tien.replace("£", "").replace("Â", "").strip())

        # --- Điểm đánh giá (số sao) ---
        the_danh_gia = the_sach.find("p", class_="star-rating")
        ten_class = the_danh_gia["class"][1]   # Ví dụ: "Three"
        so_sao = BANG_CHU_SANG_SO.get(ten_class, 0)

        # --- Tình trạng còn hàng ---
        tinh_trang = the_sach.find("p", class_="instock").text.strip()

        danh_sach_sach.append({
            "Ten_Sach":       ten_sach,    # Tiêu đề cuốn sách
            "Gia_Tien_GBP":  gia_so,      # Giá tiền (đơn vị Bảng Anh)
            "Diem_Danh_Gia": so_sao,      # Số sao đánh giá (1–5)
            "Tinh_Trang":    tinh_trang   # Còn hàng hay hết hàng
        })

    return danh_sach_sach


# --------------------------------------------------
# PHẦN 4: HÀM CHÍNH - ĐIỀU PHỐI TOÀN BỘ QUÁ TRÌNH
# --------------------------------------------------

def main():
    import os
    os.makedirs("data_source", exist_ok=True)

    print("=" * 55)
    print("  WEB SCRAPER - SÁCH TỪ books.toscrape.com")
    print("=" * 55)

    tat_ca_sach = []

    for so_trang in range(1, SO_TRANG_TOI_DA + 1):
        # Tạo đường dẫn cho từng trang
        if so_trang == 1:
            url_trang = DUONG_DAN_WEB
        else:
            url_trang = f"{DUONG_DAN_WEB}/catalogue/page-{so_trang}.html"

        print(f"\n[Trang {so_trang}/{SO_TRANG_TOI_DA}] Đang lấy dữ liệu...")

        # Lấy nội dung HTML của trang
        noi_dung = lay_noi_dung_trang(url_trang)
        if noi_dung is None:
            print(f"  Bỏ qua trang {so_trang} do lỗi kết nối.")
            continue

        # Tách thông tin sách từ HTML
        sach_tren_trang = tach_thong_tin_sach(noi_dung)
        tat_ca_sach.extend(sach_tren_trang)
        print(f"  Lấy được {len(sach_tren_trang)} cuốn sách.")

        # Nghỉ 1 giây để không làm quá tải máy chủ
        time.sleep(1)

    # --------------------------------------------------
    # PHẦN 5: LƯU KẾT QUẢ THÀNH FILE CSV
    # --------------------------------------------------
    if not tat_ca_sach:
        print("\nKhông lấy được dữ liệu nào!")
        return

    df = pd.DataFrame(tat_ca_sach)

    print(f"\n{'='*55}")
    print(f"  TỔNG KẾT")
    print(f"{'='*55}")
    print(f"  Tổng số sách lấy được : {len(df)}")
    print(f"  Giá thấp nhất         : {df['Gia_Tien_GBP'].min():.2f} GBP")
    print(f"  Giá cao nhất          : {df['Gia_Tien_GBP'].max():.2f} GBP")
    print(f"  Giá trung bình        : {df['Gia_Tien_GBP'].mean():.2f} GBP")
    print(f"  Điểm TB (số sao)      : {df['Diem_Danh_Gia'].mean():.1f}/5")

    df.to_csv(FILE_LUU, index=False, encoding="utf-8-sig")
    print(f"\n  Đã lưu dữ liệu vào: {FILE_LUU}")
    print(f"{'='*55}")
    print("  Hoàn Thành!")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()