# 🏢 IT Job Analytics Data Lakehouse

Dự án Xây dựng Hệ thống Data Lakehouse thu thập, xử lý và phân tích dữ liệu tuyển dụng IT từ các nền tảng việc làm hàng đầu tại Việt Nam. Dự án áp dụng kiến trúc **Medallion (Bronze, Silver, Gold)** kết hợp cùng **Apache Iceberg**, **Apache Spark** và **MinIO**.

---

## 📌 1. Nguồn Dữ Liệu (Data Sources)
Hệ thống cào dữ liệu tự động (Crawl) bằng thư viện `BeautifulSoup4 (bs4)` từ 4 nền tảng tuyển dụng phổ biến nhất:

1. **ITViec:**
   - *Dữ liệu thu thập:* Tên công ty, địa chỉ, hình thức làm việc (remote/office), mức lương, kỹ năng, chuyên môn, domain công việc, loại hình công ty, quy mô công ty, quốc gia, ngày làm việc, link bài viết.
2. **TopCV:**
   - *Dữ liệu thu thập:* Lương, địa điểm, kinh nghiệm, hạn nộp, yêu cầu, quyền lợi, chuyên môn, cấp bậc, học vấn, số lượng tuyển, hình thức làm việc, thời gian làm việc, kỹ năng, tên công ty, quy mô, lĩnh vực, địa điểm, link bài viết.
3. **TopDev:**
   - *Dữ liệu thu thập:* Tiêu đề, yêu cầu, domain công việc, kỹ năng, phúc lợi, tên công ty, quy mô công ty, địa điểm công ty.
4. **VietnamWorks:**
   - *Dữ liệu thu thập:* Hạn nộp hồ sơ, lương, mức độ gấp, địa điểm, yêu cầu công việc, kinh nghiệm, kỹ năng, tên công ty, địa chỉ công ty, quy mô công ty, link website, phúc lợi, cấp bậc, trình độ học vấn, giờ làm việc, ngày làm việc, loại hình làm việc, độ tuổi, lĩnh vực.

> ⏳ **Lịch trình chạy:** Toàn bộ quá trình cào dữ liệu được tự động hóa (Automated cronjob) chạy vào lúc **00:00 sáng** hằng ngày.

---

## 🗄️ 2. Kiến trúc Lưu trữ (Storage Architecture)
Hệ thống sử dụng **MinIO (Object Storage)** làm Data Lake và **Apache Iceberg** làm Table Format. Lưu trữ được chia làm 4 Buckets chính:

* `sandbox`: Bucket mặc định của Catalog (chứa `sandbox/warehouse`).
* `bronze`: Bucket lưu trữ dữ liệu thô.
* `silver`: Bucket lưu trữ dữ liệu đã làm sạch.
* `gold`: Bucket lưu trữ dữ liệu phân tích.

**Cấu hình vòng đời dữ liệu (Data Lifecycle Management):**
- **Bucket `sandbox`:** Cấu hình chính sách tự động xóa (Auto-delete) dữ liệu tối đa 30 ngày để dọn rác.
- **Bucket `bronze`:** Cấu hình chính sách lưu trữ lạnh (Cold Storage). Dữ liệu thô tồn tại trên 30 ngày sẽ tự động chuyển sang Storage khác rẻ hơn nhằm tối ưu chi phí lưu trữ.

**Cấu hình Catalog (Hive Metastore):**
- Cấu hình thư mục `warehouse` mặc định trỏ về `s3a://sandbox/warehouse/`.
- Thiết lập 3 Namespace tương ứng trỏ đích danh (`LOCATION`) về 3 bucket: `bronze`, `silver`, `gold`.

---

## 🏗️ 3. Kiến trúc Dữ liệu Medallion (Data Flow)

Dữ liệu di chuyển theo đường ống (Pipeline) thông qua **Apache Spark** từ lúc cào đến lúc đưa lên Dashboard phân tích:

### 🥉 Lớp Bronze (Raw Data)
- **Thiết kế:** Mỗi nguồn API/Website được lưu thành **1 bảng riêng biệt**.
- **Xử lý:**
  - Hàng ngày, sau khi gọi API, toàn bộ JSON/dữ liệu thô được **APPEND** thẳng vào bảng mà không can thiệp cấu trúc.
  - Tích hợp tính năng **Hidden Partitioning** của Iceberg để tự động chia Partition dữ liệu theo ngày (`ingest_date`).

### 🥈 Lớp Silver (Cleansed & Conformed Data)
- **Thiết kế:** Chuyển đổi dữ liệu từ thiết kế theo nguồn sang thiết kế theo Thực thể (Entities).
- **Xử lý:**
  - **UNION:** Gộp (Union) dữ liệu từ 4 bảng độc lập ở tầng Bronze lại thành các bảng thực thể chuẩn hóa chung.
  - Làm sạch dữ liệu, xử lý missing values, ép kiểu dữ liệu (casting).
  - Áp dụng kỹ thuật **Slowly Changing Dimension Type 2 (SCD2)** qua câu lệnh `MERGE INTO` của Iceberg để cập nhật thông tin bài đăng nhưng **vẫn giữ lại toàn bộ dữ liệu lịch sử thay đổi** (ví dụ: biến động mức lương của một vị trí theo thời gian).

### 🥇 Lớp Gold (Aggregated & Business Data)
- **Thiết kế:** Dữ liệu được nhào nặn và phân chia theo chuẩn **Kiến trúc Kimball (Star Schema)**.
- **Xử lý:**
  - Tạo các bảng **Fact** (sự kiện) và **Dimension** (danh mục).
  - Phục vụ trực tiếp cho Data Analyst (DA) kết nối với **Apache Superset** để thiết kế các Dashboard trực quan hóa và phân tích thị trường việc làm IT, mà không cần tốn chi phí thực hiện JOIN phức tạp ở runtime.
