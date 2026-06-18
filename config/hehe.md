# Đề xuất Kiến trúc & Triển khai Lớp Silver (Cleansed & Conformed Data)

Mục tiêu của lớp Silver là hợp nhất (Union) và chuẩn hóa dữ liệu từ 4 nguồn không đồng nhất (ITViec, TopCV, TopDev, VietnamWorks) về một mô hình dữ liệu thực thể (Entity-oriented) chuẩn, phục vụ cho lớp Gold. Đồng thời, áp dụng SCD Type 2 để lưu trữ lịch sử thay đổi theo đúng thiết kế hệ thống.

## Open Questions
> [!IMPORTANT]
> **1. Quy tắc chuẩn hóa tên kỹ năng (Skills):** Bạn có muốn một danh sách mapping chuẩn cho các skills không (ví dụ: `ReactJS`, `React JS`, `React` -> `React`) hay tạm thời chỉ viết hoa chữ cái đầu và xóa khoảng trắng thừa?
> **2. Xử lý mức lương (Salary):** Các trang có định dạng rất khác nhau (ví dụ: "10tr-30tr ₫/tháng", "Từ 9 triệu", "Thương lượng"). Chúng ta sẽ cần bóc tách ra `min_salary` và `max_salary` bằng RegEx. Nếu "Thương lượng" thì để NULL. Bạn có đồng ý hướng này?
> **3. SCD Type 2 cho Company:** Thông tin công ty (quy mô, địa chỉ) có thể thay đổi. Ta có nên áp dụng SCD2 cho cả bảng `companies` hay chỉ dùng SCD Type 1 (ghi đè) cho bảng này, và tập trung SCD2 cho bảng `jobs`?

## Proposed Changes

Chúng ta sẽ thiết kế 3 bảng thực thể ở tầng Silver:
1. `silver_jobs`: Chứa thông tin chính của bài đăng.
2. `silver_companies`: Chứa thông tin công ty.
3. `silver_job_skills`: Bảng trung gian lưu quan hệ nhiều-nhiều giữa Job và Skill.

### 1. Chuẩn hóa & Làm sạch dữ liệu (Data Standardization)

Dữ liệu từ 4 nguồn sẽ đi qua các hàm biến đổi chung:

*   **Tạo Surrogate Key (Khóa nhân tạo):** 
    *   Sử dụng MD5 Hash để tạo tính Deterministic.
    *   `job_id = MD5(source_name + job_url)`
    *   `company_id = MD5(lower(trim(company_name)))` (Cần xử lý đặc biệt các trường hợp tên công ty bị gõ tắt hoặc thêm đuôi JSC, TNHH).
*   **Lương (Salary):** Sử dụng Regular Expressions để extract số và quy đổi về cùng một đơn vị (VNĐ/Tháng hoặc USD).
*   **Kinh nghiệm (Experience) & Cấp bậc (Level):** Mapping các text từ tiếng Việt/Tiếng Anh về một chuẩn chung (ví dụ: `Junior`, `Middle`, `Senior`, `Manager`).
*   **Địa điểm (Location):** Chuẩn hóa tên tỉnh thành (VD: "Hồ Chí Minh", "HCM", "TP.HCM" -> "Ho Chi Minh").
*   **Ngày tháng (Dates):** Parse các chuỗi như "Hết hạn trong X ngày" hoặc "X days left" ra ngày `deadline` chính xác bằng cách cộng với ngày crawl (`ingested_at`).

### 2. Thiết kế Lược đồ (Schema Design)

#### Bảng `silver.companies`
| Cột | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `company_id` | STRING | MD5 hash của tên công ty (chuẩn hóa) |
| `company_name` | STRING | Tên công ty gốc |
| `industry` | STRING | Lĩnh vực |
| `size` | STRING | Quy mô |
| `address` | STRING | Địa chỉ |
| `link` | STRING | Link website/profile |
| `is_current` | BOOLEAN | SCD Type 2 flag |
| `valid_from` | TIMESTAMP | SCD Type 2 start |
| `valid_to` | TIMESTAMP | SCD Type 2 end |

#### Bảng `silver.jobs`
| Cột | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `job_id` | STRING | MD5 hash của source + url |
| `company_id` | STRING | FK references companies |
| `job_title` | STRING | Tiêu đề công việc |
| `salary_raw` | STRING | Lương dạng text gốc |
| `salary_min` | DOUBLE | Lương tối thiểu (sau extract) |
| `salary_max` | DOUBLE | Lương tối đa (sau extract) |
| `experience_req` | STRING | Yêu cầu kinh nghiệm (chuẩn hóa) |
| `job_level` | STRING | Cấp bậc (chuẩn hóa) |
| `location` | STRING | Địa điểm làm việc (chuẩn hóa) |
| `working_type` | STRING | Hình thức (Onsite, Remote, Hybrid) |
| `deadline` | DATE | Ngày hết hạn |
| `job_url` | STRING | Link bài tuyển dụng |
| `source` | STRING | Nguồn (itviec, topcv,...) |
| `is_current` | BOOLEAN | SCD Type 2 flag |
| `valid_from` | TIMESTAMP | SCD Type 2 start |
| `valid_to` | TIMESTAMP | SCD Type 2 end |

#### Bảng `silver.job_skills`
| Cột | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `job_id` | STRING | FK references jobs |
| `skill_name` | STRING | Tên kỹ năng (đã chuẩn hóa uppercase, bỏ khoảng trắng) |

### 3. Triển khai PySpark Pipeline

Sẽ tạo mới một pipeline trong thư mục `src/transform`:

#### [NEW] `src/transform/standardize_utils.py`
Chứa các UDFs (User Defined Functions) của PySpark để:
- Extract min/max salary bằng regex.
- Tính toán ngày deadline từ chuỗi text.
- Chuẩn hóa text (bỏ dấu, lowercase).
- Bóc tách array skills từ chuỗi phân cách bằng dấu phẩy (như bên vietnamworks).

#### [NEW] `src/transform/bronze_to_silver_pipeline.py`
Script PySpark chính thức thực hiện:
1. Đọc dữ liệu từ 4 bảng Bronze.
2. Áp dụng các UDF chuẩn hóa để đưa về schema chung.
3. Union 4 DataFrame lại thành 1 siêu DataFrame.
4. Tách siêu DataFrame ra thành 3 DataFrame: `df_jobs`, `df_companies`, `df_job_skills`.
5. **Ghi vào Silver (Iceberg) với SCD2**: Sử dụng Iceberg `MERGE INTO` để kiểm tra bản ghi đã tồn tại chưa:
   - Nếu chưa: `INSERT`.
   - Nếu có và bị đổi (vd: đổi lương): Đóng bản ghi cũ (`is_current=False`, `valid_to=now`) và `INSERT` bản ghi mới (`is_current=True`, `valid_from=now`).

#### [MODIFY] `dags/crawl_jobs_dag.py`
Thêm task `bronze_to_silver_operator` nối sau tất cả các task `ingest_..._to_bronze` đã chạy xong.

## Verification Plan
1. **Chạy Unit Test cho UDFs**: Kiểm thử các hàm Regex extract lương, chuẩn hóa ngày với các sample text phức tạp từ TopCV, VietnamWorks.
2. **Kiểm tra Iceberg Merge**: Viết dữ liệu test vào Iceberg Silver layer 2 lần (lần 2 sửa mức lương) và query để kiểm tra tính năng lưu trữ lịch sử SCD2 có hoạt động đúng (tồn tại 2 dòng, 1 dòng `is_current=true` và 1 dòng `is_current=false`).
