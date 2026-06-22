-- =======================================================================
-- KIỂM TRA CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY) TRONG POSTGRESQL (LỚP GOLD)
-- Sử dụng trong DBeaver kết nối vào database `serving_db`
-- =======================================================================

-- 1. KIỂM TRA TÍNH TOÀN VẸN DỮ LIỆU (COMPLETENESS)
-- Mục đích: Đảm bảo các trường quan trọng không bị NULL
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN job_title IS NULL THEN 1 ELSE 0 END) AS null_titles,
    SUM(CASE WHEN company_id IS NULL THEN 1 ELSE 0 END) AS null_companies,
    SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS null_locations,
    SUM(CASE WHEN source IS NULL THEN 1 ELSE 0 END) AS null_sources
FROM fact_job_postings;

-- 2. KIỂM TRA TÍNH DUY NHẤT (UNIQUENESS)
-- Mục đích: Phát hiện dữ liệu bị duplicate (kết quả phải trả về 0 dòng)

-- 2.1. Kiểm tra trùng lặp job_id trong fact_job_postings
SELECT job_id, COUNT(*) AS duplicate_count
FROM fact_job_postings
GROUP BY job_id
HAVING COUNT(*) > 1;

-- 2.2. Kiểm tra trùng lặp ID trong các bảng Dimension
SELECT company_id, COUNT(*) FROM dim_company GROUP BY company_id HAVING COUNT(*) > 1;
SELECT location_id, COUNT(*) FROM dim_location GROUP BY location_id HAVING COUNT(*) > 1;
SELECT skill_id, COUNT(*) FROM dim_skill GROUP BY skill_id HAVING COUNT(*) > 1;


-- 3. KIỂM TRA TÍNH TOÀN VẸN THAM CHIẾU (REFERENTIAL INTEGRITY)
-- Mục đích: Đảm bảo Fact không trỏ đến một ID bị thiếu ở bảng Dimension (Kết quả phải là 0)

-- 3.1. Company
SELECT COUNT(*) AS orphaned_companies
FROM fact_job_postings f
LEFT JOIN dim_company d ON f.company_id = d.company_id
WHERE d.company_id IS NULL AND f.company_id IS NOT NULL;

-- 3.2. Location
SELECT COUNT(*) AS orphaned_locations
FROM fact_job_postings f
LEFT JOIN dim_location d ON f.location_id = d.location_id
WHERE d.location_id IS NULL AND f.location_id IS NOT NULL;

-- 3.3. Job Category
SELECT COUNT(*) AS orphaned_categories
FROM fact_job_postings f
LEFT JOIN dim_job_category d ON f.job_category_id = d.job_category_id
WHERE d.job_category_id IS NULL AND f.job_category_id IS NOT NULL;


-- 4. KIỂM TRA BẢNG CẦU NỐI (BRIDGE TABLE INTEGRITY)
-- Mục đích: Đảm bảo bảng `bridge_job_skills` được map đúng

-- 4.1. Đảm bảo job_id trong bảng bridge đều tồn tại trong fact_job_postings
SELECT COUNT(*) AS invalid_bridge_jobs
FROM bridge_job_skills b
LEFT JOIN fact_job_postings f ON b.job_id = f.job_id
WHERE f.job_id IS NULL;

-- 4.2. Đảm bảo skill_id trong bảng bridge đều tồn tại trong dim_skill
SELECT COUNT(*) AS invalid_bridge_skills
FROM bridge_job_skills b
LEFT JOIN dim_skill d ON b.skill_id = d.skill_id
WHERE d.skill_id IS NULL;


-- 5. KIỂM TRA LOGIC & TÍNH HỢP LỆ (VALIDITY & CONSISTENCY)
-- Mục đích: Phát hiện dữ liệu bất thường

-- 5.1. Mức lương min không được lớn hơn mức lương max
SELECT job_id, salary_min, salary_max 
FROM fact_job_postings 
WHERE salary_min > salary_max;

-- 5.2. Lương không được mang giá trị âm
SELECT job_id, salary_min, salary_max 
FROM fact_job_postings 
WHERE salary_min < 0 OR salary_max < 0;

-- 5.3. Nguồn cào (Source) xem có bị sai chính tả hay lệch chuẩn không
SELECT source, COUNT(*) AS job_count
FROM fact_job_postings 
GROUP BY source;


-- 6. TRUY VẤN KIỂM TRA TÍNH LOGIC THEO NGHIỆP VỤ (BUSINESS LOGIC)
-- Dùng để overview dữ liệu xem có thực tế không

-- 6.1. Số lượng tin tuyển dụng theo từng nguồn
SELECT source, COUNT(*) AS job_count 
FROM fact_job_postings 
GROUP BY source 
ORDER BY job_count DESC;

-- 6.2. Top 10 địa điểm có nhiều việc làm nhất
SELECT d.location_name, COUNT(f.job_id) AS job_count
FROM fact_job_postings f
JOIN dim_location d ON f.location_id = d.location_id
GROUP BY d.location_name
ORDER BY job_count DESC
LIMIT 10;

-- 6.3. Top 10 kỹ năng được yêu cầu nhiều nhất
SELECT d.skill_name, COUNT(b.job_id) AS mention_count
FROM bridge_job_skills b
JOIN dim_skill d ON b.skill_id = d.skill_id
GROUP BY d.skill_name
ORDER BY mention_count DESC
LIMIT 10;
