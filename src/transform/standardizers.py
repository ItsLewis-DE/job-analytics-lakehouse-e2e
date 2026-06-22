from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# =============================================================================
# MAPPING CONSTANTS - Các bảng ánh xạ dùng chung
# =============================================================================

LOCATION_MAPPING = {
    # Ho Chi Minh
    "hồ chí minh": "Hồ Chí Minh",
    "hcm": "Hồ Chí Minh",
    "tp.hcm": "Hồ Chí Minh",
    "tp hcm": "Hồ Chí Minh",
    "ho chi minh": "Hồ Chí Minh",
    "thành phố hồ chí minh": "Hồ Chí Minh",
    "saigon": "Hồ Chí Minh",
    "sài gòn": "Hồ Chí Minh",
    # Ha Noi
    "hà nội": "Hà Nội",
    "ha noi": "Hà Nội",
    "hanoi": "Hà Nội",
    "hn": "Hà Nội",
    # Da Nang
    "đà nẵng": "Đà Nẵng",
    "da nang": "Đà Nẵng",
    # Hai Phong
    "hải phòng": "Hải Phòng",
    "hai phong": "Hải Phòng",
    # Can Tho
    "cần thơ": "Cần Thơ",
    "can tho": "Cần Thơ",
    # Binh Duong
    "bình dương": "Bình Dương",
    "binh duong": "Bình Dương",
    # Dong Nai
    "đồng nai": "Đồng Nai",
    "dong nai": "Đồng Nai",
    # Oversea
    "oversea": "Oversea",
    "overseas": "Oversea",
    "nước ngoài": "Oversea",
}

LEVEL_MAPPING = {
    # Intern / Fresher
    "thực tập": "Intern",
    "intern": "Intern",
    "fresher": "Fresher",
    "mới tốt nghiệp": "Fresher",
    # Junior
    "junior": "Junior",
    "nhân viên": "Junior",
    # Middle
    "middle": "Middle",
    # Senior
    "senior": "Senior",
    # Lead / Manager
    "trưởng nhóm": "Lead",
    "team lead": "Lead",
    "lead": "Lead",
    "trưởng phòng": "Manager",
    "manager": "Manager",
    "quản lý": "Manager",
    # Director
    "giám đốc": "Director",
    "director": "Director",
}

RANK_MAP = {
    "Intern": 1,
    "Fresher": 2,
    "Junior": 3,
    "Middle": 4,
    "Senior": 5,
    "Lead": 6,
    "Manager": 7,
    "Director": 8
}


# =============================================================================
# 1. SALARY STANDARDIZATION - Chuẩn hóa lương
# =============================================================================

def extract_salary(df: DataFrame, salary_col: str = "salary") -> DataFrame:
    if salary_col not in df.columns:
        return df.withColumn("salary_raw", F.lit(None).cast(T.StringType())) \
                 .withColumn("salary_min", F.lit(None).cast(T.DoubleType())) \
                 .withColumn("salary_max", F.lit(None).cast(T.DoubleType())) \
                 .withColumn("salary_currency", F.lit(None).cast(T.StringType()))

    df = df.withColumn("salary_raw", F.col(salary_col))

    cleaned = F.lower(F.trim(F.col(salary_col)))

    is_negotiable = (
        cleaned.contains("thoả thuận")
        | cleaned.contains("thỏa thuận")
        | cleaned.contains("thương lượng")
        | cleaned.contains("negotiable")
        | cleaned.contains("cạnh tranh")
        | cleaned.isNull()
    )

    # Detect đơn vị tiền tệ
    is_usd = cleaned.contains("usd") | cleaned.contains("$")

    first_number = F.regexp_extract(cleaned, r"(\d+[\.,]?\d*)", 1)
    second_number = F.regexp_extract(cleaned, r"[-–~][\s]*(\d+[\.,]?\d*)", 1)

    # Xử lý dấu phẩy/chấm khác nhau giữa USD và VND
    first_num = F.when(
        is_usd, 
        F.regexp_replace(first_number, r"[,\.]", "").cast(T.DoubleType())
    ).otherwise(
        F.regexp_replace(first_number, ",", ".").cast(T.DoubleType())
    )
    
    second_num = F.when(
        is_usd, 
        F.regexp_replace(second_number, r"[,\.]", "").cast(T.DoubleType())
    ).otherwise(
        F.regexp_replace(second_number, ",", ".").cast(T.DoubleType())
    )

    # Xác định kiểu pattern: "Từ X" (chỉ có min), "Tới X" (chỉ có max), "X - Y" (cả hai)
    has_prefix_from = (
        cleaned.startswith("từ")
        | cleaned.startswith("trên")
        | cleaned.startswith("above")
        | cleaned.startswith("from")
    )
    has_prefix_to = (
        cleaned.startswith("tới")
        | cleaned.startswith("dưới")
        | cleaned.startswith("up to")
        | cleaned.startswith("to")
    )

    df = df.withColumn(
        "salary_min",
        F.when(is_negotiable, F.lit(None).cast(T.DoubleType()))
         .when(has_prefix_to, F.lit(None).cast(T.DoubleType()))
         .otherwise(first_num)
    ).withColumn(
        "salary_max",
        F.when(is_negotiable, F.lit(None).cast(T.DoubleType()))
         .when(has_prefix_from & second_num.isNull(), F.lit(None).cast(T.DoubleType()))
         .when(second_num.isNotNull(), second_num)
         .when(has_prefix_to, first_num)
         .otherwise(F.lit(None).cast(T.DoubleType()))
    )

    df = df.withColumn(
        "salary_currency",
        F.when(is_negotiable, F.lit(None).cast(T.StringType()))
         .when(is_usd, F.lit("USD"))
         .otherwise(F.lit("tr VND"))  # Default cho thị trường VN
    )

    return df


# =============================================================================
# 2. DEADLINE STANDARDIZATION - Chuẩn hóa deadline
# =============================================================================

def parse_deadline(
    df: DataFrame,
    deadline_col: str = "deadline",
    reference_date_col: str = "ingested_at"
) -> DataFrame:
    if deadline_col not in df.columns:
        return df.withColumn("deadline_date", F.lit(None).cast(T.DateType()))

    # Extract số ngày từ chuỗi (lấy chuỗi số đầu tiên)
    days_remaining = F.regexp_extract(
        F.col(deadline_col), r"(\d+)", 1
    ).cast(T.IntegerType())

    # Lấy ngày tham chiếu (ngày crawl). Nếu cột reference là string thì parse, 
    # nếu là timestamp thì cast về date
    if reference_date_col in df.columns:
        ref_date = F.to_date(F.col(reference_date_col))
    else:
        # Fallback: dùng ngày hiện tại
        ref_date = F.current_date()

    df = df.withColumn(
        "deadline_date",
        F.when(
            days_remaining.isNotNull(),
            F.date_add(ref_date, days_remaining)
        ).otherwise(F.lit(None).cast(T.DateType()))
    )

    return df


# =============================================================================
# 3. LOCATION STANDARDIZATION - Chuẩn hóa tên tỉnh/thành
# =============================================================================

def standardize_location(df: DataFrame, place_col: str = "place") -> DataFrame:
    if place_col not in df.columns:
        return df.withColumn("location", F.lit(None).cast(T.StringType()))

    place_lower = F.lower(F.trim(F.col(place_col)))
    
    result_expr = F.lit(None).cast(T.StringType())

    # Lật ngược thứ tự: match dài nhất trước (greedy matching)
    sorted_keys = sorted(LOCATION_MAPPING.keys(), key=len, reverse=True)
    for key in sorted_keys:
        value = LOCATION_MAPPING[key]
        result_expr = F.when(
            place_lower.contains(key), F.lit(value)
        ).otherwise(result_expr)

    # Nếu không match: giữ nguyên giá trị gốc (đã trim)
    df = df.withColumn(
        "location",
        F.when(result_expr.isNotNull(), result_expr)
         .otherwise(F.trim(F.col(place_col)))
    )

    return df


# =============================================================================
# 4. EXPERIENCE STANDARDIZATION - Chuẩn hóa kinh nghiệm
# =============================================================================

def standardize_experience(df: DataFrame, exp_col: str = "experience") -> DataFrame:
    if exp_col not in df.columns:
        return df.withColumn("experience_req", F.lit(None).cast(T.StringType()))

    exp_lower = F.lower(F.trim(F.col(exp_col)))

    is_not_required = (
        exp_lower.contains("không yêu cầu")
        | exp_lower.contains("not required")
        | exp_lower.contains("không cần")
    )

    # Extract số năm đầu tiên từ chuỗi
    years_num = F.regexp_extract(exp_lower, r"(\d+)", 1).cast(T.IntegerType())

    df = df.withColumn(
        "experience_req",
        F.when(is_not_required, F.lit("Not required"))
         .when(
             exp_lower.contains("dưới") | exp_lower.contains("under") | exp_lower.contains("less"),
             F.concat(F.lit("< "), years_num.cast(T.StringType()), F.lit(" years"))
         )
         .when(
             exp_lower.contains("trên") | exp_lower.contains("above") | exp_lower.contains("over"),
             F.concat(F.lit("> "), years_num.cast(T.StringType()), F.lit(" years"))
         )
         .when(
             years_num.isNotNull(),
             F.concat(years_num.cast(T.StringType()), F.lit(" years"))
         )
         .otherwise(F.lit(None).cast(T.StringType()))
    )

    return df


# =============================================================================
# 5. JOB LEVEL STANDARDIZATION - Chuẩn hóa cấp bậc
# =============================================================================

def standardize_level(df: DataFrame, level_col: str = "level") -> DataFrame:
    if level_col not in df.columns:
        return df.withColumn("level_processed", F.lit(None).cast(T.StringType()))

    level_lower = F.lower(F.trim(F.col(level_col)))
    sorted_keys = sorted(
        LEVEL_MAPPING.keys(), key=lambda x: RANK_MAP[LEVEL_MAPPING[x]],
        reverse=False
    )
    result_level = F.lit(None).cast(T.StringType())
    for key in sorted_keys:
        value = LEVEL_MAPPING[key]
        result_level = F.when(
            level_lower.contains(key), F.lit(value)
        ).otherwise(result_level)

    df = df.withColumn(
        "level_processed",
        F.when(result_level.isNotNull(), result_level)
        .otherwise(F.trim(F.col(level_col)))
    )
    return df


# =============================================================================
# 6. WORKING TYPE STANDARDIZATION - Chuẩn hóa hình thức làm việc
# =============================================================================

def standardize_working_type(df: DataFrame, wt_col: str = "working_type") -> DataFrame:
    if wt_col not in df.columns:
        return df.withColumn("working_type_std", F.lit(None).cast(T.StringType()))

    wt_lower = F.lower(F.trim(F.col(wt_col)))

    df = df.withColumn(
        "working_type_std",
        F.when(wt_lower.isNull(), F.lit(None))
         .when(wt_lower.contains("hybrid") | wt_lower.contains("kết hợp"), F.lit("Hybrid"))
         .when(wt_lower.contains("remote") | wt_lower.contains("từ xa"), F.lit("Remote"))
         .when(
             wt_lower.contains("onsite") | wt_lower.contains("office")
             | wt_lower.contains("văn phòng") | wt_lower.contains("at office"),
             F.lit("Onsite")
         )
         .otherwise(F.trim(F.col(wt_col)))
    )

    return df


# =============================================================================
# 7. SKILLS NORMALIZATION - Chuẩn hóa kỹ năng
# =============================================================================

def normalize_skills(df: DataFrame, skills_col: str = "skills") -> DataFrame:
    if skills_col not in df.columns:
        return df.withColumn("skills_array", F.array().cast(T.ArrayType(T.StringType())))

    col_type = df.schema[skills_col].dataType

    if isinstance(col_type, T.ArrayType):
        # Đã là array -> trim từng phần tử, loại bỏ phần tử rỗng
        df = df.withColumn(
            "skills_array",
            F.expr(f"filter(transform({skills_col}, x -> trim(x)), x -> x != '' AND x IS NOT NULL)")
        )
    elif isinstance(col_type, T.StringType):
        # Là string -> split bằng dấu phẩy rồi trim
        df = df.withColumn(
            "skills_array",
            F.when(
                F.col(skills_col).isNotNull() & (F.trim(F.col(skills_col)) != ""),
                F.expr(f"filter(transform(split({skills_col}, ','), x -> trim(x)), x -> x != '' AND x IS NOT NULL)")
            ).otherwise(F.array().cast(T.ArrayType(T.StringType())))
        )
    else:
        df = df.withColumn("skills_array", F.array().cast(T.ArrayType(T.StringType())))

    return df


# =============================================================================
# 8. COMPANY SIZE STANDARDIZATION - Chuẩn hóa quy mô công ty
# =============================================================================

def standardize_company_size(df: DataFrame, size_col: str = "scale") -> DataFrame:
    if size_col not in df.columns:
        return df.withColumn("company_size_std", F.lit(None).cast(T.StringType()))

    # Loại bỏ dấu chấm, phẩy (ví dụ: 10.000 -> 10000) và xuống dòng
    cleaned = F.regexp_replace(F.trim(F.col(size_col)), r"[.,\n\r]", "")

    # Extract pattern "X-Y" hoặc "X+"

    df = df.withColumn(
        "company_size_std",
        F.when((F.col(size_col).isNull()) | (~cleaned.rlike(r"\d")), F.lit(None))
         .when(
             F.regexp_extract(cleaned, r"(\d+\+)", 1) != "",
             F.regexp_extract(cleaned, r"(\d+\+)", 1)
         )
         .when(
             F.regexp_extract(cleaned, r"(\d+\s*[-–]\s*\d+)", 1) != "",
             F.regexp_replace(
                 F.regexp_extract(cleaned, r"(\d+\s*[-–]\s*\d+)", 1),
                 r"\s*[-–]\s*", "-"
             )
         )
         .otherwise(F.trim(F.col(size_col)))
    )

    return df


# =============================================================================
# 9. SURROGATE KEY GENERATION - Tạo khóa nhân tạo
# =============================================================================

def generate_job_id(df: DataFrame, source_col: str = "source", url_col: str = "job_url") -> DataFrame:
    df = df.withColumn(
        "job_id",
        F.md5(F.concat_ws("||", F.col(source_col), F.col(url_col)))
    )
    return df


def generate_company_id(df: DataFrame, company_name_col: str = "company_name") -> DataFrame:
    df = df.withColumn(
        "company_id",
        F.md5(F.lower(F.trim(F.col(company_name_col))))
    )
    return df


# =============================================================================
# 10. ENRICH SALARY BAND
# =============================================================================
def enrich_salary_band(df: DataFrame) -> DataFrame:
    if "salary_min" not in df.columns or "salary_max" not in df.columns:
        return df.withColumn("salary_band", F.lit("Negotiable"))
    
    # Chuẩn hóa tất cả về đơn vị "Triệu VNĐ" (tr VND)
    vnd_min_tr = F.when(F.col("salary_currency") == "USD", F.col("salary_min") * 0.025)\
                  .when(F.col("salary_min") > 1000, F.col("salary_min") / 1000000)\
                  .otherwise(F.col("salary_min"))
                  
    vnd_max_tr = F.when(F.col("salary_currency") == "USD", F.col("salary_max") * 0.025)\
                  .when(F.col("salary_max") > 1000, F.col("salary_max") / 1000000)\
                  .otherwise(F.col("salary_max"))
    
    avg_salary = F.when(vnd_min_tr.isNotNull() & vnd_max_tr.isNotNull(), (vnd_min_tr + vnd_max_tr) / 2) \
                  .when(vnd_min_tr.isNotNull(), vnd_min_tr)\
                  .when(vnd_max_tr.isNotNull(), vnd_max_tr) \
                  .otherwise(F.lit(None).cast(T.DoubleType()))

    df = df.withColumn(
        "salary_band",
        F.when(avg_salary.isNull(), F.lit("Negotiable"))
         .when(avg_salary < 10, F.lit("< 10tr"))
         .when((avg_salary >= 10) & (avg_salary <= 20), F.lit("10tr - 20tr"))
         .when((avg_salary > 20) & (avg_salary <= 30), F.lit("20tr - 30tr"))
         .otherwise(F.lit("> 30tr"))
    )
    return df


# =============================================================================
# 11. ENRICH SKILLS FROM TITLE
# =============================================================================
def enrich_skills_from_title(df: DataFrame, title_col: str = "job_title") -> DataFrame:
    if title_col not in df.columns or "skills_array" not in df.columns:
        return df
        
    title_lower = F.lower(F.col(title_col))
    
    common_skills = [
        "python", "java", "javascript", "react", "angular", "vue", "nodejs",
        "c\\+\\+", "c#", "ruby", "php", "golang", "go", "swift", "kotlin", "aws", "azure", 
        "gcp", "docker", "kubernetes", "sql", "nosql", "mysql", "postgresql", 
        "mongodb", "oracle", "spark", "hadoop", "kafka", "django", "spring", "laravel",
        "flutter", "react native", "unity", "tensorflow", "pytorch", "html", "css",
        "linux", "bash", "shell", "git", "ci/cd", "elasticsearch", "redis"
    ]
    
    extracted_cols = []
    for skill in common_skills:
        pattern = f"\\b{skill}\\b"
        display_name = skill.replace("\\+", "+").title()
        if skill == "nodejs": display_name = "NodeJS"
        elif skill == "ci/cd": display_name = "CI/CD"
        elif skill == "c\\+\\+": display_name = "C++"
        elif skill == "c#": display_name = "C#"
        elif skill == "sql": display_name = "SQL"
        elif skill == "aws": display_name = "AWS"
        elif skill == "gcp": display_name = "GCP"
        elif skill == "react native": display_name = "React Native"
        
        extracted_cols.append(
            F.when(title_lower.rlike(pattern), F.lit(display_name)).otherwise(F.lit(None).cast(T.StringType()))
        )
        
    extracted_array = F.array_except(F.array(*extracted_cols), F.array(F.lit(None).cast(T.StringType())))
    
    df = df.withColumn(
        "skills_array",
        F.array_distinct(F.concat(F.col("skills_array"), extracted_array))
    )
    return df


# =============================================================================
# 12. STANDARDIZE JOB CATEGORY
# =============================================================================
def standardize_job_category(df: DataFrame, title_col: str = "job_title") -> DataFrame:
    if title_col not in df.columns:
        return df.withColumn("job_category", F.lit("Others"))
        
    title_lower = F.lower(F.col(title_col))
    
    category_mapping = {
        "Data Engineer": ["data engineer", "data warehouse", "etl", "big data", "kỹ sư dữ liệu"],
        "Data Analyst": ["data analyst", "phân tích dữ liệu", "business intelligence", "bi analyst", 'data analysis', "analytics"],
        "Data Scientist": ["data scientist", "khoa học dữ liệu", "machine learning", "deep learning"],
        "Fullstack Developer": ["fullstack", "full-stack", "full stack"],
        "Backend Developer": ["backend", "back-end", "back end", "python", "java", "php", "nodejs", "c#", "c\\+\\+", "\\.net", "golang", "ruby", "software"],
        "Frontend Developer": ["frontend", "front-end", "front end", "react", "angular", "vue", "html", "css", "web developer"],
        "Mobile Developer": ["mobile", "ios", "android", "flutter", "react native", "swift", "kotlin"],
        "DevOps / SRE": ["devops", "sre", "system admin", "sysadmin", "infrastructure", "quản trị hệ thống", "cloud"],
        "QA/QC / Tester": ["qa", "qc", "tester", "kiểm thử", "automation test"],
        "Project Manager": ["project manager", "quản lý dự án", "scrum master", "product manager", "po"],
        "Business Analyst": ["business analyst", "ba", "phân tích nghiệp vụ"],
        "Security / IT Support": ["security", "bảo mật", "it support", "helpdesk", "kỹ thuật viên it"],
        "AI Engineer": ["ai","ai engineer","trí tuệ nhân tạo"]
    }
    
    job_cat_col = F.lit(None).cast(T.StringType())
    
    for category, keywords in category_mapping.items():
        pattern = "|".join([f"\\b{k}\\b" for k in keywords])
        job_cat_col = F.when(title_lower.rlike(pattern), F.lit(category)).otherwise(job_cat_col)
        
    df = df.withColumn(
        "job_category",
        F.when(job_cat_col.isNotNull(), job_cat_col).otherwise(F.lit("Others"))
    )
    return df
