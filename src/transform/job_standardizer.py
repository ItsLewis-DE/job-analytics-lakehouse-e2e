from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime
from src.utils.spark_util import get_spark_session
from src.transform.standardizers import (
    extract_salary,
    parse_deadline,
    standardize_location,
    standardize_experience,
    standardize_level,
    standardize_working_type,
    normalize_skills,
    standardize_company_size,
    generate_company_id,
    generate_job_id
)

# =============================================================================
# COLUMN MAPPINGS PER SOURCE
# =============================================================================
TOPCV_COLUMN_MAPPING = {
    "name_company": "company_name",
    "field": "industry",
    "scale": "scale",         
    "address": "company_address",
    "link_company": "company_link",
}

ITVIEC_COLUMN_MAPPING = {
    "company_name": "company_name",
    "company_industry": "industry",
    "company_size": "scale",
    "company_place": "company_country",
    "company_link": "company_link",
}

TOPDEV_COLUMN_MAPPING = {
    "company_name": "company_name",
    "company_industry": "industry",
    "company_size": "scale",
    "company_country": "company_country",
    "company_link": "company_link",
}

VIETNAMWORKS_COLUMN_MAPPING = {
    "name_company": "company_name",
    "field": "industry",
    "scale": "scale",
    "address": "company_address",
    "link_company": "company_link",
}


# =============================================================================
# ORCHESTRATOR CLASS
# =============================================================================
class JobStandardizer:
    """
    Lớp quản lý Pipeline chuẩn hóa dữ liệu cho từng Source cụ thể.
    """
    def __init__(self, source_name: str, column_mapping: dict,date: datetime):
        self.source_name = source_name
        self.column_mapping = column_mapping

        self.logger = logging.getLogger(f"ingestor.{source_name}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        self.bronze_path = "s3a://bronze/{source_name}"
        self.date = date
        self.spark = get_spark_session(f"standardize_{source_name}_to_silver")
        self.start_time = date.strftime("%Y-%m-%d 00:00:00")
        self.end_time = date.strftime("%Y-%m-%d 23:59:59")
    def read_bronze(self):
        table_path = f"my_catalog.bronze.{self.source_name}_jobs"
        df = self.spark.table(table_path)
        df_filtered = df.filter(
            (F.col("ingested_at") >= self.start_time) &
            (F.col("ingested_at")<= self.end_time)
        )
        return df_filtered
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Thực thi toàn bộ pipeline chuẩn hóa từ Bronze lên Silver.
        """
        if "_source_name" in df.columns:
            df = df.withColumnRenamed("_source_name", "source")
        else:
            df = df.withColumn("source", F.lit(self.source_name))

        for old_name, new_name in self.column_mapping.items():
            if old_name in df.columns and old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)

        df = (df.transform(extract_salary, salary_col="salary")
                .transform(parse_deadline, deadline_col="deadline", reference_date_col="ingested_at")
                .transform(standardize_location, place_col="place")
                .transform(standardize_experience, exp_col="experience")
                .transform(standardize_level, level_col="level")
                .transform(standardize_working_type, wt_col="working_type")
                .transform(normalize_skills, skills_col="skills")
                .transform(standardize_company_size, size_col="scale")
                .transform(generate_company_id, company_name_col="company_name")
                .transform(generate_job_id, source_col="source", url_col="job_url"))

        # Bước 3: Lọc và sắp xếp các cột cuối cùng cho lớp Silver (Final Schema)
        final_columns = [
            "job_id",
            "source",
            "job_url",
            "job_title",
            "company_id",
            "company_name",
            "company_link",
            "company_address",
            "company_country",
            "industry",
            "location",              # Đã chuẩn hóa từ place
            "company_size_std",      # Đã chuẩn hóa từ scale
            "experience_req",        # Đã chuẩn hóa từ experience
            "level_processed",       # Đã chuẩn hóa từ level
            "education",             # Giữ nguyên
            "working_type_std",      # Đã chuẩn hóa từ working_type
            "working_day",           # Giữ nguyên
            "working_hour",          # Giữ nguyên
            "skills_array",          # Đã chuẩn hóa từ skills
            "salary_raw",            # Giữ lại cột text của salary để tiện debug
            "salary_min",
            "salary_max",
            "deadline_date",         # Đã chuẩn hóa từ deadline
            "inserted_at"            # Ngày crawler quét được
        ]
        
        # Chỉ select những cột có tồn tại trong df hiện tại
        existing_cols = [c for c in final_columns if c in df.columns]
        df = df.select(*existing_cols)

        return df
    def upload_to_silver(self,df: DataFrame):
        if df.count() >0:
            # Xóa trùng lặp trước khi ghi để tránh lỗi Iceberg Merge "Multiple updates for a single row"
            df = df.dropDuplicates(["job_id"])
            
            table_name = f"my_catalog.silver.{self.source_name}_jobs"
            self.logger.info(f"Đang ghi dữ liệu xuống Iceberg table: {table_name}")
            
            # Kiểm tra xem bảng đã tồn tại trong catalog chưa
            if self.spark.catalog.tableExists(table_name):
                # UPSERT (MERGE INTO) khi bảng đã tồn tại
                df.createOrReplaceTempView("updates_temp")
                
                merge_query = f"""
                MERGE INTO {table_name} target
                USING updates_temp source
                ON target.job_id = source.job_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_query)
            else:
                # Lần chạy đầu tiên: Tạo bảng Iceberg v2 với tính năng Hidden Partitioning theo ngày
                df.writeTo(table_name) \
                    .tableProperty("format-version", "2") \
                    .partitionedBy("days(ingested_at)") \
                    .create()
                    
            self.logger.info("Ghi thành công")
        else:
            self.logger.warning("Không có dữ liệu hợp lệ!")
    def run(self):
        self.logger.info("=" * 60)
        self.logger.info(
            f"BẮT ĐẦU CHUẨN HÓA DỮ LIỆU RỒI ĐƯA LÊN SILVER: source={self.source_name}"
        )
        self.logger.info("=" * 60)
        try:
            df = self.read_bronze()
            df_proccessed = self.transform(df)
            self.upload_to_silver(df_proccessed)
        except Exception as e:
            self.logger.error(f"There is an error while Transform Data to silver: {e}")
            