import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime
from src.utils.spark_util import get_spark_session
import logging
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
    generate_job_id,
    enrich_salary_band,
    enrich_skills_from_title,
    standardize_job_category
)

# =============================================================================
# COLUMN MAPPINGS PER SOURCE
# =============================================================================
TOPCV_COLUMN_MAPPING = {
    "name_company": "company_name",
    "field": "company_industry",
    "scale": "scale",         
    "address": "company_address",
    "link_company": "company_link",
}

ITVIEC_COLUMN_MAPPING = {
    "company_name": "company_name",
    "company_industry": "company_industry",
    "company_size": "scale",
    "company_place": "company_address",
    "company_link": "company_link",
}


VIETNAMWORKS_COLUMN_MAPPING = {
    "name_company": "company_name",
    "field": "company_industry",
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
                .transform(enrich_salary_band)
                .transform(parse_deadline, deadline_col="deadline", reference_date_col="ingested_at")
                .transform(standardize_location, place_col="place")
                .transform(standardize_experience, exp_col="experience")
                .transform(standardize_level, level_col="level")
                .transform(standardize_working_type, wt_col="working_type")
                .transform(normalize_skills, skills_col="skills")
                .transform(enrich_skills_from_title, title_col="job_title")
                .transform(standardize_job_category, title_col="job_title")
                .transform(standardize_company_size, size_col="scale")
                .transform(generate_company_id, company_name_col="company_name")
                .transform(generate_job_id, source_col="source", url_col="job_url"))

        # Bước 3: Lọc và sắp xếp các cột cuối cùng cho lớp Silver (Final Schema)
        final_columns = [
            "job_id",
            "source",
            "job_url",
            "job_title",
            "job_category",
            "company_id",
            "company_name",
            "company_link",
            "company_address",
            "company_industry",
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
            "salary_currency",       # Đơn vị tiền tệ (VND/USD)
            "salary_band",
            "deadline_date",         # Đã chuẩn hóa từ deadline
            "inserted_at",           # Ngày crawler quét được
            "ingested_at"            # Timestamp ingest vào Bronze (dùng làm partition key)
        ]

        # Chỉ select những cột có tồn tại trong df hiện tại
        existing_cols = [c for c in final_columns if c in df.columns]
        df = df.select(*existing_cols)

        # Bước 4: Loại bỏ records thiếu thông tin bắt buộc
        required_cols = ["job_url", "job_title", "company_name", "source"]
        valid_condition = F.lit(True)
        
        # Ap dung DLQ (tach df loi ra mot bucket rieng)
        for col_name in required_cols:
            if col_name in df.columns:
                valid_condition = valid_condition & F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != "")
                
        df_valid = df.filter(valid_condition)
        df_invalid = df.filter(~valid_condition)
        
        if df_invalid.head(1):
            self.logger.warning('Phat hien loi !!! Missing mandatory fields in some records.')
            dlq_table_name = f"my_catalog.silver.{self.source_name}_jobs_dlq"
            df_invalid_with_reason = df_invalid.withColumn(
                'error_reason', F.lit('Missing mandatory fields')
            ).withColumn(
                'failed_at', F.current_timestamp()
            )
            
            if self.spark.catalog.tableExists(dlq_table_name):
                df_invalid_with_reason.writeTo(dlq_table_name).append()
                self.logger.info(f"Da dua du lieu loi vao bang DLQ: {dlq_table_name}")
            else:
                df_invalid_with_reason.writeTo(dlq_table_name) \
                                    .tableProperty('format-version', '2') \
                                    .create()
                self.logger.info(f"Da tao bang va dua du lieu loi vao DLQ: {dlq_table_name}")
                
        return df_valid
    def upload_to_silver(self, df: DataFrame):
        if df.head(1):
            # Xóa trùng lặp trước khi ghi để tránh lỗi Iceberg Merge "Multiple updates for a single row"
            df = df.dropDuplicates(["job_id"])
            
            table_name = f"my_catalog.silver.{self.source_name}_jobs"
            self.logger.info(f"Đang ghi dữ liệu xuống Iceberg table: {table_name}")
            
            # Kiểm tra xem bảng đã tồn tại trong catalog chưa
            if self.spark.catalog.tableExists(table_name):
                # UPSERT (MERGE INTO) khi bảng đã tồn tại
                df.createOrReplaceTempView("updates_temp")
                country_update_str = ""
                merge_query = f"""
                MERGE INTO {table_name} target
                USING updates_temp source
                ON target.job_id = source.job_id
                WHEN MATCHED AND (
                    NOT (target.job_title <=> source.job_title) OR
                    NOT (target.job_category <=> source.job_category) OR
                    NOT (target.company_name <=> source.company_name) OR
                    NOT (target.location <=> source.location) OR
                    NOT (target.salary_raw <=> source.salary_raw) OR
                    NOT (target.deadline_date <=> source.deadline_date) OR
                    NOT (target.experience_req <=> source.experience_req) OR
                    NOT (target.level_processed <=> source.level_processed) OR
                    NOT (target.working_type_std <=> source.working_type_std)
                ) THEN UPDATE SET 
                    target.job_title = COALESCE(source.job_title, target.job_title),
                    target.job_category = COALESCE(source.job_category, target.job_category),
                    target.company_id = COALESCE(source.company_id, target.company_id),
                    target.company_name = COALESCE(source.company_name, target.company_name),
                    target.company_link = COALESCE(source.company_link, target.company_link),
                    target.company_address = COALESCE(source.company_address, target.company_address),
                    target.industry = COALESCE(source.industry, target.industry),
                    target.location = COALESCE(source.location, target.location),
                    target.company_size_std = COALESCE(source.company_size_std, target.company_size_std),
                    target.experience_req = COALESCE(source.experience_req, target.experience_req),
                    target.level_processed = COALESCE(source.level_processed, target.level_processed),
                    target.education = COALESCE(source.education, target.education),
                    target.working_type_std = COALESCE(source.working_type_std, target.working_type_std),
                    target.working_day = COALESCE(source.working_day, target.working_day),
                    target.working_hour = COALESCE(source.working_hour, target.working_hour),
                    target.skills_array = COALESCE(source.skills_array, target.skills_array),
                    target.salary_raw = COALESCE(source.salary_raw, target.salary_raw),
                    target.salary_min = COALESCE(source.salary_min, target.salary_min),
                    target.salary_max = COALESCE(source.salary_max, target.salary_max),
                    target.salary_currency = COALESCE(source.salary_currency, target.salary_currency),
                    target.salary_band = COALESCE(source.salary_band, target.salary_band),
                    target.deadline_date = COALESCE(source.deadline_date, target.deadline_date),
                    target.inserted_at = COALESCE(source.inserted_at, target.inserted_at)
                WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_query)
            else:
                # Lần chạy đầu tiên: Tạo bảng Iceberg v2 với tính năng Hidden Partitioning theo ngày
                df.writeTo(table_name) \
                    .tableProperty("format-version", "2") \
                    .partitionedBy(F.days("ingested_at")) \
                    .create()
                    
            self.logger.info("Ghi thành công")
        else:
            self.logger.warning("Không có dữ liệu hợp lệ!")
    def compute_quality_metrics(self, df_bronze: DataFrame, df_silver: DataFrame) -> dict:
        """
        Tính toán và log các chỉ số chất lượng dữ liệu sau mỗi batch transform.
        """
        bronze_count = df_bronze.count()
        silver_count = df_silver.count()

        metrics = {
            "source": self.source_name,
            "run_date": self.date.strftime("%Y-%m-%d"),
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "dropped_count": bronze_count - silver_count,
            "drop_rate": round(1 - (silver_count / max(bronze_count, 1)), 4),
        }
        self.logger.info(f"Quality Metrics:\n{json.dumps(metrics, indent=2, ensure_ascii=False)}")
        return metrics

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info(
            f"BẮT ĐẦU CHUẨN HÓA DỮ LIỆU RỒI ĐƯA LÊN SILVER: source={self.source_name}"
        )
        self.logger.info("=" * 60)
        try:
            df_bronze = self.read_bronze()
            df_silver = self.transform(df_bronze)
            self.compute_quality_metrics(df_bronze, df_silver)
            self.upload_to_silver(df_silver)

        except Exception as e:
            self.logger.error(f"There is an error while Transform Data to silver: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()
        self.logger.info("="*60)
        self.logger.info("THANH CONG ROI !!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
