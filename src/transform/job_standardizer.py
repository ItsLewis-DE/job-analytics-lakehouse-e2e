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

CAREERVIET_COLUMN_MAPPING = {
    "industry": "company_industry"
}

TOPCV_COLUMN_MAPPING = {
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
        #Tránh trùng lặp
        df = df.dropDuplicates(["job_id"])
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
            table_name = f"my_catalog.silver.{self.source_name}_jobs"
            self.logger.info(f"Đang ghi dữ liệu xuống Iceberg table: {table_name}")
            
            # Kiểm tra xem bảng đã tồn tại trong catalog chưa
            if self.spark.catalog.tableExists(table_name):
                # UPSERT (MERGE INTO) khi bảng đã tồn tại
                df.createOrReplaceTempView("updates_temp")
                
                # Lấy danh sách các cột thực sự có trong Dataframe để update (trừ các cột không nên update như job_id)
                update_cols = [c for c in df.columns if c not in ("job_id", "source", "ingested_at")]
                
                # Các cột làm điều kiện kiểm tra xem bản ghi có thay đổi không
                condition_cols = ["job_title", "job_category", "company_name", "location", "salary_raw", "deadline_date", "experience_req", "level_processed", "working_type_std"]
                condition_cols_present = [c for c in condition_cols if c in df.columns]
                
                if condition_cols_present:
                    match_cond = " OR ".join([f"NOT (target.{c} <=> source.{c})" for c in condition_cols_present])
                    when_matched = f"WHEN MATCHED AND ({match_cond}) THEN UPDATE SET"
                else:
                    when_matched = "WHEN MATCHED THEN UPDATE SET"

                update_set = ",".join([f"target.{c} = COALESCE(source.{c}, target.{c})" for c in update_cols])
                
                merge_query = f"""
                MERGE INTO {table_name} target
                USING updates_temp source
                ON target.job_id = source.job_id
                {when_matched}
                    {update_set}
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
