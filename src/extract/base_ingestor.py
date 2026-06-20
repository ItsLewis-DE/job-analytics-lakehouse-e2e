import json
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql.types import StructType
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col, days

PROJECT_ROOT = Path(__file__).resolve().parents[2]

from src.utils.spark_util import get_spark_session


class BaseIngestor:

    # Các nguồn dữ liệu hợp lệ (đảm bảo phải có file schema tương ứng)
    VALID_SOURCES = ['itviec', 'vietnamworks', 'careerviet']

    def __init__(self, source_name: str, date: datetime):
        # --- Validate source ---
        if source_name not in self.VALID_SOURCES:
            raise ValueError(
                f"Source '{source_name}' không hợp lệ. "
                f"Các nguồn hỗ trợ: {self.VALID_SOURCES}"
            )

        self.source_name = source_name
        self.date = date
        self.year = date.strftime("%Y")
        self.month = date.strftime("%m")
        self.day = date.strftime("%d")

        # --- Setup Logger ---
        self.logger = logging.getLogger(f"ingestor.{source_name}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        # --- Load Schema từ file JSON ---
        schema_path = PROJECT_ROOT / "config" / "schemas" / f"{source_name}_schema.json"
        if not schema_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file schema: {schema_path}")

        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_json = json.load(f)

        # Thêm cột _corrupt_record vào schema để bắt các dòng bị lỗi format
        schema_json['fields'].append({
            "name": "_corrupt_record",
            "type": "string",
            "nullable": True,
            "metadata": {}
        })
        self.schema = StructType.fromJson(schema_json)
        self.logger.info(f"Đã load schema cho '{source_name}' với {len(schema_json['fields'])} cột.")

        # --- Khởi tạo Spark Session ---
        self.spark = get_spark_session(f"ingest_{source_name}_to_bronze")

        self.landing_path = (
            f"s3a://landing/year={self.year}/month={self.month}"
            f"/day={self.day}/{source_name}_jobs.json"
        )
        self.quarantine_path = (
            f"s3a://bronze/_quarantine/{source_name}_jobs/"
            f"year={self.year}/month={self.month}/day={self.day}/"
        )

    def read_landing(self):
        self.logger.info(f"Đang đọc dữ liệu từ: {self.landing_path}")
        try:
            df = self.spark.read \
                .schema(self.schema) \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .json(self.landing_path)
            
            df.cache() 
            total_count = df.rdd.count()
        except Exception as e:
            self.logger.error("Co loi xay ra!")
            raise
        self.logger.info(f"Tổng số bản ghi đọc được: {total_count}")

        # Tách dữ liệu lỗi ra khỏi dữ liệu hợp lệ
        error_df = df.filter(col("_corrupt_record").isNotNull())
        valid_df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

        error_count = error_df.count()
        valid_count = valid_df.count()

        if error_count > 0:
            self.logger.warning(
                f"Phát hiện {error_count}/{total_count} bản ghi bị lỗi format! "
                f"Sẽ ghi vào quarantine."
            )
        self.logger.info(f"Số bản ghi hợp lệ: {valid_count}")

        return valid_df, error_df

    def add_audit_columns(self, df):
        df_audited = df \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("_source_name", lit(self.source_name)) 

        return df_audited

    def write_bronze(self, valid_df, error_df):
        # --- Ghi dữ liệu hợp lệ vào Iceberg Table ---
        if valid_df.count() > 0:
            table_name = f"my_catalog.bronze.{self.source_name}_jobs"
            self.logger.info(f"Đang ghi dữ liệu hợp lệ xuống Iceberg table: {table_name}")
            
            # Kiểm tra xem bảng đã tồn tại trong catalog chưa
            if self.spark.catalog.tableExists(table_name):
                valid_df.writeTo(table_name).append()
            else:
                # Lần chạy đầu tiên: Tạo bảng Iceberg với tính năng Hidden Partitioning theo ngày
                valid_df.writeTo(table_name) \
                    .partitionedBy(days("ingested_at")) \
                    .create()
                    
            self.logger.info(f"Ghi thành công vào bảng Iceberg {table_name}!")
        else:
            self.logger.warning("Không có dữ liệu hợp lệ để ghi xuống Bronze.")

        # --- Ghi dữ liệu lỗi vào quarantine ---
        if error_df.count() > 0:
            self.logger.warning(
                f"Đang ghi {error_df.count()} bản ghi lỗi vào quarantine: "
                f"{self.quarantine_path}"
            )
            error_df.write \
                .mode("append") \
                .json(self.quarantine_path)
            self.logger.warning("Ghi quarantine thành công. Hãy kiểm tra dữ liệu lỗi!")

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info(
            f"BẮT ĐẦU INGEST: source={self.source_name}, "
            f"date={self.year}-{self.month}-{self.day}"
        )
        self.logger.info("=" * 60)

        try:
            # Step 1: Đọc dữ liệu từ Landing
            valid_df, error_df = self.read_landing()

            # Step 2: Thêm audit columns vào dữ liệu hợp lệ
            valid_df_audited = self.add_audit_columns(valid_df)

            # Step 3: Ghi xuống Bronze và Quarantine
            self.write_bronze(valid_df_audited, error_df)

            self.logger.info("=" * 60)
            self.logger.info("INGEST HOÀN TẤT THÀNH CÔNG!")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"INGEST THẤT BẠI: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()