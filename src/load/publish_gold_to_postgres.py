import os
import logging
from pyspark.sql import DataFrame
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_util import get_spark_session

# Setup logging
logger = logging.getLogger("publish_postgres")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def sync_table_to_postgres(spark, iceberg_table: str, pg_table: str, jdbc_url: str, properties: dict):
    if not spark.catalog.tableExists(iceberg_table):
        logger.warning(f"Bảng {iceberg_table} không tồn tại trên Iceberg. Bỏ qua.")
        return
        
    df = spark.table(iceberg_table)
    temp_pg_table = f"{pg_table}_temp"
    
    logger.info(f"Đang ghi dữ liệu từ {iceberg_table} sang bảng tạm {temp_pg_table}...")
    
    # 1. Ghi vào bảng tạm (overwrite)
    df.write.jdbc(
        url=jdbc_url,
        table=temp_pg_table,
        mode="overwrite",
        properties=properties
    )
    
    logger.info(f"Ghi xong bảng tạm. Tiến hành Swap bảng (Blue/Green Deployment)...")
    
    # 2. Swap table using JDBC connection directly via py4j
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager # Lấy ra class java.sql.DriverManager để cbi tạo kết nối với database
    java_props = spark._sc._gateway.jvm.java.util.Properties() #Lấy ra để truyền user và password cho RDBMS
    for k, v in properties.items():
        java_props.setProperty(k, v)
        
    conn = driver_manager.getConnection(jdbc_url, java_props)
    conn.setAutoCommit(False) #Mọi câu lệnh bênh dưới sẽ chưa được áp dụng lập tức mà sẽ được lưu vào transaction
    
    try:
        stmt = conn.createStatement()
        old_pg_table = f"{pg_table}_old"
        
        # Thực hiện Swap không gây gián đoạn
        stmt.execute(f"DROP TABLE IF EXISTS {old_pg_table}")
        stmt.execute(f"ALTER TABLE IF EXISTS {pg_table} RENAME TO {old_pg_table}")
        stmt.execute(f"ALTER TABLE {temp_pg_table} RENAME TO {pg_table}")
        stmt.execute(f"DROP TABLE IF EXISTS {old_pg_table}")
        
        conn.commit()
        logger.info(f"Hoàn tất Swap bảng cho {pg_table}!")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Lỗi khi Swap bảng {pg_table}: {e}")
        raise
    finally:
        conn.close()

def main():
    spark = get_spark_session("publish_gold_to_postgres")
    
    is_docker = os.path.exists('/.dockerenv')
    db_host = "postgres-serving" if is_docker else "localhost"
    db_port = "5432" if is_docker else "5435"
    
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/serving_db"
    properties = {
        "user": "superset_user",
        "password": "superset_password",
        "driver": "org.postgresql.Driver"
    }
    
    # Danh sách các bảng cần đồng bộ
    tables_to_sync = {
        "my_catalog.gold.dim_company": "dim_company",
        "my_catalog.gold.dim_location": "dim_location",
        "my_catalog.gold.dim_job_category": "dim_job_category",
        "my_catalog.gold.dim_skill": "dim_skill",
        "my_catalog.gold.fact_job_postings": "fact_job_postings",
        "my_catalog.gold.bridge_job_skills": "bridge_job_skills",
    }
    
    for iceberg_table, pg_table in tables_to_sync.items():
        logger.info(f"--- Bắt đầu đồng bộ bảng {iceberg_table} ---")
        try:
            sync_table_to_postgres(spark, iceberg_table, pg_table, jdbc_url, properties)
        except Exception as e:
            logger.error(f"Đồng bộ bảng {iceberg_table} thất bại. Error: {e}")
            
    logger.info("HOÀN TẤT ĐỒNG BỘ TOÀN BỘ GOLD MARTS SANG POSTGRESQL!")
    spark.stop()

if __name__ == "__main__":
    main()
