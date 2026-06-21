import os
from pyspark.sql import SparkSession 

# Fix Java 17+ ExceptionInInitializerError (DirectByteBuffer)
java_opts = "-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-java-options '{java_opts}' pyspark-shell" 

def get_spark_session(app_name: str) -> SparkSession:
    # 1. Kiểm tra môi trường (chạy trong Docker hay chạy Local)
    is_docker = os.path.exists('/.dockerenv')
    
    # 2. Lấy thông tin kết nối MinIO và Hive Metastore (có fallback cho local)
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000" if is_docker else "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "root")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "password")
    hive_metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://metastore:9083" if is_docker else "thrift://localhost:9083")

    builder = SparkSession.builder.appName(app_name)

    if not is_docker:
        builder = builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # 4. Cấu hình Hive & Iceberg (Cần thiết để quản lý metadata bảng)
        builder = builder \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hive") \
            .config("spark.sql.catalog.my_catalog.uri", hive_metastore_uri) \
            .config("spark.sql.catalog.my_catalog.warehouse", "s3a://sandbox/warehouse/") \
            .enableHiveSupport()

    # Khởi tạo Spark Session
    spark = builder.getOrCreate()
    
    return spark