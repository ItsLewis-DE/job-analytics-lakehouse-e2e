from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.utils.spark_util import get_spark_session
def main():
    print("="*60)
    print("KHỞI TẠO SPARK SESSION ĐỂ ĐỌC DỮ LIỆU GOLD...")
    print("="*60)
    
    spark = get_spark_session('hehe')

    try:
        print("Đang đọc các bảng từ catalog my_catalog.gold...")
        # Đọc các bảng
        fact_job = spark.table("my_catalog.gold.fact_job_postings")
        dim_company = spark.table("my_catalog.gold.dim_company")
        dim_location = spark.table("my_catalog.gold.dim_location")
        dim_category = spark.table("my_catalog.gold.dim_job_category")
        dim_skill = spark.table("my_catalog.gold.dim_skill")
        bridge = spark.table("my_catalog.gold.bridge_job_skills")

        # 1. Gom nhóm kỹ năng (skill) theo từng job_id thành 1 mảng (array) để không bị lặp dòng
        job_skills = bridge.join(dim_skill, "skill_id", "left") \
            .groupBy("job_id") \
            .agg(F.concat_ws(", ", F.collect_list("skill_name")).alias("skills"))

        # 2. Join bảng Fact với các bảng Dimension
        joined_df = fact_job \
            .join(dim_company, "company_id", "left") \
            .join(dim_location, "location_id", "left") \
            .join(dim_category, "job_category_id", "left") \
            .join(job_skills, "job_id", "left")

        # 3. Lựa chọn các cột quan trọng và xuất ra màn hình
        print("\n" + "="*80)
        print("KẾT QUẢ: DANH SÁCH VIỆC LÀM ĐÃ ĐƯỢC CHUẨN HÓA VÀ LIÊN KẾT (GOLD LAYER)")
        print("="*80)
        
        joined_df.write.mode('overwrite').json('output')
        
        # In ra tổng số lượng việc làm
        total_jobs = fact_job.count()
        print(f"\n=> Tổng số lượng việc làm trong hệ thống: {total_jobs}")
        print("="*80)

    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
