import logging
from bs4 import BeautifulSoup
from seleniumbase import SB
import time
import json
import os
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import boto3
load_dotenv()

WORKING_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = WORKING_DIR / 'data'

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID','root')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY','password')
AWS_REGION = os.getenv('AWS_REGION','us-east-1')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#Setup Pyspark
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
is_docker = os.path.exists('/.dockerenv')
metastore_uri = "thrift://metastore:9083" if is_docker else "thrift://localhost:9083"
minio_endpoint = "http://minio:9000" if is_docker else "http://localhost:9000"

print(f"Đang chạy trong môi trường: {'DOCKER' if is_docker else 'LOCAL'}")
print("Đang khởi tạo Spark Session với cấu hình Iceberg & S3...")

#Khởi tại minio client
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000' if is_docker else 'http://localhost:9000',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)
if not is_docker:
    # Khi chạy Local, Spark cần khai báo thủ công các config vì khi chạy trong docker Spark đã có file cấu hình
    spark = SparkSession.builder \
        .appName("Iceberg S3 Test") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hive") \
        .config("spark.sql.catalog.my_catalog.uri", metastore_uri) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://sandbox/warehouse/") \
        .getOrCreate()
else:
    # Khi chạy trong Docker, Spark sẽ tự động đọc từ spark-defaults.conf
    spark = SparkSession.builder\
        .appName("Iceberg S3") \
        .getOrCreate()

logger.info("Spark Session đã sẵn sàng!")

def extract_job_details(soup):
    try:
        deadline_elem = soup.select_one('.deadline >strong')
        deadline = deadline_elem.text if deadline_elem else None
        
        title_elem = soup.select_one('.job-detail__info--title')
        job_title = title_elem.text.strip() if title_elem else None
        
        salary_elem = soup.select_one('.section-salary .job-detail__info--section-content-value')
        salary = salary_elem.text.strip() if salary_elem else None
        
        place_elem = soup.select_one('.section-location .job-detail__info--section-content-value a')
        place = place_elem.text.strip() if place_elem else None
        
        experience_elem = soup.select_one('.section-experience .job-detail__info--section-content-value')
        experience = experience_elem.text.strip() if experience_elem else None
    
        name_company_elem = soup.select_one('.company-name-label >a')
        name_company = name_company_elem.text.strip() if name_company_elem else None
        
        scale_elem = soup.select_one('.company-scale .company-value')
        scale = scale_elem.text.strip() if scale_elem else None
        
        field_elem = soup.select_one('.company-field .company-value')
        field = field_elem.text.strip() if field_elem else None
        
        address_elem = soup.select_one('.company-address .company-value')
        address = address_elem.text.strip() if address_elem else None
        
        link_company_elem = soup.select_one('.job-detail__company--link >a')
        link_company = link_company_elem['href'] if link_company_elem else None
        
        info_add = soup.select('.box-general-content >.box-general-group')
        level = None
        education = None
        working_type = None
        working_day = None
        for info in info_add:
            name_ele = info.select_one('.box-general-group-info-title')
            name = name_ele.text.strip() if name_ele else None
            value_ele = info.select_one('.box-general-group-info-value')
            value = value_ele.text.strip() if value_ele else None
            if name=='Cấp bậc':
                level = value
            elif name =='Học vấn':
                education = value
            elif name=='Hình thức làm việc':
                working_type=value
            elif name =='Loại hình làm việc':
                working_day = value

        working_hour_ele = soup.select_one('.job-description__item--content-list')
        working_hour = working_hour_ele.text.strip() if working_hour_ele else None

        skills = []
        for list_job in soup.select('.box-category.collapsed'):
            skill_ele = list_job.select('.box-category-tag')
            for skill in skill_ele:
                if skill:
                    skills.append(skill.text.strip())
        return {
            'job_title': job_title,
            'salary': salary,
            'deadline': deadline,
            'place': place,
            'experience': experience,
            'level': level,
            'education': education,
            'working_type': working_type,
            'working_day': working_day,
            'working_hour': working_hour,
            'skills': skills,
            'name_company': name_company,
            'scale': scale,
            'field': field,
            'address': address,
            'link_company': link_company
        }
    except Exception as e:
        print(f"Lỗi khi parse chi tiết công việc: {e}")
        return None

def crawl_data():
    page = 1
    len_total =0
    with SB(uc=True, headless=True) as sb:
        while True:
            page_data = []
            url = f'https://www.topcv.vn/tim-viec-lam-data-kcr257?type_keyword=1&page={page}&category_family=r257&saturday_status=0&sba=1'
            print(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            sb.get(url)
            sb.sleep(3)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'lxml')
            
            job_links = soup.select('.body-box .title > a')
            if not job_links:
                print(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
                
            job_urls = [link['href'] for link in job_links]
            print(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            
            for job_url in job_urls:
                print(f"Đang lấy chi tiết: {job_url}")
                try:
                    sb.get(job_url)
                    sb.sleep(3)
                    html_job = sb.get_page_source()
                    job_soup = BeautifulSoup(html_job, 'lxml')
                    
                    job_data = extract_job_details(job_soup)
                    if job_data:
                        job_data['job_url'] = job_url
                        page_data.append(job_data)
                        print(f"-> Đã lấy thành công: {job_data['job_title']}")
                except Exception as e:
                    print(f"Lỗi khi truy cập {job_url}: {e}")
            #Lưu dữ liệu sau mỗi trang để tránh mất mát nếu có lỗi
            if page_data:
                logger.info(f"Đang tạo Data Frame cho {page}")
                job_schema = StructType([
                    StructField("job_title", StringType(), True),
                    StructField("salary", StringType(), True),
                    StructField("deadline", StringType(), True),
                    StructField("place", StringType(), True),
                    StructField("experience", StringType(), True),
                    StructField("level", StringType(), True),
                    StructField("education", StringType(), True),
                    StructField("working_type", StringType(), True),
                    StructField("working_day", StringType(), True),
                    StructField("working_hour", StringType(), True),
                    StructField("skills", ArrayType(StringType()), True),
                    StructField("name_company", StringType(), True),
                    StructField("scale", StringType(), True),
                    StructField("field", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("link_company", StringType(), True),
                    StructField("job_url", StringType(), True)
                ])
                df = spark.createDataFrame(page_data, schema=job_schema)
                table_name ="my_catalog.bronze.top_cv_raw_jobs"
                logger.info(f"Đang ghi dữ liệu trang {page} cho {table_name}")
                df.write \
                    .format('iceberg') \
                    .mode("append") \
                    .saveAsTable(table_name) 
                logger.info(f"Đã ghi dữ liệu thành công vào {table_name} cho {page}")
            len_total += len(page_data)
            page += 1
            
    print(f"Hoàn thành! Đã lấy xong tổng cộng {len_total} việc làm và lưu vào file topcv_jobs.json")

if __name__ == "__main__":
    crawl_data()
