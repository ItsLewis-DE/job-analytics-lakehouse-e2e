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

url_base = 'https://topdev.vn/jobs/search?job_categories_ids=3%2C2%2C8'

def extract_job_details(soup):
    try:
        title_elem = soup.select_one('.flex.w-full.flex-col.justify-between >a')
        job_title = title_elem.text.strip() if title_elem else None
        
        place = None
        experience=None
        level=None
        for i,info in enumerate(soup.select('.grid.grid-cols-2 .flex.items-center.gap-1')):
            if i==0:
                place = info.select_one('.line-clamp-1').text.strip()
            elif i==1:
                level = info.text.strip()
            else:
                experience = info.text.strip()

        deadline=None
        deadline_ele = soup.select_one('.flex.items-center.gap-2.text-sm.text-text-900')
        if deadline_ele:
            list_deadline = list(deadline_ele.stripped_strings)
            if list_deadline:
                deadline = list_deadline[0]
        
        demands = []
        benefits = []
        working_time = []
        list_info = soup.select('.border-text-200.text-text-200.rounded-xl.border.p-4 .mt-2') 
        if list_info:
            for i,info in enumerate(list_info):
                if i==1:
                    demands = [demand.text.strip() for demand in info.select('li')]
                if i==2:
                    benefits = [benefit.text.strip() for benefit in info.select('.prose-ul.text-text-900 li')]
                    for j,items in enumerate(info.select('ul')):
                        if j==1:
                            for li in items.select('li'):
                                working_time.append(li.text.strip())
        company_industry = None
        company_country = None
        company_size = None
        company_info = soup.select_one('.relative.flex.flex-col.p-6.pb-2 >a')
        company_link = company_info.get('href')
        if company_link:
            company_link = 'https://topdev.vn' + company_link
        company_name_ele = company_info.select_one('span')
        company_name = company_name_ele.text.strip() if company_name_ele else None
        list_info = company_info.select('.flex.items-center.justify-between.gap-1')
        for i,info in enumerate(list_info):
            value_ele = info.select_one('.font-semibold')
            if value_ele:
                value = value_ele.text.strip() if value_ele else None
            if i==0:
                company_industry = value
            elif i==1:
                company_size = value
            else:
                company_country = value


        return {
                'job_title': job_title,
                'place': place,
                'experience': experience,
                'level': level,
                'deadline': deadline,
                'demands': demands,
                'benefits': benefits,
                'working_time': working_time,
                'company_name':company_name,
                'company_size':company_size,
                'company_industry':company_industry,
                'comnpany_country':company_country,
                'company_link':company_link
                }
    except Exception as e:
        logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
        return None

def crawl_data():
    page=1
    len_total =0
    with SB(uc=True, headless=True) as sb:
        while True:
            page_data = []
            url = f'{url_base}&page={page}'
            logger.info(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            sb.get(url)
            sb.sleep(5)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract job links
            job_links_tags = soup.find_all('a')
            job_urls = []
            for a in job_links_tags:
                href = a.get('href')
                if href and ('/detail-jobs/' in href or '/viec-lam/' in href):
                    if href.startswith('/'):
                        href = 'https://topdev.vn' + href
                    if href not in job_urls:
                        job_urls.append(href)
            if not job_urls:
                print(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
            
            logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            
            for job_url in job_urls:
                logger.info(f"Đang lấy chi tiết: {job_url}")
                try:
                    sb.get(job_url)
                    sb.sleep(3)
                    html_job = sb.get_page_source()
                    job_soup = BeautifulSoup(html_job, 'lxml')
                    
                    job_data = extract_job_details(job_soup)
                    if job_data:
                        job_data['job_url'] = job_url
                        page_data.append(job_data)
                        logger.info(f"-> Đã lấy thành công: {job_data['job_title']}")
                except Exception as e:
                    logger.error(f"Lỗi khi truy cập {job_url}: {e}")
            
            # Lưu dữ liệu sau mỗi trang để tránh mất mát nếu có lỗi
            if page_data:
                logger.info(f"Đang tạo Data Frame cho trang {page}")
                job_schema = StructType([
                    StructField("job_title", StringType(), True),
                    StructField("place", StringType(), True),
                    StructField("experience", StringType(), True),
                    StructField("level", StringType(), True),
                    StructField("deadline", StringType(), True),
                    StructField("demands", ArrayType(StringType()), True),
                    StructField("benefits", ArrayType(StringType()), True),
                    StructField("working_time", ArrayType(StringType()), True),
                    StructField("company_name", StringType(), True),
                    StructField("company_size", StringType(), True),
                    StructField("company_industry", StringType(), True),
                    StructField("comnpany_country", StringType(), True),
                    StructField("company_link", StringType(), True),
                    StructField("job_url", StringType(), True)
                ])
                df = spark.createDataFrame(page_data, schema=job_schema)
                table_name ="my_catalog.bronze.topdev_raw_jobs"
                logger.info(f"Đang ghi dữ liệu trang {page} cho {table_name}")
                df.write \
                    .format('iceberg') \
                    .mode("append") \
                    .saveAsTable(table_name) 
                logger.info(f"Đã ghi dữ liệu thành công vào {table_name} cho {page}")
            len_total += len(page_data)
            page+=1
            
    logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {len_total} việc làm và lưu vào file topdev_jobs.json")

if __name__ == "__main__":
    crawl_data()