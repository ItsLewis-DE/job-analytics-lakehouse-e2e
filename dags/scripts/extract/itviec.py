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

url_base = 'https://itviec.com/segments/viec-lam-ai-data?gclid=Cj0KCQjw_7PRBhDcARIsAMjV7jnMzspHCFzRYRiAt0Q6MehzxrAVMZCXYyY3pCaXoCbQ7inNq8KDKCcaAtX3EALw_wcB&utm_campaign=gsn_brand_hcm_mc&utm_content=itviec_ad2&utm_medium=cpc&utm_source=google&utm_term=itviec&click_action=banner_clicked&touchpoint_type=header_menu'

def extract_job_details(soup):
    try:
        title_elem = soup.select_one('h1')
        job_title = title_elem.text.strip() if title_elem else None
        
        place_elem = soup.select_one('.normal-text.text-rich-grey')
        place = place_elem.text.strip() if place_elem else None
        
        is_accepted_intern_ele = soup.select_one('.text-reset.normal-text')
        is_accepted_intern = is_accepted_intern_ele.text.strip() if is_accepted_intern_ele else None

        working_type_ele = soup.select_one('.normal-text.text-rich-grey.ms-1')
        working_type = working_type_ele.text.strip() if working_type_ele else None

        skills = []
        skills_list = soup.select('.d-flex.flex-wrap.igap-2 >a')
        for skill_ele in skills_list:
            skill = skill_ele.text.strip() if skill_ele else None
            skills.append(skill)
        
        company_name = None
        company_industry=None
        company_size=None
        company_place=None
        working_days=None

        list_info_company = soup.select('.imt-4 > .row.ipy-2.gx-0.border-bottom-dashed')
        for info in list_info_company:
            type_ele = info.select_one('.col.text-dark-grey')
            type = type_ele.text.strip() if type_ele else None
            if type == 'Company type':
                company_name = info.select_one('.col.text-end.text-it-black').text.strip()
            elif type == 'Company industry':
                company_industry = info.select_one('.d-inline-flex.text-wrap').text.strip()
            elif type == 'Company size':
                company_size = info.select_one('.col.text-end.text-it-black').text.strip()
            elif type=='Country':
                company_place = info.select_one('span.align-middle').text.strip()
            elif type=='Working days':
                working_days = info.select_one('.col.text-end.text-it-black').text.strip()

        company_link_ele = soup.select_one('.ipt-3.ipt-xl-1.ipb-2.text-clamp-3 >a ')
        company_link = company_link_ele.get('href') if company_link_ele else None
        company_link = 'https://itviec.com' + company_link

        list_demand = soup.select('.imy-5.paragraph')
        job_demands = []
        job_benefits = []
        for i, item in enumerate(list_demand):
            if i == 1:
                demand_ele = item.select('li')
                for demand in demand_ele:
                    job_demands.append(demand.text.strip())
            elif i == len(list_demand)-1:
                benefit_ele = item.select('li')
                for benefit in benefit_ele:
                    job_benefits.append(benefit.text.strip())

        return {
            'job_title': job_title,
            'place': place,
            'is_accepted_intern': is_accepted_intern,
            'working_type': working_type,
            'skills': skills,
            'company_name': company_name,
            'company_industry': company_industry,
            'company_size': company_size,
            'company_place': company_place,
            'company_link':company_link,
            'working_days': working_days,
            'demands': job_demands,
            'benefits': job_benefits,
        }
    except Exception as e:
        logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
        return None

def crawl_data():
    with SB(uc=True, headless=True) as sb:
        url = url_base
        logger.info(f"=== Đang mở trang danh sách việc làm ===")
        sb.get(url)
        sb.sleep(3)
        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'lxml')
        
        job_links_tags = soup.select('h3 a')
        job_urls = []
        for a in job_links_tags:
            href = a.get('href')
            if href and '/it-jobs/' in href:
                if href.startswith('/'):
                    href = 'https://itviec.com' + href
                job_urls.append(href)
                   
        logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang")
        
        job_total = 0
        all_jobs_data = []
        for job_url in job_urls:
            job_total+=1
            logger.info(f"Đang lấy chi tiết: {job_url}")
            try:
                sb.get(job_url)
                sb.sleep(3)
                html_job = sb.get_page_source()
                job_soup = BeautifulSoup(html_job, 'lxml')
                
                job_data = extract_job_details(job_soup)
                if job_data:
                    job_data['job_url'] = job_url
                    all_jobs_data.append(job_data)
                    logger.info(f"-> Đã lấy thành công: {job_data['job_title']}")
            except Exception as e:
                logger.error(f"Lỗi khi truy cập {job_url}: {e}")
            if job_total %50==0:
                logger.info("Đang tạo data frame trên minIO ")
                job_schema = StructType([
                    StructField("job_title", StringType(), True),
                    StructField("place", StringType(), True),
                    StructField("is_accepted_intern", StringType(), True),
                    StructField("working_type", StringType(), True),
                    StructField("skills", ArrayType(StringType()), True),
                    StructField("company_name", StringType(), True),
                    StructField("company_industry", StringType(), True),
                    StructField("company_size", StringType(), True),
                    StructField("company_place", StringType(), True),
                    StructField("company_link", StringType(), True),
                    StructField("working_days", StringType(), True),
                    StructField("demands", ArrayType(StringType()), True),
                    StructField("benefits", ArrayType(StringType()), True),
                    StructField("job_url", StringType(), True)
                ])
                df = spark.createDataFrame(all_jobs_data, schema=job_schema)
                table_name = 'my_catalog.bronze.itviec_raw_jobs'
                logger.info(f"Đang ghi dữ liệu vào {table_name}")
                df.write \
                    .format('iceberg') \
                    .mode('append') \
                    .saveAsTable(table_name)
                logger.info("Ghi dữ liệu thành công")
                all_jobs_data = []
    # Lưu dữ liệu cuối cùng
        if all_jobs_data: 
            logger.info("Đang tạo data frame trên minIO ")
            job_schema = StructType([
                StructField("job_title", StringType(), True),
                StructField("place", StringType(), True),
                StructField("is_accepted_intern", StringType(), True),
                StructField("working_type", StringType(), True),
                StructField("skills", ArrayType(StringType()), True),
                StructField("company_name", StringType(), True),
                StructField("company_industry", StringType(), True),
                StructField("company_size", StringType(), True),
                StructField("company_place", StringType(), True),
                StructField("company_link", StringType(), True),
                StructField("working_days", StringType(), True),
                StructField("demands", ArrayType(StringType()), True),
                StructField("benefits", ArrayType(StringType()), True),
                StructField("job_url", StringType(), True)
            ])
            df = spark.createDataFrame(all_jobs_data, schema=job_schema)
            table_name = 'my_catalog.bronze.itviec_raw_jobs'
            logger.info(f"Đang ghi dữ liệu vào {table_name}")
            df.write \
                .format('iceberg') \
                .mode('append') \
                .saveAsTable(table_name)
            logger.info("Ghi dữ liệu thành công")
                
    logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {job_total} việc làm và lưu vào file itviec_jobs.json")

if __name__ == "__main__":
    crawl_data()
