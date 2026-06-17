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
        title_elem = soup.select_one('.sc-ab270149-0.hAejeW')
        job_title = title_elem.text.strip() if title_elem else None
        
        salary_elem = soup.select_one('.sc-ab270149-0.cVbwLK')
        salary = salary_elem.text.strip() if salary_elem else None

        is_urgent_elem = soup.select_one('.sc-ab270149-0.guKwvE')
        is_urgent = is_urgent_elem.text.strip() if is_urgent_elem else None
        
        deadline_elem = soup.select_one('.sc-ab270149-0.ePOHWr')
        deadline = deadline_elem.text.strip() if deadline_elem else None
        
        place_elem = soup.select_one('.sc-ab270149-0.ePOHWr')
        place = place_elem.text.strip() if place_elem else None

        list_info = soup.select('.sc-7bf5461f-0.dHvFzj .sc-f098d520-0.dpBvbX')
        level=None
        skills=None
        field=None
        experience=None
        education=None
        year=None
        slot=None
        working_day=None
        working_hour=None
        working_type=None
        age=None
        for i,info in enumerate(list_info):
            value_ele = info.select_one('.sc-ab270149-0.cLLblL')
            value = value_ele.text.strip() if value_ele else None
            if i==1:
                level=value
            elif i==3:
                skills=value
            elif i==4:
                value_2 = info.select_one('span')
                field = value_2.text.strip() if value_2 else None
            elif i==6:
                experience=value
            elif i==8:
                education=value
            elif i==10:
                age=value
            elif i==12:
                slot=value
            elif i==13:
                working_day=value
            elif i==14:
                working_hour=value
            elif i==15:
                working_type=value

        link_company_elem = soup.select_one('.sc-ab270149-0.egZKeY.sc-f0821106-0.gWSkfE')
        link_company = link_company_elem.get('href') if link_company_elem else None
        name_company = link_company_elem.text.strip() if link_company_elem else None
        
        info_company = soup.select('.sc-37577279-4.kNdlhJ > .sc-37577279-5.kQCIWi')
        address_company =None
        company_size=None
        for i,info in enumerate(info_company):
            value_ele = info.select_one('.sc-ab270149-0.ePOHWr')
            value = value_ele.text.strip() if value_ele else None
            if i==0:
                address_company = value
            elif i ==1:
                company_size=value

        return {
            'job_title': job_title,
            'salary': salary,
            'deadline': deadline,
            'place': place,
            'experience': experience,
            'name_company': name_company,
            'scale': company_size,
            'field': field,
            'address': address_company,
            'link_company': link_company 
        }
    except Exception as e:
        logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
        return None

def crawl_data():
    page = 1
    url_base = 'https://www.vietnamworks.com/viec-lam?q=data&sorting=relevant'
    total_job=0
    with SB(uc=True, headless=True) as sb:
        while True:
            page_data = []
            # Construct page URL
            url = f"{url_base}&page={page}"
            logger.info(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            sb.get(url)
            sb.sleep(10)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract job links
            job_links_tags = soup.find_all('a')
            job_urls = []
            for a in job_links_tags:
                href = a.get('href')
                # Lọc các link việc làm dựa trên /job/ hoặc /viec-lam/ hoặc kết thúc với -jv
                if href and ('-jv' in href or '/viec-lam/' in href or '/job/' in href):
                    if len(href) > 20: 
                        if href.startswith('/'):
                            href = 'https://www.vietnamworks.com' + href
                        if 'vietnamworks.com' in href and href not in job_urls:
                            job_urls.append(href)
            
            if not job_urls:
                logger.info(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
                
            logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            
            #Truy cập vào bài đăng tuyển dụng đó để lấy thêm thông tin chi tiết
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
            
            # Đưa dữ liệu lên minIO sau mỗi trang để tránh mất mát nếu có lỗi
            if page_data:
                logger.info("Đang tạo df cho trang hiện tại")
                job_schema = StructType([
                    StructField("job_title", StringType(), True),
                    StructField("salary", StringType(), True),
                    StructField("deadline", StringType(), True),
                    StructField("place", StringType(), True),
                    StructField("experience", StringType(), True),
                    StructField("yeu_cau", StringType(), True),
                    StructField("chuyen_mon", StringType(), True),
                    StructField("name_company", StringType(), True),
                    StructField("scale", StringType(), True),
                    StructField("field", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("link_company", StringType(), True),
                    StructField("job_url", StringType(), True)
                ])
                df = spark.createDataFrame(page_data, schema=job_schema)
                table_name = 'my_catalog.bronze.vietnamworks_raw_jobs'
                logger.info(f"Đang ghi dữ liệu trang {page} vào bảng Iceberg {table_name}")
                df.write \
                    .format('iceberg') \
                    .mode("append") \
                    .saveAsTable(table_name)
            total_job += len(page_data)
            page += 1
            
    logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {total_job} việc làm và lưu vào file vietnamworks_jobs.json")

if __name__ == "__main__":
    crawl_data()