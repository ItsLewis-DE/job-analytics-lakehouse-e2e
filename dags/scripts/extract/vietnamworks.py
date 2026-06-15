import logging
from bs4 import BeautifulSoup
from seleniumbase import SB
import time
import json
import os
from pathlib import Path
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
        
        place_elem = soup.select_one('.sc-ab270149-0.ePOHWr')
        place = place_elem.text.strip() if place_elem else None
        
        experience_elem = soup.select_one('.sc-7bf5461f-2.JtIju .sc-ab270149-0.cLLblL')
        experience = experience_elem.text.strip() if experience_elem else None

        working_hour_ele = soup.select_one('.sc-7bf5461f-1.jseBPO .sc-ab270149-0.cLLblL' )
        working_hour = working_hour_ele.text.strip() if working_hour_ele else None

        working_day_ele = soup.select_one('.sc-7bf5461f-2.JtIju .sc-ab270149-0.cLLblL')
        working_day = working_day_ele.text.strip() if working_day_ele else None

        age_ele = soup.select_one('.sc-7bf5461f-1.jseBPO .sc-ab270149-0.cLLblL' )
        age = age_ele.text.strip() if age_ele else None

        education_ele = soup.select_one('.sc-7bf5461f-2.JtIju sc-ab270149-0.cLLblL')
        education = education_ele.text.strip() if education_ele else None


        yeu_cau = []
        # Vietnamworks skill tags
        skill_elem = soup.select_one('.sc-ab270149-0.cLLblL')
        skill = [','].join(skill_elem.text.strip().split(',')) if skill_ele else None
        
        tags = soup.select('.sc-1671001a-6.dVvinc p')
        yeu_cau = [tag.text.strip() for tag in tags]
        deadline_ele = soup.select_one('.sc-ab270149-0.ePOHWr')
        deadline = deadline_ele.text.strip() if deadline_ele else None
        link_company_elem = spup.select_one('.sc-ab270149-0.egZKeY.sc-f0821106-0.gWSkfE')
        link_company = link_company.elem.text.strip() if link_company_elem else None
        name_company_elem = soup.select_one('.company-name') or soup.select_one('a[href*="/company/"]')
        name_company = name_company_elem.text.strip() if name_company_elem else None
        
        scale_elem = soup.select_one('.company-size') or soup.select_one('.size')
        scale = scale_elem.text.strip() if scale_elem else None
        
        field_elem = soup.select_one('.industry') or soup.select_one('.job-industry')
        field = field_elem.text.strip() if field_elem else None
        
        address_elem = soup.select_one('.address') or soup.select_one('.company-address')
        address = address_elem.text.strip() if address_elem else None
        
        return {
            'job_title': job_title,
            'salary': salary,
            'deadline': deadline,
            'place': place,
            'experience': experience,
            'yeu_cau': yeu_cau,
            'chuyen_mon': chuyen_mon,
            'name_company': name_company,
            'scale': scale,
            'field': field,
            'address': address,
            'link_company': None 
        }
    except Exception as e:
        logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
        return None

def crawl_data():
    page = 1
    url_base = 'https://www.vietnamworks.com/viec-lam?q=data&sorting=relevant'
    with SB(uc=True, headless=True) as sb:
        while True:
            page_data = []
            # Construct page URL
            url = f"{url_base}&page={page}"
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
                df = spark.createDataFrame(page_data)
                table_name = 'my_catalog.bronze.vietnamworks_raw_jobs'
                logger.info(f"Đang ghi dữ liệu tramg {page} vào bảng Iceberg {table_name}")
                df.write \
                    .format('iceberg') \
                    .mode("append") \
                    .saveAsTable(table_name)
                logger.info("Đã ghi dữ liệu thành công")
            page += 1
            
    logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {len(all_jobs_data)} việc làm và lưu vào file vietnamworks_jobs.json")

if __name__ == "__main__":
    crawl_data()