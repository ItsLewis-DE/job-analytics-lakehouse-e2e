import logging
import json
import os
from datetime import datetime as dt
from pathlib import Path
import boto3
from seleniumbase import SB

WORKING_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = WORKING_DIR / 'data'

class BaseCrawler:
    def __init__(self, site_name, start_url=None):
        self.site_name = site_name
        self.start_url = start_url
        
        # Ensure DATA_DIR exists
        os.makedirs(DATA_DIR, exist_ok=True)
        self.local_file = DATA_DIR / f"{site_name}_jobs.json"
        
        # Setup logging
        self.logger = logging.getLogger(self.site_name)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
            
        # Setup MinIO S3 client
        self.is_docker = os.path.exists('/.dockerenv')
        
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID', 'root')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
        aws_region = os.getenv('AWS_REGION', 'us-east-1')
        
        self.s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000' if self.is_docker else 'http://localhost:9000',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )

    def extract_job_details(self, soup):
        raise NotImplementedError("Subclasses must implement this method")

    def save_to_json(self, data):
        if not data:
            return
        os.makedirs(os.path.dirname(self.local_file),exist_ok=True)          
        with open(self.local_file, 'a', encoding='utf-8') as f:
            for job in data:
                json_string = json.dumps(job,ensure_ascii=False)
                f.write(json_string+'\n')
        self.logger.info(f"Đã lưu {len(data)} jobs vào file local {self.local_file}")

    def upload_to_minio(self, bucket_name='landing'):
        if not os.path.exists(self.local_file):
            self.logger.warning(f"File {self.local_file} không tồn tại. Bỏ qua upload.")
            return
            
        now = dt.now()
        s3_key = f"year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}/{self.site_name}_jobs.json"
            
        try:
            self.logger.info(f"Đang upload {self.local_file} lên bucket {bucket_name} tại {s3_key}...")
            self.s3.upload_file(str(self.local_file), bucket_name, s3_key)
            self.logger.info("Upload thành công!")
        except Exception as e:
            self.logger.error(f"Lỗi khi upload lên MinIO: {e}")
            raise

    def do_crawl(self, sb):
        raise NotImplementedError("Subclasses must implement the crawling logic")

    def run(self):
        if os.path.exists(self.local_file):
            os.remove(self.local_file)
            self.logger.info("Da xoa file cu de trach trung du lieu!")
        use_xvfb = getattr(self, 'is_docker', os.path.exists('/.dockerenv'))
        
        sb_context = SB(uc=True, headless=False, xvfb=use_xvfb, incognito=True, page_load_strategy="none")
        sb = sb_context.__enter__()
        
        try:
            self.do_crawl(sb)
        finally:
            self.upload_to_minio()
            
            self.logger.info("Đang đóng trình duyệt...")
            import signal
            class BrowserQuitTimeout(BaseException): pass
            def quit_handler(signum, frame):
                raise BrowserQuitTimeout("Quá thời gian 10s đóng trình duyệt")
            
            old_handler = signal.signal(signal.SIGALRM, quit_handler)
            signal.alarm(10)
            try:
                sb_context.__exit__(None, None, None)
                self.logger.info("Đóng trình duyệt thành công.")
            except BrowserQuitTimeout:
                self.logger.warning("Trình duyệt bị đơ nên không thể đóng sạch sẽ. Bỏ qua để hoàn thành task!")
            except Exception as e:
                self.logger.warning(f"Lỗi khi đóng trình duyệt: {e}")
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
