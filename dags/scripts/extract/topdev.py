import logging
from bs4 import BeautifulSoup
from seleniumbase import SB
import time
import json
import os
from pathlib import Path

WORKING_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = WORKING_DIR / 'data'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

url_base = 'https://topdev.vn/jobs/search?job_categories_ids=3%2C2%2C8'

def extract_job_details(soup):
    try:
        title_elem = soup.select_one('h1') or soup.select_one('.job-title')
        job_title = title_elem.text.strip() if title_elem else None
        
        salary_elem = soup.select_one('.text-primary-500') or soup.select_one('.salary')
        salary = salary_elem.text.strip() if salary_elem else None
        
        place_elem = soup.select_one('div:has(svg.lucide-map-pin)') or soup.select_one('.location')
        place = place_elem.text.strip() if place_elem else None
        
        experience_elem = soup.select_one('div:has(svg.lucide-briefcase)') or soup.select_one('.experience')
        experience = experience_elem.text.strip() if experience_elem else None
        
        yeu_cau = []
        chuyen_mon = []
        # TopDev skill tags
        skill_tags = soup.select('.job-tags a, .skill-tag, div.flex.flex-wrap.gap-2 span')
        if skill_tags:
            chuyen_mon = [tag.text.strip() for tag in skill_tags]
            
        name_company_elem = soup.select_one('.company-name') or soup.select_one('a[href*="/companies/"]')
        name_company = name_company_elem.text.strip() if name_company_elem else None
        
        scale_elem = soup.select_one('div:has(svg.lucide-users)') or soup.select_one('.company-size')
        scale = scale_elem.text.strip() if scale_elem else None
        
        field_elem = soup.select_one('.industry')
        field = field_elem.text.strip() if field_elem else None
        
        address_elem = soup.select_one('.address')
        address = address_elem.text.strip() if address_elem else None
        
        return {
            'job_title': job_title,
            'salary': salary,
            'deadline': None,
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
    all_jobs_data = []
    page = 1
    
    with SB(uc=True, headless=True) as sb:
        while True:
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
                if href and ('/detail-jobs/' in href or '/viec-lam/' in href):
                    if href.startswith('/'):
                        href = 'https://topdev.vn' + href
                    if href not in job_urls:
                        job_urls.append(href)
            
            if not job_urls:
                logger.info(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
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
                        all_jobs_data.append(job_data)
                        logger.info(f"-> Đã lấy thành công: {job_data['job_title']}")
                except Exception as e:
                    logger.error(f"Lỗi khi truy cập {job_url}: {e}")
            
            # Lưu dữ liệu sau mỗi trang để tránh mất mát nếu có lỗi
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            with open(f'{DATA_DIR}/topdev_jobs.json', 'w', encoding='utf-8') as f:
                json.dump(all_jobs_data, f, ensure_ascii=False, indent=4)
                
            page += 1
            
    logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {len(all_jobs_data)} việc làm và lưu vào file topdev_jobs.json")

if __name__ == "__main__":
    crawl_data()