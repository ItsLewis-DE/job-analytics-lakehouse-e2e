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

url_base = 'https://www.vietnamworks.com/viec-lam?q=data&sorting=relevant'

def extract_job_details(soup):
    try:
        title_elem = soup.select_one('h1') or soup.select_one('.job-title')
        job_title = title_elem.text.strip() if title_elem else None
        
        salary_elem = soup.select_one('.salary') or soup.select_one('span.text-primary') or soup.select_one('.job-salary')
        salary = salary_elem.text.strip() if salary_elem else None
        
        place_elem = soup.select_one('.location') or soup.select_one('.job-location') or soup.select_one('.company-location')
        place = place_elem.text.strip() if place_elem else None
        
        experience_elem = soup.select_one('.experience') or soup.select_one('.job-experience')
        experience = experience_elem.text.strip() if experience_elem else None
        
        yeu_cau = []
        chuyen_mon = []
        # Vietnamworks skill tags
        skill_tags = soup.select('.job-tags a, .skill-tag, .skills span, .tag, span.job-skill')
        if skill_tags:
            chuyen_mon = [tag.text.strip() for tag in skill_tags]
            
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
            with open(f'{DATA_DIR}/vietnamworks_jobs.json', 'w', encoding='utf-8') as f:
                json.dump(all_jobs_data, f, ensure_ascii=False, indent=4)
                
            page += 1
            
    logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {len(all_jobs_data)} việc làm và lưu vào file vietnamworks_jobs.json")

if __name__ == "__main__":
    crawl_data()